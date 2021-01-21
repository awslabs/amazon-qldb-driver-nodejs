/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

import { CommitTransactionResult, ExecuteStatementResult, ValueHolder } from "aws-sdk/clients/qldbsession";
import { dumpBinary, toBase64 } from "ion-js";
import { Lock } from "semaphore-async-await";

import { Communicator } from "./Communicator";
import { ClientException, isOccConflictException, TransactionClosedError } from "./errors/Errors";
import { warn } from "./LogUtil";
import { QldbHash } from "./QldbHash";
import { Result } from "./Result";
import { ResultReadable } from "./ResultReadable";
import { TransactionExecutable } from "./TransactionExecutable";

/**
 * A class representing a QLDB transaction.
 *
 * Every transaction is tied to a parent (Pooled)QldbSession, meaning that if the parent session is closed or
 * invalidated, the child transaction is automatically closed and cannot be used. Only one transaction can be active at
 * any given time per parent session, and thus every transaction should call {@linkcode Transaction.abort} or
 * {@linkcode Transaction.commit} when it is no longer needed, or when a new transaction is desired from the parent
 * session.
 *
 * An InvalidSessionException indicates that the parent session is dead, and a new transaction cannot be created
 * without a new (Pooled)QldbSession being created from the parent driver.
 *
 * Any unexpected errors that occur within a transaction should not be retried using the same transaction, as the state
 * of the transaction is now ambiguous.
 *
 * When an OCC conflict occurs, the transaction is closed and must be handled manually by creating a new transaction
 * and re-executing the desired queries.
 */
export class Transaction implements TransactionExecutable {
    private _communicator: Communicator;
    private _txnId: string;
    private _isClosed: boolean;
    private _txnHash: QldbHash;
    private _hashLock: Lock;

    /**
     * Create a Transaction.
     * @param communicator The Communicator object representing a communication channel with QLDB.
     * @param txnId The ID of the transaction.
     */
    constructor(communicator: Communicator, txnId: string) {
        this._communicator = communicator;
        this._txnId = txnId;
        this._isClosed = false;
        this._txnHash = QldbHash.toQldbHash(txnId);
        this._hashLock = new Lock();
    }

    /**
     * Abort this transaction and close child ResultReadable objects. No-op if already closed by commit or previous abort.
     * @returns Promise which fulfills with void.
     */
    async abort(): Promise<void> {
        if (this._isClosed) {
            return;
        }
        this._internalClose();
        await this._communicator.abortTransaction();
    }

    /**
     * Commits and closes child ResultReadable objects.
     * @returns Promise which fulfills with void.
     * @throws {@linkcode TransactionClosedError} when this transaction is closed.
     * @throws {@linkcode ClientException} when the commit digest from commit transaction result does not match.
     */
    async commit(): Promise<void> {
        if (this._isClosed) {
            throw new TransactionClosedError();
        }
        try {
            await this._hashLock.acquire();
            const commitTxnResult: CommitTransactionResult = await this._communicator.commit(
                this._txnId,
                this._txnHash.getQldbHash()
            );
            if (toBase64(this._txnHash.getQldbHash()) !== toBase64(<Uint8Array>(commitTxnResult.CommitDigest))) {
                throw new ClientException(
                    `Transaction's commit digest did not match returned value from QLDB.
                    Please retry with a new transaction. Transaction ID: ${this._txnId}.`
                );
            }
            this._isClosed = true;
        } catch (e) {
            if (isOccConflictException(e)) {
                throw e;
            }
            try {
                await this._communicator.abortTransaction();
            } catch (e2) {
                warn(`Ignored error aborting transaction after a failed commit: ${e2}.`);
            }
            throw e;
        } finally {
            this._internalClose();
            this._hashLock.release();
        }
    }

    /**
     * Execute the specified statement in the current transaction. This method returns a promise
     * which eventually returns all the results loaded into memory.
     *
     * @param statement A statement to execute against QLDB as a string.
     * @param parameters Variable number of arguments, where each argument corresponds to a
     *                  placeholder (?) in the PartiQL query.
     *                  The argument could be any native JavaScript type or an Ion DOM type.
     *                  [Details of Ion DOM type and JavaScript type](https://github.com/amzn/ion-js/blob/master/src/dom/README.md#iondom-data-types)
     * @returns Promise which fulfills with all results loaded into memory
     * @throws [Error](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Error) when the passed argument value cannot be converted into Ion
     */
    async execute(statement: string, ...parameters: any[]): Promise<Result> {
        const result: ExecuteStatementResult = await this._sendExecute(statement, parameters);
        return Result.create(this._txnId, result, this._communicator);
    }

    /**
     * Execute the specified statement in the current transaction. This method returns a promise
     * which fulfills with Readable Stream, which allows you to stream one record at time
     *
     * @param statement A statement to execute against QLDB as a string.
     * @param parameters Variable number of arguments, where each argument corresponds to a
     *                  placeholder (?) in the PartiQL query.
     *                  The argument could be any native JavaScript type or an Ion DOM type.
     *                  [Details of Ion DOM type and JavaScript type](https://github.com/amzn/ion-js/blob/master/src/dom/README.md#iondom-data-types)
     * @returns Promise which fulfills with a Readable Stream
     * @throws {@linkcode TransactionClosedError} when the transaction is closed.
     * @throws [Error](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Error) when the passed argument value cannot be converted into Ion
     */
    async executeAndStreamResults(statement: string, ...parameters: any[]): Promise<ResultReadable> {
        const result: ExecuteStatementResult = await this._sendExecute(statement, parameters);
        return new ResultReadable(this._txnId, result, this._communicator);
    }

    /**
     * Retrieve the transaction ID associated with this transaction.
     * @returns The transaction ID.
     */
    getTransactionId(): string {
        return this._txnId;
    }

    /**
     * Mark the transaction as closed.
     */
    private _internalClose(): void {
        this._isClosed = true;
    }

    /**
     * Helper method to execute statement against QLDB.
     * @param statement A statement to execute against QLDB as a string.
     * @param parameters An optional list of Ion values or JavaScript native types that are convertible to Ion for
     *                   filling in parameters of the statement.
     * @returns Promise which fulfills with a ExecuteStatementResult object.
     * @throws {@linkcode TransactionClosedError} when transaction is closed.
     */
    private async _sendExecute(statement: string, parameters: any[]): Promise<ExecuteStatementResult> {
        if (this._isClosed) {
            throw new TransactionClosedError();
        }

        try {
            await this._hashLock.acquire();
            let statementHash: QldbHash = QldbHash.toQldbHash(statement);

            const valueHolderList: ValueHolder[] = parameters.map((param: any) => {
                let ionBinary: Uint8Array;
                try {
                    ionBinary = dumpBinary(param);
                } catch(e) {
                    e.message = `Failed to convert parameter ${String(param)} to Ion Binary: ${e.message}`;
                    throw e;
                }
                statementHash = statementHash.dot(QldbHash.toQldbHash(ionBinary));
                const valueHolder: ValueHolder = {
                    IonBinary: ionBinary
                };
                return valueHolder;
            });

            this._txnHash = this._txnHash.dot(statementHash);

            const result: ExecuteStatementResult = await this._communicator.executeStatement(
                this._txnId,
                statement,
                valueHolderList
            );
            return result;
        } finally {
            this._hashLock.release();
        }
    }
}
