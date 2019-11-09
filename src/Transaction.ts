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
import { toBase64 } from "ion-js";
import { Lock } from "semaphore-async-await";
import { Readable } from "stream";

import { Communicator } from "./Communicator";
import { ClientException, isOccConflictException, TransactionClosedError } from "./errors/Errors";
import { warn } from "./logUtil";
import { QldbHash } from "./QldbHash";
import { QldbWriter } from "./QldbWriter";
import { Result } from "./Result";
import { ResultStream } from "./ResultStream";

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
 *
 * Child {@linkcode ResultStream} objects will be closed when this transaction is aborted or committed.
 */
export class Transaction {
    private _communicator: Communicator;
    private _txnId: string;
    private _isClosed: boolean;
    private _resultStreams: ResultStream[];
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
        this._resultStreams = [];
        this._txnHash = QldbHash.toQldbHash(txnId);
        this._hashLock = new Lock();
    }

    /**
     * Abort this transaction and close child ResultStream objects. No-op if already closed by commit or previous abort.
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
     * Commits and closes child ResultStream objects.
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
     * Execute the specified statement in the current transaction.
     * @param statement A statement to execute against QLDB as a string.
     * @param parameters An optional list of QLDB writers containing Ion values to execute against QLDB.
     * @returns Promise which fulfills with a fully-buffered Result.
     */
    async executeInline(statement: string, parameters: QldbWriter[] = []): Promise<Result> {
        const result: ExecuteStatementResult = await this._sendExecute(statement, parameters);
        const inlineResult = Result.create(this._txnId, result.FirstPage, this._communicator);
        return inlineResult;
    }

    /**
     * Execute the specified statement in the current transaction.
     * @param statement A statement to execute against QLDB as a string.
     * @param parameters An optional list of QLDB writers containing Ion values to execute against QLDB.
     * @returns Promise which fulfills with a Readable.
     */
    async executeStream(statement: string, parameters: QldbWriter[] = []): Promise<Readable> {
        const result: ExecuteStatementResult = await this._sendExecute(statement, parameters);
        const resultStream = new ResultStream(this._txnId, result.FirstPage, this._communicator);
        this._resultStreams.push(resultStream);
        return resultStream;
    }

    /**
     * Retrieve the transaction ID associated with this transaction.
     * @returns The transaction ID.
     */
    getTransactionId(): string {
        return this._txnId;
    }

    /**
     * Mark the transaction as closed, and stop streaming for any ResultStream objects.
     */
    private _internalClose(): void {
        this._isClosed = true;
        while (this._resultStreams.length !== 0) {
            this._resultStreams.pop().close();
        }
    }

    /**
     * Helper method to execute statement against QLDB.
     * @param statement A statement to execute against QLDB as a string.
     * @param parameters A list of QLDB writers containing Ion values to execute against QLDB.
     * @returns Promise which fulfills with a ExecuteStatementResult object.
     * @throws {@linkcode TransactionClosedError} when transaction is closed.
     */
    private async _sendExecute(statement: string, parameters: QldbWriter[]): Promise<ExecuteStatementResult> {
        if (this._isClosed) {
            throw new TransactionClosedError();
        }

        try {
            await this._hashLock.acquire();
            let statementHash: QldbHash = QldbHash.toQldbHash(statement);

            const valueHolderList: ValueHolder[] = parameters.map((writer: QldbWriter) => {
                try {
                    writer.close();
                } catch (e) {
                    warn(
                        "Error encountered when attempting to close parameter writer. This warning can be ignored if " +
                        `the writer was manually closed: ${e}.`
                    );
                }
                const ionBinary: Uint8Array = writer.getBytes();
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
