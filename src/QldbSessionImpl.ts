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

import { StartTransactionResult } from "aws-sdk/clients/qldbsession";
import { dom } from "ion-js";

import { Communicator } from "./Communicator";
import {
    isInvalidSessionException,
    isOccConflictException,
    isRetriableException,
    LambdaAbortedError,
    SessionClosedError
} from "./errors/Errors";
import { info, warn } from "./LogUtil";
import { QldbSession } from "./QldbSession";
import { Result } from "./Result";
import { ResultStream } from "./ResultStream";
import { Transaction } from "./Transaction";
import { TransactionExecutor } from "./TransactionExecutor";

const SLEEP_CAP_MS: number = 5000;
const SLEEP_BASE_MS: number = 10;

/**
 * @deprecated [NOT RECOMMENDED} It is not recommended to use this class directly during transaction execution.
 * Instead, please use {@linkcode QldbDriver.executeLambda} to execute the transaction.
 */
export class QldbSessionImpl implements QldbSession {
    private _communicator: Communicator;
    private _retryLimit: number;
    private _isClosed: boolean;

    constructor(communicator: Communicator, retryLimit: number) {
        this._communicator = communicator;
        this._retryLimit = retryLimit;
        this._isClosed = false;
    }

    /**
     * @deprecated [NOT RECOMMENDED} It is not recommended to use this method during transaction execution.
     * Instead, please use {@linkcode QldbDriver.executeLambda} to execute the transaction.
     */
    close(): void {
        if (this._isClosed) {
            return;
        }
        this._isClosed = true;
        this._communicator.endSession();
    }

    /**
     * @deprecated [NOT RECOMMENDED} It is not recommended to use this method during transaction execution.
     * Instead, please use {@linkcode QldbDriver.executeLambda} to execute the transaction.
     */
    async executeLambda(
        queryLambda: (transactionExecutor: TransactionExecutor) => any,
        retryIndicator?: (retryAttempt: number) => void
    ): Promise<any> {
        this._throwIfClosed();
        let transaction: Transaction;
        let retryAttempt: number = 0;
        while (true) {
            try {
                transaction = null;
                transaction = await this.startTransaction();
                const transactionExecutor = new TransactionExecutor(transaction);
                let returnedValue: any = await queryLambda(transactionExecutor);
                if (returnedValue instanceof ResultStream) {
                    returnedValue = await Result.bufferResultStream(returnedValue);
                }
                await transaction.commit();
                return returnedValue;
            } catch (e) {
                await this._noThrowAbort(transaction);
                if (retryAttempt >= this._retryLimit || e instanceof LambdaAbortedError) {
                    throw e;
                }
                if (isOccConflictException(e) || isRetriableException(e) || isInvalidSessionException(e)) {
                    warn(`OCC conflict or retriable exception occurred: ${e}.`);
                    if (isInvalidSessionException(e)) {
                        info(`Creating a new session to QLDB; previous session is no longer valid: ${e}.`);
                        this._communicator = await Communicator.create(
                            this._communicator.getQldbClient(),
                            this._communicator.getLedgerName()
                        );
                    }

                    retryAttempt++;
                    if (retryIndicator !== undefined) {
                        retryIndicator(retryAttempt);
                    }
                    await this._retrySleep(retryAttempt);
                } else {
                    throw e;
                }
            }
        }
    }

    getLedgerName(): string {
        return this._communicator.getLedgerName();
    }

    getSessionToken(): string {
        return this._communicator.getSessionToken();
    }

    /**
     * @deprecated [NOT RECOMMENDED} It is not recommended to use this method.
     * Instead, please use {@linkcode QldbDriver.getTableNames} to get table names.
     */
    async getTableNames(): Promise<string[]> {
        const statement: string = "SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'";
        return await this.executeLambda(async (transactionExecutor: TransactionExecutor) : Promise<string[]> => {
            const result: Result = await transactionExecutor.execute(statement);
            const resultStructs: dom.Value[] = result.getResultList();
            const listOfTableNames: string[] = resultStructs.map(tableNameStruct =>
                tableNameStruct.get("name").stringValue());
            return listOfTableNames;
        });
    }

    /**
     * @deprecated [NOT RECOMMENDED} It is not recommended to use this method during transaction execution.
     * Instead, please use {@linkcode QldbDriver.executeLambda} to execute the transaction.
     */
    async startTransaction(): Promise<Transaction> {
        this._throwIfClosed();
        const startTransactionResult: StartTransactionResult = await this._communicator.startTransaction();
        const transaction: Transaction = new Transaction(
            this._communicator,
            startTransactionResult.TransactionId
        );
        return transaction;
    }

    /**
     * @deprecated [NOT RECOMMENDED} It is not recommended to use this method during transaction execution.
     * Instead, please use {@linkcode QldbDriver.executeLambda} to execute the transaction.
     */
    async _abortOrClose(): Promise<boolean> {
        if (this._isClosed) {
            return false;
        }
        try {
            await this._communicator.abortTransaction();
            return true;
        } catch (e) {
            this._isClosed = true;
            return false;
        }
    }

    private async _noThrowAbort(transaction: Transaction): Promise<void> {
        try {
            if (null == transaction) {
                await this._communicator.abortTransaction()
            } else {
                await transaction.abort();
            }
        } catch (e) {
            warn(`Ignored error while aborting transaction during execution: ${e}.`);
        }
    }

    private _retrySleep(attemptNumber: number) {
        const delayTime = this._calculateDelayTime(attemptNumber);
        return this._sleep(delayTime);
    }

    private _calculateDelayTime(attemptNumber: number) {
        const exponentialBackoff: number = Math.min(SLEEP_CAP_MS, Math.pow(2,  attemptNumber) * SLEEP_BASE_MS);
        const jitterRand: number = this._getRandomArbitrary(0, (exponentialBackoff/2 + 1));
        const delayTime: number = (exponentialBackoff/2) + jitterRand;
        return delayTime;
    }

    private _sleep(sleepTime:number) {
        return new Promise(resolve => setTimeout(resolve, sleepTime));
    }

    private _getRandomArbitrary(min:number, max:number) {
        return Math.random() * (max - min) + min;
    }

    private _throwIfClosed(): void {
        if (this._isClosed) {
            throw new SessionClosedError();
        }
    }
}
