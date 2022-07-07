/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import { StartTransactionResult } from "@aws-sdk/client-qldb-session";

import { Communicator } from "./Communicator";
import {
    ExecuteError,
    isInvalidSessionException,
    isOccConflictException,
    isRetryableException,
    isTransactionExpiredException
} from "./errors/Errors";
import { AWSError } from "aws-sdk";
import { warn } from "./LogUtil";
import { Transaction } from "./Transaction";
import { TransactionExecutor } from "./TransactionExecutor";

/**
 * @internal
 */
export class QldbSession {
    private _communicator: Communicator;
    private _isAlive: boolean;

    constructor(communicator: Communicator) {
        this._communicator = communicator;
        this._isAlive = true;
    }

    isAlive(): boolean {
        return this._isAlive;
    }

    async endSession(): Promise<void> {
        try {
            this._isAlive = false;
            await this._communicator.endSession();
        } catch (e) {
            // We will only log issues ending the session, as QLDB will clean them after a timeout.
            warn(`Errors ending session: ${e}.`);
        }
    }

    async executeLambda<Type>(
        transactionLambda: (transactionExecutor: TransactionExecutor) => Promise<Type>
    ): Promise<Type> {
        let transaction: Transaction;
        let transactionId: string = null;
        let onCommit: boolean = false;
        try {
            transaction = await this._startTransaction();
            transactionId = transaction.getTransactionId();
            const executor: TransactionExecutor = new TransactionExecutor(transaction);
            const returnedValue: Type = await transactionLambda(executor);
            onCommit = true;
            await transaction.commit();
            return returnedValue;
        } catch (e) {
            const isRetryable: boolean = isRetryableException(e as Error, onCommit);
            const isISE: boolean = isInvalidSessionException(e as Error);
            if (isISE && !isTransactionExpiredException(e as Error)) {
                // Underlying session is dead on InvalidSessionException except for transaction expiry
                this._isAlive = false;
            } else if (!isOccConflictException(e as Error)) {
                // OCC does not need session state reset as the transaction is implicitly closed
                await this._cleanSessionState();
            }
            throw new ExecuteError(e as Error, isRetryable, isISE, transactionId);
        }
    }

    async _startTransaction(): Promise<Transaction> {
        const startTransactionResult: StartTransactionResult = await this._communicator.startTransaction();
        return new Transaction(this._communicator, startTransactionResult.TransactionId);
    }

    private async _cleanSessionState(): Promise<void> {
        try {
            await this._communicator.abortTransaction();
        } catch (e) {
            warn(`Ignored error while aborting transaction during execution: ${e}.`);
            this._isAlive = false;
        }
    }
}
