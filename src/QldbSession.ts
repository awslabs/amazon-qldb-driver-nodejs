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
import { Communicator } from "./Communicator";
import {
    ExecuteError,
    isInvalidSessionException,
    isOccConflictException,
    isRetriableException,
    isTransactionExpiredException
} from "./errors/Errors";
import { warn } from "./LogUtil";
import { Transaction } from "./Transaction";
import { TransactionExecutor, TransactionExecutorImpl } from "./TransactionExecutor";

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
        try {
            const startTransactionResult: StartTransactionResult = await this._communicator.startTransaction();
            transaction = new Transaction(this._communicator, startTransactionResult.TransactionId);
            transactionId = transaction.getTransactionId();
            const executor: TransactionExecutorImpl = new TransactionExecutorImpl(transaction);
            const returnedValue: Type = await transactionLambda(executor);
            await transaction.commit();
            return returnedValue;
        } catch (e) {
            const isRetriable: boolean = isRetriableException(e);
            const isISE: boolean = isInvalidSessionException(e);
            if (isISE && !isTransactionExpiredException(e)) {
                // Underlying session is dead on InvalidSessionException except for transaction expiry
                this._isAlive = false;
            } else if (!isOccConflictException(e)) {
                // OCC does not need session state reset as the transaction is implicitly closed
                await this._tryAbort();
            }
            throw new ExecuteError(e, isRetriable, isISE, transactionId);
        }
    }

    private async _tryAbort(): Promise<void> {
        try {
            await this._communicator.abortTransaction();
        } catch (e) {
            warn(`Ignored error while aborting transaction during execution: ${e}.`);
            this._isAlive = false;
        }
    }
}
