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

import { Readable } from "stream";

import { LambdaAbortedError } from "./errors/Errors";
import { QldbWriter } from "./QldbWriter";
import { Result } from "./Result";
import { Transaction } from "./Transaction";

/**
 * A class to handle lambda execution.
 */
export class TransactionExecutor {
    _transaction: Transaction;

    /**
     * Creates a TransactionExecutor.
     * @param transaction The transaction that this executor is running within.
     */
    constructor(transaction: Transaction) {
        this._transaction = transaction;
    }

    /**
     * Abort the transaction and roll back any changes.
     * @throws {@linkcode LambdaAbortedError} when called.
     */
    abort(): void {
        throw new LambdaAbortedError();
    }

    /**
     * Execute the specified statement in the current transaction.
     * @param statement The statement to execute.
     * @param parameters An optional list of QLDB writers containing Ion values to execute.
     * @returns Promise which fulfills with a Result.
     * @throws {@linkcode TransactionClosedError} when the transaction is closed.
     */
    async executeInline(statement: string, parameters: QldbWriter[] = []): Promise<Result> {
        return await this._transaction.executeInline(statement, parameters);
    }

    /**
     * Execute the specified statement in the current transaction.
     * @param statement The statement to execute.
     * @param parameters An optional list of QLDB writers containing Ion values to execute.
     * @returns Promise which fulfills with a ResultStream.
     * @throws {@linkcode TransactionClosedError} when the transaction is closed.
     */
    async executeStream(statement: string, parameters: QldbWriter[] = []): Promise<Readable> {
        return await this._transaction.executeStream(statement, parameters);
    }

    /**
    * Get the transaction ID.
    * @returns The transaction ID.
    */
    getTransactionId(): string {
        return this._transaction.getTransactionId();
    }
}
