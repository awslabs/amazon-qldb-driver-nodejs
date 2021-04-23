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

import { LambdaAbortedError } from "./errors/Errors";
import { Result } from "./Result";
import { ResultReadable } from "./ResultReadable";
import { Transaction } from "./Transaction";

/**
 * A class to handle lambda execution.
 */
export class TransactionExecutor {
    private _transaction: Transaction;

    /**
     * Creates a TransactionExecutor.
     * @param transaction The transaction that this executor is running within.
     * 
     * @internal
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
     * Execute the specified statement in the current transaction. This method returns a promise
     * which eventually returns all the results loaded into memory.
     *
     * The PartiQL statement executed via this transaction is not immediately committed.
     * The entire transaction will be committed once the all the code in `transactionFunction`
     * (passed as an argument to {@link QldbDriver.executeLambda}) completes.
     *
     * @param statement The statement to execute.
     * @param parameters Variable number of arguments, where each argument corresponds to a
     *                  placeholder (?) in the PartiQL query.
     *                  The argument could be any native JavaScript type or an Ion DOM type.
     *                  [Details of Ion DOM type and JavaScript type](https://github.com/amzn/ion-js/blob/master/src/dom/README.md#iondom-data-types)
     * @returns Promise which fulfills with all results loaded into memory
     * @throws [Error](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Error) when the passed argument value cannot be converted into Ion
     */
    async execute(statement: string, ...parameters: any[]): Promise<Result> {
        return await this._transaction.execute(statement, ...parameters);
    }

    /**
     * Execute the specified statement in the current transaction. This method returns a promise
     * which fulfills with Readable interface, which allows you to stream one record at time
     *
     * The PartiQL statement executed via this transaction is not immediately committed.
     * The entire transaction will be committed once the all the code in `transactionFunction`
     * (passed as an argument to {@link QldbDriver.executeLambda}) completes.
     *
     * @param statement The statement to execute.
     * @param parameters Variable number of arguments, where each argument corresponds to a
     *                  placeholder (?) in the PartiQL query.
     *                  The argument could be any native JavaScript type or an Ion DOM type.
     *                  [Details of Ion DOM type and JavaScript type](https://github.com/amzn/ion-js/blob/master/src/dom/README.md#iondom-data-types)
     * @returns Promise which fulfills with a Readable Stream
     * @throws {@linkcode TransactionClosedError} when the transaction is closed.
     * @throws [Error](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Error) when the passed argument value cannot be converted into Ion
     */
    async executeAndStreamResults(statement: string, ...parameters: any[]): Promise<ResultReadable> {
        return await this._transaction.executeAndStreamResults(statement, ...parameters);
    }

    /**
    * Get the transaction ID.
    * @returns The transaction ID.
    */
    getTransactionId(): string {
        return this._transaction.getTransactionId();
    }
}
