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

import { TransactionExecutor } from "./TransactionExecutor";

/**
 * Interface for execution against QLDB.
 */
export interface Executable {
    /**
     * Execute a lambda within a new transaction and commit the transaction, retrying up to the retry limit if an OCC 
     * conflict or retriable exception occurs.
     * 
     * @param queryLambda A lambda representing the block of code to be executed within the transaction. This cannot 
     *                    have any side effects as it may be invoked multiple times, and the result cannot be trusted 
     *                    until the transaction is committed.
     * @param retryIndicator An optional lambda that is invoked when the `querylambda` is about to be retried due to an 
     *                       OCC conflict or retriable exception.
     * @returns Promise which fulfills with the return value of the `queryLambda` which could be a {@linkcode Result} 
     *          on the result set of a statement within the lambda.
     */
    executeLambda: (queryLambda: (transactionExecutor: TransactionExecutor) => any,
                    retryIndicator?: (retryAttempt: number) => void) => Promise<any>;
}
