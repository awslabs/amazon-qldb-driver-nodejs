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

import { Result } from "./Result";
import { ResultReadable } from "./ResultReadable";

/**
 * Interface for execution against QLDB in the context of a transaction.
 */
export interface TransactionExecutable {
    /**
     * Execute the specified statement in the current transaction.
     * @param statement A statement to execute against QLDB as a string.
     * @param parameters Rest parameters of Ion values or JavaScript native types that are convertible to Ion for
     *                   filling in parameters of the statement.
     * @returns Promise which fulfills with a fully-buffered Result.
     */
    execute(statement: string, ...parameters: any[]): Promise<Result>;

    /**
     * Execute the specified statement in the current transaction.
     * @param statement A statement to execute against QLDB as a string.
     * @param parameters Rest parameters of Ion values or JavaScript native types that are convertible to Ion for
     *                   filling in parameters of the statement.
     * @returns Promise which fulfills with a Readable.
     */
    executeAndStreamResults(statement: string, ...parameters: any[]): Promise<ResultReadable>;
}
