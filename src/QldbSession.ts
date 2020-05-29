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

import { Executable } from "./Executable";
import { Transaction } from "./Transaction";

/**
 * @deprecated [NOT RECOMMENDED] It is not recommended to use this class directly during transaction execution.
 * Instead, please use {@linkcode QldbDriver.executeLambda} to execute the transaction.
 */
export interface QldbSession extends Executable {
    
    /**
     * @deprecated [NOT RECOMMENDED] It is not recommended to use this class directly during transaction execution.
     * Instead, please use {@linkcode QldbDriver.executeLambda} to execute the transaction.
     */
    close: () => void;

    /**
     * @deprecated [NOT RECOMMENDED] It is not recommended to use this method.
     */
    getLedgerName: () => string;

    /**
     * @deprecated [NOT RECOMMENDED] It is not recommended to use this method.
     */
    getSessionToken: () => string;

    /**
     * @deprecated [NOT RECOMMENDED] It is not recommended to use this method.
     * Instead, please use {@linkcode QldbDriver.getTableNames} to get table names.
     */
    getTableNames: () => Promise<string[]>;

    /**
     * @deprecated [NOT RECOMMENDED] It is not recommended to use this class directly during transaction execution.
     * Instead, please use {@linkcode QldbDriver.executeLambda} to execute the transaction.
     */
    startTransaction: () => Promise<Transaction>;
}
