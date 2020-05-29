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

import { ClientConfiguration } from "aws-sdk/clients/qldbsession";
import { QldbDriver } from "./QldbDriver";
import { warn } from "./LogUtil";


/**
 * @deprecated Use {@linkcode QldbDriver} instead
 * */
export class PooledQldbDriver extends QldbDriver  {
    constructor(
        ledgerName: string,
        qldbClientOptions: ClientConfiguration = {},
        retryLimit: number = 4,
        poolLimit: number = 0,
        timeoutMillis: number = 30000
    ) {
        warn(`Please use QldbDriver instead of PooledQldbDriver to execute transactions against Amazon QLDB`);
        super(ledgerName, qldbClientOptions, retryLimit, poolLimit, timeoutMillis);
    }
}
