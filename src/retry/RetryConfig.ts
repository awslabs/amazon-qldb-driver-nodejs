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

import { BackoffFunction } from "./BackoffFunction";
import { defaultBackoffFunction } from "./DefaultRetryConfig";

export class RetryConfig {
    private _retryLimit: number;
    private _backoffFunction: BackoffFunction; 

    /** 
    * Retry and Backoff config for Qldb Driver. 

    * @param retryLimit When there is a failure while executing the transaction like OCC or any other retryable failure, the driver will try running your transaction block again.
    *                   This parameter tells the driver how many times to retry when there are failures. The value must be greater than 0. The default value is 4.
    *                   See {@link https://docs.aws.amazon.com/qldb/latest/developerguide/driver.best-practices.html#driver.best-practices.configuring} for more details. 
    * 
    * @param backoffFunction A custom function that accepts a retry count, error, transaction id and returns the amount
    *                        of time to delay in milliseconds. If the result is a non-zero negative value the backoff will
    *                        be considered to be zero. If no backoff function is provided then {@linkcode defaultBackoffFunction} will be used.
    *
    * @throws RangeError if `retryLimit` is less than 0.
    */
    constructor(retryLimit: number = 4, backoffFunction: BackoffFunction = defaultBackoffFunction) {
        if (retryLimit < 0) {
            throw new RangeError("Value for retryLimit cannot be negative.");
        }
        this._retryLimit = retryLimit;
        this._backoffFunction = backoffFunction;
    }

    getRetryLimit(): number {
        return this._retryLimit;
    }

    getBackoffFunction(): BackoffFunction {
        return this._backoffFunction;
    } 

}
