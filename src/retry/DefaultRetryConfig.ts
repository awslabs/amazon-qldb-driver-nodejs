/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import { RetryConfig } from "./RetryConfig";

const SLEEP_CAP_MS: number = 5000;
const SLEEP_BASE_MS: number = 10;

/**
 * A default backoff function which returns the amount of time(in milliseconds) to delay the next retry attempt
 * 
 * @param retryAttempt The number of attempts done till now 
 * @param error The error that occurred while executing the previous transaction
 * @param transactionId  The transaction Id for which the execution was attempted
 */
export const defaultBackoffFunction: BackoffFunction = (retryAttempt: number, error: Error, transactionId: string) => {
    const exponentialBackoff: number = Math.min(SLEEP_CAP_MS, Math.pow(2,  retryAttempt) * SLEEP_BASE_MS);
    const min: number = 0;
    const max: number = exponentialBackoff/2 + 1;
    const jitterRand: number = Math.random() * (max - min) + min;
    const delayTime: number = (exponentialBackoff/2) + jitterRand;
    return delayTime;
}

export const defaultRetryConfig: RetryConfig = new RetryConfig(4, defaultBackoffFunction);
