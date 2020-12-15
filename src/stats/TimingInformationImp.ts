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

import { TimingInformation as sdkTimingInformation } from "aws-sdk/clients/qldbsession";

import { TimingInformation } from "./TimingInformation";

export class TimingInformationImp implements TimingInformation {
    private _processingTimeMilliseconds: number;

    /**
     * Creates a TimingInformationImp.
     * @param processingTimeMilliseconds The server-side processing time in milliseconds.
     */
    constructor(processingTimeMilliseconds: number) {
        this._processingTimeMilliseconds = processingTimeMilliseconds;
    }

    /**
     * Provides the server-side time spent on a request.
     * @returns The server-side processing time in millisecond.
     */
    getProcessingTimeMilliseconds(): number {
        return this._processingTimeMilliseconds;
    }

    private _setProcessingTimeMilliseconds(value: number) {
        this._processingTimeMilliseconds = value;
    }

    /**
     * Accumulates the processing time to the current instance, for stateful cases.
     * @param timingInfo Server-side object, containing processing time to be added to the current time information.
     */
    accumulateTimingInfo(timingInfo: sdkTimingInformation) {
        if (timingInfo != null) {
            this._setProcessingTimeMilliseconds(
                this.getProcessingTimeMilliseconds() + timingInfo.ProcessingTimeMilliseconds);
        }
    }

}
