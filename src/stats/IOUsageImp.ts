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

import { IOUsage as sdkIOUsage } from "aws-sdk/clients/qldbsession";

import { IOUsage } from "./IOUsage";

export class IOUsageImp implements IOUsage {
    private _readIOs: number;

    /**
     * Creates an IOUsageImp.
     * @param readIOs The number of Read IOs.
     */
    constructor(readIOs: number) {
        this._readIOs = readIOs;
    }

    /**
     * Provides the number of Read IOs for a request.
     * @returns The number of Reads for a request.
     */
    getReadIOs(): number {
        return this._readIOs;
    }

    private _setReadIOs(value: number) {
        this._readIOs = value;
    }

    /**
     * Accumulates the number of IOs to the current instance, for stateful cases.
     * @param consumedIOs Server-side object, containing consumed IOs to be added to the current number of IOs.
     */
    accumulateIOUsage(consumedIOs: sdkIOUsage) {
        if (consumedIOs != null) {
            this._setReadIOs(this.getReadIOs() + consumedIOs.ReadIOs);
        }
    }

}
