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

import { QLDBSession } from "aws-sdk";
import { ClientConfiguration } from "aws-sdk/clients/qldbsession";

import { version } from "../package.json";
import { Communicator } from "./Communicator";
import { DriverClosedError } from "./errors/Errors";
import { debug } from "./logUtil";
import { QldbSession } from "./QldbSession";
import { QldbSessionImpl } from "./QldbSessionImpl";

/**
 * Represents a factory for creating sessions to a specific ledger within QLDB. This class or
 * {@linkcode PooledQldbDriver} should be the main entry points to any interaction with QLDB.
 * {@linkcode QldbDriver.getSession} will create a {@linkcode QldbSession} to the specified edger within QLDB as a
 * communication channel. Any sessions acquired should be cleaned up with {@linkcode QldbSession.close} to free up
 * resources.
 *
 * This factory does not attempt to re-use or manage sessions in any way. It is recommended to use
 * {@linkcode PooledQldbDriver} for both less resource usage and lower latency.
 */
export class QldbDriver {
    protected _qldbClient: QLDBSession;
    protected _ledgerName: string;
    protected _retryLimit: number;
    protected _isClosed: boolean;

    /**
     * Creates a QldbDriver.
     * @param qldbClientOptions The object containing options for configuring the low level client.
     *                          See {@link https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/QLDBSession.html#constructor-details|Low Level Client Constructor}.
     * @param ledgerName The QLDB ledger name.
     * @param retryLimit The number of automatic retries for statement executions using convenience methods on sessions
                         when an OCC conflict or retriable exception occurs. This value must not be negative.
     * @throws RangeError if `retryLimit` is less than 0.
     */
    constructor(ledgerName: string, qldbClientOptions: ClientConfiguration = {}, retryLimit: number = 4) {
        if (retryLimit < 0) {
            throw new RangeError("Value for retryLimit cannot be negative.");
        }
        qldbClientOptions.customUserAgent = `QLDB Driver for Node.js v${version}`;
        qldbClientOptions.maxRetries = 0;

        this._qldbClient = new QLDBSession(qldbClientOptions);
        this._ledgerName = ledgerName;
        this._retryLimit = retryLimit;
        this._isClosed = false;
    }

    /**
     * Close this driver.
     */
    close(): void {
        this._isClosed = true;
    }

    /**
     * Create and return a newly instantiated QldbSession object. This will implicitly start a new session with QLDB.
     * @returns Promise which fulfills with a QldbSession.
     * @throws {@linkcode DriverClosedError} when this driver is closed.
     */
    async getSession(): Promise<QldbSession> {
        if (this._isClosed) {
            throw new DriverClosedError();
        }
        debug("Creating a new session.");
        const communicator: Communicator = await Communicator.create(this._qldbClient, this._ledgerName);
        return new QldbSessionImpl(communicator, this._retryLimit);
    }
}
