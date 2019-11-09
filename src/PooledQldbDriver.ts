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
import { globalAgent } from "http";
import Semaphore from "semaphore-async-await";

import { DriverClosedError, SessionPoolEmptyError } from "./errors/Errors";
import { debug } from "./logUtil";
import { PooledQldbSession } from "./PooledQldbSession";
import { QldbDriver } from "./QldbDriver";
import { QldbSession } from "./QldbSession";
import { QldbSessionImpl } from "./QldbSessionImpl";

/**
 * Represents a factory for accessing pooled sessions to a specific ledger within QLDB. This class or
 * {@linkcode QldbDriver} should be the main entry points to any interaction with QLDB.
 * {@linkcode PooledQldbDriver.getSession} will create a {@linkcode PooledQldbSession} to the specified ledger within
 * QLDB as a communication channel. Any acquired sessions must be cleaned up with {@linkcode PooledQldbSession.close}
 * when they are no longer needed in order to return the session to the pool. If this is not done, this driver may
 * become unusable if the pool limit is exceeded.
 *
 * This factory pools sessions and attempts to return unused but available sessions when getting new sessions. The
 * advantage to using this over the non-pooling driver is that the underlying connection that sessions use to
 * communicate with QLDB can be recycled, minimizing resource usage by preventing unnecessary connections and reducing
 * latency by not making unnecessary requests to start new connections and end reusable, existing, ones.
 *
 * The pool does not remove stale sessions until a new session is retrieved. The default pool size is the maximum
 * amount of connections the session client allows. {@linkcode PooledQldbDriver.close} should be called when this
 * factory is no longer needed in order to clean up resources, ending all sessions in the pool.
 */
export class PooledQldbDriver extends QldbDriver {
    private _poolLimit: number;
    private _timeoutMillis: number;
    private _availablePermits: number;
    private _sessionPool: QldbSessionImpl[];
    private _semaphore: Semaphore;

    /**
     * Creates a PooledQldbDriver.
     * @param qldbClientOptions The object containing options for configuring the low level client.
     *                          See {@link https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/QLDBSession.html#constructor-details|Low Level Client Constructor}.
     * @param ledgerName The QLDB ledger name.
     * @param retryLimit The number of automatic retries for statement executions using convenience methods on sessions
                         when an OCC conflict or retriable exception occurs. This value must not be negative.
     * @param poolLimit The session pool limit. Set to `undefined` to use the maximum sockets from the `globalAgent`.
     * @param timeoutMillis The timeout in milliseconds while attempting to retrieve a session from the session pool.
     * @throws RangeError if `retryLimit` is less than 0 or `poolLimit` is greater than the client limit.
     */
    constructor(
        ledgerName: string,
        qldbClientOptions: ClientConfiguration = {},
        retryLimit: number = 4,
        poolLimit: number = 0,
        timeoutMillis: number = 30000
    ) {
        super(ledgerName, qldbClientOptions, retryLimit);
        if (timeoutMillis < 0) {
            throw new RangeError("Value for timeout cannot be negative.");
        }
        if (poolLimit < 0) {
            throw new RangeError("Value for poolLimit cannot be negative.");
        }

        let maxSockets: number;
        if (qldbClientOptions.httpOptions && qldbClientOptions.httpOptions.agent) {
            maxSockets = qldbClientOptions.httpOptions.agent.maxSockets;
        } else {
            maxSockets = globalAgent.maxSockets;
        }

        if (0 === poolLimit) {
            this._poolLimit = maxSockets;
        } else {
            this._poolLimit = poolLimit;
        }
        if (this._poolLimit > maxSockets) {
            throw new RangeError(
                `The session pool limit given, ${this._poolLimit}, exceeds the limit set by the client,
                 ${maxSockets}. Please lower the limit and retry.`
            );
        }

        this._timeoutMillis = timeoutMillis;
        this._availablePermits = this._poolLimit;
        this._sessionPool = [];
        this._semaphore = new Semaphore(this._poolLimit);
    }

    /**
     * Close this driver and any sessions in the pool.
     */
    close(): void {
        super.close();
        this._sessionPool.forEach(session => {
            session.close();
        });
    }

    /**
     * This method will attempt to retrieve an active, existing session, or it will start a new session with QLDB if
     * none are available and the session pool limit has not been reached. If the pool limit has been reached, it will
     * attempt to retrieve a session from the pool until the timeout is reached.
     * @returns Promise which fulfills with a QldbSession.
     * @throws {@linkcode DriverClosedError} when this driver is closed.
     * @throws {@linkcode SessionPoolEmptyError} if the timeout is reached while attempting to retrieve a session.
     */
    async getSession(): Promise<QldbSession> {
        if (this._isClosed) {
            throw new DriverClosedError();
        }
        debug(
            `Getting session. Current free session count: ${this._sessionPool.length}. ` +
            `Currently available permit count: ${this._availablePermits}.`
        );
        const isPermitAcquired: boolean = await this._semaphore.waitFor(this._timeoutMillis);
        if (isPermitAcquired) {
            this._availablePermits--;
            while (this._sessionPool.length > 0) {
                const session: QldbSessionImpl = this._sessionPool.pop();
                const isSessionAvailable: boolean = await session._abortOrClose();
                if (isSessionAvailable) {
                    debug("Reusing session from pool.")
                    return new PooledQldbSession(session, this._returnSessionToPool);
                }
            }
            try {
                debug("Creating new pooled session.");
                const newSession: QldbSessionImpl = <QldbSessionImpl> (await super.getSession());
                return new PooledQldbSession(newSession, this._returnSessionToPool);
            } catch (e) {
                this._semaphore.release();
                this._availablePermits++;
                throw e;
            }
        }
        throw new SessionPoolEmptyError(this._timeoutMillis)
    }

    /**
     * Release a session back into the pool.
     */
    private _returnSessionToPool = (session: QldbSessionImpl): void => {
        this._sessionPool.push(session);
        this._semaphore.release();
        this._availablePermits++;
        debug(`Session returned to pool; size is now ${this._sessionPool.length}.`);
    };
}
