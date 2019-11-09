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

import { SessionClosedError } from "./errors/Errors";
import { QldbSession } from "./QldbSession";
import { QldbSessionImpl } from "./QldbSessionImpl";
import { QldbWriter } from "./QldbWriter";
import { Result } from "./Result";
import { Transaction } from "./Transaction";
import { TransactionExecutor } from "./TransactionExecutor";

/**
 * Represents a pooled session object. See {@linkcode QldbSessionImpl} for more details.
 */
export class PooledQldbSession implements QldbSession {
    private _session: QldbSessionImpl;
    private _returnSessionToPool: (session: QldbSessionImpl) => void;
    private _isClosed: boolean;

    /**
     * Creates a PooledQldbSession.
     * @param session The QldbSession object that represents a session to a QLDB ledger.
     * @param returnSessionToPool A lambda that is invoked when {@linkcode PooledQldbSession.close} is called.
     */
    constructor(session: QldbSessionImpl, returnSessionToPool: (Session: QldbSessionImpl) => void) {
        this._session = session;
        this._returnSessionToPool = returnSessionToPool;
        this._isClosed = false;
    }

    /**
     * Close this {@linkcode PooledQldbSession} and return the underlying {@linkcode QldbSession} to the pool.
     */
    close(): void {
        if (this._isClosed) {
            return;
        }
        this._isClosed = true;
        this._returnSessionToPool(this._session);
    }

    /**
     * Implicitly start a transaction, execute the lambda, and commit the transaction, retrying up to the
     * retry limit if an OCC conflict or retriable exception occurs.
     * 
     * @param queryLambda A lambda representing the block of code to be executed within the transaction. This cannot 
     *                    have any side effects as it may be invoked multiple times, and the result cannot be trusted 
     *                    until the transaction is committed.
     * @param retryIndicator An optional lambda that is invoked when the `querylambda` is about to be retried due to an 
     *                       OCC conflict or retriable exception.
     * @returns Promise which fulfills with the return value of the `queryLambda` which could be a {@linkcode Result} 
     *          on the result set of a statement within the lambda.
     * @throws {@linkcode SessionClosedError} when this session is closed.
     */
    async executeLambda(
        queryLambda: (transactionExecutor: TransactionExecutor) => any, 
        retryIndicator?: (retryAttempt: number) => void
    ): Promise<any> {
        this._throwIfClosed()
        return await this._session.executeLambda(queryLambda, retryIndicator);
    }
    
    /**
     * Implicitly start a transaction, execute the statement, and commit the transaction, retrying up to the
     * retry limit if an OCC conflict or retriable exception occurs.
     * 
     * @param statement The statement to execute.
     * @param parameters An optional list of QLDB writers containing Ion values to execute.
     * @param retryIndicator An optional lambda that is invoked when the `statement` is about to be retried due to an 
     *                       OCC conflict or retriable exception.
     * @returns Promise which fulfills with a Result.
     * @throws {@linkcode SessionClosedError} when this session is closed.
     */
    async executeStatement(
        statement: string, 
        parameters: QldbWriter[] = [],
        retryIndicator?: (retryAttempt: number) => void
    ): Promise<Result> {
        this._throwIfClosed();
        return await this._session.executeStatement(statement, parameters, retryIndicator);
    }

    /**
     * Return the name of the ledger for the session.
     * @returns Returns the name of the ledger as a string.
     */
    getLedgerName(): string {
        this._throwIfClosed();
        return this._session.getLedgerName();
    }

    /**
     * Return the session token for this session.
     * @returns Returns the session token as a string.
     */
    getSessionToken(): string {
        this._throwIfClosed();
        return this._session.getSessionToken();
    }

    /**
     * Lists all tables in the ledger.
     * @returns Promise which fulfills with an array of table names.
     */
    async getTableNames(): Promise<string[]> {
        this._throwIfClosed();
        return await this._session.getTableNames();
    }

    /**
     * Start a transaction using an available database session.
     * @returns Promise which fulfills with a Transaction object.
     * @throws {@linkcode SessionClosedError} when this session is closed.
     */
    async startTransaction(): Promise<Transaction> {
        this._throwIfClosed();
        return await this._session.startTransaction();
    }

    /**
     * Check and throw if this session is closed.
     * @throws {@linkcode SessionClosedError} when this session is closed.
     */
    private _throwIfClosed(): void {
        if (this._isClosed) {
            throw new SessionClosedError();
        }
    }
}
