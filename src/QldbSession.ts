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
import { QldbWriter } from "./QldbWriter";
import { Transaction } from "./Transaction";
import { TransactionExecutor } from "./TransactionExecutor";

/**
 * The top-level interface for a QldbSession object for interacting with QLDB. A QldbSession is linked to the specified 
 * ledger in the parent driver of the instance of the QldbSession. In any given QldbSession, only one transaction can be 
 * active at a time. This object can have only one underlying session to QLDB, and therefore the lifespan of a 
 * QldbSession is tied to the underlying session, which is not indefinite, and on expiry this QldbSession will become 
 * invalid, and a new QldbSession needs to be created from the parent driver in order to continue usage.
 *
 * When a QldbSession is no longer needed, {@linkcode QldbSession.close} should be invoked in order to clean up any 
 * resources.
 *
 * See {@linkcode PooledQldbDriver} for an example of session lifecycle management, allowing the re-use of sessions 
 * when possible.
 *
 * There are three methods of execution, ranging from simple to complex; the first two are recommended for inbuilt 
 * error handling:
 *  - {@linkcode QldbSession.executeStatement} allows for a single statement to be executed within a transaction where 
 *    the transaction is implicitly created and committed, and any recoverable errors are transparently handled.
 *  - {@linkcode QldbSession.executeLambda} allow for more complex execution sequences where more than one execution can 
 *    occur, as well as other method calls. The transaction is implicitly created and committed, and any recoverable 
 *    errors are transparently handled.
 *  - {@linkcode QldbSession.startTransaction} allows for full control over when the transaction is committed and 
 *    leaves the responsibility of OCC conflict handling up to the user. Transactions' methods cannot be automatically 
 *    retried, as the state of the transaction is ambiguous in the case of an unexpected error.
 */
export interface QldbSession {
    
    /**
     * Close this session. No-op if already closed.
     */
    close: () => void;

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
    executeLambda: (queryLambda: (transactionExecutor: TransactionExecutor) => any,
                    retryIndicator?: (retryAttempt: number) => void) => Promise<any>;

    /**
     * Implicitly start a transaction, execute the statement, and commit the transaction, retrying up to the
     * retry limit if an OCC conflict or retriable exception occurs.
     * 
     * @param statement The statement to execute.
     * @param parameters An optional list of QLDB writers containing Ion values to execute.
     * @param retryIndicator An optional lambda that is invoked when the `statement` is about to be retried due to an 
     *                       OCC conflict or retriable exception.
     * @returns Promise which fulfills with a Result.
     * @throws {@linkcode SessionClosedError} when the session is closed.
     */
    executeStatement: (statement: string,
                       parameters: QldbWriter[],
                       retryIndicator?: (retryAttempt: number) => void) => Promise<Result>;

    /**
     * Return the name of the ledger for the session.
     * @returns Returns the name of the ledger as a string.
     */
    getLedgerName: () => string;

    /**
     * Returns the token for this session.
     * @returns Returns the session token as a string.
     */
    getSessionToken: () => string;

    /**
     * Lists all tables in the ledger.
     * @returns Promise which fulfills with an array of table names.
     */
    getTableNames: () => Promise<string[]>;

    /**
     * Start a transaction using an available database session.
     * @returns Promise which fulfills with a transaction object.
     * @throws {@linkcode SessionClosedError} when the session is closed.
     */
    startTransaction: () => Promise<Transaction>;
}
