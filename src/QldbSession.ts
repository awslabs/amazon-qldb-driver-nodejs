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
 * @deprecated Use {@linkcode QldbDriver.executeLambda} instead to execute
 * transactions.
 *
 * A QldbSession is linked to the specified ledger in the parent driver of the
 * instance of the QldbSession. In any given QldbSession, only one transaction
 * can be active at a time. This object can have only one underlying session to
 * QLDB, and therefore the lifespan of a  QldbSession is tied to the underlying
 * session, which is not indefinite, and on expiry this QldbSession will become
 * invalid, and a new QldbSession needs to be created from the parent driver in
 * order to continue usage.
 *
 * See {@linkcode QldbDriver} for an example of session lifecycle management,
 * allowing the re-use of sessions when possible.
 *
 * There are three methods of execution, ranging from simple to complex; the
 * first two are recommended for inbuilt error handling:
 *  - {@linkcode QldbSession.executeStatement} allows for a single statement to
 *    be executed within a transaction where the transaction is implicitly
 *    created and committed, and any recoverable errors are transparently
 *    handled.
 *  - {@linkcode QldbSession.executeLambda} allow for more complex execution
 *    sequences where more than one execution can occur, as well as other method
 *    calls. The transaction is implicitly created and committed, and any
 *    recoverable errors are transparently handled.
 *  - {@linkcode QldbSession.startTransaction} allows for full control over when
 *    the transaction is committed and leaves the responsibility of OCC conflict
 *    handling up to the user. Transactions' methods cannot be automatically
 *    retried, as the state of the transaction is ambiguous in the case of an
 *    unexpected error.
 */
export interface QldbSession extends Executable {
    
    /**
     * Close this session. No-op if already closed.
     * @deprecated Use {@linkcode QldbDriver} to interact with QLDB as it 
     * manages the lifecycle of the sessions ounder the hood.
     */
    close: () => void;

    /**
     * @deprecated
     * 
     * Return the name of the ledger for the session.
     * @returns Returns the name of the ledger as a string.
     */
    getLedgerName: () => string;

    /**
     * @deprecated Use {@linkcode QldbDriver} to interact with QLDB as it 
     * manages the lifecycle of the sessions ounder the hood.
     * 
     * Returns the token for this session.
     * @returns Returns the session token as a string.
     */
    getSessionToken: () => string;

    /**
     * @deprecated Use {@linkcode QldbDriver.getTableNames} instead to execute
     * transactions.
     * 
     * Lists all tables in the ledger.
     * @returns Promise which fulfills with an array of table names.
     */
    getTableNames: () => Promise<string[]>;

    /**
     * @deprecated Use {@linkcode QldbDriver.executeLambda} instead to execute
     * transactions.
     *
     * Start a transaction using an available database session.
     * @returns Promise which fulfills with a transaction object.
     * @throws {@linkcode SessionClosedError} when the session is closed.
     */
    startTransaction: () => Promise<Transaction>;
}
