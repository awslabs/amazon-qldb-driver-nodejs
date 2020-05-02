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

import { AWSError } from "aws-sdk";
import { ClientConfiguration } from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";

import { SessionPoolEmptyError, DriverClosedError } from "../errors/Errors";
import { PooledQldbDriver } from "../PooledQldbDriver";
import { TransactionExecutor } from "../TransactionExecutor";
import * as constants from "./TestConstants";
import { TestUtils } from "./TestUtils";

chai.use(chaiAsPromised);

describe("SessionManagement", function() {
    this.timeout(0);
    let testUtils: TestUtils;
    let config: ClientConfiguration; 

    before(async () => {
        testUtils = new TestUtils(constants.LEDGER_NAME);
        config = testUtils.createClientConfiguration();

        await testUtils.runForceDeleteLedger();
        await testUtils.runCreateLedger();
    });

    after(async () => {
        await testUtils.runDeleteLedger();
    });

    it("Throws excepton when connecting to a non-existent ledger", async () => {
        let driver: PooledQldbDriver = new PooledQldbDriver("NonExistentLedger", config);
        let error: AWSError;
        try {
            error = await chai.expect(driver.executeLambda(async (txn: TransactionExecutor) => {
                await txn.execute("SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'");
            })).to.be.rejected;
        } finally {
            chai.assert.equal(error.code, "BadRequestException");
            driver.close();
        }
    });

    it("Can get a session when the pool has no sessions and hasn't hit the limit", async () => {
        // Start a pooled driver with default pool limit so it doesn't have sessions in the pool
        // and has not hit the limit
        let driver: PooledQldbDriver = new PooledQldbDriver(constants.LEDGER_NAME, config);
        try {
            // Execute a statement to implicitly create a session and return it to the pool
            await driver.executeLambda(async (txn: TransactionExecutor) => {
                await txn.execute("SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'");
            });
        } finally {
            driver.close();
        }
    });

    it("Can get a session when the pool has a session and hasn't hit the limit", async () => {
        // Start a pooled driver with default pool limit so it doesn't have sessions in the pool
        // and has not hit the limit
        const driver: PooledQldbDriver = new PooledQldbDriver(constants.LEDGER_NAME, config);
        try {
            // Execute a statement to implicitly create a session and return it to the pool
            await driver.executeLambda(async (txn: TransactionExecutor) => {
                await txn.execute("SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'");
            });
            // Execute a statement again to implcitly used the session in the pool and
            await driver.executeLambda(async (txn: TransactionExecutor) => {
                await txn.execute("SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'");
            });
        } finally {
            driver.close();
        }
    });

    it("Throws exception when the pool has no sessions and has hit the limit exceeding timeout", async () => {
        // Set the timeout to 1ms and pool limit to 1
        const driver: PooledQldbDriver = new  PooledQldbDriver(constants.LEDGER_NAME, config, undefined, 1, 1);
        try {
            // Execute and do not wait for the promise to resolve, exhausting the pool
            driver.executeLambda((txn: TransactionExecutor) => {
                txn.execute("SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'");
            });
            // Attempt to implicitly get a session by executing
            await driver.executeLambda(async (txn: TransactionExecutor) => {
                await txn.execute("SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'");
            });
            chai.assert.fail("SessionPoolEmptyError was not thrown")
        } catch (e) {
            if (!(e instanceof SessionPoolEmptyError)) {
                throw e;
            }
        } finally {
            driver.close();
        }
    });

    it("Can get a session when the pool has no sessions and has hit the limit within timeout", async () => {
        // Set the timeout to 30000ms and pool limit to 1
        const driver: PooledQldbDriver = new  PooledQldbDriver(constants.LEDGER_NAME, config, undefined, 1, 30000);
        try {
            // Execute and do not wait for the promise to resolve, exhausting the pool
            driver.executeLambda((txn: TransactionExecutor) => {
                txn.execute("SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'");
            });
            // Attempt to implicitly get a session by executing, waiting for up to 30000ms
            await driver.executeLambda(async (txn: TransactionExecutor) => {
                await txn.execute("SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'");
            });
        } finally {
            driver.close();
        }
    });

    it("Throws exception when the driver has been closed", async () => {
        const driver: PooledQldbDriver = new PooledQldbDriver(constants.LEDGER_NAME, config);
        driver.close();
        try {
            await driver.executeLambda((txn: TransactionExecutor) => {
                txn.execute("SELECT name FROM information_schema.user_tables WHERE status = 'ACTIVE'");
            });
        } catch (e) {
            if (!(e instanceof DriverClosedError)) {
                throw e;
            }
        }
    });
});
