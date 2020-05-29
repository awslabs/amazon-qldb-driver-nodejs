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

// Test environment imports
import "mocha";

import { QLDBSession } from "aws-sdk";
import { ClientConfiguration } from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import { Agent } from "https";
import Semaphore from "semaphore-async-await";
import * as sinon from "sinon";

import { PooledQldbDriver } from "../PooledQldbDriver";
import { QldbDriver } from "../QldbDriver";
import { QldbSession } from "../QldbSession";
import { Result } from "../Result";
import { TransactionExecutor } from "../TransactionExecutor";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testDefaultTimeout: number = 30000;
const testDefaultRetryLimit: number = 4;
const testLedgerName: string = "LedgerName";
const testMaxRetries: number = 0;
const testMaxSockets: number = 10;

let pooledQldbDriver: PooledQldbDriver;
let testQldbLowLevelClient: QLDBSession;

const mockAgent: Agent = <Agent><any> sandbox.mock(Agent);
mockAgent.maxSockets = testMaxSockets;
const testLowLevelClientOptions: ClientConfiguration = {
    region: "fakeRegion",
    httpOptions: {
        agent: mockAgent
    }
};

const mockResult: Result = <Result><any> sandbox.mock(Result);
const mockQldbSession: QldbSession = <QldbSession><any> sandbox.mock(QLDBSession);
mockQldbSession.executeLambda = async () => {
    return mockResult;
}

mockQldbSession.close = () => {
    return;
}

describe("PooledQldbDriver", () => {
    beforeEach(() => {
        testQldbLowLevelClient = new QLDBSession(testLowLevelClientOptions);
        pooledQldbDriver = new PooledQldbDriver(testLedgerName, testLowLevelClientOptions);
    });

    afterEach(() => {
        mockAgent.maxSockets = testMaxSockets;
        sandbox.restore();
    });

    describe("#constructor()", () => {
        it("should have all attributes equal to mock values when constructor called", () => {
            chai.assert.equal(pooledQldbDriver["_ledgerName"], testLedgerName);
            chai.assert.equal(pooledQldbDriver["_retryLimit"], testDefaultRetryLimit);
            chai.assert.equal(pooledQldbDriver["_isClosed"], false);
            chai.assert.instanceOf(pooledQldbDriver["_qldbClient"], QLDBSession);
            chai.assert.equal(pooledQldbDriver["_qldbClient"].config.maxRetries, testMaxRetries);
            chai.assert.equal(pooledQldbDriver["_timeoutMillis"], testDefaultTimeout);
            chai.assert.equal(pooledQldbDriver["_poolLimit"], mockAgent.maxSockets);
            chai.assert.equal(pooledQldbDriver["_availablePermits"], mockAgent.maxSockets);
            chai.assert.deepEqual(pooledQldbDriver["_sessionPool"], []);
            chai.assert.instanceOf(pooledQldbDriver["_semaphore"], Semaphore);
            chai.assert.equal(pooledQldbDriver["_semaphore"]["permits"], mockAgent.maxSockets);
        });

        it("should throw a RangeError when timeOutMillis less than zero passed in", () => {
            const constructorFunction: () => void = () => {
                new PooledQldbDriver(testLedgerName, testLowLevelClientOptions, 4, 0, -1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });

        it("should throw a RangeError when retryLimit less than zero passed in", () => {
            const constructorFunction: () => void = () => {
                new PooledQldbDriver(testLedgerName, testLowLevelClientOptions, -1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });

        it("should throw a RangeError when poolLimit greater than maxSockets", () => {
            const constructorFunction: () => void  = () => {
                new PooledQldbDriver(testLedgerName, testLowLevelClientOptions, 4, testMaxSockets + 1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });

        it("should throw a RangeError when poolLimit less than zero", () => {
            const constructorFunction: () => void = () => {
                new PooledQldbDriver(testLedgerName, testLowLevelClientOptions, 4, -1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });

    });

    describe("#close", () => {
        it("should call close of super class", async () => {
            const qldbDriverCloseSpy = sandbox.spy(QldbDriver.prototype, "close");
            pooledQldbDriver.close();
            sinon.assert.calledOnce(qldbDriverCloseSpy);
        });
    });

    describe("#getSession", () => {
        it("should call getSession of super class", async () => {
            const qldbDriverGetSessionStub = sandbox.stub(QldbDriver.prototype, "getSession");
            qldbDriverGetSessionStub.returns(Promise.resolve(mockQldbSession));
            await pooledQldbDriver.getSession();
            sinon.assert.calledOnce(qldbDriverGetSessionStub);
        });
    });

    describe("#executeLambda", () => {
        it("should call executeLambda of super class", async () => {
            const qldbDriverGetSessionStub = sandbox.stub(QldbDriver.prototype, "executeLambda");
            qldbDriverGetSessionStub.returns(Promise.resolve(true));

            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };
            const retryIndicator = (retry: number) => {
                return;
            };
            await pooledQldbDriver.executeLambda(lambda, retryIndicator);
            sinon.assert.calledOnce(qldbDriverGetSessionStub);
        });
    });

    describe("#getTableNames", () => {
        it("should call getTableNames of super class", async () => {
            const qldbDriverGetTableNamesStub = sandbox.stub(QldbDriver.prototype, "getTableNames");
            qldbDriverGetTableNamesStub.returns(Promise.resolve(["some-table-name"]));
            await pooledQldbDriver.getTableNames();
            sinon.assert.calledOnce(qldbDriverGetTableNamesStub);
        });
    });
});
