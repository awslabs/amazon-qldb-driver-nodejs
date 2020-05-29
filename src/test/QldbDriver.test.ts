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
import { ClientConfiguration, SendCommandResult } from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import { Agent } from "https";
import Semaphore from "semaphore-async-await";
import * as sinon from "sinon";

import { DriverClosedError, SessionPoolEmptyError } from "../errors/Errors";
import * as LogUtil from "../LogUtil";
import { QldbDriver } from "../QldbDriver";
import { QldbSession } from "../QldbSession";
import { QldbSessionImpl } from "../QldbSessionImpl";
import { Result } from "../Result";
import { TransactionExecutor } from "../TransactionExecutor";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testDefaultTimeout: number = 30000;
const testDefaultRetryLimit: number = 4;
const testLedgerName: string = "LedgerName";
const testMaxRetries: number = 0;
const testMaxSockets: number = 10;
const testMessage: string = "testMessage";
const testTableNames: string[] = ["Vehicle", "Person"];
const testSendCommandResult: SendCommandResult = {
    StartSession: {
        SessionToken: "sessionToken"
    }
};

let qldbDriver: QldbDriver;
let sendCommandStub;
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

describe("QldbDriver", () => {
    beforeEach(() => {
        testQldbLowLevelClient = new QLDBSession(testLowLevelClientOptions);
        sendCommandStub = sandbox.stub(testQldbLowLevelClient, "sendCommand");
        sendCommandStub.returns({
            promise: () => {
                return testSendCommandResult;
            }
        });

        qldbDriver = new QldbDriver(testLedgerName, testLowLevelClientOptions);
    });

    afterEach(() => {
        mockAgent.maxSockets = testMaxSockets;
        sandbox.restore();
    });

    describe("#constructor()", () => {
        it("should have all attributes equal to mock values when constructor called", () => {
            chai.assert.equal(qldbDriver["_ledgerName"], testLedgerName);
            chai.assert.equal(qldbDriver["_retryLimit"], testDefaultRetryLimit);
            chai.assert.equal(qldbDriver["_isClosed"], false);
            chai.assert.instanceOf(qldbDriver["_qldbClient"], QLDBSession);
            chai.assert.equal(qldbDriver["_qldbClient"].config.maxRetries, testMaxRetries);
            chai.assert.equal(qldbDriver["_timeoutMillis"], testDefaultTimeout);
            chai.assert.equal(qldbDriver["_poolLimit"], mockAgent.maxSockets);
            chai.assert.equal(qldbDriver["_availablePermits"], mockAgent.maxSockets);
            chai.assert.deepEqual(qldbDriver["_sessionPool"], []);
            chai.assert.instanceOf(qldbDriver["_semaphore"], Semaphore);
            chai.assert.equal(qldbDriver["_semaphore"]["permits"], mockAgent.maxSockets);
        });

        it("should throw a RangeError when timeOutMillis less than zero passed in", () => {
            const constructorFunction: () => void = () => {
                new QldbDriver(testLedgerName, testLowLevelClientOptions, 4, 0, -1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });

        it("should throw a RangeError when retryLimit less than zero passed in", () => {
            const constructorFunction: () => void = () => {
                new QldbDriver(testLedgerName, testLowLevelClientOptions, -1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });

        it("should throw a RangeError when poolLimit greater than maxSockets", () => {
            const constructorFunction: () => void  = () => {
                new QldbDriver(testLedgerName, testLowLevelClientOptions, 4, testMaxSockets + 1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });

        it("should throw a RangeError when poolLimit less than zero", () => {
            const constructorFunction: () => void = () => {
                new QldbDriver(testLedgerName, testLowLevelClientOptions, 4, -1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });
    });

    describe("#close()", () => {
        it("should close qldbDriver and any session present in the pool when called", () => {
            const mockSession1: QldbSessionImpl = <QldbSessionImpl><any> sandbox.mock(QldbSessionImpl);
            const mockSession2: QldbSessionImpl = <QldbSessionImpl><any> sandbox.mock(QldbSessionImpl);
            mockSession1.close = () => {};
            mockSession2.close = () => {};

            const close1Spy = sandbox.spy(mockSession1, "close");
            const close2Spy = sandbox.spy(mockSession2, "close");

            qldbDriver["_sessionPool"] = [mockSession1, mockSession2];
            qldbDriver.close();

            sinon.assert.calledOnce(close1Spy);
            sinon.assert.calledOnce(close2Spy);
            chai.assert.equal(qldbDriver["_isClosed"], true);
        });
    });

    describe("#executeLambda()", () => {
        it("should start a session and return the delegated call to the session", async () => {
            const getSessionStub = sandbox.stub(qldbDriver, "getSession");
            getSessionStub.returns(Promise.resolve(mockQldbSession));
            const executeLambdaSpy = sandbox.spy(mockQldbSession, "executeLambda");
            const closeSessionSpy = sandbox.spy(mockQldbSession, "close");
            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };
            const retryIndicator = (retry: number) => {
                return;
            };
            const result = await qldbDriver.executeLambda(lambda, retryIndicator);

            chai.assert.equal(result, mockResult);
            sinon.assert.calledOnce(executeLambdaSpy);
            sinon.assert.calledWith(executeLambdaSpy, lambda, retryIndicator);
            sinon.assert.calledOnce(closeSessionSpy);
        });

        it("should throw DriverClosedError wrapped in a rejected promise when closed", async () => {
            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };
            const retryIndicator = (retry: number) => {
                return;
            };

            qldbDriver["_isClosed"] = true;
            const error = await chai.expect(qldbDriver.executeLambda(lambda, retryIndicator)).to.be.rejected;
            chai.assert.instanceOf(error, DriverClosedError);
        });
    });

    describe("#getSession()", () => {
        it("should return a DriverClosedError wrapped in a rejected promise when closed", async () => {
            qldbDriver["_isClosed"] = true;
            const error = await chai.expect(qldbDriver.getSession()).to.be.rejected;
            chai.assert.instanceOf(error, DriverClosedError);
        });

        it("should return a new session when called", async () => {
            qldbDriver["_qldbClient"] = testQldbLowLevelClient;

            const semaphoreStub = sandbox.stub(qldbDriver["_semaphore"], "waitFor");
            semaphoreStub.returns(Promise.resolve(true));

            const logDebugSpy = sandbox.spy(LogUtil, "debug");

            const pooledQldbSession: QldbSession = await qldbDriver.getSession();

            sinon.assert.calledOnce(semaphoreStub);
            sinon.assert.calledThrice(logDebugSpy);

            chai.assert.instanceOf(pooledQldbSession["_session"], QldbSessionImpl);
            chai.assert.equal(pooledQldbSession["_returnSessionToPool"], qldbDriver["_returnSessionToPool"]);
            chai.assert.equal(qldbDriver["_availablePermits"], testMaxSockets - 1);

        });

        it("should return the existing session already present in the session pool when called", async () => {
            const mockSession: QldbSessionImpl = <QldbSessionImpl><any> sandbox.mock(QldbSessionImpl);
            mockSession["_abortOrClose"] = async () => {
                return true;
            };

            qldbDriver["_sessionPool"] = [mockSession];
            qldbDriver["_qldbClient"] = testQldbLowLevelClient;

            const logDebugSpy = sandbox.spy(LogUtil, "debug");
            const abortOrCloseSpy = sandbox.spy(mockSession as any, "_abortOrClose");

            const semaphoreStub = sandbox.stub(qldbDriver["_semaphore"], "waitFor");
            semaphoreStub.returns(Promise.resolve(true));

            const pooledQldbSession: QldbSession = await qldbDriver.getSession();

            sinon.assert.calledTwice(logDebugSpy);
            sinon.assert.calledOnce(abortOrCloseSpy);

            chai.assert.equal(pooledQldbSession["_session"], mockSession);
            chai.assert.equal(pooledQldbSession["_returnSessionToPool"], qldbDriver["_returnSessionToPool"]);
            chai.assert.deepEqual(qldbDriver["_sessionPool"], []);
            chai.assert.equal(qldbDriver["_availablePermits"], testMaxSockets - 1);
        });

        it("should return a rejected promise when error is thrown", async () => {
            let error:Error = new Error("popping from pool failed");
            const sessionPoolStub = sandbox.stub(qldbDriver["_sessionPool"], "pop");
            sessionPoolStub.throws(error);

            const semaphoreReleaseSpy = sandbox.spy(qldbDriver["_semaphore"], "release");

            await chai.expect(qldbDriver.getSession()).to.be.rejected;
            chai.assert.equal(qldbDriver["_availablePermits"], testMaxSockets);
            sinon.assert.calledOnce(semaphoreReleaseSpy);
        });

        it("should return a SessionPoolEmptyError wrapped in a rejected promise when session pool empty", async () => {
            const semaphoreStub = sandbox.stub(qldbDriver["_semaphore"], "waitFor");
            semaphoreStub.returns(Promise.resolve(false));

            const error = await chai.expect(qldbDriver.getSession()).to.be.rejected;
            chai.assert.instanceOf(error, SessionPoolEmptyError);
        });
    });

    describe("#releaseSession()", () => {
        it("should return a session back to the session pool when called", () => {
            const logDebugSpy = sandbox.spy(LogUtil, "debug");
            const semaphoreReleaseSpy = sandbox.spy(qldbDriver["_semaphore"], "release")
            const mockSession: QldbSessionImpl = <QldbSessionImpl><any> sandbox.mock(QldbSessionImpl);

            qldbDriver["_returnSessionToPool"](mockSession);

            chai.assert.deepEqual(qldbDriver["_sessionPool"], [mockSession])
            chai.assert.deepEqual(qldbDriver["_availablePermits"], testMaxSockets + 1)

            sinon.assert.calledOnce(logDebugSpy);
            sinon.assert.calledOnce(semaphoreReleaseSpy);
        });
    });
    describe("#getTableNames()", () => {
        it("should return a list of table names when called", async () => {
            const executeStub = sandbox.stub(qldbDriver, "executeLambda");
            executeStub.returns(Promise.resolve(testTableNames));
            const listOfTableNames: string[] = await qldbDriver.getTableNames();
            chai.assert.equal(listOfTableNames.length, testTableNames.length);
            chai.assert.equal(listOfTableNames, testTableNames);
        });

        it("should return a DriverClosedError wrapped in a rejected promise when closed", async () => {
            qldbDriver["_isClosed"] = true;
            const error = await chai.expect(qldbDriver.getTableNames()).to.be.rejected;
            chai.assert.instanceOf(error, DriverClosedError);
        });
    });
});
