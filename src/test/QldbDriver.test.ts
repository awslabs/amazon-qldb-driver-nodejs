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

import { AWSError, QLDBSession } from "aws-sdk";
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
import { defaultRetryConfig } from "../retry/DefaultRetryConfig";
import { Result } from "../Result";
import { TransactionExecutor } from "../TransactionExecutor";
import { TransactionExecutionContext } from "../TransactionExecutionContext";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testDefaultRetryLimit: number = 4;
const testLedgerName: string = "LedgerName";
const testMaxRetries: number = 0;
const testMaxSockets: number = 10;
const mockSessionToken: string = "sessionToken1";
const testTableNames: string[] = ["Vehicle", "Person"];
const testSendCommandResult: SendCommandResult = {
    StartSession: {
        SessionToken: "sessionToken"
    }
};

let qldbDriver: QldbDriver;
let sendCommandStub;
let testQldbLowLevelClient: QLDBSession;
let executionContext: TransactionExecutionContext;

const mockAgent: Agent = <Agent><any> sandbox.mock(Agent);
mockAgent.maxSockets = testMaxSockets;
const testLowLevelClientOptions: ClientConfiguration = {
    region: "fakeRegion",
    httpOptions: {
        agent: mockAgent
    }
};

const mockResult: Result = <Result><any> sandbox.mock(Result);
const mockQldbSession: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
mockQldbSession.executeLambda = async () => {
    return mockResult;
}
mockQldbSession.endSession = () => {
    return;
}
mockQldbSession.getSessionToken = () => {
    return mockSessionToken;
}

mockQldbSession.isSessionOpen = () => {
    return true;
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
        executionContext = new TransactionExecutionContext();
    });

    afterEach(() => {
        mockAgent.maxSockets = testMaxSockets;
        sandbox.restore();
    });

    describe("#constructor()", () => {
        it("should have all attributes equal to mock values when constructor called", () => {
            chai.assert.equal(qldbDriver["_ledgerName"], testLedgerName);
            chai.assert.equal(qldbDriver["_isClosed"], false);
            chai.assert.instanceOf(qldbDriver["_qldbClient"], QLDBSession);
            chai.assert.equal(qldbDriver["_qldbClient"].config.maxRetries, testMaxRetries);
            chai.assert.equal(qldbDriver["_maxConcurrentTransactions"], mockAgent.maxSockets);
            chai.assert.equal(qldbDriver["_availablePermits"], mockAgent.maxSockets);
            chai.assert.deepEqual(qldbDriver["_sessionPool"], []);
            chai.assert.instanceOf(qldbDriver["_semaphore"], Semaphore);
            chai.assert.equal(qldbDriver["_semaphore"]["permits"], mockAgent.maxSockets);
            chai.assert.equal(qldbDriver["_retryConfig"], defaultRetryConfig);
            chai.assert.equal(qldbDriver["_retryConfig"]["_retryLimit"], testDefaultRetryLimit);
        });

        it("should throw a RangeError when retryLimit less than zero passed in", () => {
            const constructorFunction: () => void = () => {
                new QldbDriver(testLedgerName, testLowLevelClientOptions, -1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });

        it("should throw a RangeError when maxConcurrentTransactions greater than maxSockets", () => {
            const constructorFunction: () => void  = () => {
                new QldbDriver(testLedgerName, testLowLevelClientOptions, testMaxSockets + 1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });

        it("should throw a RangeError when maxConcurrentTransactions less than zero", () => {
            const constructorFunction: () => void = () => {
                new QldbDriver(testLedgerName, testLowLevelClientOptions, -1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });
    });

    describe("#close()", () => {
        it("should close qldbDriver and any session present in the pool when called", () => {
            const mockSession1: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
            const mockSession2: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
            mockSession1.endSession = () => {};
            mockSession2.endSession = () => {};

            const close1Spy = sandbox.spy(mockSession1, "endSession");
            const close2Spy = sandbox.spy(mockSession2, "endSession");

            qldbDriver["_sessionPool"] = [mockSession1, mockSession2];
            qldbDriver.close();

            sinon.assert.calledOnce(close1Spy);
            sinon.assert.calledOnce(close2Spy);
            chai.assert.equal(qldbDriver["_isClosed"], true);
        });
    });

    describe("#executeLambda()", () => {
        it("should start a session and return the delegated call to the session", async () => {
            qldbDriver["_sessionPool"] = [mockQldbSession];
            const semaphoreStub = sandbox.stub(qldbDriver["_semaphore"], "tryAcquire");
            semaphoreStub.returns(true);

            const executeLambdaSpy = sandbox.spy(mockQldbSession, "executeLambda");
            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };

            const result = await qldbDriver.executeLambda(lambda, defaultRetryConfig);

            chai.assert.equal(result, mockResult);
            sinon.assert.calledOnce(executeLambdaSpy);
            sinon.assert.calledWith(executeLambdaSpy, lambda, defaultRetryConfig, executionContext);
        });

        /**
         * This test covers the following rules:
         *   1) If the permit is available, and there is/are session(s) in the pool, then return the last session from the pool
         *   2) If the session throws InvalidSessionException, then do not return that session back to the pool. Also,
         *   the driver will proceed with the next available session in the pool.
         *   3) If the session is good, then return it to the pool.
         */
        it("should pick next session from the pool when the current session throws InvalidSessionException", async() => {
            const mockSession1: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
            const mockSession2: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);

            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };

            mockSession1.executeLambda = async () => {
                const error = new Error("InvalidSession") as AWSError;
                error.code = "InvalidSessionException";
                throw error;
            };
            mockSession1["_isClosed"] = true;

            mockSession1.getSessionToken = () => {
                return "sessionToken1";
            }

            mockSession1.isSessionOpen = () => {
                return false;
            }

            mockSession2.executeLambda = async () => {
                return true;
            };

            mockSession2.getSessionToken = () => {
                return "sessionToken2";
            }

            mockSession2.isSessionOpen = () => {
                return true;
            }

            qldbDriver["_sessionPool"] = [mockSession2, mockSession1];
            const semaphoreStub = sandbox.stub(qldbDriver["_semaphore"], "tryAcquire");
            semaphoreStub.returns(true);

            let initialPermits = qldbDriver["_availablePermits"];
            const result = await qldbDriver.executeLambda(lambda, defaultRetryConfig);

            //Ensure that the transaction was eventually completed
            chai.assert.isTrue(result);
            //Ensure that the mockSession1 is not returned back to the pool since it threw ISE. Only mockSession2 should be present
            chai.assert.equal(qldbDriver["_sessionPool"].length, 1);
            chai.assert.equal(qldbDriver["_sessionPool"][0].getSessionToken(), mockSession2.getSessionToken());
            // Ensure that although mockSession1 is not returned to the pool the total number of permits are same before beginning
            // the transaction
            chai.assert.equal(qldbDriver["_availablePermits"], initialPermits);
        });

        it("should throw Error, without retrying, when Transaction expires", async () => {
            const mockSession1: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
            const mockSession2: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };
            const error = new Error("InvalidSession") as AWSError;
            error.code = "InvalidSessionException";
            error.message = "Transaction ABC has expired";

            mockSession1.executeLambda = async () => {
                throw error;
            };
            mockSession1["_isClosed"] = true;

            mockSession1.getSessionToken = () => {
                return "sessionToken1";
            }

            mockSession1.isSessionOpen = () => {
                return false;
            }

            mockSession2.executeLambda = async () => {
                // This should never be called
                return true;
            };

            qldbDriver["_sessionPool"] = [mockSession2, mockSession1];
            const executeLambdaSpy1 = sandbox.spy(mockSession1, "executeLambda");
            const executeLambdaSpy2 = sandbox.spy(mockSession2, "executeLambda");
            const result = await chai.expect(qldbDriver.executeLambda(lambda, defaultRetryConfig)).to.be.rejected;
            chai.assert.equal(result.code, error.code);

            sinon.assert.calledOnce(executeLambdaSpy1);
            sinon.assert.calledWith(executeLambdaSpy1, lambda, defaultRetryConfig, executionContext);

            sinon.assert.notCalled(executeLambdaSpy2);
        });

        it("should retry only up to maxConcurrentTransactions + 3 times when there is ISE", async () => {
            const mockSession1: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
            mockSession1.executeLambda = async () => {
                const error = new Error("InvalidSession") as AWSError;
                error.code = "InvalidSessionException";
                throw error;
            };
            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };

            const error = new Error("InvalidSession") as AWSError;
            error.code = "InvalidSessionException";
            mockSession1.executeLambda = async () => {
                throw error;
            };
            mockSession1["_isClosed"] = true;

            mockSession1.getSessionToken = () => {
                return "sessionToken1";
            }

            mockSession1.isSessionOpen = () => {
                return false;
            }

            //The driver will check the maxConcurrentTransactions + 3, regardless the size of _sessionPool
            qldbDriver["_maxConcurrentTransactions"] = 1;
            qldbDriver["_sessionPool"] = [mockSession1, mockSession1, mockSession1, mockSession1, mockSession1];
            const executeLambdaSpy1 = sandbox.spy(mockSession1, "executeLambda");
            const result = await chai.expect(qldbDriver.executeLambda(lambda, defaultRetryConfig)).to.be.rejected;
            chai.assert.equal(result.code, error.code);
            sinon.assert.callCount(executeLambdaSpy1, qldbDriver["_maxConcurrentTransactions"] + 3);
        });

        it("should throw DriverClosedError wrapped in a rejected promise when closed", async () => {
            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };

            qldbDriver["_isClosed"] = true;
            chai.expect(qldbDriver.executeLambda(lambda, defaultRetryConfig)).to.be.rejectedWith(DriverClosedError);
        });

        it("should return a SessionPoolEmptyError wrapped in a rejected promise when session pool empty", async () => {
            const semaphoreStub = sandbox.stub(qldbDriver["_semaphore"], "tryAcquire");
            semaphoreStub.returns(false);

            const lambda = (transactionExecutor: TransactionExecutor) => {
                return true;
            };

            const result = await chai.expect(qldbDriver.executeLambda(lambda)).to.be.rejected;
            chai.assert.equal(result.name, SessionPoolEmptyError.name);
        });
    });


    describe("#releaseSession()", () => {
        it("should return a session back to the session pool when called", () => {
            const logDebugSpy = sandbox.spy(LogUtil, "debug");
            const semaphoreReleaseSpy = sandbox.spy(qldbDriver["_semaphore"], "release")
            qldbDriver["_returnSessionToPool"](mockQldbSession);

            chai.assert.deepEqual(qldbDriver["_sessionPool"], [mockQldbSession])
            chai.assert.deepEqual(qldbDriver["_availablePermits"], testMaxSockets + 1)

            sinon.assert.calledOnce(logDebugSpy);
            sinon.assert.calledOnce(semaphoreReleaseSpy);
        });

        it("should NOT return a closed session back to the pool but should release the permit", () => {
            const semaphoreReleaseSpy = sandbox.spy(qldbDriver["_semaphore"], "release");
            let initalPermits = qldbDriver["_availablePermits"];

            mockQldbSession.isSessionOpen = () => {
                return false;
            };

            qldbDriver["_returnSessionToPool"](mockQldbSession);
            //Since the session was not open, it won't be returneed to the pool
            chai.assert.deepEqual(qldbDriver["_sessionPool"], []);
            //The permit is released even if session is not returned to the pool
            chai.assert.deepEqual(qldbDriver["_availablePermits"], initalPermits + 1);
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
            chai.expect(qldbDriver.getTableNames()).to.be.rejectedWith(DriverClosedError);
        });
    });
});
