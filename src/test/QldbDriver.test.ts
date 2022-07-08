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

// Test environment imports
import "mocha";

import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import { Agent } from "https";
import Semaphore from "semaphore-async-await";
import * as sinon from "sinon";

import { DriverClosedError, ExecuteError, SessionPoolEmptyError } from "../errors/Errors";
import { QldbDriver } from "../QldbDriver";
import { QldbSession } from "../QldbSession";
import { defaultRetryConfig } from "../retry/DefaultRetryConfig";
import { Result } from "../Result";
import { TransactionExecutor } from "../TransactionExecutor";
import { RetryConfig } from "../retry/RetryConfig";
import { InvalidSessionException, OccConflictException, QLDBSession, QLDBSessionClientConfig, SendCommandResult } from "@aws-sdk/client-qldb-session";
import { NodeHttpHandlerOptions } from "@aws-sdk/node-http-handler";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testDefaultRetryLimit: number = 4;
const testLedgerName: string = "LedgerName";
const testMaxRetries: number = 0;
const testMaxSockets: number = 10;
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
const testLowLevelClientOptions: QLDBSessionClientConfig = {
    region: "fakeRegion"
};
const testLowLevelClientHttpOptions: NodeHttpHandlerOptions = {
    httpAgent: mockAgent
};

const mockResult: Result = <Result><any> sandbox.mock(Result);
const mockQldbSession: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
mockQldbSession.executeLambda = async function<Result>(txnLambda: (txn: TransactionExecutor) => Promise<Result>): Promise<Result> {
    return <Result><any> mockResult;
}
mockQldbSession.endSession = async function(): Promise<void> {
    return;
}
mockQldbSession.isAlive = () => true;

describe("QldbDriver", () => {
    beforeEach(() => {
        testQldbLowLevelClient = new QLDBSession(testLowLevelClientOptions);
        sendCommandStub = sandbox.stub(testQldbLowLevelClient, "send");
        sendCommandStub.resolves(testSendCommandResult);

        qldbDriver = new QldbDriver(testLedgerName, testLowLevelClientOptions, testLowLevelClientHttpOptions);
    });

    afterEach(() => {
        mockAgent.maxSockets = testMaxSockets;
        sandbox.restore();
    });

    describe("#constructor()", () => {
        it("should have all attributes equal to mock values when constructor called", async () => {
            chai.assert.equal(qldbDriver["_ledgerName"], testLedgerName);
            chai.assert.equal(qldbDriver["_isClosed"], false);
            chai.assert.instanceOf(qldbDriver["_qldbClient"], QLDBSession);
            chai.assert.equal(await qldbDriver["_qldbClient"].config.maxAttempts(), testMaxRetries);
            chai.assert.equal(qldbDriver["_maxConcurrentTransactions"], mockAgent.maxSockets);
            chai.assert.deepEqual(qldbDriver["_sessionPool"], []);
            chai.assert.instanceOf(qldbDriver["_semaphore"], Semaphore);
            chai.assert.equal(qldbDriver["_semaphore"]["permits"], mockAgent.maxSockets);
            chai.assert.equal(qldbDriver["_retryConfig"], defaultRetryConfig);
            chai.assert.equal(qldbDriver["_retryConfig"]["_retryLimit"], testDefaultRetryLimit);
        });

        it("should throw a RangeError when retryLimit less than zero passed in", () => {
            const constructorFunction: () => void = () => {
                new QldbDriver(testLedgerName, testLowLevelClientOptions, testLowLevelClientHttpOptions, -1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });

        it("should throw a RangeError when maxConcurrentTransactions greater than maxSockets", () => {
            const constructorFunction: () => void  = () => {
                new QldbDriver(testLedgerName, testLowLevelClientOptions, testLowLevelClientHttpOptions, testMaxSockets + 1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });

        it("should throw a RangeError when maxConcurrentTransactions less than zero", () => {
            const constructorFunction: () => void = () => {
                new QldbDriver(testLedgerName, testLowLevelClientOptions, testLowLevelClientHttpOptions, -1);
            };
            chai.assert.throws(constructorFunction, RangeError);
        });
    });

    describe("#close()", () => {
        it("should close qldbDriver and any session present in the pool when called", () => {
            const mockSession1: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
            const mockSession2: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
            mockSession1.endSession = async function() {};
            mockSession2.endSession = async function() {};

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

            const executeStub = sandbox.stub(mockQldbSession, "executeLambda");
            executeStub.returns(Promise.resolve(mockResult));
            const lambda = async (transactionExecutor: TransactionExecutor) => {
                return <Result><any>true;
            };

            const result = await qldbDriver.executeLambda(lambda, defaultRetryConfig);

            chai.assert.equal(result, mockResult);
            sinon.assert.calledOnce(executeStub);
            sinon.assert.calledWith(executeStub, lambda);
        });

        it("should throw Error, without retrying, when Transaction expires", async () => {
            const mockSession1: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
            const mockSession2: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
            mockSession1.isAlive = () => true;
            mockSession2.isAlive = () => true;
            const lambda = async (transactionExecutor: TransactionExecutor) => {
                return true;
            };
            const error = new InvalidSessionException({ $metadata: {}});
            error.message = "Transaction ABC has expired";

            mockSession1.executeLambda = async () => {
                throw new ExecuteError(error, false, true);
            };

            mockSession2.executeLambda = async function<Type>(txnLambda: (txn: TransactionExecutor) => Promise<Type>): Promise<Type> {
                // This should never be called
                return;
            };

            qldbDriver["_sessionPool"] = [mockSession2, mockSession1];
            const executeLambdaSpy1 = sandbox.spy(mockSession1, "executeLambda");
            const executeLambdaSpy2 = sandbox.spy(mockSession2, "executeLambda");
            const result = await chai.expect(qldbDriver.executeLambda(lambda, defaultRetryConfig)).to.be.rejected;
            chai.assert.equal(result.name, error.name);

            sinon.assert.calledOnce(executeLambdaSpy1);
            sinon.assert.calledWith(executeLambdaSpy1, lambda);

            sinon.assert.notCalled(executeLambdaSpy2);
        });

        it("should retry only up to retry limit times when there is retryable error", async () => {
            const mockSession: QldbSession = <QldbSession><any> sandbox.mock(QldbSession);
            const errorCode: string = "OccConflictException";
            mockSession.executeLambda = async () => {
                const error = new OccConflictException({ $metadata: {}});
                throw new ExecuteError(error, true, false);
            };
            const executeLambdaSpy = sandbox.spy(mockSession, "executeLambda");
            const lambda = async (transactionExecutor: TransactionExecutor) => {
                return true;
            };
            mockSession.isAlive = () => true;

            const retryConfig: RetryConfig = new RetryConfig(2)
            qldbDriver["_sessionPool"] = [mockSession];
            const result = await chai.expect(qldbDriver.executeLambda(lambda, retryConfig)).to.be.rejected;
            chai.assert.equal(result.name, errorCode);
            sinon.assert.callCount(executeLambdaSpy, retryConfig.getRetryLimit() + 1);
        });

        it("should throw DriverClosedError wrapped in a rejected promise when closed", async () => {
            const lambda = async (transactionExecutor: TransactionExecutor) => {
                return true;
            };

            qldbDriver["_isClosed"] = true;
            chai.expect(qldbDriver.executeLambda(lambda, defaultRetryConfig)).to.be.rejectedWith(DriverClosedError);
        });

        it("should return a SessionPoolEmptyError wrapped in a rejected promise when session pool empty", async () => {
            const semaphoreStub = sandbox.stub(qldbDriver["_semaphore"], "tryAcquire");
            semaphoreStub.returns(false);

            const lambda = async (transactionExecutor: TransactionExecutor) => {
                return true;
            };

            const result = await chai.expect(qldbDriver.executeLambda(lambda)).to.be.rejected;
            chai.assert.equal(result.name, SessionPoolEmptyError.name);
        });

        it("should not increment semaphore permit count if called when pool empty", async () => {
            const onePermitDriver = new QldbDriver(testLedgerName, testLowLevelClientOptions, testLowLevelClientHttpOptions, 1);
            onePermitDriver["_sessionPool"] = [mockQldbSession];
            const executeStub = sandbox.stub(mockQldbSession, "executeLambda");
            executeStub.returns(new Promise(resolve => setTimeout(resolve, 10)));

            let promise1 = onePermitDriver.executeLambda(async (txn) => {
                return true;
            });
            let promise2 = onePermitDriver.executeLambda(async (txn) => {
                return true;
            });

            // Two concurrent transactions will fail due to session pool being empty
            promise1.catch((e) => {
                chai.assert.fail(e);
            });
            promise2.catch((e) => {
            });
            await promise1;
            let result = await chai.expect(promise2).to.be.rejected;
            chai.assert.equal(result.name, SessionPoolEmptyError.name);

            promise1 = onePermitDriver.executeLambda(async (txn) => {
                return true;
            });
            promise2 = onePermitDriver.executeLambda(async (txn) => {
                return true;
            });
            // If permit leaked, this will succeed since now there's two permits
            promise1.catch((e) => {
                chai.assert.fail(e);
            });
            promise2.catch((e) => {
            });
            await promise1;
            result = await chai.expect(promise2).to.be.rejected;
            chai.assert.equal(result.name, SessionPoolEmptyError.name);
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
