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
import {
    AbortTransactionResult,
    ClientConfiguration,
    ExecuteStatementResult,
    PageToken,
    StartTransactionResult,
    ValueHolder
} from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";

import { Communicator } from "../Communicator";
import * as Errors from "../errors/Errors";
import * as LogUtil from "../LogUtil";
import { QldbSession } from "../QldbSession";
import { Result } from "../Result";
import { ResultReadable } from "../ResultReadable";
import { BackoffFunction } from "../retry/BackoffFunction";
import { defaultRetryConfig } from "../retry/DefaultRetryConfig";
import { Transaction } from "../Transaction";
import { TransactionExecutionContext } from "../TransactionExecutionContext";
import { AWSError } from "aws-sdk";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testRetryLimit: number = 4;
const testSessionToken: string = "sessionToken";
const testTransactionId: string = "txnId";
const testStartTransactionResult: StartTransactionResult = {
    TransactionId: testTransactionId
};
const testMessage: string = "foo";
const testStatement: string = "SELECT * FROM foo";
const testAbortTransactionResult: AbortTransactionResult = {};

const testValueHolder: ValueHolder[] = [{IonBinary: "{ hello:\"world\" }"}];
const testPageToken: PageToken = "foo";
const testExecuteStatementResult: ExecuteStatementResult = {
    FirstPage: {
        NextPageToken: testPageToken,
        Values: testValueHolder
    }
};
const mockLowLevelClientOptions: ClientConfiguration = {
    region: "fakeRegion"
};
const testQldbLowLevelClient: QLDBSession = new QLDBSession(mockLowLevelClientOptions);

const mockCommunicator: Communicator = <Communicator><any> sandbox.mock(Communicator);
const mockResult: Result = <Result><any> sandbox.mock(Result);
const mockTransaction: Transaction = <Transaction><any> sandbox.mock(Transaction);
mockTransaction.getTransactionId = () => {
    return "mockTransactionId";
};

const resultReadableObject: ResultReadable = new ResultReadable(testTransactionId, testExecuteStatementResult, mockCommunicator);
let qldbSession: QldbSession;
let executionContext: TransactionExecutionContext;

describe("QldbSession", () => {

    beforeEach(() => {
        qldbSession = new QldbSession(mockCommunicator);
        mockCommunicator.endSession = async () => {};
        mockCommunicator.getSessionToken = () => {
            return testSessionToken;
        };
        mockCommunicator.startTransaction = async () => {
            return testStartTransactionResult;
        };
        mockCommunicator.abortTransaction = async () => {
            return testAbortTransactionResult;
        };
        mockCommunicator.executeStatement = async () => {
            return testExecuteStatementResult;
        };
        mockCommunicator.getQldbClient = () => {
            return testQldbLowLevelClient;
        };
        executionContext = new TransactionExecutionContext();
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe("#constructor()", () => {
        it("should have all attributes equal to mock values when constructor called", () => {
            chai.assert.equal(qldbSession["_communicator"], mockCommunicator);
            chai.assert.equal(qldbSession["_isClosed"], false);
        });
    });

    describe("#endSession()", () => {
        it("should end qldbSession when called", async () => {
            const communicatorEndSpy = sandbox.spy(mockCommunicator, "endSession");
            await qldbSession.endSession();
            chai.assert.equal(qldbSession["_isClosed"], true);
            sinon.assert.calledOnce(communicatorEndSpy);
        });

        it("should be a no-op when already closed", async () => {
            const communicatorEndSpy = sandbox.spy(mockCommunicator, "endSession");
            qldbSession["_isClosed"] = true;
            await qldbSession.endSession();
            sinon.assert.notCalled(communicatorEndSpy);
        });
    });

    describe("#executeLambda()", () => {
        it("should return a Result object when called with execute as the lambda", async () => {
            qldbSession.startTransaction = async () => {
                return mockTransaction;
            };
            mockTransaction.execute = async () => {
                return mockResult;
            };
            mockTransaction.commit = async () => {};

            const executeSpy = sandbox.spy(mockTransaction, "execute");
            const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
            const commitSpy = sandbox.spy(mockTransaction, "commit");

            const result = await qldbSession.executeLambda(async (txn) => {
                return await txn.execute(testStatement);
            }, defaultRetryConfig, executionContext);
            sinon.assert.calledOnce(executeSpy);
            sinon.assert.calledWith(executeSpy, testStatement);
            sinon.assert.calledOnce(startTransactionSpy);
            sinon.assert.calledOnce(commitSpy);
            chai.assert.equal(result, mockResult);
        });

        it("should return a Result object when called with executeAndStreamResults as the lambda", async () => {
            const resultStub = sandbox.stub(Result, "bufferResultReadable");
            resultStub.returns(Promise.resolve(mockResult));

            qldbSession.startTransaction = async () => {
                return mockTransaction;
            };
            mockTransaction.executeAndStreamResults = async () => {
                return resultReadableObject;
            };
            mockTransaction.commit = async () => {};

            const executeAndStreamResultsSpy = sandbox.spy(mockTransaction, "executeAndStreamResults");
            const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
            const commitSpy = sandbox.spy(mockTransaction, "commit");

            const result = await qldbSession.executeLambda(async (txn) => {
                return await txn.executeAndStreamResults(testStatement);
            }, defaultRetryConfig, executionContext);
            sinon.assert.calledOnce(executeAndStreamResultsSpy);
            sinon.assert.calledWith(executeAndStreamResultsSpy, testStatement);
            sinon.assert.calledOnce(startTransactionSpy);
            sinon.assert.calledOnce(resultStub);
            sinon.assert.calledOnce(commitSpy);
            chai.assert.equal(result, mockResult);
        });

        it("should return a rejected promise when non-retryable error is thrown", async () => {
            qldbSession.startTransaction = async () => {
                throw new Error(testMessage);
            };

            const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
            const noThrowAbortSpy = sandbox.spy(qldbSession as any, "_noThrowAbort");

            await chai.expect(qldbSession.executeLambda(async (txn) => {
                return await txn.execute(testStatement);
            }, defaultRetryConfig, executionContext)).to.be.rejected;
            sinon.assert.calledOnce(startTransactionSpy);
        });

        it("should retry with same session when StartTransaction fails with BadRequestException", async () => {
            const isBadRequestStub = sandbox.stub(Errors, "isBadRequestException");
            isBadRequestStub.returns(true);

            mockCommunicator.startTransaction = async () => {
                throw new Error(testMessage);
            };

            const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
            const noThrowAbortSpy = sandbox.spy(qldbSession as any, "_noThrowAbort");
            await chai.expect(qldbSession.executeLambda(async (txn) => {
                return await txn.execute(testStatement);
            }, defaultRetryConfig, executionContext)).to.be.rejectedWith(Errors.StartTransactionError);
            sinon.assert.callCount(startTransactionSpy, testRetryLimit + 1);
            sinon.assert.callCount(noThrowAbortSpy, testRetryLimit + 1);
        });

        it("should not retry with same session when startTransaction fails with InvalidSessionException", async () => {
            const isInvalidSessionExceptionStub = sandbox.stub(Errors, "isInvalidSessionException");
            isInvalidSessionExceptionStub.returns(true);

            mockCommunicator.startTransaction = async () => {
                throw new Error(testMessage);
            };

            const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
            const noThrowAbortSpy = sandbox.spy(qldbSession as any, "_noThrowAbort");
            await chai.expect(qldbSession.executeLambda(async (txn) => {
                return await txn.execute(testStatement);
            }, defaultRetryConfig, executionContext)).to.be.rejected;
            sinon.assert.callCount(startTransactionSpy, 1);
            sinon.assert.neverCalledWith(noThrowAbortSpy);
        });

        it("should retry when OccConflictException occurs", async () => {
            const isOccStub = sandbox.stub(Errors, "isOccConflictException");
            isOccStub.returns(true);

            const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
            const noThrowAbortSpy = sandbox.spy(qldbSession as any, "_noThrowAbort");
            const logSpy = sandbox.spy(LogUtil, "warn");

            await chai.expect(qldbSession.executeLambda(async (txn) => {
                throw new Error(testMessage);
            }, defaultRetryConfig, executionContext)).to.be.rejected;

            sinon.assert.callCount(startTransactionSpy, testRetryLimit + 1);
            sinon.assert.neverCalledWith(noThrowAbortSpy, testRetryLimit + 1);
            sinon.assert.callCount(logSpy, testRetryLimit);
        });

        it("should retry when retriable exception occurs", async () => {
            const isRetriableStub = sandbox.stub(Errors, "isRetriableException");
            isRetriableStub.returns(true);

            const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
            const noThrowAbortSpy = sandbox.spy(qldbSession as any, "_noThrowAbort");
            const logSpy = sandbox.spy(LogUtil, "warn");

            await chai.expect(qldbSession.executeLambda(async (txn) => {
                throw new Error(testMessage);
            }, defaultRetryConfig, executionContext)).to.be.rejected;

            sinon.assert.callCount(startTransactionSpy, testRetryLimit + 1);
            sinon.assert.callCount(noThrowAbortSpy, testRetryLimit + 1);
            sinon.assert.callCount(logSpy, testRetryLimit);
        });

        it("should return a rejected promise with the exception when InvalidSessionException occurs", async () => {
            const isInvalidSessionStub = sandbox.stub(Errors, "isInvalidSessionException");
            isInvalidSessionStub.returns(true);

            await chai.expect(qldbSession.executeLambda(async (txn) => {
                throw new Error("ISE");
            }, defaultRetryConfig, executionContext)).to.be.rejected;

           chai.assert.isFalse(qldbSession.isSessionOpen());
        });

        it("should return a rejected promise when Transaction expires", async () => {
            const error = new Error("InvalidSession") as AWSError;
            error.code = "InvalidSessionException";
            error.message = "Transaction ABC has expired";
            const communicatorTransactionSpy = sandbox.spy(mockCommunicator, "startTransaction");

            let result = await chai.expect(qldbSession.executeLambda(async (txn) => {
                throw error;
            }, defaultRetryConfig, executionContext)).to.be.rejected;
            sinon.assert.calledOnce(communicatorTransactionSpy);
            chai.assert.equal(result.message, error.message);
        });

        it("should return a rejected promise when a LambdaAbortedError occurs", async () => {
            const lambdaAbortedError: Errors.LambdaAbortedError = new Errors.LambdaAbortedError();
            await chai.expect(qldbSession.executeLambda(async (txn) => {
                throw lambdaAbortedError;
            }, defaultRetryConfig, executionContext)).to.be.rejected;
        });

    });

    describe("#getSessionToken()", () => {
        it("should return the session token when called", () => {
            const communicatorTokenSpy = sandbox.spy(mockCommunicator, "getSessionToken");
            const sessionToken: string = qldbSession.getSessionToken();
            chai.assert.equal(sessionToken, testSessionToken);
            sinon.assert.calledOnce(communicatorTokenSpy);
        });
    });

    describe("#startTransaction()", () => {
        it("should return a Transaction object when called", async () => {
            const communicatorTransactionSpy = sandbox.spy(mockCommunicator, "startTransaction");
            const transaction = await qldbSession.startTransaction();
            chai.expect(transaction).to.be.an.instanceOf(Transaction);
            chai.assert.equal(transaction["_txnId"], testTransactionId);
            sinon.assert.calledOnce(communicatorTransactionSpy);
        });

    });

    describe("#_noThrowAbort()", () => {
        it("should call Transaction's abort() when Transaction is not null", async () => {
            mockTransaction.abort = async () => {};
            const communicatorAbortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
            const transactionAbortSpy = sandbox.spy(mockTransaction, "abort");
            await qldbSession["_noThrowAbort"](mockTransaction);
            sinon.assert.notCalled(communicatorAbortSpy);
            sinon.assert.calledOnce(transactionAbortSpy);
        });

        it("should log warning message when error is thrown", async () => {
            mockTransaction.abort = async () => {
                throw new Error(testMessage);
            };
            const logSpy = sandbox.spy(LogUtil, "warn");
            const communicatorAbortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
            const transactionAbortSpy = sandbox.spy(mockTransaction, "abort");
            await qldbSession["_noThrowAbort"](mockTransaction);
            sinon.assert.notCalled(communicatorAbortSpy);
            sinon.assert.calledOnce(transactionAbortSpy);
            sinon.assert.calledOnce(logSpy);
        });
    });

    describe("#RetryDelayTime()", () => {
        /* Test that the
            1. Default retry policy increases delay exponentially (when math.random is overriden to return 1)
            2. _sleep method gets called with the calculated delay time
        */
        it("should increase delay exponentially when called with DefaultRetryConfig", async () => {
            const sleepSpy = sandbox.stub(qldbSession as any ,"_sleep");
            const mathRandStub = sandbox.stub(Math, "random");
            mathRandStub.returns(1);
            let executionContext: TransactionExecutionContext = new TransactionExecutionContext();
            let defaultBackOffFunction: BackoffFunction = defaultRetryConfig.getBackoffFunction();

            //Increment the attempt number to 1 and determine what the delay time would be when using default retry policy
            executionContext.incrementExecutionAttempt();
            const delayTime1: number = defaultBackOffFunction(executionContext.getExecutionAttempt(), null, null);
            await qldbSession["_retrySleep"](executionContext, defaultRetryConfig, mockTransaction);
            //Verify sleep method was called with correct delay Time
            sleepSpy.calledWith(delayTime1);

            //Increment the attempt number to 2 and determine what the delay time would be when using default retry policy
            executionContext.incrementExecutionAttempt();
            const delayTime2: number = defaultBackOffFunction(executionContext.getExecutionAttempt(), null, null);
            await qldbSession["_retrySleep"](executionContext, defaultRetryConfig, mockTransaction);
            //Verify sleep method was called with correct delay Time
            sleepSpy.calledWith(delayTime2);

            //Increment the attempt number to 3 and determine what the delay time would be when using default retry policy
            executionContext.incrementExecutionAttempt();
            const delayTime3: number = defaultBackOffFunction(executionContext.getExecutionAttempt(), null, null);
            await qldbSession["_retrySleep"](executionContext, defaultRetryConfig, mockTransaction);
            //Verify sleep method was called with correct delay Time
            sleepSpy.calledWith(delayTime3);

            //Verify that delayTime3 = 2 * delayTime2 and delayTime2 = 2 * delayTime1
            chai.expect(delayTime2 - 1).to.equal((delayTime1 - 1) * 2);
            chai.expect(delayTime3 - 1).to.equal((delayTime1 - 1) * 4);
            chai.expect(delayTime3 - 1).to.equal((delayTime2 - 1) * 2);
        });
    });
});
