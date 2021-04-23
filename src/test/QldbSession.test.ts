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
import { Transaction } from "../Transaction";
import { AWSError } from "aws-sdk";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

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

describe("QldbSession", () => {

    beforeEach(() => {
        qldbSession = new QldbSession(mockCommunicator);
        mockCommunicator.endSession = async () => {
            return null;
        };
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
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe("#constructor()", () => {
        it("should have all attributes equal to mock values when constructor called", () => {
            chai.assert.equal(qldbSession["_communicator"], mockCommunicator);
            chai.assert.equal(qldbSession["_isAlive"], true);
        });
    });

    describe("#endSession()", () => {
        it("should end qldbSession when called", async () => {
            const communicatorEndSpy = sandbox.spy(mockCommunicator, "endSession");
            await qldbSession.endSession();
            chai.assert.equal(qldbSession["_isAlive"], false);
            sinon.assert.calledOnce(communicatorEndSpy);
        });
    });

    describe("#executeLambda()", () => {
        it("should return a Result object when called with execute as the lambda", async () => {
            qldbSession._startTransaction = async () => {
                return mockTransaction;
            };
            mockTransaction.execute = async () => {
                return mockResult;
            };
            mockTransaction.commit = async () => {};

            const executeSpy = sandbox.spy(mockTransaction, "execute");
            const startTransactionSpy = sandbox.spy(qldbSession, "_startTransaction");
            const commitSpy = sandbox.spy(mockTransaction, "commit");

            const result = await qldbSession.executeLambda(async (txn) => {
                return await txn.execute(testStatement);
            });
            sinon.assert.calledOnce(executeSpy);
            sinon.assert.calledWith(executeSpy, testStatement);
            sinon.assert.calledOnce(startTransactionSpy);
            sinon.assert.calledOnce(commitSpy);
            chai.assert.equal(result, mockResult);
        });

        it("should return a Result object when called with executeAndStreamResults as the lambda", async () => {
            const resultStub = sandbox.stub(Result, "bufferResultReadable");
            resultStub.returns(Promise.resolve(mockResult));

            qldbSession._startTransaction = async () => {
                return mockTransaction;
            };
            mockTransaction.executeAndStreamResults = async () => {
                return resultReadableObject;
            };
            mockTransaction.commit = async () => {};

            const executeAndStreamResultsSpy = sandbox.spy(mockTransaction, "executeAndStreamResults");
            const startTransactionSpy = sandbox.spy(qldbSession, "_startTransaction");
            const commitSpy = sandbox.spy(mockTransaction, "commit");

            const result = await qldbSession.executeLambda(async (txn) => {
                const resultReadable: ResultReadable = await txn.executeAndStreamResults(testStatement);
                return Result.bufferResultReadable(resultReadable);
            });
            sinon.assert.calledOnce(executeAndStreamResultsSpy);
            sinon.assert.calledWith(executeAndStreamResultsSpy, testStatement);
            sinon.assert.calledOnce(startTransactionSpy);
            sinon.assert.calledOnce(resultStub);
            sinon.assert.calledOnce(commitSpy);
            chai.assert.equal(result, mockResult);
        });

        it("should return a rejected promise when non-retryable error is thrown", async () => {
            qldbSession._startTransaction = async () => {
                throw new Error(testMessage);
            };

            const startTransactionSpy = sandbox.spy(qldbSession, "_startTransaction");

            await chai.expect(qldbSession.executeLambda(async (txn) => {
                return await txn.execute(testStatement);
            })).to.be.rejected;
            sinon.assert.calledOnce(startTransactionSpy);
        });

        it("should throw a wrapped exception when fails containing the original exception", async () => {
            const testError = new Error(testMessage) as AWSError;
            mockCommunicator.startTransaction = async () => {
                throw testError;
            };
            
            const result = await chai.expect(qldbSession.executeLambda(async (txn) => {
                return await txn.execute(testStatement);
            })).to.be.rejectedWith(Errors.ExecuteError);
            chai.assert.equal(result.cause, testError);
        });

        it("should wrap when fails with InvalidSessionException and close the session", async () => {
            const testError = new Error(testMessage) as AWSError;
            testError.code = "InvalidSessionException";

            mockCommunicator.startTransaction = async () => {
                throw testError;
            };

            const startTransactionSpy = sandbox.spy(qldbSession, "_startTransaction");
            const result = await chai.expect(qldbSession.executeLambda(async (txn) => {
                return await txn.execute(testStatement);
            })).to.be.rejected;
            sinon.assert.callCount(startTransactionSpy, 1);
            chai.assert.equal(result.cause, testError);
            chai.assert.isFalse(qldbSession.isAlive());
        });

        it("should wrap when fails with OccConflictException and session is still alive", async () => {
            const testError = new Error(testMessage) as AWSError;
            testError.code = "OccConflictException";
            const tryAbortSpy = sandbox.spy(mockCommunicator, "abortTransaction");

            mockCommunicator.startTransaction = async () => {
                throw testError;
            };

            const startTransactionSpy = sandbox.spy(qldbSession, "_startTransaction");
            const result = await chai.expect(qldbSession.executeLambda(async (txn) => {
                return await txn.execute(testStatement);
            })).to.be.rejected;
            sinon.assert.callCount(startTransactionSpy, 1);
            chai.assert.equal(result.cause, testError);
            chai.assert.isTrue(qldbSession.isAlive());
            sinon.assert.notCalled(tryAbortSpy);
        });

        it("should return a rejected promise with wrapped retriable error when retriable exception occurs", async () => {
            const isRetriableStub = sandbox.stub(Errors, "isRetriableException");
            isRetriableStub.returns(true);

            const startTransactionSpy = sandbox.spy(qldbSession, "_startTransaction");

            const result = await chai.expect(qldbSession.executeLambda(async (txn) => {
                throw new Error(testMessage);
            })).to.be.rejected;

            sinon.assert.calledOnce(startTransactionSpy);
            chai.assert.isTrue(result.isRetriable);
        });

        it("should return a rejected promise with a wrapped error when Transaction expires", async () => {
            const error = new Error("InvalidSession") as AWSError;
            error.code = "InvalidSessionException";
            error.message = "Transaction ABC has expired";
            const communicatorTransactionSpy = sandbox.spy(mockCommunicator, "startTransaction");
            const communicatorAbortTransactionSpy = sandbox.spy(mockCommunicator, "abortTransaction");

            let result = await chai.expect(qldbSession.executeLambda(async (txn) => {
                throw error;
            })).to.be.rejected;
            sinon.assert.calledOnce(communicatorTransactionSpy);
            chai.assert.equal(result.cause.message, error.message);
            sinon.assert.calledOnce(communicatorAbortTransactionSpy);
            chai.assert.isTrue(qldbSession.isAlive());
        });

        it("should return a rejected promise when a LambdaAbortedError occurs", async () => {
            const lambdaAbortedError: Errors.LambdaAbortedError = new Errors.LambdaAbortedError();
            await chai.expect(qldbSession.executeLambda(async (txn) => {
                throw lambdaAbortedError;
            })).to.be.rejected;
        });
    });

    describe("#_startTransaction()", () => {
        it("should return a Transaction object when called", async () => {
            const communicatorTransactionSpy = sandbox.spy(mockCommunicator, "startTransaction");
            const transaction = await qldbSession._startTransaction();
            chai.expect(transaction).to.be.an.instanceOf(Transaction);
            chai.assert.equal(transaction["_txnId"], testTransactionId);
            sinon.assert.calledOnce(communicatorTransactionSpy);
        });
    });

    describe("#_cleanSessionState()", () => {
        it("should call abortTransaction()", async () => {
            const communicatorAbortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
            await qldbSession["_cleanSessionState"]();
            sinon.assert.calledOnce(communicatorAbortSpy);
        });

        it("should log warning message when error is thrown and set alive state to false", async () => {
            const communicatorAbortStub = sandbox.stub(mockCommunicator, "abortTransaction");
            communicatorAbortStub.throws(new Error("testError"));
            const logSpy = sandbox.spy(LogUtil, "warn");
            await qldbSession["_cleanSessionState"]();
            sinon.assert.calledOnce(communicatorAbortStub);
            sinon.assert.calledOnce(logSpy);
            chai.assert.isFalse(qldbSession.isAlive());
        });
    });
});
