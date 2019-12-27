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
    Page,
    PageToken,
    StartTransactionResult,
    ValueHolder
} from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import { makeReader, Reader } from "ion-js";
import * as sinon from "sinon";
import { Readable } from "stream";
import { format } from "util";

import { Communicator } from "../Communicator";
import * as Errors from "../errors/Errors";
import * as LogUtil from "../LogUtil";
import { QldbSessionImpl } from "../QldbSessionImpl";
import { createQldbWriter, QldbWriter } from "../QldbWriter";
import { Result } from "../Result";
import { ResultStream } from "../ResultStream";
import { Transaction } from "../Transaction";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testRetryLimit: number = 4;
const testLedgerName: string = "fakeLedgerName";
const testSessionToken: string = "sessionToken";
const testTransactionId: string = "txnId";
const testStartTransactionResult: StartTransactionResult = {
    TransactionId: testTransactionId
};
const testMessage: string = "foo";
const testTableNames: string[] = ["Vehicle", "Person"];
const testStatement: string = "SELECT * FROM foo";
const testAbortTransactionResult: AbortTransactionResult = {};

const TEST_SLEEP_CAP_MS: number = 5000;
const TEST_SLEEP_BASE_MS: number = 10;

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
const testPage: Page = {};

const mockCommunicator: Communicator = <Communicator><any> sandbox.mock(Communicator);
const mockResult: Result = <Result><any> sandbox.mock(Result);
const mockTransaction: Transaction = <Transaction><any> sandbox.mock(Transaction);

const resultStreamObject: ResultStream = new ResultStream(testTransactionId, testPage, mockCommunicator);
let qldbSession: QldbSessionImpl;

describe("QldbSession", () => {

    beforeEach(() => {
        qldbSession = new QldbSessionImpl(mockCommunicator, testRetryLimit);
        mockCommunicator.endSession = async () => {};
        mockCommunicator.getLedgerName = () => {
            return testLedgerName;
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
            chai.assert.equal(qldbSession["_retryLimit"], testRetryLimit);
            chai.assert.equal(qldbSession["_isClosed"], false);
        });
    });

    describe("#close()", () => {
        it("should close qldbSession when called", async () => {
            const communicatorEndSpy = sandbox.spy(mockCommunicator, "endSession");
            await qldbSession.close();
            chai.assert.equal(qldbSession["_isClosed"], true);
            sinon.assert.calledOnce(communicatorEndSpy);
        });

        it("should be a no-op when already closed", async () => {
            const communicatorEndSpy = sandbox.spy(mockCommunicator, "endSession");
            qldbSession["_isClosed"] = true;
            await qldbSession.close();
            sinon.assert.notCalled(communicatorEndSpy);
        });
    });

    describe("#executeLambda()", () => {
        it("should return a Result object when called with executeInline as the lambda", async () => {
            qldbSession.startTransaction = async () => {
                return mockTransaction;
            };
            mockTransaction.executeInline = async () => {
                return mockResult;
            };
            mockTransaction.commit = async () => {};

            const executeInlineSpy = sandbox.spy(mockTransaction, "executeInline");
            const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
            const commitSpy = sandbox.spy(mockTransaction, "commit");

            const result = await qldbSession.executeLambda(async (txn) => {
                return await txn.executeInline(testStatement);
            });
            sinon.assert.calledOnce(executeInlineSpy);
            sinon.assert.calledWith(executeInlineSpy, testStatement);
            sinon.assert.calledOnce(startTransactionSpy);
            sinon.assert.calledOnce(commitSpy);
            chai.assert.equal(result, mockResult);
        });

        it("should return a Result object when called with executeStream as the lambda", async () => {
            const resultStub = sandbox.stub(Result, "bufferResultStream");
            resultStub.returns(Promise.resolve(mockResult));

            qldbSession.startTransaction = async () => {
                return mockTransaction;
            };
            mockTransaction.executeStream = async () => {
                return resultStreamObject;
            };
            mockTransaction.commit = async () => {};

            const executeStreamSpy = sandbox.spy(mockTransaction, "executeStream");
            const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
            const commitSpy = sandbox.spy(mockTransaction, "commit");

            const result = await qldbSession.executeLambda(async (txn) => {
                return await txn.executeStream(testStatement);
            });
            sinon.assert.calledOnce(executeStreamSpy);
            sinon.assert.calledWith(executeStreamSpy, testStatement);
            sinon.assert.calledOnce(startTransactionSpy);
            sinon.assert.calledOnce(resultStub);
            sinon.assert.calledOnce(commitSpy);
            chai.assert.equal(result, mockResult);
        });

        it("should return a SessionClosedError wrapped in a rejected promise when closed", async () => {
            qldbSession["_isClosed"] = true;

            const error = await chai.expect(qldbSession.executeLambda(async (txn) => {
                return await txn.executeInline(testStatement);
            })).to.be.rejected;
            chai.assert.equal(error.name, "SessionClosedError");
        });

        it("should return a rejected promise when error is thrown", async () => {
            qldbSession.startTransaction = async () => {
                throw new Error(testMessage);
            };

            const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
            const noThrowAbortSpy = sandbox.spy(qldbSession as any, "_noThrowAbort");
            const throwIfClosedSpy = sandbox.spy(qldbSession as any, "_throwIfClosed");

            await chai.expect(qldbSession.executeLambda(async (txn) => {
                return await txn.executeInline(testStatement);
            })).to.be.rejected;
            sinon.assert.calledOnce(startTransactionSpy);
            sinon.assert.calledOnce(noThrowAbortSpy);
            sinon.assert.calledOnce(throwIfClosedSpy);
        });

        it("should retry when OccConflictException occurs", async () => {
            const isOccStub = sandbox.stub(Errors, "isOccConflictException");
            isOccStub.returns(true);

            const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
            const noThrowAbortSpy = sandbox.spy(qldbSession as any, "_noThrowAbort");
            const logSpy = sandbox.spy(LogUtil, "warn");

            await chai.expect(qldbSession.executeLambda(async (txn) => {
                throw new Error(testMessage);
            }, () => {})).to.be.rejected;

            sinon.assert.callCount(startTransactionSpy, testRetryLimit + 1);
            sinon.assert.callCount(noThrowAbortSpy, testRetryLimit + 1);
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
            }, () => {})).to.be.rejected;

            sinon.assert.callCount(startTransactionSpy, testRetryLimit + 1);
            sinon.assert.callCount(noThrowAbortSpy, testRetryLimit + 1);
            sinon.assert.callCount(logSpy, testRetryLimit);
        });

        it("should create a new session and retry when InvalidSessionException occurs", async () => {
            const isInvalidSessionStub = sandbox.stub(Errors, "isInvalidSessionException");
            isInvalidSessionStub.returns(true);

            const logWarnSpy = sandbox.spy(LogUtil, "warn");
            const logInfoSpy = sandbox.spy(LogUtil, "info");

            Communicator.create = async () => {
                return mockCommunicator;
            };
            const communicatorSpy = sandbox.spy(Communicator, "create");

            await chai.expect(qldbSession.executeLambda(async (txn) => {
                throw new Error(testMessage);
            }, () => {})).to.be.rejected;

            sinon.assert.callCount(logWarnSpy, testRetryLimit);
            sinon.assert.callCount(logInfoSpy, testRetryLimit);
            sinon.assert.callCount(communicatorSpy, testRetryLimit);
        });

        it("should retry and execute provided retryIndicator lambda when retriable exception occurs", async () => {
            const isRetriableStub = sandbox.stub(Errors, "isRetriableException");
            isRetriableStub.returns(true);
            const retryIndicator = () =>
                LogUtil.log("Retrying test retry indicator...");

            const startTransactionSpy = sandbox.spy(qldbSession, "startTransaction");
            const noThrowAbortSpy = sandbox.spy(qldbSession as any, "_noThrowAbort");
            const logSpy = sandbox.spy(LogUtil, "warn");
            const retryIndicatorSpy = sandbox.spy(LogUtil, "log");

            await chai.expect(qldbSession.executeLambda(async (txn) => {
                throw new Error(testMessage);
            }, retryIndicator)).to.be.rejected;

            sinon.assert.callCount(startTransactionSpy, testRetryLimit + 1);
            sinon.assert.callCount(noThrowAbortSpy, testRetryLimit + 1);
            sinon.assert.callCount(logSpy, testRetryLimit);
            sinon.assert.callCount(retryIndicatorSpy, testRetryLimit);
            sinon.assert.alwaysCalledWith(retryIndicatorSpy, "Retrying test retry indicator...");
        });

        it("should return a rejected promise when a LambdaAbortedError occurs", async () => {
            const lambdaAbortedError: Errors.LambdaAbortedError = new Errors.LambdaAbortedError();
            await chai.expect(qldbSession.executeLambda(async (txn) => {
                throw lambdaAbortedError;
            }, () => {})).to.be.rejected;
        });
    });

    describe("#executeStatement()", () => {
        it("should return a Result object when provided with a statement", async () => {
            const executeStub = sandbox.stub(qldbSession, "executeLambda");
            executeStub.returns(Promise.resolve(mockResult));
            const result: Result = await qldbSession.executeStatement(testStatement);
            chai.assert.equal(result, mockResult);
            sinon.assert.calledOnce(executeStub);
        });

        it("should return a Result object when provided with a statement and parameters", async () => {
            qldbSession.startTransaction = async () => {
                return mockTransaction;
            };

            const mockQldbWriter = <QldbWriter><any>sandbox.mock(createQldbWriter);
            const executeInlineSpy = sandbox.spy(mockTransaction, "executeInline");
            const result: Result = await qldbSession.executeStatement(testStatement, [mockQldbWriter]);
            chai.assert.equal(result, mockResult);
            sinon.assert.calledWith(executeInlineSpy, testStatement, [mockQldbWriter]);
        });
    });

    describe("#getLedgerName()", () => {
        it("should return the ledger name when called", () => {
            const communicatorLedgerSpy = sandbox.spy(mockCommunicator, "getLedgerName");
            const ledgerName: string = qldbSession.getLedgerName();
            chai.assert.equal(ledgerName, testLedgerName);
            sinon.assert.calledOnce(communicatorLedgerSpy);
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

    describe("#getTableNames()", () => {
        it("should return a list of table names when called", async () => {
            const executeStub = sandbox.stub(qldbSession, "executeLambda");
            executeStub.returns(Promise.resolve(testTableNames));
            const listOfTableNames: string[] = await qldbSession.getTableNames();
            chai.assert.equal(listOfTableNames.length, testTableNames.length);
            chai.assert.equal(listOfTableNames, testTableNames);
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

        it("should return a SessionClosedError wrapped in a rejected promise when closed", async () => {
            const communicatorTransactionSpy = sandbox.spy(mockCommunicator, "startTransaction");
            qldbSession["_isClosed"] = true;
            const error = await chai.expect(qldbSession.startTransaction()).to.be.rejected;
            chai.assert.equal(error.name, "SessionClosedError");
            sinon.assert.notCalled(communicatorTransactionSpy);
        });
    });

    describe("#abortTransaction()", () => {
        it("should call Communicator's abortTransaction() and return true when called", async () => {
            const communicatorAbortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
            chai.assert.equal(await qldbSession._abortOrClose(), true);
            sinon.assert.calledOnce(communicatorAbortSpy);
        });

        it("should return false and close qldbSession when error is thrown", async () => {
            mockCommunicator.abortTransaction = async () => {
                throw new Error(testMessage);
            };
            chai.assert.equal(await qldbSession._abortOrClose(), false);
            chai.assert.equal(await qldbSession["_isClosed"], true);
        });

        it("should return false when error is thrown", async () => {
            const communicatorAbortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
            qldbSession["_isClosed"] = true;
            chai.assert.equal(await qldbSession._abortOrClose(), false);
            sinon.assert.notCalled(communicatorAbortSpy);
        });
    });

    describe("#_throwIfClosed()", () => {
        it("should not throw if not closed", () => {
            chai.expect(qldbSession["_throwIfClosed"]()).to.not.throw;
        });

        it("should throw SessionClosedError if closed", () => {
            qldbSession["_isClosed"] = true;
            chai.expect(() => {
                qldbSession["_throwIfClosed"]();
            }).to.throw(Errors.SessionClosedError);
        });
    });

    describe("#_noThrowAbort()", () => {
        it("should call Communicator's abortTransaction() when Transaction is null", async () => {
            const communicatorAbortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
            await qldbSession["_noThrowAbort"](null);
            sinon.assert.calledOnce(communicatorAbortSpy);
        });

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

    describe("#_retrySleep()", () => {
        it("should sleep when called", async () => {
            const mathRandStub = sandbox.stub(Math, "random");
            mathRandStub.returns(1);
            const attemptNumber: number = 1;
            const exponentialBackoff: number = Math.min(TEST_SLEEP_CAP_MS, Math.pow(TEST_SLEEP_BASE_MS, attemptNumber));
            const sleepValue: number = 1 * (exponentialBackoff + 1);

            const clock = sinon.useFakeTimers();
            const timeoutSpy = sandbox.spy(clock, "setTimeout");
            await qldbSession["_retrySleep"](attemptNumber);
            sinon.assert.calledOnce(timeoutSpy);
            sinon.assert.calledWith(timeoutSpy, sinon.match.any, sleepValue);
        });
    });

    describe("#_tableNameHelper()", () => {
        it("should return a list of table names when called with a Stream containing valid Readers", async () => {
            const value1: ValueHolder = {IonBinary: format("{ name:\"%s\" }", testTableNames[0])};
            const value2: ValueHolder = {IonBinary: format("{ name:\"%s\" }", testTableNames[1])};
            const readers: Reader[] = [
                makeReader(Result._handleBlob(value1.IonBinary)),
                makeReader(Result._handleBlob(value2.IonBinary))
            ];
            let eventCount: number = 0;
            const mockResultStream: Readable = new Readable({
                objectMode: true,
                read: function(size) {
                    if (eventCount < readers.length) {
                        eventCount += 1;
                        return this.push(readers[eventCount-1]);
                    } else {
                        return this.push(null);
                    }
                }
            });
            const tableNames: string[] = await qldbSession["_tableNameHelper"](<ResultStream> mockResultStream);
            tableNames.forEach((tableName, i) => {
                chai.assert.equal(tableName, testTableNames[i]);
            });
        });

        it("should return a rejected promise when called with a Stream containing Readers with no struct", async () => {
            const value1: ValueHolder = {IonBinary: "notAStruct"};
            const readers: Reader[] = [makeReader(Result._handleBlob(value1.IonBinary))];
            let eventCount: number = 0;
            const mockResultStream: Readable = new Readable({
                objectMode: true,
                read: function(size) {
                    if (eventCount < readers.length) {
                        eventCount += 1;
                        return this.push(readers[eventCount-1]);
                    } else {
                        return this.push(null);
                    }
                }
            });
            const error =
                await chai.expect(qldbSession["_tableNameHelper"](<ResultStream> mockResultStream)).to.be.rejected;
            chai.assert.equal(error.name, "ClientException");
        });

        it("should return a rejected promise when called with a Stream containing Readers with no string", async () => {
            const value1: ValueHolder = {IonBinary: "{ structKeyName:1 }"};
            const readers: Reader[] = [makeReader(Result._handleBlob(value1.IonBinary))];
            let eventCount: number = 0;
            const mockResultStream: Readable = new Readable({
                objectMode: true,
                read: function(size) {
                    if (eventCount < readers.length) {
                        eventCount += 1;
                        return this.push(readers[eventCount-1]);
                    } else {
                        return this.push(null);
                    }
                }
            });
            const error =
                await chai.expect(qldbSession["_tableNameHelper"](<ResultStream> mockResultStream)).to.be.rejected;
            chai.assert.equal(error.name, "ClientException");
        });
    });
});
