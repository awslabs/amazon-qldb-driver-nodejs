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

import {
    CommitTransactionResult,
    ExecuteStatementResult,
    Page,
    PageToken,
    ValueHolder
} from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as ionJs from "ion-js";
import { Lock } from "semaphore-async-await";
import * as sinon from "sinon";
import { Readable } from "stream";

import { Communicator } from "../Communicator";
import * as Errors from "../errors/Errors";
import * as logUtil from "../logUtil";
import { QldbHash } from "../QldbHash";
import { createQldbWriter, QldbWriter } from "../QldbWriter";
import { Result } from "../Result";
import { ResultStream } from "../ResultStream";
import { Transaction } from "../Transaction";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testMessage: string = "foo";
const testStatement: string = "SELECT * FROM foo";
const testPageToken: PageToken = "foo";
const testTransactionId: string = "txnId";
const testHash: Uint8Array = new Uint8Array([1, 2, 3]);
const testCommitTransactionResult: CommitTransactionResult = {
    TransactionId: testTransactionId,
    CommitDigest: QldbHash.toQldbHash(testTransactionId).getQldbHash()
};
const pageToken: Page = {NextPageToken: testPageToken};
const testExecuteStatementResult: ExecuteStatementResult = {
    FirstPage: {
        NextPageToken: testPageToken
    }
};

const mockCommunicator: Communicator = <Communicator><any> sandbox.mock(Communicator);
const mockResult: Result = <Result><any> sandbox.mock(Result);

let transaction: Transaction;

describe("Transaction", () => {

    beforeEach(() => {
        transaction = new Transaction(mockCommunicator, testTransactionId);
        mockCommunicator.executeStatement = async () => {
            return testExecuteStatementResult;
        };
        mockCommunicator.commit = async () => {
            return testCommitTransactionResult;
        };
        mockCommunicator.abortTransaction = async () => {
            return {};
        };
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe("#constructor()", () => {
        it("should have all attributes equal to mock values when constructor called", () => {
            chai.assert.equal(transaction["_communicator"], mockCommunicator);
            chai.assert.equal(transaction["_txnId"], testTransactionId);
            chai.assert.equal(transaction["_isClosed"], false);
            chai.assert.equal(transaction["_resultStreams"].length, 0);
            chai.assert.deepEqual(transaction["_txnHash"], QldbHash.toQldbHash(testTransactionId));
            chai.expect(transaction["_hashLock"]).to.be.an.instanceOf(Lock);
        });
    });

    describe("#abort()", () => {
        it("should call Communicator's abortTransaction() once when called", async () => {
            const abortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
            const transactionInternalCloseSpy = sandbox.spy(transaction as any, "_internalClose");
            await transaction.abort();
            await transaction.abort();
            sinon.assert.calledOnce(abortSpy);
            sinon.assert.calledOnce(transactionInternalCloseSpy);
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockCommunicator.abortTransaction = async () => {
                throw new Error(testMessage);
            };
            const abortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
            await chai.expect(transaction.abort()).to.be.rejected;
            sinon.assert.calledOnce(abortSpy);
        });

        it("should be a no-op when called after commit() was called", async () => {
            const abortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
            await transaction.commit();
            // This should be a no-op.
            await transaction.abort();
            sinon.assert.notCalled(abortSpy);
        });
    });

    describe("#commit()", () => {
        it("should call Communicator's commit() when called", async () => {
            const commitSpy = sandbox.spy(mockCommunicator, "commit");
            const transactionInternalCloseSpy = sandbox.spy(transaction as any, "_internalClose");
            await transaction.commit();
            sinon.assert.calledOnce(commitSpy);
            sinon.assert.calledOnce(transactionInternalCloseSpy);
        });

        it("should return a rejected promise when commit() was already called", async () => {
            const commitSpy = sandbox.spy(mockCommunicator, "commit");
            await transaction.commit();
            await chai.expect(transaction.commit()).to.be.rejected;
            sinon.assert.calledOnce(commitSpy);
        });

        it("should return a rejected promise when hashes don't match", async () => {
            const invalidHashCommitTransactionResult: CommitTransactionResult = {
                TransactionId: testTransactionId,
                CommitDigest: testHash
            };
            mockCommunicator.commit = async () => {
                return invalidHashCommitTransactionResult;
            };
            const commitSpy = sandbox.spy(mockCommunicator, "commit");
            const error = await chai.expect(transaction.commit()).to.be.rejected;
            chai.assert.equal(error.name, "ClientException");
            sinon.assert.calledOnce(commitSpy);
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockCommunicator.commit = async () => {
                throw new Error(testMessage);
            };
            const isOccStub = sandbox.stub(Errors, "isOccConflictException");
            isOccStub.returns(false);
            const commitSpy = sandbox.spy(mockCommunicator, "commit");
            await chai.expect(transaction.commit()).to.be.rejected;
            sinon.assert.calledOnce(isOccStub);
            sinon.assert.calledOnce(commitSpy);
        });

        it("should return a rejected promise when an OccConflictException occurs", async () => {
            mockCommunicator.commit = async () => {
                throw new Error(testMessage);
            };
            const isOccStub = sandbox.stub(Errors, "isOccConflictException");
            isOccStub.returns(true);
            const commitSpy = sandbox.spy(mockCommunicator, "commit");
            await chai.expect(transaction.commit()).to.be.rejected;
            sinon.assert.calledOnce(commitSpy);
            sinon.assert.calledOnce(isOccStub);
        });

        it("should return a rejected promise when an exception that is not OccConflictException occurs", async () => {
            mockCommunicator.commit = async () => {
                throw new Error(testMessage);
            };
            const isOccStub = sandbox.stub(Errors, "isOccConflictException");
            isOccStub.returns(false);
            const commitSpy = sandbox.spy(mockCommunicator, "commit");
            const abortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
            await chai.expect(transaction.commit()).to.be.rejected;
            sinon.assert.calledOnce(commitSpy);
            sinon.assert.calledOnce(abortSpy);
            sinon.assert.calledOnce(isOccStub);
        });

        it("should log a warning and return a rejected promise when abortTransaction() throws error", async () => {
            mockCommunicator.commit = async () => {
                throw new Error("mockMessage");
            };
            mockCommunicator.abortTransaction = async () => {
                throw new Error("foo2");
            };
            const isOccStub = sandbox.stub(Errors, "isOccConflictException");
            const logSpy = sandbox.spy(logUtil, "warn");
            isOccStub.returns(false);
            const commitSpy = sandbox.spy(mockCommunicator, "commit");
            const abortSpy = sandbox.spy(mockCommunicator, "abortTransaction");
            await chai.expect(transaction.commit()).to.be.rejected;
            sinon.assert.calledOnce(commitSpy);
            sinon.assert.calledOnce(abortSpy);
            sinon.assert.calledOnce(isOccStub);
            sinon.assert.calledOnce(logSpy);
        });
    });

    describe("#executeInline()", () => {
        it("should return a Result object when provided with a statement", async () => {
            Result.create = async () => {
                return mockResult
            };
            const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
            const result: Result = await transaction.executeInline(testStatement);
            sinon.assert.calledOnce(executeSpy);
            sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, []);
            chai.assert.equal(result, mockResult);
        });

        it("should return a Result object when provided with a statement and parameters", async () => {
            Result.create = async () => {
                return mockResult
            };
            const sendExecuteSpy = sandbox.spy(transaction as any, "_sendExecute");
            const qldbWriter1: QldbWriter = createQldbWriter();
            const qldbWriter2: QldbWriter = createQldbWriter();

            const result: Result = await transaction.executeInline(testStatement, [qldbWriter1, qldbWriter2]);
            sinon.assert.calledOnce(sendExecuteSpy);
            sinon.assert.calledWith(sendExecuteSpy, testStatement, [qldbWriter1, qldbWriter2]);
            chai.assert.equal(result, mockResult);
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockCommunicator.executeStatement = async () => {
                throw new Error(testMessage);
            };
            const isOccStub = sandbox.stub(Errors, "isOccConflictException");
            isOccStub.returns(false);
            const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
            await chai.expect(transaction.executeInline(testStatement)).to.be.rejected;
            sinon.assert.calledOnce(executeSpy);
            sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, []);
        });

        it("should call Communicator's executeStatement() twice when called twice", async () => {
            const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
            await transaction.executeInline(testStatement);
            await transaction.executeInline(testStatement);
            sinon.assert.calledTwice(executeSpy);
        });
    });

    describe("#executeStream()", () => {
        it("should return a Stream object when provided with a statement", async () => {
            const sampleResultStreamObject: ResultStream = new ResultStream(
                testTransactionId,
                pageToken,
                mockCommunicator
            );
            const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
            const result: Readable = await transaction.executeStream(testStatement);
            sinon.assert.calledOnce(executeSpy);
            sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, []);
            chai.assert.equal(JSON.stringify(result), JSON.stringify(sampleResultStreamObject));
        });

        it("should return a Stream object when provided with a statement and parameters", async () => {
            const sampleResultStreamObject: ResultStream = new ResultStream(
                testTransactionId,
                pageToken,
                mockCommunicator
            );
            const sendExecuteSpy = sandbox.spy(transaction as any, "_sendExecute");
            const qldbWriter1: QldbWriter = createQldbWriter();
            const qldbWriter2: QldbWriter = createQldbWriter();
            const result: Readable = await transaction.executeStream(testStatement, [qldbWriter1, qldbWriter2]);
            sinon.assert.calledOnce(sendExecuteSpy);
            sinon.assert.calledWith(sendExecuteSpy, testStatement, [qldbWriter1, qldbWriter2]);
            chai.assert.equal(JSON.stringify(result), JSON.stringify(sampleResultStreamObject));
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockCommunicator.executeStatement = async () => {
                throw new Error(testMessage);
            };
            const isOccStub = sandbox.stub(Errors, "isOccConflictException");
            isOccStub.returns(false);
            const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
            await chai.expect(transaction.executeStream(testStatement)).to.be.rejected;
            sinon.assert.calledOnce(executeSpy);
            sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, []);
        });

        it("should call Communicator's executeStatement() twice when called twice", async () => {
            const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
            await transaction.executeStream(testStatement);
            await transaction.executeStream(testStatement);
            sinon.assert.calledTwice(executeSpy);
        });
    });

    describe("#getTransactionId()", () => {
        it("should return the transaction ID when called", () => {
            const transactionIdSpy = sandbox.spy(transaction, "getTransactionId");
            const transactionId: string = transaction.getTransactionId();
            chai.assert.equal(transactionId, testTransactionId);
            sinon.assert.calledOnce(transactionIdSpy);
        });
    });

    describe("#_internalClose()", () => {
        it("should close transaction and pop and close all Streams in _resultStreams list", async () => {
            const sampleResultStreamObject: ResultStream = new ResultStream(
                testTransactionId,
                pageToken,
                mockCommunicator
            );
            const sampleResultStreamObject2: ResultStream = new ResultStream(
                testTransactionId,
                pageToken,
                mockCommunicator
            );
            transaction["_resultStreams"] = [sampleResultStreamObject, sampleResultStreamObject2];
            transaction["_internalClose"]();
            chai.expect(transaction["_isClosed"]).to.be.true;
            chai.assert.equal(transaction["_resultStreams"].length, 0);
            chai.assert.equal(sampleResultStreamObject["_isClosed"], true);
            chai.assert.equal(sampleResultStreamObject2["_isClosed"], true);
        });
    });

    describe("#_sendExecute()", () => {
        it("should return a rejected promise when called after commmit() called", async () => {
            await transaction.commit();
            await chai.expect(transaction["_sendExecute"](testStatement, [])).to.be.rejected;
        });

        it("should return a rejected promise when closed", async () => {
            transaction["_isClosed"] = true;
            await chai.expect(transaction["_sendExecute"](testStatement, [])).to.be.rejected;
        });

        it("should compute hashes correctly when called", async () => {
            const qldbWriter1: QldbWriter = createQldbWriter();
            const qldbWriter2: QldbWriter = createQldbWriter();

            const parameters: QldbWriter[] = [qldbWriter1, qldbWriter2];

            let testStatementHash: QldbHash = QldbHash.toQldbHash(testStatement);
            parameters.forEach((writer: QldbWriter) => {
                testStatementHash = testStatementHash.dot(QldbHash.toQldbHash(writer.getBytes()));
            });
            const updatedHash: Uint8Array = transaction["_txnHash"].dot(testStatementHash).getQldbHash();

            const toQldbHashSpy = sandbox.spy(QldbHash, "toQldbHash");

            const result: ExecuteStatementResult = await transaction["_sendExecute"](testStatement, parameters);

            sinon.assert.calledThrice(toQldbHashSpy);
            sinon.assert.calledWith(toQldbHashSpy, testStatement);
            sinon.assert.calledWith(toQldbHashSpy, qldbWriter1.getBytes());
            sinon.assert.calledWith(toQldbHashSpy, qldbWriter2.getBytes());

            chai.assert.equal(ionJs.toBase64(transaction["_txnHash"].getQldbHash()), ionJs.toBase64(updatedHash));
            chai.assert.equal(testExecuteStatementResult, result);

        });

        it("should convert QldbWriters to ValueHolders correctly when called", async () => {
            const qldbWriter1: QldbWriter = createQldbWriter();
            const qldbWriter2: QldbWriter = createQldbWriter();

            qldbWriter1.close();
            qldbWriter2.close();
            const parameters: QldbWriter[] = [qldbWriter1, qldbWriter2];

            const valueHolderList: ValueHolder[] = [];
            parameters.forEach((writer: QldbWriter) => {
                const valueHolder: ValueHolder = {
                    IonBinary: writer.getBytes()
                };
                valueHolderList.push(valueHolder);
            });

            const executeStatementSpy = sandbox.spy(transaction["_communicator"], "executeStatement");
            const result: ExecuteStatementResult = await transaction["_sendExecute"](testStatement, parameters);

            sinon.assert.calledWith(executeStatementSpy, transaction["_txnId"], testStatement, valueHolderList);
            chai.assert.equal(testExecuteStatementResult, result);
        });
    });
});
