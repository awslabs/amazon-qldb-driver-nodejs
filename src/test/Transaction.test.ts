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
import * as LogUtil from "../LogUtil";
import { QldbHash } from "../QldbHash";
import { Result } from "../Result";
import { ResultReadable } from "../ResultReadable";
import { Transaction } from "../Transaction";
import { expect } from "chai";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testMessage: string = "foo";
const testStatement: string = "SELECT * FROM foo";
const testStatementWithQuotes: string = `SELECT * FROM "foo"`;
const testPageToken: PageToken = "foo";
const testTransactionId: string = "txnId";
const testHash: Uint8Array = new Uint8Array([1, 2, 3]);
const testCommitTransactionResult: CommitTransactionResult = {
    TransactionId: testTransactionId,
    CommitDigest: QldbHash.toQldbHash(testTransactionId).getQldbHash()
};
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
            const logSpy = sandbox.spy(LogUtil, "warn");
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

    describe("#execute()", () => {
        it("should return a Result object when provided with a statement", async () => {
            Result.create = async () => {
                return mockResult
            };
            const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
            const result: Result = await transaction.execute(testStatement);
            sinon.assert.calledOnce(executeSpy);
            sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, []);
            chai.assert.equal(result, mockResult);
        });

        it("should return a Result object when provided with a statement and parameters", async () => {
            Result.create = async () => {
                return mockResult
            };
            const sendExecuteSpy = sandbox.spy(transaction as any, "_sendExecute");
            const param1: number = 5;
            const param2: string = "a";

            const result: Result = await transaction.execute(testStatement, param1, param2);
            sinon.assert.calledOnce(sendExecuteSpy);
            sinon.assert.calledWith(sendExecuteSpy, testStatement, [param1, param2]);
            chai.assert.equal(result, mockResult);
        });

        it("should properly map a list as a single parameter", async () => {
            Result.create = async () => {
                return mockResult
            };
            const sendExecuteSpy = sandbox.spy(transaction as any, "_sendExecute");
            const param1: number = 5;
            const param2: string = "a";

            const result: Result = await transaction.execute(testStatement, [param1, param2]);
            sinon.assert.calledOnce(sendExecuteSpy);
            sinon.assert.calledWith(sendExecuteSpy, testStatement, [[param1, param2]]);
            chai.assert.equal(result, mockResult);
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockCommunicator.executeStatement = async () => {
                throw new Error(testMessage);
            };
            const isOccStub = sandbox.stub(Errors, "isOccConflictException");
            isOccStub.returns(false);
            const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
            await chai.expect(transaction.execute(testStatement)).to.be.rejected;
            sinon.assert.calledOnce(executeSpy);
            sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, []);
        });

        it("should call Communicator's executeStatement() twice when called twice", async () => {
            const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
            await transaction.execute(testStatement);
            await transaction.execute(testStatement);
            sinon.assert.calledTwice(executeSpy);
        });
    });

    describe("#executeAndStreamResults()", () => {
        it("should return a Stream object when provided with a statement", async () => {
            const sampleResultReadableObject: ResultReadable = new ResultReadable(
                testTransactionId,
                testExecuteStatementResult,
                mockCommunicator
            );
            const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
            const result: Readable = await transaction.executeAndStreamResults(testStatement);
            sinon.assert.calledOnce(executeSpy);
            sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, []);
            chai.assert.equal(JSON.stringify(result), JSON.stringify(sampleResultReadableObject));
        });

        it("should return a Stream object when provided with a statement and parameters", async () => {
            const sampleResultReadableObject: ResultReadable = new ResultReadable(
                testTransactionId,
                testExecuteStatementResult,
                mockCommunicator
            );
            const sendExecuteSpy = sandbox.spy(transaction as any, "_sendExecute");
            const param1: number = 5;
            const param2: string = "a";
            const result: Readable = await transaction.executeAndStreamResults(testStatement, param1, param2);
            sinon.assert.calledOnce(sendExecuteSpy);
            sinon.assert.calledWith(sendExecuteSpy, testStatement, [param1, param2]);
            chai.assert.equal(JSON.stringify(result), JSON.stringify(sampleResultReadableObject));
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockCommunicator.executeStatement = async () => {
                throw new Error(testMessage);
            };
            const isOccStub = sandbox.stub(Errors, "isOccConflictException");
            isOccStub.returns(false);
            const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
            await chai.expect(transaction.executeAndStreamResults(testStatement)).to.be.rejected;
            sinon.assert.calledOnce(executeSpy);
            sinon.assert.calledWith(executeSpy, testTransactionId, testStatement, []);
        });

        it("should call Communicator's executeStatement() twice when called twice", async () => {
            const executeSpy = sandbox.spy(mockCommunicator, "executeStatement");
            await transaction.executeAndStreamResults(testStatement);
            await transaction.executeAndStreamResults(testStatement);
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
        it("should close transaction", async () => {
            transaction["_internalClose"]();
            chai.expect(transaction["_isClosed"]).to.be.true;
        });
    });

    describe("#_sendExecute()", () => {
        it("should return a rejected promise when called after commit() called", async () => {
            await transaction.commit();
            await chai.expect(transaction["_sendExecute"](testStatement, [])).to.be.rejected;
        });

        it("should return a rejected promise when closed", async () => {
            transaction["_isClosed"] = true;
            await chai.expect(transaction["_sendExecute"](testStatement, [])).to.be.rejected;
        });

        it("should compute hashes correctly when called", async () => {
            let testStatementHash: QldbHash = QldbHash.toQldbHash(testStatement);

            const parameters: any[] = [5, "a"];
            let ionBinaryValues: Uint8Array[] = parameters.map((value: any): Uint8Array => {
                let valueIonBinary:Uint8Array = ionJs.dumpBinary(value);
                testStatementHash = testStatementHash.dot(QldbHash.toQldbHash(valueIonBinary));
                return valueIonBinary;
            });

            const updatedHash: Uint8Array = transaction["_txnHash"].dot(testStatementHash).getQldbHash();

            const toQldbHashSpy = sandbox.spy(QldbHash, "toQldbHash");

            const result: ExecuteStatementResult = await transaction["_sendExecute"](testStatement, parameters);

            sinon.assert.calledThrice(toQldbHashSpy);
            sinon.assert.calledWith(toQldbHashSpy, testStatement);
            sinon.assert.calledWith(toQldbHashSpy, ionBinaryValues[0]);
            sinon.assert.calledWith(toQldbHashSpy, ionBinaryValues[1]);

            chai.assert.equal(ionJs.toBase64(transaction["_txnHash"].getQldbHash()), ionJs.toBase64(updatedHash));
            chai.assert.equal(testExecuteStatementResult, result);

        });

        it("should compute hashes correctly when called from a statement that contain quotes", async () => {
            let testStatementHash: QldbHash = QldbHash.toQldbHash(testStatementWithQuotes);

            const parameters: any[] = [5, "a"];
            let ionBinaryValues: Uint8Array[] = parameters.map((value: any): Uint8Array => {
                let valueIonBinary:Uint8Array = ionJs.dumpBinary(value);
                testStatementHash = testStatementHash.dot(QldbHash.toQldbHash(valueIonBinary));
                return valueIonBinary;
            });
            const updatedHash: Uint8Array = transaction["_txnHash"].dot(testStatementHash).getQldbHash();

            const toQldbHashSpy = sandbox.spy(QldbHash, "toQldbHash");

            const result: ExecuteStatementResult = await transaction["_sendExecute"](
                testStatementWithQuotes,
                parameters
            );

            sinon.assert.calledThrice(toQldbHashSpy);
            sinon.assert.calledWith(toQldbHashSpy, testStatementWithQuotes);
            sinon.assert.calledWith(toQldbHashSpy, ionBinaryValues[0]);
            sinon.assert.calledWith(toQldbHashSpy, ionBinaryValues[1]);

            chai.assert.equal(ionJs.toBase64(transaction["_txnHash"].getQldbHash()), ionJs.toBase64(updatedHash));
            chai.assert.equal(testExecuteStatementResult, result);

        });

        it("should compute different hashes when called from different statements that contain quotes", async () => {
            const firstStatement: string = `INSERT INTO "first_table" VALUE {'test': 'hello world'}`;
            const secondStatement: string = `INSERT INTO "second_table" VALUE {'test': 'hello world'}`;

            const firstStatementHash: QldbHash = QldbHash.toQldbHash(firstStatement);
            const secondStatementHash: QldbHash = QldbHash.toQldbHash(secondStatement);

            // If the different statements that contain quotes are hashed incorrectly, then the hash of
            // 92Hs4IGd3Gnq4O9sVQX/S0AanTKWolpiAXzv+9GLzP0= would be produced every time.
            // It's asserted here that the hashes are different and computed correctly.
            chai.assert.notEqual(
                ionJs.toBase64(firstStatementHash.getQldbHash()),
                ionJs.toBase64(secondStatementHash.getQldbHash())
            );
        });

        it("should have different hashes when called from same statements, one with quotes one without", async () => {
            const firstStatement: string = `INSERT INTO "first_table" VALUE {'test': 'hello world'}`;
            const secondStatement: string = `INSERT INTO first_table VALUE {'test': 'hello world'}`;

            const firstStatementHash: QldbHash = QldbHash.toQldbHash(firstStatement);
            const secondStatementHash: QldbHash = QldbHash.toQldbHash(secondStatement);

            chai.assert.notEqual(
                ionJs.toBase64(firstStatementHash.getQldbHash()),
                ionJs.toBase64(secondStatementHash.getQldbHash())
            );
        });

        it("should convert native types to ValueHolders correctly when called", async () => {
            const parameters: any[] = [
                true,
                Date.now(),
                3e2,
                5,
                2.2,
                "a",
                new ionJs.Timestamp(0, 2000),
                new Uint8Array(3)
            ];

            const executeStatementSpy = sandbox.spy(transaction["_communicator"], "executeStatement");
            const result: ExecuteStatementResult = await transaction["_sendExecute"](testStatement, parameters);

            let expectedValueHolders: ValueHolder[] = [];
            parameters.forEach((value: any) => {
                const valueHolder: ValueHolder = {
                    IonBinary:  ionJs.dumpBinary(value)
                };
                expectedValueHolders.push(valueHolder);
            });

            sinon.assert.calledWith(
                executeStatementSpy,
                transaction["_txnId"],
                testStatement,
                sinon.match.array.deepEquals(expectedValueHolders)
            );
            chai.assert.equal(testExecuteStatementResult, result);
        });

        it("should throw Error when called with parameters which cannot be converted to Ion", async () => {
            const validParameter1 = 5;
            const invalidParameter =  Symbol('foo');
            const validParameter2 = 3;


            const toQldbHashSpy = sandbox.spy(QldbHash, "toQldbHash");
            const executeStatementSpy = sandbox.spy(transaction["_communicator"], "executeStatement");
            await expect(
                transaction["_sendExecute"](testStatement, [validParameter1, invalidParameter, validParameter2])
            ).to.be.rejected;

            sinon.assert.notCalled(executeStatementSpy);

            sinon.assert.calledTwice(toQldbHashSpy);
            sinon.assert.calledWith(toQldbHashSpy, testStatement);
            //Ensure that the first valid parameter was added to qldbHash
            sinon.assert.calledWith(toQldbHashSpy, ionJs.dumpBinary(validParameter1));
            //Ensure that the second valid parameter was not called as the invalid parameter throws an error before it
            sinon.assert.neverCalledWith(toQldbHashSpy, ionJs.dumpBinary(validParameter2));

        });
    });
});
