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

import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";

import { LambdaAbortedError } from "../errors/Errors";
import { Result } from "../Result";
import { ResultReadable } from "../ResultReadable";
import { Transaction } from "../Transaction";
import { TransactionExecutor } from "../TransactionExecutor";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testStatement: string = "SELECT * FROM foo";
const testMessage: string = "foo";
const testTransactionId: string = "txnId";

const mockResult: Result = <Result><any> sandbox.mock(Result);
const mockResultReadable: ResultReadable = <ResultReadable><any> sandbox.mock(ResultReadable);
const mockTransaction: Transaction = <Transaction><any> sandbox.mock(Transaction);

let transactionExecutor: TransactionExecutor;

describe("TransactionExecutor", () => {

    beforeEach(() => {
        transactionExecutor = new TransactionExecutor(mockTransaction);
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe("#constructor()", () => {
        it("should have all attributes equal to mock values when constructor called", () => {
            chai.assert.equal(transactionExecutor["_transaction"], mockTransaction);
        });
    });

    describe("#abort()", () => {
        it("should throw LambdaAbortedError when called", () => {
            chai.expect(() => {
                transactionExecutor.abort();
            }).to.throw(LambdaAbortedError);
        });
    });

    describe("#execute()", () => {
        it("should return a Result object when provided with a statement", async () => {
            mockTransaction.execute = async () => {
                return mockResult;
            };
            const transactionExecuteSpy = sandbox.spy(mockTransaction, "execute");
            const result = await transactionExecutor.execute(testStatement);
            chai.assert.equal(mockResult, result);
            sinon.assert.calledOnce(transactionExecuteSpy);
            sinon.assert.calledWith(transactionExecuteSpy, testStatement);
        });

        it("should return a Result object when provided with a statement and parameters", async () => {
            mockTransaction.execute = async () => {
                return mockResult;
            };

            const transactionExecuteSpy = sandbox.spy(mockTransaction, "execute");
            const result = await transactionExecutor.execute(testStatement, ["a"]);
            chai.assert.equal(mockResult, result);
            sinon.assert.calledOnce(transactionExecuteSpy);
            sinon.assert.calledWith(transactionExecuteSpy, testStatement, ["a"]);
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockTransaction.execute = async () => {
                throw new Error(testMessage);
            };
            const transactionExecuteSpy = sandbox.spy(mockTransaction, "execute");
            const errorMessage = await chai.expect(transactionExecutor.execute(testStatement)).to.be.rejected;
            chai.assert.equal(errorMessage.name, "Error");
            sinon.assert.calledOnce(transactionExecuteSpy);
            sinon.assert.calledWith(transactionExecuteSpy, testStatement);
        });
    });

    describe("#executeAndStreamResults()", () => {
        it("should return a Result object when provided with a statement", async () => {
            mockTransaction.executeAndStreamResults = async () => {
                return mockResultReadable;
            };
            const transactionExecuteSpy = sandbox.spy(mockTransaction, "executeAndStreamResults");
            const resultReadable = await transactionExecutor.executeAndStreamResults(testStatement);
            chai.assert.equal(mockResultReadable, resultReadable);
            sinon.assert.calledOnce(transactionExecuteSpy);
            sinon.assert.calledWith(transactionExecuteSpy, testStatement);
        });

        it("should return a Result object when provided with a statement and parameters", async () => {
            mockTransaction.executeAndStreamResults = async () => {
                return mockResultReadable;
            };

            const transactionExecuteSpy = sandbox.spy(mockTransaction, "executeAndStreamResults");
            const resultReadable = await transactionExecutor.executeAndStreamResults(testStatement, [5]);
            chai.assert.equal(mockResultReadable, resultReadable);
            sinon.assert.calledOnce(transactionExecuteSpy);
            sinon.assert.calledWith(transactionExecuteSpy, testStatement, [5]);
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockTransaction.executeAndStreamResults = async () => {
                throw new Error(testMessage);
            };
            const transactionExecuteSpy = sandbox.spy(mockTransaction, "executeAndStreamResults");
            const errorMessage = await chai.expect(transactionExecutor.executeAndStreamResults(testStatement)).to.be.rejected;
            chai.assert.equal(errorMessage.name, "Error");
            sinon.assert.calledOnce(transactionExecuteSpy);
            sinon.assert.calledWith(transactionExecuteSpy, testStatement);
        });
    });

    describe("#getTransactionId()", () => {
        it("should return the transaction ID when called", async () => {
            mockTransaction.getTransactionId = () => {
                return testTransactionId;
            };
            const transactionIdSpy = sandbox.spy(mockTransaction, "getTransactionId");
            const transactionId = transactionExecutor.getTransactionId();
            chai.assert.equal(transactionId, testTransactionId);
            sinon.assert.calledOnce(transactionIdSpy);
        });
    });
});
