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

import { makeBinaryWriter, Writer } from "ion-js";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";

import { LambdaAbortedError } from "../errors/Errors";
import { createQldbWriter, QldbWriter } from "../QldbWriter";
import { Result } from "../Result";
import { ResultStream } from "../ResultStream";
import { Transaction } from "../Transaction";
import { TransactionExecutor } from "../TransactionExecutor";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testStatement: string = "SELECT * FROM foo";
const testMessage: string = "foo";
const testTransactionId: string = "txnId";
const testWriter: Writer = makeBinaryWriter();
testWriter.writeString(testMessage);

const mockResult: Result = <Result><any> sandbox.mock(Result);
const mockResultStream: ResultStream = <ResultStream><any> sandbox.mock(ResultStream);
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

    describe("#executeInline()", () => {
        it("should return a Result object when provided with a statement", async () => {
            mockTransaction.executeInline = async () => {
                return mockResult;
            };
            const transactionExecuteSpy = sandbox.spy(mockTransaction, "executeInline");
            const result = await transactionExecutor.executeInline(testStatement);
            chai.assert.equal(mockResult, result);
            sinon.assert.calledOnce(transactionExecuteSpy);
            sinon.assert.calledWith(transactionExecuteSpy, testStatement);
        });

        it("should return a Result object when provided with a statement and parameters", async () => {
            mockTransaction.executeInline = async () => {
                return mockResult;
            };
            const qldbWriter: QldbWriter = createQldbWriter();

            const transactionExecuteSpy = sandbox.spy(mockTransaction, "executeInline");
            const result = await transactionExecutor.executeInline(testStatement, [qldbWriter]);
            chai.assert.equal(mockResult, result);
            sinon.assert.calledOnce(transactionExecuteSpy);
            sinon.assert.calledWith(transactionExecuteSpy, testStatement, [qldbWriter]);
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockTransaction.executeInline = async () => {
                throw new Error(testMessage);
            };
            const transactionExecuteSpy = sandbox.spy(mockTransaction, "executeInline");
            const errorMessage = await chai.expect(transactionExecutor.executeInline(testStatement)).to.be.rejected;
            chai.assert.equal(errorMessage.name, "Error");
            sinon.assert.calledOnce(transactionExecuteSpy);
            sinon.assert.calledWith(transactionExecuteSpy, testStatement);
        });
    });

    describe("#executeStream()", () => {
        it("should return a Result object when provided with a statement", async () => {
            mockTransaction.executeStream = async () => {
                return mockResultStream;
            };
            const transactionExecuteSpy = sandbox.spy(mockTransaction, "executeStream");
            const resultStream = await transactionExecutor.executeStream(testStatement);
            chai.assert.equal(mockResultStream, resultStream);
            sinon.assert.calledOnce(transactionExecuteSpy);
            sinon.assert.calledWith(transactionExecuteSpy, testStatement);
        });

        it("should return a Result object when provided with a statement and parameters", async () => {
            mockTransaction.executeStream = async () => {
                return mockResultStream;
            };

            const qldbWriter: QldbWriter = createQldbWriter();

            const transactionExecuteSpy = sandbox.spy(mockTransaction, "executeStream");
            const resultStream = await transactionExecutor.executeStream(testStatement, [qldbWriter]);
            chai.assert.equal(mockResultStream, resultStream);
            sinon.assert.calledOnce(transactionExecuteSpy);
            sinon.assert.calledWith(transactionExecuteSpy, testStatement, [qldbWriter]);
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockTransaction.executeStream = async () => {
                throw new Error(testMessage);
            };
            const transactionExecuteSpy = sandbox.spy(mockTransaction, "executeStream");
            const errorMessage = await chai.expect(transactionExecutor.executeStream(testStatement)).to.be.rejected;
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
