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

import * as LogUtil from "../LogUtil";
import { SessionClosedError } from "../errors/Errors";
import { PooledQldbSession } from "../PooledQldbSession";
import { QldbSessionImpl } from "../QldbSessionImpl";
import { createQldbWriter, QldbWriter } from "../QldbWriter";
import { Result } from "../Result";
import { Transaction } from "../Transaction";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testLambda = () => LogUtil.log("Test returning session to pool...");
const testLedgerName: string = "fakeLedgerName";
const testMessage: string = "foo";
const testSessionToken: string = "sessionToken";
const testStatement: string = "SELECT * FROM foo";
const testTableNames: string[] = ["Vehicle", "Person"];

const mockQldbSession: QldbSessionImpl = <QldbSessionImpl><any> sandbox.mock(QldbSessionImpl);
const mockResult: Result = <Result><any> sandbox.mock(Result);
const mockTransaction: Transaction = <Transaction><any> sandbox.mock(Transaction);

let pooledQldbSession: PooledQldbSession;

describe("PooledQldbSession", () => {

    beforeEach(() => {
        pooledQldbSession = new PooledQldbSession(mockQldbSession, testLambda);
        mockQldbSession.getLedgerName = () => {
            return testLedgerName;
        };
        mockQldbSession.executeStatement = async () => {
            return mockResult;
        };
        mockQldbSession.executeLambda = async () => {
            return mockResult;
        };
        mockQldbSession.getSessionToken = () => {
            return testSessionToken;
        };
        mockQldbSession.getTableNames = async () => {
            return testTableNames;
        };
        mockQldbSession.startTransaction = async () => {
            return mockTransaction;
        };
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe("#constructor()", () => {
        it("should have all attributes equal to mock values when constructor called", () => {
            chai.assert.equal(pooledQldbSession["_session"], mockQldbSession);
            chai.assert.equal(pooledQldbSession["_returnSessionToPool"], testLambda);
            chai.assert.equal(pooledQldbSession["_isClosed"], false);
        });
    });

    describe("#close()", () => {
        it("should close pooledQldbSession and execute the lambda when called", () => {
            const logSpy = sandbox.spy(LogUtil, "log");
            pooledQldbSession.close();
            chai.assert.equal(pooledQldbSession["_isClosed"], true);
            sinon.assert.calledOnce(logSpy);
            sinon.assert.calledWith(logSpy, "Test returning session to pool...");
        });

        it("should be a no-op when already closed", () => {
            const logSpy = sandbox.spy(LogUtil, "log");
            pooledQldbSession["_isClosed"] = true;
            pooledQldbSession.close();
            sinon.assert.notCalled(logSpy);
        });
    });

    describe("#executeLambda()", () => {
        it("should return a Result object when called", async () => {
            const executeSpy = sandbox.spy(mockQldbSession, "executeLambda");
            const query = async (txn) => {
                return await txn.executeInline(testStatement);
            };
            const retryIndicator = () => {};
            const result: Result = await pooledQldbSession.executeLambda(query, retryIndicator);
            chai.assert.equal(result, mockResult);
            sinon.assert.calledOnce(executeSpy);
            sinon.assert.calledWith(executeSpy, query, retryIndicator);
        });

        it("should return a SessionClosedError wrapped in a rejected promise when closed", async () => {
            pooledQldbSession["_isClosed"] = true;
            const executeSpy = sandbox.spy(mockQldbSession, "executeLambda");
            const error = await chai.expect(pooledQldbSession.executeLambda(async (txn) => {
                return await txn.executeInline(testStatement);
            })).to.be.rejected;
            chai.assert.equal(error.name, "SessionClosedError");
            sinon.assert.notCalled(executeSpy);
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockQldbSession.executeLambda = async () => {
                throw new Error(testMessage);
            };
            const executeSpy = sandbox.spy(mockQldbSession, "executeLambda");
            await chai.expect(pooledQldbSession.executeLambda(async (txn) => {
                return await txn.executeInline(testStatement);
            })).to.be.rejected;
            sinon.assert.calledOnce(executeSpy);
        });
    });

    describe("#executeStatement()", () => {
        it("should return a Result object when provided with a statement", async () => {
            const executeSpy = sandbox.spy(mockQldbSession, "executeStatement");
            const result: Result = await pooledQldbSession.executeStatement(testStatement);
            chai.assert.equal(result, mockResult);
            sinon.assert.calledOnce(executeSpy);
        });

        it("should return a Result object when provided with a statement and parameters", async () => {
            const executeSpy = sandbox.spy(mockQldbSession, "executeStatement");
            const mockQldbWriter = <QldbWriter><any> sandbox.mock(createQldbWriter);
            const result: Result = await pooledQldbSession.executeStatement(testStatement, [mockQldbWriter]);
            chai.assert.equal(result, mockResult);
            sinon.assert.calledOnce(executeSpy);
            sinon.assert.calledWith(executeSpy, testStatement, [mockQldbWriter]);
        });

        it("should return a SessionClosedError wrapped in a rejected promise when closed", async () => {
            pooledQldbSession["_isClosed"] = true;
            const executeSpy = sandbox.spy(mockQldbSession, "executeStatement");
            const error = await chai.expect(pooledQldbSession.executeStatement(testStatement)).to.be.rejected;
            chai.assert.equal(error.name, "SessionClosedError");
            sinon.assert.notCalled(executeSpy);
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockQldbSession.executeStatement = async () => {
                throw new Error(testMessage);
            };
            const executeSpy = sandbox.spy(mockQldbSession, "executeStatement");
            await chai.expect(pooledQldbSession.executeStatement(testStatement)).to.be.rejected;
            sinon.assert.calledOnce(executeSpy);
        });
    });

    describe("#getLedgerName()", () => {
        it("should return the ledger name when called", () => {
            const ledgerNameSpy = sandbox.spy(mockQldbSession, "getLedgerName");
            const ledgerName: string = pooledQldbSession.getLedgerName();
            chai.assert.equal(ledgerName, testLedgerName);
            sinon.assert.calledOnce(ledgerNameSpy);
        });

        it("should throw a SessionClosedError when closed", () => {
            pooledQldbSession["_isClosed"] = true;
            const ledgerNameSpy = sandbox.spy(mockQldbSession, "getLedgerName");
            chai.expect(() => {
                pooledQldbSession.getLedgerName();
            }).to.throw(SessionClosedError);
            sinon.assert.notCalled(ledgerNameSpy);
        });
    });

    describe("#getSessionToken()", () => {
        it("should return the session token when called", () => {
            const sessionTokenSpy = sandbox.spy(mockQldbSession, "getSessionToken");
            const sessionToken: string = pooledQldbSession.getSessionToken();
            chai.assert.equal(sessionToken, testSessionToken);
            sinon.assert.calledOnce(sessionTokenSpy);
        });

        it("should throw a SessionClosedError when closed", () => {
            pooledQldbSession["_isClosed"] = true;
            const sessionTokenSpy = sandbox.spy(mockQldbSession, "getSessionToken");
            chai.expect(() => {
                pooledQldbSession.getSessionToken();
            }).to.throw(SessionClosedError);
            sinon.assert.notCalled(sessionTokenSpy);
        });
    });

    describe("#getTableNames()", () => {
        it("should return a list of table names when called", async () => {
            const tableNamesSpy = sandbox.spy(mockQldbSession, "getTableNames");
            const tableNames: string[] = await pooledQldbSession.getTableNames();
            chai.assert.equal(tableNames, testTableNames);
            sinon.assert.calledOnce(tableNamesSpy);
        });

        it("should return a SessionClosedError wrapped in a rejected promise when closed", async () => {
            pooledQldbSession["_isClosed"] = true;
            const tableNamesSpy = sandbox.spy(mockQldbSession, "getTableNames");
            const error = await chai.expect(pooledQldbSession.getTableNames()).to.be.rejected;
            chai.assert.equal(error.name, "SessionClosedError");
            sinon.assert.notCalled(tableNamesSpy);
        });
    });

    describe("#startTransaction()", () => {
        it("should return a Transaction object when called", async () => {
            const transactionSpy = sandbox.spy(mockQldbSession, "startTransaction");
            const transaction: Transaction = await pooledQldbSession.startTransaction();
            chai.assert.equal(transaction, mockTransaction);
            sinon.assert.calledOnce(transactionSpy);
        });

        it("should return a SessionClosedError wrapped in a rejected promise when closed", async () => {
            pooledQldbSession["_isClosed"] = true;
            const transactionSpy = sandbox.spy(mockQldbSession, "startTransaction");
            const error = await chai.expect(pooledQldbSession.startTransaction()).to.be.rejected;
            chai.assert.equal(error.name, "SessionClosedError");
            sinon.assert.notCalled(transactionSpy);
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockQldbSession.startTransaction = async () => {
                throw new Error(testMessage);
            };
            const transactionSpy = sandbox.spy(mockQldbSession, "startTransaction");
            await chai.expect(pooledQldbSession.startTransaction()).to.be.rejected;
            sinon.assert.calledOnce(transactionSpy);
        });
    });

    describe("#_isClosed()", () => {
        it("should throw a SessionClosedError when closed", () => {
            pooledQldbSession["_isClosed"] = true;
            chai.expect(() => {
                pooledQldbSession["_throwIfClosed"]();
            }).to.throw(SessionClosedError);
        });

        it("should close the pooledQldbSession when called", () => {
            pooledQldbSession["_throwIfClosed"]();
            chai.assert.equal(pooledQldbSession["_isClosed"], false);
            pooledQldbSession.close();
            chai.assert.equal(pooledQldbSession["_isClosed"], true);
        });
    });
});
