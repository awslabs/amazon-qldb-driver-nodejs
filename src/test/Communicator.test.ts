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
    ClientConfiguration,
    CommitTransactionResult,
    ExecuteStatementResult,
    Page,
    PageToken,
    SendCommandRequest,
    SendCommandResult,
    ValueHolder
} from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";

import { Communicator } from "../Communicator";
import * as logUtil from "../logUtil";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testLedgerName: string = "fakeLedgerName";
const testMessage: string = "foo";
const testPageToken: PageToken = "pageToken";
const testSessionToken: string = "sessionToken";
const testValueHolder: ValueHolder = {
    IonBinary: 'test'
};
const testParameters: ValueHolder[] = [testValueHolder];
const testStatement: string = "SELECT * FROM foo";
const testTransactionId: string = "txnId";
const testHashToQldb: Uint8Array = new Uint8Array([1, 2, 3]);
const testHashFromQldb: Uint8Array = new Uint8Array([4, 5, 6]);
const testLowLevelClientOptions: ClientConfiguration = {
    region: "fakeRegion"
};

const testPage: Page = {};
const testExecuteStatementResult: ExecuteStatementResult = {
    FirstPage: testPage
};
const testCommitTransactionResult: CommitTransactionResult = {
    TransactionId: testTransactionId,
    CommitDigest: testHashFromQldb
};
const testSendCommandResult: SendCommandResult = {
    StartSession: {
        SessionToken: testSessionToken
    },
    StartTransaction: {
        TransactionId: testTransactionId
    },
    FetchPage: {
        Page: testPage
    },
    ExecuteStatement: testExecuteStatementResult,
    CommitTransaction: testCommitTransactionResult
};

let sendCommandStub: sinon.SinonStub;
let testQldbLowLevelClient: QLDBSession;
let communicator: Communicator;

describe("Communicator", () => {

    beforeEach(async () => {
        testQldbLowLevelClient = new QLDBSession(testLowLevelClientOptions);
        sendCommandStub = sandbox.stub(testQldbLowLevelClient, "sendCommand");
        sendCommandStub.returns({
            promise: () => {
                return testSendCommandResult;
            }
        });
        communicator = await Communicator.create(testQldbLowLevelClient, testLedgerName);
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe("#create()", () => {
        it("should have all attributes equal to mock values when static factory method called", async () => {
            chai.assert.equal(communicator["_qldbClient"], testQldbLowLevelClient);
            chai.assert.equal(communicator["_ledgerName"], testLedgerName);
            chai.assert.equal(communicator["_sessionToken"], testSessionToken);
        });

        it("should return a rejected promise when error is thrown", async () => {
            sendCommandStub.returns({
                promise: () => {
                    throw new Error(testMessage);
                }
            });
            await chai.expect(Communicator.create(testQldbLowLevelClient, testLedgerName)).to.be.rejected;
            sinon.assert.calledTwice(sendCommandStub);
        });
    });

    describe("#abortTransaction()", () => {
        it("should call AWS SDK's sendCommand with abort request when called", async () => {
            await communicator.abortTransaction();
            const testRequest: SendCommandRequest = {
                SessionToken: testSessionToken,
                AbortTransaction: {}
            };
            sinon.assert.calledTwice(sendCommandStub);
            sinon.assert.calledWith(sendCommandStub, testRequest);
        });

        it("should return a rejected promise when error is thrown", async () => {
            sendCommandStub.returns({
                promise: () => {
                    throw new Error(testMessage);
                }
            });
            const testRequest: SendCommandRequest = {
                SessionToken: testSessionToken,
                AbortTransaction: {}
            };
            await chai.expect(communicator.abortTransaction()).to.be.rejected;
            sinon.assert.calledTwice(sendCommandStub);
            sinon.assert.calledWith(sendCommandStub, testRequest);
        });
    });

    describe("#commit()", () => {
        it("should call AWS SDK's sendCommand with commit request when called", async () => {
            const commitResult: CommitTransactionResult = await communicator.commit(testTransactionId, testHashToQldb);
            const testRequest: SendCommandRequest = {
                SessionToken: testSessionToken,
                CommitTransaction: {
                    TransactionId: testTransactionId,
                    CommitDigest: testHashToQldb
                }
            };
            sinon.assert.calledTwice(sendCommandStub);
            sinon.assert.calledWith(sendCommandStub, testRequest);
            chai.assert.equal(commitResult, testSendCommandResult.CommitTransaction);
        });

        it("should return a rejected promise when error is thrown", async () => {
            sendCommandStub.returns({
                promise: () => {
                    throw new Error(testMessage);
                }
            });
            const testRequest: SendCommandRequest = {
                SessionToken: testSessionToken,
                CommitTransaction: {
                    TransactionId: testTransactionId,
                    CommitDigest: testHashToQldb
                }
            };
            await chai.expect(communicator.commit(testTransactionId, testHashToQldb)).to.be.rejected;
            sinon.assert.calledTwice(sendCommandStub);
            sinon.assert.calledWith(sendCommandStub, testRequest);
        });
    });

    describe("#executeStatement()", () => {
        it("should return an ExecuteStatementResult object when provided with a statement", async () => {
            const result: ExecuteStatementResult = await communicator.executeStatement(
                testTransactionId,
                testStatement,
                []
            );
            const testRequest: SendCommandRequest = {
                SessionToken: testSessionToken,
                ExecuteStatement: {
                    Statement: testStatement,
                    TransactionId: testTransactionId,
                    Parameters: []
                }
            };
            sinon.assert.calledTwice(sendCommandStub);
            sinon.assert.calledWith(sendCommandStub, testRequest);
            chai.assert.equal(result, testExecuteStatementResult);
        });

        it("should return an ExecuteStatementResult object when provided with a statement and parameters", async () => {
            const result: ExecuteStatementResult = await communicator.executeStatement(
                testTransactionId,
                testStatement,
                testParameters
            );
            const testRequest: SendCommandRequest = {
                SessionToken: testSessionToken,
                ExecuteStatement: {
                    Statement: testStatement,
                    TransactionId: testTransactionId,
                    Parameters: testParameters
                }
            };
            sinon.assert.calledTwice(sendCommandStub);
            sinon.assert.calledWith(sendCommandStub, testRequest);
            chai.assert.equal(result, testExecuteStatementResult);
        });

        it("should return a rejected promise when error is thrown", async () => {
            sendCommandStub.returns({
                promise: () => {
                    throw new Error(testMessage);
                }
            });
            const testRequest: SendCommandRequest = {
                SessionToken: testSessionToken,
                ExecuteStatement: {
                    Statement: testStatement,
                    TransactionId: testTransactionId,
                    Parameters: []
                }
            };
            await chai.expect(communicator.executeStatement(testTransactionId, testStatement, [])).to.be.rejected;
            sinon.assert.calledTwice(sendCommandStub);
            sinon.assert.calledWith(sendCommandStub, testRequest);
        });
    });

    describe("#endSession()", () => {
        it("should call AWS SDK's sendCommand with end session request when called", async () => {
            await communicator.endSession();
            const testRequest: SendCommandRequest = {
                EndSession: {},
                SessionToken: testSessionToken
            };
            sinon.assert.calledTwice(sendCommandStub);
            sinon.assert.calledWith(sendCommandStub, testRequest);
        });

        it("should log a warning when error thrown", async () => {
            sendCommandStub.returns({
                promise: () => {
                    throw new Error(testMessage);
                }
            });
            const logSpy = sandbox.spy(logUtil, "warn");
            await communicator.endSession();
            const testRequest: SendCommandRequest = {
                EndSession: {},
                SessionToken: testSessionToken
            };
            sinon.assert.calledTwice(sendCommandStub);
            sinon.assert.calledWith(sendCommandStub, testRequest);
            sinon.assert.calledOnce(logSpy);
        });
    });

    describe("#fetchPage()", () => {
        it("should return a Page object when called", async () => {
            const page: Page = (await communicator.fetchPage(testTransactionId, testPageToken)).Page;
            const testRequest: SendCommandRequest = {
                SessionToken: testSessionToken,
                FetchPage: {
                    TransactionId: testTransactionId,
                    NextPageToken: testPageToken
                }
            };
            sinon.assert.calledTwice(sendCommandStub);
            sinon.assert.calledWith(sendCommandStub, testRequest);
            chai.assert.equal(page, testPage);
        });

        it("should return a rejected promise when error is thrown", async () => {
            sendCommandStub.returns({
                promise: () => {
                    throw new Error(testMessage);
                }
            });
            const testRequest: SendCommandRequest = {
                SessionToken: testSessionToken,
                FetchPage: {
                    TransactionId: testTransactionId,
                    NextPageToken: testPageToken
                }
            };
            await chai.expect(communicator.fetchPage(testTransactionId, testPageToken)).to.be.rejected;
            sinon.assert.calledTwice(sendCommandStub);
            sinon.assert.calledWith(sendCommandStub, testRequest);
        });
    });

    describe("#getLedgerName()", () => {
        it("should return the ledger name when called", () => {
            const ledgerName: string = communicator.getLedgerName();
            chai.assert.equal(ledgerName, testLedgerName);
        });
    });

    describe("#getQldbClient()", () => {
        it("should return the low level client when called", () => {
            const lowLevelClient: QLDBSession = communicator.getQldbClient();
            chai.assert.equal(lowLevelClient, testQldbLowLevelClient);
        });
    });

    describe("#getSessionToken()", () => {
        it("should return the session token when called", () => {
            const sessionToken: string = communicator.getSessionToken();
            chai.assert.equal(sessionToken, testSessionToken);
        });
    });

    describe("#startTransaction()", () => {
        it("should return the newly started transaction's transaction ID when called", async () => {
            const txnId: string = (await communicator.startTransaction()).TransactionId;
            const testRequest: SendCommandRequest = {
                SessionToken: testSessionToken,
                StartTransaction: {}
            };
            sinon.assert.calledTwice(sendCommandStub);
            sinon.assert.calledWith(sendCommandStub, testRequest);
            chai.assert.equal(txnId, testTransactionId);
        });

        it("should return a rejected promise when error is thrown", async () => {
            sendCommandStub.returns({
                promise: () => {
                    throw new Error(testMessage);
                }
            });
            const testRequest: SendCommandRequest = {
                SessionToken: testSessionToken,
                StartTransaction: {}
            };
            await chai.expect(communicator.startTransaction()).to.be.rejected;
            sinon.assert.calledTwice(sendCommandStub);
            sinon.assert.calledWith(sendCommandStub);
        });
    });

    describe("#_sendCommand()", () => {
        it("should return a SendCommandResult object when called", async () => {
            const mockSendCommandRequest: SendCommandRequest = {};
            const result: SendCommandResult = await communicator["_sendCommand"](mockSendCommandRequest);
            sinon.assert.calledTwice(sendCommandStub);
            sinon.assert.calledWith(sendCommandStub, mockSendCommandRequest);
            chai.assert.equal(result, testSendCommandResult);
        });

        it("should return a rejected promise when error is thrown", async () => {
            sendCommandStub.returns({
                promise: () => {
                    throw new Error(testMessage);
                }
            });
            const mockSendCommandRequest: SendCommandRequest = {};
            const sendCommand = communicator["_sendCommand"];
            await chai.expect(sendCommand(mockSendCommandRequest)).to.be.rejected;
        });
    });
});
