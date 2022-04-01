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

import { ExecuteStatementResult, Page, ValueHolder, TimingInformation as sdkTimingInformation, IOUsage as sdkIOUsage } from "@aws-sdk/client-qldb-session";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import { dom } from "ion-js";
import * as sinon from "sinon";

import { Communicator } from "../Communicator";
import { Result } from "../Result";
import { ResultReadable } from "../ResultReadable";
import { IOUsage } from "../stats/IOUsage";
import { TimingInformation } from "../stats/TimingInformation";
import { TextEncoder } from "util";

const enc = new TextEncoder();

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testTransactionId: string = "txnId";
const testMessage: string = "foo";
const testValueHolder: ValueHolder = {
    IonBinary: enc.encode("ionBinary")
};
const testValues: ValueHolder[] = [testValueHolder, testValueHolder, testValueHolder];
const testPage: Page = {
    Values: testValues
};
const testPageWithToken: Page = {
    Values: testValues,
    NextPageToken: "nextPageToken"
};
const testExecuteStatementResult: ExecuteStatementResult = {
    FirstPage: testPageWithToken,
    TimingInformation: {
        ProcessingTimeMilliseconds: 20
    },
    ConsumedIOs: {
        ReadIOs: 5
    }
};
const testIOUsage: IOUsage = new IOUsage(5);
const testTimingInfo: TimingInformation = new TimingInformation(20);

const mockCommunicator: Communicator = <Communicator><any> sandbox.mock(Communicator);
mockCommunicator.fetchPage = async () => {
    return {
        Page: testPage
    };
};

let resultReadable: ResultReadable;

describe("ResultReadable", () => {

    beforeEach(() => {
        resultReadable = new ResultReadable(
            testTransactionId,
            testExecuteStatementResult,
            mockCommunicator
        );
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe("#constructor()", () => {
        it("should have all attributes equal to mock values when constructor called", () => {
            chai.assert.equal(mockCommunicator, resultReadable["_communicator"]);
            chai.assert.equal(testPageWithToken, resultReadable["_cachedPage"]);
            chai.assert.equal(testTransactionId, resultReadable["_txnId"]);
            chai.assert.isTrue(resultReadable["_shouldPushCachedPage"]);
            chai.assert.equal(0, resultReadable["_retrieveIndex"]);
            chai.assert.equal(testIOUsage.getReadIOs(), resultReadable["_readIOs"]);
            chai.assert.equal(testTimingInfo.getProcessingTimeMilliseconds(), resultReadable["_processingTime"]);
        });
    });

    describe("#_read()", () => {
        it("should call _pushPageValues() when called", () => {
            resultReadable["_pushPageValues"] = async (): Promise<void> => {
                return;
            };
            const _pushPageValuesSpy = sandbox.spy(resultReadable as any, "_pushPageValues");
            resultReadable._read();
            sinon.assert.calledOnce(_pushPageValuesSpy);
            chai.assert.isTrue(resultReadable["_isPushingData"]);
        });

        it("should return if _isPushingData is true", () => {
            resultReadable["_isPushingData"] = true;
            const _pushPageValuesSpy = sandbox.spy(resultReadable as any, "_pushPageValues");
            resultReadable._read();
            sinon.assert.notCalled(_pushPageValuesSpy);
        });
    });

    describe("#_pushPageValues()", () => {
        it("should fully push all pages when _shouldPushCachedPage is true and next token exists", async () => {
            resultReadable["_isPushingData"] = true;
            const _readStub = sandbox.stub(resultReadable as any, "_read");
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
            sandbox.stub(Result, "_handleBlob");
            const domLoadStub = sandbox.stub(dom as any, "load");
            domLoadStub.onCall(0).returns(1);
            domLoadStub.onCall(1).returns(2);
            domLoadStub.returns(3);
            const pushStub = sandbox.stub(resultReadable, "push");
            pushStub.returns(true);

            await resultReadable["_pushPageValues"]();

            sinon.assert.notCalled(fetchPageSpy);
            sinon.assert.calledThrice(pushStub);
            sinon.assert.calledWith(pushStub.getCall(0), 1);
            sinon.assert.calledWith(pushStub.getCall(1), 2);
            sinon.assert.calledWith(pushStub.getCall(2), 3);
            sinon.assert.calledOnce(_readStub);
            chai.assert.isFalse(resultReadable["_shouldPushCachedPage"]);
        });

        it("should fully push all pages when _shouldPushCachedPage is true and next token does not exist", async () => {
            resultReadable["_isPushingData"] = true;
            resultReadable["_cachedPage"] = testPage;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
            sandbox.stub(Result, "_handleBlob");
            const domLoadStub = sandbox.stub(dom as any, "load");
            domLoadStub.onCall(0).returns(1);
            domLoadStub.onCall(1).returns(2);
            domLoadStub.returns(3);
            const pushStub = sandbox.stub(resultReadable, "push");
            pushStub.returns(true);

            await resultReadable["_pushPageValues"]();

            sinon.assert.notCalled(fetchPageSpy);
            chai.assert.equal(pushStub.callCount, 4);
            sinon.assert.calledWith(pushStub.getCall(0), 1);
            sinon.assert.calledWith(pushStub.getCall(1), 2);
            sinon.assert.calledWith(pushStub.getCall(2), 3);
            sinon.assert.calledWith(pushStub.getCall(3), null);
            chai.assert.isFalse(resultReadable["_shouldPushCachedPage"]);
        });

        it("should fully push relevant pages when _shouldPushCachedPage is false and next token exists", async () => {
            resultReadable["_isPushingData"] = true;
            resultReadable["_shouldPushCachedPage"] = false;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
            sandbox.stub(Result, "_handleBlob");
            const domLoadStub = sandbox.stub(dom as any, "load");
            domLoadStub.onCall(0).returns(1);
            domLoadStub.onCall(1).returns(2);
            domLoadStub.returns(3);
            const pushStub = sandbox.stub(resultReadable, "push");
            pushStub.returns(true);

            await resultReadable["_pushPageValues"]();

            sinon.assert.called(fetchPageSpy);
            chai.assert.equal(pushStub.callCount, 4);
            sinon.assert.calledWith(pushStub.getCall(0), 1);
            sinon.assert.calledWith(pushStub.getCall(1), 2);
            sinon.assert.calledWith(pushStub.getCall(2), 3);
            sinon.assert.calledWith(pushStub.getCall(3), null);
            chai.assert.isFalse(resultReadable["_shouldPushCachedPage"]);
        });

        it("should push cached page and rest of the pages when previous push failed", async () => {
            resultReadable["_isPushingData"] = true;
            const _readStub = sandbox.stub(resultReadable as any, "_read");
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
            sandbox.stub(Result, "_handleBlob");

            const domLoadStub = sandbox.stub(dom as any, "load");
            domLoadStub.onCall(0).returns(1);
            domLoadStub.onCall(1).returns(2);
            domLoadStub.onCall(2).returns(3);
            domLoadStub.returns(4);
            const pushStub = sandbox.stub(resultReadable, "push");
            pushStub.onCall(0).returns(true);
            pushStub.onCall(1).returns(false);
            pushStub.returns(true);

            await resultReadable["_pushPageValues"]();

            sinon.assert.calledTwice(domLoadStub);
            sinon.assert.calledTwice(pushStub);
            sinon.assert.notCalled(_readStub);
            chai.assert.isTrue(resultReadable["_shouldPushCachedPage"]);
            chai.assert.equal(2, resultReadable["_retrieveIndex"]);

            await resultReadable["_pushPageValues"]();

            sinon.assert.notCalled(fetchPageSpy);
            chai.assert.equal(pushStub.callCount, 3);
            sinon.assert.calledWith(pushStub.getCall(0), 1);
            sinon.assert.calledWith(pushStub.getCall(1), 2);
            sinon.assert.calledWith(pushStub.getCall(2), 3);
            sinon.assert.calledOnce(_readStub);

            chai.assert.isFalse(resultReadable["_shouldPushCachedPage"]);
        });

        it("should call destroy when fetching page causes exception", async () => {
            resultReadable["_isPushingData"] = true;
            resultReadable["_shouldPushCachedPage"] = false;
            resultReadable["_cachedPage"] = testPageWithToken;

            const destroyStub = sandbox.stub(resultReadable, "destroy");
            const fetchPageStub: sinon.SinonStub = sandbox.stub(mockCommunicator, "fetchPage");
            fetchPageStub.throws(new Error(testMessage));

            await resultReadable["_pushPageValues"]();

            sinon.assert.calledOnce(destroyStub);
        });

        it("should set isPushingData to false after pushing data", async () => {
            resultReadable["_isPushingData"] = true;
            const _readStub = sandbox.stub(resultReadable as any, "_read");
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
            sandbox.stub(Result, "_handleBlob");
            const domLoadStub = sandbox.stub(dom as any, "load");
            domLoadStub.onCall(0).returns(1);
            domLoadStub.onCall(1).returns(2);
            domLoadStub.returns(3);
            const pushStub = sandbox.stub(resultReadable, "push");
            pushStub.returns(true);

            await resultReadable["_pushPageValues"]();

            sinon.assert.notCalled(fetchPageSpy);
            sinon.assert.calledThrice(pushStub);
            sinon.assert.calledWith(pushStub.getCall(0), 1);
            sinon.assert.calledWith(pushStub.getCall(1), 2);
            sinon.assert.calledWith(pushStub.getCall(2), 3);
            sinon.assert.calledOnce(_readStub);
            chai.assert.isFalse(resultReadable["_shouldPushCachedPage"]);
            chai.assert.isFalse(resultReadable["_isPushingData"]);
        });

        it("should not call _read if stream buffer is full", async () => {
            const _readStub = sandbox.stub(resultReadable as any, "_read");
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
            sandbox.stub(Result, "_handleBlob");
            const domLoadStub = sandbox.stub(dom as any, "load");
            domLoadStub.onCall(0).returns(1);
            const pushStub = sandbox.stub(resultReadable, "push");
            pushStub.returns(false);

            await resultReadable["_pushPageValues"]();

            sinon.assert.notCalled(fetchPageSpy);
            sinon.assert.calledOnce(pushStub);
            sinon.assert.calledWith(pushStub.getCall(0), 1);
            sinon.assert.notCalled(_readStub);
        });

        it("should call _read if ResultReadable is open and stream buffer has room after pushing data", async () => {
            const _readStub = sandbox.stub(resultReadable as any, "_read");
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
            sandbox.stub(Result, "_handleBlob");
            const domLoadStub = sandbox.stub(dom as any, "load");
            domLoadStub.onCall(0).returns(1);
            domLoadStub.onCall(1).returns(2);
            domLoadStub.returns(3);
            const pushStub = sandbox.stub(resultReadable, "push");
            pushStub.returns(true);

            await resultReadable["_pushPageValues"]();

            sinon.assert.notCalled(fetchPageSpy);
            sinon.assert.calledThrice(pushStub);
            sinon.assert.calledWith(pushStub.getCall(0), 1);
            sinon.assert.calledWith(pushStub.getCall(1), 2);
            sinon.assert.calledWith(pushStub.getCall(2), 3);
            sinon.assert.calledOnce(_readStub);
            chai.assert.isFalse(resultReadable["_shouldPushCachedPage"]);
        });
    });

    describe("#getConsumedIOs", () => {
        it("should return an IOUsage object with correct value without IO on next page in result", async () => {
            mockCommunicator.fetchPage = async () => {
                return {
                    Page: testPage,
                    ConsumedIOs: null
                };
            };
            await resultReadable["_pushPageValues"]();

            const ioUsage: IOUsage = resultReadable.getConsumedIOs();
            chai.expect(ioUsage).to.be.an.instanceOf(IOUsage);
            chai.expect(ioUsage.getReadIOs()).to.be.eq(testIOUsage.getReadIOs());
        });

        it("should return null if there are no IOs", async () => {
            const testExecuteStatementResult: ExecuteStatementResult = {
                FirstPage: testPageWithToken,
                ConsumedIOs: null
            };
            resultReadable = new ResultReadable(
                testTransactionId,
                testExecuteStatementResult,
                mockCommunicator
            );

            resultReadable["_shouldPushCachedPage"] = false;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");

            await resultReadable["_pushPageValues"]();

            sinon.assert.called(fetchPageSpy);
            chai.expect(resultReadable.getConsumedIOs()).to.be.null;
        });

        it("should return accumulated number of IOs of the first page and next pages", async () => {
            const nextPageConsumedIOs: sdkIOUsage = {
                ReadIOs: 2
            };
            mockCommunicator.fetchPage = async () => {
                return {
                    Page: testPage,
                    ConsumedIOs: nextPageConsumedIOs
                };
            };
            const expectedAccumulatedIOs: number = testIOUsage.getReadIOs() + nextPageConsumedIOs.ReadIOs;

            resultReadable["_shouldPushCachedPage"] = false;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");

            await resultReadable["_pushPageValues"]();

            const ioUsage: IOUsage = resultReadable.getConsumedIOs();

            sinon.assert.called(fetchPageSpy);
            chai.expect(ioUsage).to.be.an.instanceOf(IOUsage);
            chai.expect(ioUsage.getReadIOs()).to.be.eq(expectedAccumulatedIOs);
        });

        it("should return correct number of IOs if first page's IOs is null but next pages have IOs", async () => {
            const testExecuteStatementResult: ExecuteStatementResult = {
                FirstPage: testPageWithToken,
                ConsumedIOs: null
            };
            resultReadable = new ResultReadable(
                testTransactionId,
                testExecuteStatementResult,
                mockCommunicator
            );
            const nextPageConsumedIOs: sdkIOUsage = {
                ReadIOs: 2
            };
            mockCommunicator.fetchPage = async () => {
                return {
                    Page: testPage,
                    ConsumedIOs: nextPageConsumedIOs
                };
            };

            resultReadable["_shouldPushCachedPage"] = false;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");

            await resultReadable["_pushPageValues"]();

            const ioUsage: IOUsage = resultReadable.getConsumedIOs();

            sinon.assert.called(fetchPageSpy);
            chai.expect(ioUsage).to.be.an.instanceOf(IOUsage);
            chai.expect(ioUsage.getReadIOs()).to.be.eq(nextPageConsumedIOs.ReadIOs);
        });
    });

    describe("#getTimingInformation", () => {
        it("should return a TimingInformation object when called without TimingInformation on next page", async () => {
            mockCommunicator.fetchPage = async () => {
                return {
                    Page: testPage,
                    TimingInformation: null
                };
            };
            await resultReadable["_pushPageValues"]();

            const timingInformation: TimingInformation = resultReadable.getTimingInformation();
            chai.expect(timingInformation).to.be.an.instanceOf(TimingInformation);
            chai.expect(timingInformation.getProcessingTimeMilliseconds())
                .to.be.eq(timingInformation.getProcessingTimeMilliseconds());
        });

        it("should return null if there is no processing time", async () => {
            const testExecuteStatementResult: ExecuteStatementResult = {
                FirstPage: testPageWithToken,
                TimingInformation: null
            };
            resultReadable = new ResultReadable(
                testTransactionId,
                testExecuteStatementResult,
                mockCommunicator
            );

            resultReadable["_shouldPushCachedPage"] = false;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");

            await resultReadable["_pushPageValues"]();

            sinon.assert.called(fetchPageSpy);
            chai.expect(resultReadable.getTimingInformation()).to.be.null;
        });

        it("should return accumulated processing time for the first page and next pages", async () => {
            const nextPageProcessingTime: sdkTimingInformation = {
                ProcessingTimeMilliseconds: 10
            };
            mockCommunicator.fetchPage = async () => {
                return {
                    Page: testPage,
                    TimingInformation: nextPageProcessingTime
                };
            };
            const expectedAccumulatedProcessingTime: number = testTimingInfo.getProcessingTimeMilliseconds() +
                nextPageProcessingTime.ProcessingTimeMilliseconds;

            resultReadable["_shouldPushCachedPage"] = false;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");

            await resultReadable["_pushPageValues"]();

            const timingInformation: TimingInformation = resultReadable.getTimingInformation();

            sinon.assert.called(fetchPageSpy);
            chai.expect(timingInformation).to.be.an.instanceOf(TimingInformation);
            chai.expect(timingInformation.getProcessingTimeMilliseconds()).to.be.eq(expectedAccumulatedProcessingTime);
        });

        it("should return correct processing time if there is no time on first page but next pages has", async () => {
            const testExecuteStatementResult: ExecuteStatementResult = {
                FirstPage: testPageWithToken,
                TimingInformation: null
            };
            resultReadable = new ResultReadable(
                testTransactionId,
                testExecuteStatementResult,
                mockCommunicator
            );
            const nextPageProcessingTime: sdkTimingInformation = {
                ProcessingTimeMilliseconds: 10
            };
            mockCommunicator.fetchPage = async () => {
                return {
                    Page: testPage,
                    TimingInformation: nextPageProcessingTime
                };
            };

            resultReadable["_shouldPushCachedPage"] = false;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");

            await resultReadable["_pushPageValues"]();

            const timingInformation: TimingInformation = resultReadable.getTimingInformation();

            sinon.assert.called(fetchPageSpy);
            chai.expect(timingInformation).to.be.an.instanceOf(TimingInformation);
            chai.expect(timingInformation.getProcessingTimeMilliseconds())
                .to.be.eq(nextPageProcessingTime.ProcessingTimeMilliseconds);
        });
    });
 });
