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

import { IOUsage as ConsumedIOs, Page, TimingInformation as TimingInfo, ValueHolder} from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import { dom } from "ion-js";
import * as sinon from "sinon";

import { Communicator } from "../Communicator";
import { Result } from "../Result";
import { ResultStream } from "../ResultStream";
import { IOUsage } from "../stats/IOUsage";
import { TimingInformation } from "../stats/TimingInformation";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testTransactionId: string = "txnId";
const testMessage: string = "foo";
const testValueHolder: ValueHolder = {
    IonBinary: "ionBinary"
};
const testValues: ValueHolder[] = [testValueHolder, testValueHolder, testValueHolder];
const testPage: Page = {
    Values: testValues
};
const testPageWithToken: Page = {
    Values: testValues,
    NextPageToken: "nextPageToken"
};
const testIOUsage: IOUsage = new IOUsage(5);
const testTimingInfo: TimingInformation = new TimingInformation(20);

const mockCommunicator: Communicator = <Communicator><any> sandbox.mock(Communicator);
mockCommunicator.fetchPage = async () => {
    return {
        Page: testPage
    };
};

let resultStream: ResultStream;

describe("ResultStream", () => {

    beforeEach(() => {
        resultStream = new ResultStream(
            testTransactionId,
            testPageWithToken,
            testIOUsage,
            testTimingInfo,
            mockCommunicator
        );
    });

    afterEach(() => {
        sandbox.restore();
    });

    describe("#constructor()", () => {
        it("should have all attributes equal to mock values when constructor called", () => {
            chai.assert.equal(mockCommunicator, resultStream["_communicator"]);
            chai.assert.equal(testPageWithToken, resultStream["_cachedPage"]);
            chai.assert.equal(testTransactionId, resultStream["_txnId"]);
            chai.assert.isTrue(resultStream["_shouldPushCachedPage"]);
            chai.assert.equal(0, resultStream["_retrieveIndex"]);
            chai.assert.equal(testIOUsage, resultStream["_ioUsage"]);
            chai.assert.equal(testTimingInfo, resultStream["_timingInformation"]);
        });
    });

    describe("#_read()", () => {
        it("should call _pushPageValues() when called", () => {
            resultStream["_pushPageValues"] = async (): Promise<void> => {
                return;
            };
            const _pushPageValuesSpy = sandbox.spy(resultStream as any, "_pushPageValues");
            resultStream._read();
            sinon.assert.calledOnce(_pushPageValuesSpy);
            chai.assert.isTrue(resultStream["_isPushingData"]);
        });

        it("should return if _isPushingData is true", () => {
            resultStream["_isPushingData"] = true;
            const _pushPageValuesSpy = sandbox.spy(resultStream as any, "_pushPageValues");
            resultStream._read();
            sinon.assert.notCalled(_pushPageValuesSpy);
        });
    });

    describe("#_pushPageValues()", () => {
        it("should fully push all pages when _shouldPushCachedPage is true and next token exists", async () => {
            resultStream["_isPushingData"] = true;
            const _readStub = sandbox.stub(resultStream as any, "_read");
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
            sandbox.stub(Result, "_handleBlob");
            const domLoadStub = sandbox.stub(dom as any, "load");
            domLoadStub.onCall(0).returns(1);
            domLoadStub.onCall(1).returns(2);
            domLoadStub.returns(3);
            const pushStub = sandbox.stub(resultStream, "push");
            pushStub.returns(true);

            await resultStream["_pushPageValues"]();

            sinon.assert.notCalled(fetchPageSpy);
            sinon.assert.calledThrice(pushStub);
            sinon.assert.calledWith(pushStub.getCall(0), 1);
            sinon.assert.calledWith(pushStub.getCall(1), 2);
            sinon.assert.calledWith(pushStub.getCall(2), 3);
            sinon.assert.calledOnce(_readStub);
            chai.assert.isFalse(resultStream["_shouldPushCachedPage"]);
        });

        it("should fully push all pages when _shouldPushCachedPage is true and next token does not exist", async () => {
            resultStream["_isPushingData"] = true;
            resultStream["_cachedPage"] = testPage;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
            sandbox.stub(Result, "_handleBlob");
            const domLoadStub = sandbox.stub(dom as any, "load");
            domLoadStub.onCall(0).returns(1);
            domLoadStub.onCall(1).returns(2);
            domLoadStub.returns(3);
            const pushStub = sandbox.stub(resultStream, "push");
            pushStub.returns(true);

            await resultStream["_pushPageValues"]();

            sinon.assert.notCalled(fetchPageSpy);
            chai.assert.equal(pushStub.callCount, 4);
            sinon.assert.calledWith(pushStub.getCall(0), 1);
            sinon.assert.calledWith(pushStub.getCall(1), 2);
            sinon.assert.calledWith(pushStub.getCall(2), 3);
            sinon.assert.calledWith(pushStub.getCall(3), null);
            chai.assert.isFalse(resultStream["_shouldPushCachedPage"]);
        });

        it("should fully push relevant pages when _shouldPushCachedPage is false and next token exists", async () => {
            resultStream["_isPushingData"] = true;
            resultStream["_shouldPushCachedPage"] = false;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
            sandbox.stub(Result, "_handleBlob");
            const domLoadStub = sandbox.stub(dom as any, "load");
            domLoadStub.onCall(0).returns(1);
            domLoadStub.onCall(1).returns(2);
            domLoadStub.returns(3);
            const pushStub = sandbox.stub(resultStream, "push");
            pushStub.returns(true);

            await resultStream["_pushPageValues"]();

            sinon.assert.called(fetchPageSpy);
            chai.assert.equal(pushStub.callCount, 4);
            sinon.assert.calledWith(pushStub.getCall(0), 1);
            sinon.assert.calledWith(pushStub.getCall(1), 2);
            sinon.assert.calledWith(pushStub.getCall(2), 3);
            sinon.assert.calledWith(pushStub.getCall(3), null);
            chai.assert.isFalse(resultStream["_shouldPushCachedPage"]);
        });

        it("should push cached page and rest of the pages when previous push failed", async () => {
            resultStream["_isPushingData"] = true;
            const _readStub = sandbox.stub(resultStream as any, "_read");
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
            sandbox.stub(Result, "_handleBlob");

            const domLoadStub = sandbox.stub(dom as any, "load");
            domLoadStub.onCall(0).returns(1);
            domLoadStub.onCall(1).returns(2);
            domLoadStub.onCall(2).returns(3);
            domLoadStub.returns(4);
            const pushStub = sandbox.stub(resultStream, "push");
            pushStub.onCall(0).returns(true);
            pushStub.onCall(1).returns(false);
            pushStub.returns(true);

            await resultStream["_pushPageValues"]();

            sinon.assert.calledTwice(domLoadStub);
            sinon.assert.calledTwice(pushStub);
            sinon.assert.notCalled(_readStub);
            chai.assert.isTrue(resultStream["_shouldPushCachedPage"]);
            chai.assert.equal(2, resultStream["_retrieveIndex"]);

            await resultStream["_pushPageValues"]();

            sinon.assert.notCalled(fetchPageSpy);
            chai.assert.equal(pushStub.callCount, 3);
            sinon.assert.calledWith(pushStub.getCall(0), 1);
            sinon.assert.calledWith(pushStub.getCall(1), 2);
            sinon.assert.calledWith(pushStub.getCall(2), 3);
            sinon.assert.calledOnce(_readStub);

            chai.assert.isFalse(resultStream["_shouldPushCachedPage"]);
        });

        it("should call destroy when fetching page causes exception", async () => {
            resultStream["_isPushingData"] = true;
            resultStream["_shouldPushCachedPage"] = false;
            resultStream["_cachedPage"] = testPageWithToken;

            const destroyStub = sandbox.stub(resultStream, "destroy");
            const fetchPageStub: sinon.SinonStub = sandbox.stub(mockCommunicator, "fetchPage");
            fetchPageStub.throws(new Error(testMessage));

            await resultStream["_pushPageValues"]();

            sinon.assert.calledOnce(destroyStub);
        });

        it("should set isPushingData to false after pushing data", async () => {
            resultStream["_isPushingData"] = true;
            const _readStub = sandbox.stub(resultStream as any, "_read");
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
            sandbox.stub(Result, "_handleBlob");
            const domLoadStub = sandbox.stub(dom as any, "load");
            domLoadStub.onCall(0).returns(1);
            domLoadStub.onCall(1).returns(2);
            domLoadStub.returns(3);
            const pushStub = sandbox.stub(resultStream, "push");
            pushStub.returns(true);

            await resultStream["_pushPageValues"]();

            sinon.assert.notCalled(fetchPageSpy);
            sinon.assert.calledThrice(pushStub);
            sinon.assert.calledWith(pushStub.getCall(0), 1);
            sinon.assert.calledWith(pushStub.getCall(1), 2);
            sinon.assert.calledWith(pushStub.getCall(2), 3);
            sinon.assert.calledOnce(_readStub);
            chai.assert.isFalse(resultStream["_shouldPushCachedPage"]);
            chai.assert.isFalse(resultStream["_isPushingData"]);
        });

        it("should not call _read if stream buffer is full", async () => {
            const _readStub = sandbox.stub(resultStream as any, "_read");
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
            sandbox.stub(Result, "_handleBlob");
            const domLoadStub = sandbox.stub(dom as any, "load");
            domLoadStub.onCall(0).returns(1);
            const pushStub = sandbox.stub(resultStream, "push");
            pushStub.returns(false);

            await resultStream["_pushPageValues"]();

            sinon.assert.notCalled(fetchPageSpy);
            sinon.assert.calledOnce(pushStub);
            sinon.assert.calledWith(pushStub.getCall(0), 1);
            sinon.assert.notCalled(_readStub);
        });

        it("should call _read if ResultStream is open and stream buffer has room after pushing data", async () => {
            const _readStub = sandbox.stub(resultStream as any, "_read");
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");
            sandbox.stub(Result, "_handleBlob");
            const domLoadStub = sandbox.stub(dom as any, "load");
            domLoadStub.onCall(0).returns(1);
            domLoadStub.onCall(1).returns(2);
            domLoadStub.returns(3);
            const pushStub = sandbox.stub(resultStream, "push");
            pushStub.returns(true);

            await resultStream["_pushPageValues"]();

            sinon.assert.notCalled(fetchPageSpy);
            sinon.assert.calledThrice(pushStub);
            sinon.assert.calledWith(pushStub.getCall(0), 1);
            sinon.assert.calledWith(pushStub.getCall(1), 2);
            sinon.assert.calledWith(pushStub.getCall(2), 3);
            sinon.assert.calledOnce(_readStub);
            chai.assert.isFalse(resultStream["_shouldPushCachedPage"]);
        });
    });

    describe("#getConsumedIOs", () => {
        it("should return an IOUsage object when called", async () => {
            const ioUsage: IOUsage = resultStream.getConsumedIOs();
            chai.expect(ioUsage).to.be.an.instanceOf(IOUsage);
            chai.expect(ioUsage.getReadIOs()).to.be.eq(testIOUsage.getReadIOs());
        });

        it("should return accumulated number of IOs of first page and next pages", async () => {
            const nextPageConsumedIOs: ConsumedIOs = {
                ReadIOs: 2
            };
            mockCommunicator.fetchPage = async () => {
                return {
                    Page: testPage,
                    ConsumedIOs: nextPageConsumedIOs
                };
            };
            const expectedAccumulatedIOs: number = testIOUsage.getReadIOs() + nextPageConsumedIOs.ReadIOs;

            resultStream["_shouldPushCachedPage"] = false;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");

            await resultStream["_pushPageValues"]();

            const ioUsage: IOUsage = resultStream.getConsumedIOs();

            sinon.assert.called(fetchPageSpy);
            chai.expect(ioUsage).to.be.an.instanceOf(IOUsage);
            chai.expect(ioUsage.getReadIOs()).to.be.eq(expectedAccumulatedIOs);
        });

        it("should return correct number of IOs if first page's IOs is null but next pages have IOs", async () => {
            resultStream = new ResultStream(
                testTransactionId,
                testPageWithToken,
                null,
                testTimingInfo,
                mockCommunicator
            );
            const nextPageConsumedIOs: ConsumedIOs = {
                ReadIOs: 2
            };
            mockCommunicator.fetchPage = async () => {
                return {
                    Page: testPage,
                    ConsumedIOs: nextPageConsumedIOs
                };
            };

            resultStream["_shouldPushCachedPage"] = false;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");

            await resultStream["_pushPageValues"]();

            const ioUsage: IOUsage = resultStream.getConsumedIOs();

            sinon.assert.called(fetchPageSpy);
            chai.expect(ioUsage).to.be.an.instanceOf(IOUsage);
            chai.expect(ioUsage.getReadIOs()).to.be.eq(nextPageConsumedIOs.ReadIOs);
        });

        it("should return correct null if there is no IOs", async () => {
            resultStream = new ResultStream(
                testTransactionId,
                testPageWithToken,
                null,
                testTimingInfo,
                mockCommunicator
            );

            resultStream["_shouldPushCachedPage"] = false;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");

            await resultStream["_pushPageValues"]();

            const ioUsage: IOUsage = resultStream.getConsumedIOs();

            sinon.assert.called(fetchPageSpy);
            chai.expect(ioUsage).to.be.an.instanceOf(IOUsage);
            chai.expect(ioUsage.getReadIOs()).to.be.null;
        });
    });

    describe("#getTimingInformation", () => {
        it("should return a TimingInformation object when called", async () => {
            const timingInformation: TimingInformation = resultStream.getTimingInformation();
            chai.expect(timingInformation).to.be.an.instanceOf(TimingInformation);
            chai.expect(timingInformation.getProcessingTimeMilliseconds())
                .to.be.eq(timingInformation.getProcessingTimeMilliseconds());
        });

        it("should return accumulated processing time for first page and next pages", async () => {
            const nextPageProcessingTime: TimingInfo = {
                ProcessingTimeMilliseconds: 10
            };
            mockCommunicator.fetchPage = async () => {
                return {
                    Page: testPage,
                    TimingInformation: nextPageProcessingTime
                };
            };
            const expectedAccumulatedProcessingTim: number = testTimingInfo.getProcessingTimeMilliseconds() +
                nextPageProcessingTime.ProcessingTimeMilliseconds;

            resultStream["_shouldPushCachedPage"] = false;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");

            await resultStream["_pushPageValues"]();

            const timingInformation: TimingInformation = resultStream.getTimingInformation();

            sinon.assert.called(fetchPageSpy);
            chai.expect(timingInformation).to.be.an.instanceOf(TimingInformation);
            chai.expect(timingInformation.getProcessingTimeMilliseconds()).to.be.eq(expectedAccumulatedProcessingTim);
        });

        it("should return correct processing time if there is no time on first page but next pages has", async () => {
            resultStream = new ResultStream(
                testTransactionId,
                testPageWithToken,
                testIOUsage,
                null,
                mockCommunicator
            );
            const nextPageProcessingTime: TimingInfo = {
                ProcessingTimeMilliseconds: 10
            };
            mockCommunicator.fetchPage = async () => {
                return {
                    Page: testPage,
                    TimingInformation: nextPageProcessingTime
                };
            };

            resultStream["_shouldPushCachedPage"] = false;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");

            await resultStream["_pushPageValues"]();

            const timingInformation: TimingInformation = resultStream.getTimingInformation();

            sinon.assert.called(fetchPageSpy);
            chai.expect(timingInformation).to.be.an.instanceOf(TimingInformation);
            chai.expect(timingInformation.getProcessingTimeMilliseconds())
                .to.be.eq(nextPageProcessingTime.ProcessingTimeMilliseconds);
        });

        it("should return null if there is no processing time", async () => {
            resultStream = new ResultStream(
                testTransactionId,
                testPageWithToken,
                testIOUsage,
                null,
                mockCommunicator
            );

            resultStream["_shouldPushCachedPage"] = false;
            const fetchPageSpy = sandbox.spy(mockCommunicator, "fetchPage");

            await resultStream["_pushPageValues"]();

            const timingInformation: TimingInformation = resultStream.getTimingInformation();

            sinon.assert.called(fetchPageSpy);
            chai.expect(timingInformation).to.be.an.instanceOf(TimingInformation);
            chai.expect(timingInformation.getProcessingTimeMilliseconds()).to.be.null;
        });
    });
 });
