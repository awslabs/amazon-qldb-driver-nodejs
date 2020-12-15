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
    ExecuteStatementResult,
    IonBinary,
    IOUsage as ConsumedIOs,
    Page,
    TimingInformation as TimingInfo,
    ValueHolder
} from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import { dom, IonType, IonTypes} from "ion-js";
import * as sinon from "sinon";
import { Readable } from "stream";

import { Communicator } from "../Communicator";
import { ClientException } from "../errors/Errors";
import { Result } from "../Result";
import { ResultStream } from "../ResultStream";
import { IOUsageImp } from "../stats/IOUsageImp";
import { TimingInformationImp } from "../stats/TimingInformationImp";
import { IOUsage } from "../stats/IOUsage";
import { TimingInformation } from "../stats/TimingInformation";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testMessage: string = "foo";
const testNextPageToken: string = "nextPageToken";
const testTransactionId: string = "txnId";

const testValueHolder: ValueHolder[] = [];
const testPage: Page = {
    Values: testValueHolder
};
const testPageWithToken: Page = {
    Values: testValueHolder,
    NextPageToken: testNextPageToken
};
const testExecuteResultWithNextPage: ExecuteStatementResult = {
    FirstPage: testPageWithToken,
    TimingInformation: {
        ProcessingTimeMilliseconds: 20
    },
    ConsumedIOs: {
        ReadIOs: 5
    }
};
const testExecuteResult: ExecuteStatementResult = {
    FirstPage: testPage,
    TimingInformation: {
        ProcessingTimeMilliseconds: 20
    },
    ConsumedIOs: {
        ReadIOs: 5
    }
};
const testIOUsage: IOUsageImp = new IOUsageImp(5);
const testTimingInfo: TimingInformationImp = new TimingInformationImp(20);

const mockCommunicator: Communicator = <Communicator><any> sandbox.mock(Communicator);

describe("Result", () => {

    afterEach(() => {
        sandbox.restore();
    });

    describe("#create()", () => {
        it("should return a Result object when called", async () => {
            const result = await Result.create(testTransactionId, testExecuteResult, mockCommunicator);
            chai.expect(result).to.be.an.instanceOf(Result);
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockCommunicator.fetchPage = async () => {
                throw new Error(testMessage);
            };
            await chai.expect(Result.create(testTransactionId, testExecuteResultWithNextPage, mockCommunicator)).to.be.rejected;
        });
    });

    describe("#bufferResultStream()", () => {
        it("should return a Result object when called", async () => {
            const sampleResultStreamObject: ResultStream = new ResultStream(
                testTransactionId,
                testExecuteResult,
                mockCommunicator
            );
            const result = await Result.bufferResultStream(sampleResultStreamObject);
            chai.expect(result).to.be.an.instanceOf(Result);
        });
    });

    describe("#getResultList()", () => {
        it("should return a list of Ion values when called", async () => {
            const value1: ValueHolder = {IonBinary: "a"};
            const value2: ValueHolder = {IonBinary: "b"};
            const value3: ValueHolder = {IonBinary: "c"};
            const value4: ValueHolder = {IonBinary: "d"};
            const allValues: ValueHolder[] = [value1, value2, value3, value4];
            const finalTestPage: Page = {Values: allValues};

            mockCommunicator.fetchPage = async () => {
                return {
                    Page: finalTestPage
                };
            };
            const result: Result = await Result.create(testTransactionId, testExecuteResultWithNextPage, mockCommunicator);
            const resultList: dom.Value[] = result.getResultList();

            resultList.forEach((result, i) => {
                chai.assert.equal(
                    result,
                    allValues[i].IonBinary
                );
            });
        });

        it("should return a list of Ion values with correct Ion types when called", async () => {
            const expectedTypes: IonType[] = [
                IonTypes.NULL,
                IonTypes.BOOL,
                IonTypes.INT,
                IonTypes.FLOAT,
                IonTypes.DECIMAL,
                IonTypes.TIMESTAMP,
                IonTypes.SYMBOL,
                IonTypes.STRING,
                IonTypes.CLOB,
                IonTypes.BLOB,
                IonTypes.SEXP,
                IonTypes.LIST,
                IonTypes.STRUCT
            ];
            const nullValue: ValueHolder = {IonBinary: "null"};
            const bool: ValueHolder = {IonBinary: "true"};
            const int: ValueHolder = {IonBinary: "5"};
            const float: ValueHolder = {IonBinary: "5e3"};
            const decimal: ValueHolder = {IonBinary: "5.5"};
            const time: ValueHolder = {IonBinary: "2017-01-01"};
            const symbol: ValueHolder = {IonBinary: "Symbol"};
            const string: ValueHolder = {IonBinary: "\"String\""};
            const clob: ValueHolder = {IonBinary: "{{ \"clob\" }}"};
            const blob: ValueHolder = {IonBinary: "{{ blob }}"};
            const sexp: ValueHolder = {IonBinary: "(1 2 3)"};
            const list: ValueHolder = {IonBinary: "[1, 2, 3]"};
            const struct: ValueHolder = {IonBinary: "{key: val}"};
            const allValues: ValueHolder[] = [
                nullValue,
                bool,
                int,
                float,
                decimal,
                time,
                symbol,
                string,
                clob,
                blob,
                sexp,
                list,
                struct
            ];
            const finalTestPage: Page = {Values: allValues};

            mockCommunicator.fetchPage = async () => {
                return {
                    Page: finalTestPage
                };
            };
            const testExecuteStatementResult: ExecuteStatementResult = {
                FirstPage: testPageWithToken,
            };
            const result: Result = await Result.create(testTransactionId, testExecuteStatementResult, mockCommunicator);
            const resultList: dom.Value[] = result.getResultList();

            resultList.forEach((result, i) => {
                chai.assert.equal(
                    result.getType(),
                    expectedTypes[i]
                );
            });
        });

        it("should return a list of Ion values for nested ion containers with correct Ion types", async () => {
            const allValues: ValueHolder[] = [{IonBinary: "{key: ([1, 2] {innerStruct: [3]} 4)}"}];
            const finalTestPage: Page = {Values: allValues};

            mockCommunicator.fetchPage = async () => {
                return {
                    Page: finalTestPage
                };
            };
            const result: Result = await Result.create(testTransactionId, testExecuteResultWithNextPage, mockCommunicator);
            const value: dom.Value = result.getResultList()[0];

            chai.assert.equal(value.getType(), IonTypes.STRUCT);
            chai.assert.equal(value.get("key").getType(), IonTypes.SEXP);
            chai.assert.equal(value.get("key").get(0).getType(), IonTypes.LIST);
            chai.assert.equal(value.get("key").get(1).getType(), IonTypes.STRUCT);
            chai.assert.equal(value.get("key").get(1).get("innerStruct").getType(), IonTypes.LIST);
            chai.assert.equal(value.get("key").get(2).getType(), IonTypes.INT);
        });

        it("should return a list of Ion values that include the initial Page when called", async () => {
            const value1: ValueHolder = {IonBinary: "a"};
            const value2: ValueHolder = {IonBinary: "b"};
            const value3: ValueHolder = {IonBinary: "c"};
            const value4: ValueHolder = {IonBinary: "d"};
            const allValues: ValueHolder[] = [value1, value2, value3, value4];
            const finalTestPage: Page = {Values: allValues};

            const testValueHolder: ValueHolder[] = [{
                IonBinary: "testVal"
            }];
            const testPageWithTokenAndValue: Page = {
                Values: testValueHolder,
                NextPageToken: testNextPageToken
            };
            const testExecuteStatementResult: ExecuteStatementResult = {
                FirstPage: testPageWithTokenAndValue,
            };

            mockCommunicator.fetchPage = async () => {
                return {
                    Page: finalTestPage
                };
            };
            const result: Result = await Result.create(testTransactionId, testExecuteStatementResult, mockCommunicator);
            const resultList: dom.Value[] = result.getResultList();

            chai.assert.equal(allValues.length + testValueHolder.length, resultList.length);
            // Need to check if the initial Page's value and the first element in resultList is equivalent.
            chai.assert.equal(
                resultList[0],
                testValueHolder[0].IonBinary
            );

            // Now check if the rest of the resultList matches up with the Page's values returned from the Communicator.
            for (let i = 0; i < allValues.length; i++) {
                chai.assert.equal(
                    resultList[i+1],
                    allValues[i].IonBinary
                );
            }
        });

        it("should return a list of Ion values when Result object created with bufferResultStream()", async () => {
            const value1: ValueHolder = {IonBinary: "a"};
            const value2: ValueHolder = {IonBinary: "b"};
            const value3: ValueHolder = {IonBinary: "c"};
            const value4: ValueHolder = {IonBinary: "d"};
            const values: dom.Value[] = [
                dom.load(Result._handleBlob(value1.IonBinary)),
                dom.load(Result._handleBlob(value2.IonBinary)),
                dom.load(Result._handleBlob(value3.IonBinary)),
                dom.load(Result._handleBlob(value4.IonBinary))
            ];
            const allValues: ValueHolder[] = [value1, value2, value3, value4];
            const testPage: Page = {Values: allValues};

            mockCommunicator.fetchPage = async () => {
                return {
                    Page: testPage
                };
            };
            const testExecuteResult: ExecuteStatementResult = {
                FirstPage: testPage,
            };
            const mockResultStream: ResultStream = new ResultStream(testTransactionId, testExecuteResult, mockCommunicator);

            const result: Result = await Result.bufferResultStream(<ResultStream> mockResultStream);
            const resultList: dom.Value[] = result.getResultList();

            chai.assert.equal(values.length, resultList.length);
            resultList.forEach((result, i) => {
                chai.assert.deepEqual(
                    result,
                    values[i]
                );
            });
        });
    });

    describe("#getConsumedIOs", () => {
       it("should return an IOUsage object with correct value when called without next page", async () => {
           const result: Result = await Result.create(
               testTransactionId,
               testExecuteResult,
               mockCommunicator
           );
           const ioUsage: IOUsage = result.getConsumedIOs();
           chai.expect(ioUsage).to.be.an.instanceOf(IOUsageImp);
           chai.expect(ioUsage.getReadIOs()).to.be.eq(testIOUsage.getReadIOs());
       });

        it("should return null if there are no IOs", async () => {
            const testExecuteResult: ExecuteStatementResult = {
                FirstPage: testPage,
                ConsumedIOs: null
            };

            const result: Result = await Result.create(
                testTransactionId,
                testExecuteResult,
                mockCommunicator
            );

            chai.expect(result.getConsumedIOs()).to.be.null;
        });

       it("should return accumulated number of IOs of the first page and next pages", async () => {
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
           const result: Result = await Result.create(
               testTransactionId,
               testExecuteResultWithNextPage,
               mockCommunicator
           );
           const ioUsage: IOUsage = result.getConsumedIOs();
           chai.expect(ioUsage).to.be.an.instanceOf(IOUsageImp);
           chai.expect(ioUsage.getReadIOs()).to.be.eq(expectedAccumulatedIOs);
       });

       it("should return correct number of IOs if first page's IOs is null but next pages have IOs", async () => {
           const testExecuteResultWithNextPage: ExecuteStatementResult = {
               FirstPage: testPageWithToken,
               ConsumedIOs: null
           };

           const result: Result = await Result.create(
               testTransactionId,
               testExecuteResultWithNextPage,
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

           const ioUsage: IOUsage = result.getConsumedIOs();
           chai.expect(ioUsage.getReadIOs()).to.be.eq(nextPageConsumedIOs.ReadIOs);
       });
    });

    describe("#getTimingInformation", () => {
       it("should return an TimeInformation object with correct value when called without next page", async () => {
           const result: Result = await Result.create(
               testTransactionId,
               testExecuteResult,
               mockCommunicator
           );
           const timingInformation: TimingInformation = result.getTimingInformation();
           chai.expect(timingInformation).to.be.an.instanceOf(TimingInformationImp);
           chai.expect(timingInformation.getProcessingTimeMilliseconds())
               .to.be.eq(testTimingInfo.getProcessingTimeMilliseconds());
       });

       it("should return null if there are is no processing time", async () => {
            const testExecuteResult: ExecuteStatementResult = {
                FirstPage: testPage,
                TimingInformation: null
            };

            const result: Result = await Result.create(
                testTransactionId,
                testExecuteResult,
                mockCommunicator
            );

            chai.expect(result.getTimingInformation()).to.be.null;
        });

       it("should return accumulated processing time of the first page and next pages", async () => {
           const nextTimingInfo: TimingInfo = {
               ProcessingTimeMilliseconds: 10
           };
           mockCommunicator.fetchPage = async () => {
               return {
                   Page: testPage,
                   TimingInformation: nextTimingInfo
               };
           };
           const expectedAccumulatedTime: number = testTimingInfo.getProcessingTimeMilliseconds()
               + nextTimingInfo.ProcessingTimeMilliseconds;
           const result: Result = await Result.create(
               testTransactionId,
               testExecuteResultWithNextPage,
               mockCommunicator
           );
           const timingInformation: TimingInformation = result.getTimingInformation();
           chai.expect(timingInformation).to.be.an.instanceOf(TimingInformationImp);
           chai.expect(timingInformation.getProcessingTimeMilliseconds()).to.be.eq(expectedAccumulatedTime);
       });

       it("should return correct number of IOs if first page's IOs is null but next pages have IOs", async () => {
           const testExecuteResultWithNextPage: ExecuteStatementResult = {
               FirstPage: testPageWithToken,
               TimingInformation: null
           };

           const result: Result = await Result.create(
               testTransactionId,
               testExecuteResultWithNextPage,
               mockCommunicator
           );
           const nextPageTimingInfo: TimingInfo = {
               ProcessingTimeMilliseconds: 10
           };
           mockCommunicator.fetchPage = async () => {
               return {
                   Page: testPage,
                   TimingInformation: {
                       ProcessingTimeMilliseconds: 10
                   }
               };
           };

           const timingInformation: TimingInformation = result.getTimingInformation();
           chai.expect(timingInformation.getProcessingTimeMilliseconds())
               .to.be.eq(nextPageTimingInfo.ProcessingTimeMilliseconds);
       });
    });

    describe("#_handleBlob()", () => {
        it("should return a Buffer object when Blob is an instance of Buffer", async () => {
            const blobBuffer: IonBinary = Buffer.from([0x62, 0x75, 0x66, 0x66, 0x65, 0x72]);
            chai.expect(Result["_handleBlob"](blobBuffer)).to.be.an.instanceOf(Buffer);
        });

        it("should return a Uint8Array object when Blob is an instance of Uint8Array", async () => {
            const blobUint8Array: IonBinary = new Uint8Array([1, 2, 3]);
            chai.expect(Result["_handleBlob"](blobUint8Array)).to.be.an.instanceOf(Uint8Array);
        });

        it("should return a string when Blob is an instance of string", async () => {
            const blobString: IonBinary = "test";
            chai.expect(Result["_handleBlob"](blobString)).to.be.a("string");
        });

        it("should throw a ClientException when Blob is an invalid type", async () => {
            const invalidBlob: IonBinary = 123;
            chai.expect(() => {
                Result["_handleBlob"](invalidBlob);
            }).to.throw(ClientException);
        });
    });

    describe("#_getIOUsage()", () => {
        it("should return correct IOUsage", async () => {
            chai.expect(Result["_getIOUsage"](null)).to.be.null;
        });

        it("should return null when there is no IOUsage", async () => {
            const consumedIOs: ConsumedIOs = testExecuteResult.ConsumedIOs;

            const ioUsage = Result["_getIOUsage"](consumedIOs);

            chai.expect(ioUsage).to.be.an.instanceOf(IOUsageImp);
            chai.assert.deepEqual(ioUsage, testIOUsage);
        });
    });

    describe("#_getTimingInformation()", () => {
        it("should return correct TimingInformation", async () => {
            chai.expect(Result["_getTimingInformation"](null)).to.be.null;
        });

        it("should return null when there is no TimingInformation", async () => {
            const timingInfo: TimingInfo = testExecuteResult.TimingInformation;

            const timingInformation = Result["_getTimingInformation"](timingInfo);

            chai.expect(timingInformation).to.be.an.instanceOf(TimingInformationImp);
            chai.assert.deepEqual(timingInformation, testTimingInfo);
        });
    });
});
