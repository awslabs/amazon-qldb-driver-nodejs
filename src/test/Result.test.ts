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

import { IonBinary, Page, ValueHolder} from "aws-sdk/clients/qldbsession";
import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import { makeReader, Reader} from "ion-js";
import * as sinon from "sinon";
import { Readable } from "stream";

import { Communicator } from "../Communicator";
import { ClientException } from "../errors/Errors";
import { Result } from "../Result";
import { ResultStream } from "../ResultStream";

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

const mockCommunicator: Communicator = <Communicator><any> sandbox.mock(Communicator);

describe("Result", () => {

    afterEach(() => {
        sandbox.restore();
    });

    describe("#create()", () => {
        it("should return a Result object when called", async () => {
            const result = await Result.create(testTransactionId, testPage, mockCommunicator);
            chai.expect(result).to.be.an.instanceOf(Result);
        });

        it("should return a rejected promise when error is thrown", async () => {
            mockCommunicator.fetchPage = async () => {
                throw new Error(testMessage);
            };
            await chai.expect(Result.create(testTransactionId, testPageWithToken, mockCommunicator)).to.be.rejected;
        });
    });

    describe("#bufferResultStream()", () => {
        it("should return a Result object when called", async () => {
            const sampleResultStreamObject: ResultStream = new ResultStream(
                testTransactionId,
                testPage,
                mockCommunicator
            );
            const result = await Result.bufferResultStream(sampleResultStreamObject);
            chai.expect(result).to.be.an.instanceOf(Result);
        });
    });

    describe("#getResultList()", () => {
        it("should return a list of Readers when called", async () => {
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
            const result: Result = await Result.create(testTransactionId, testPageWithToken, mockCommunicator);
            const resultList: Reader[] = result.getResultList();

            chai.assert.equal(allValues.length, resultList.length);
            resultList.forEach((result, i) => {
                chai.assert.equal(
                    JSON.stringify(result),
                    JSON.stringify(makeReader(Result._handleBlob(allValues[i].IonBinary)))
                );
            });
        });

        it("should return a list of Readers that include the initial Page when called", async () => {
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

            mockCommunicator.fetchPage = async () => {
                return {
                    Page: finalTestPage
                };
            };
            const result: Result = await Result.create(testTransactionId, testPageWithTokenAndValue, mockCommunicator);
            const resultList: Reader[] = result.getResultList();

            chai.assert.equal(allValues.length + testValueHolder.length, resultList.length);
            // Need to check if the initial Page's value and the first element in resultList is equivalent.
            chai.assert.equal(
                JSON.stringify(makeReader(Result._handleBlob(testValueHolder[0].IonBinary))),
                JSON.stringify(resultList[0]));

            // Now check if the rest of the resultList matches up with the Page's values returned from the Communicator.
            for (let i = 0; i < allValues.length; i++) {
                chai.assert.equal(
                    JSON.stringify(makeReader(Result._handleBlob(allValues[i].IonBinary))),
                    JSON.stringify(resultList[i+1])
                );
            }
        });

        it("should return a list of Readers when Result object created with bufferResultStream()", async () => {
            const value1: ValueHolder = {IonBinary: "a"};
            const value2: ValueHolder = {IonBinary: "b"};
            const value3: ValueHolder = {IonBinary: "c"};
            const value4: ValueHolder = {IonBinary: "d"};
            const readers: Reader[] = [
                makeReader(Result._handleBlob(value1.IonBinary)),
                makeReader(Result._handleBlob(value2.IonBinary)),
                makeReader(Result._handleBlob(value3.IonBinary)),
                makeReader(Result._handleBlob(value4.IonBinary))
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

            const result: Result = await Result.bufferResultStream(<ResultStream> mockResultStream);
            const resultList: Reader[] = result.getResultList();

            chai.assert.equal(readers.length, resultList.length);
            resultList.forEach((result, i) => {
                chai.assert.equal(
                    JSON.stringify(result),
                    JSON.stringify(readers[i])
                );
            });
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
});
