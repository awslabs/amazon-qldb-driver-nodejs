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

import {
    ExecuteStatementResult,
    FetchPageResult,
    IonBinary,
    IOUsage as sdkIOUsage,
    Page,
    TimingInformation as sdkTimingInformation,
    ValueHolder
} from "aws-sdk/clients/qldbsession";
import { dom } from "ion-js";

import { Communicator } from "./Communicator";
import { ClientException } from "./errors/Errors"
import { ResultStream } from "./ResultStream";
import { IOUsage } from "./stats/IOUsage";
import { IOUsageImp } from "./stats/IOUsageImp";
import { TimingInformation } from "./stats/TimingInformation";
import { TimingInformationImp } from "./stats/TimingInformationImp";

/**
 * A class representing a fully buffered set of results returned from QLDB.
 */
export class Result {
    private _resultList: dom.Value[];
    private _ioUsage: IOUsage;
    private _timingInformation: TimingInformation;

    /**
     * Creates a Result.
     * @param resultList A list of Ion values containing the statement execution's result returned from QLDB.
     * @param ioUsage IOUsage object, containing the number of consumed IO requests for the executed statement.
     * @param timingInformation Holds server side processing time for the executed statement.
     */
    private constructor(resultList: dom.Value[], ioUsage: IOUsage, timingInformation: TimingInformation) {
        this._resultList = resultList;
        this._ioUsage = ioUsage;
        this._timingInformation = timingInformation;
    }

    /**
     * Static factory method that creates a Result object, containing the results of a statement execution from QLDB.
     * @param txnId The ID of the transaction the statement was executed in.
     * @param executeResult The returned result from the statement execution.
     * @param communicator The Communicator used for the statement execution.
     * @returns Promise which fulfills with a Result.
     */
    static async create(
        txnId: string,
        executeResult: ExecuteStatementResult,
        communicator: Communicator
    ): Promise<Result> {
        const result: Result = await Result._fetchResultPages(txnId, executeResult, communicator);
        return result;
    }

    /**
     * Static method that creates a Result object by reading and buffering the contents of a ResultStream.
     * @param resultStream A ResultStream object to convert to a Result object.
     * @returns Promise which fulfills with a Result.
     */
    static async bufferResultStream(resultStream: ResultStream): Promise<Result> {
        const resultList: dom.Value[] = await Result._readResultStream(resultStream);
        return new Result(resultList, resultStream.getConsumedIOs(), resultStream.getTimingInformation());
    }

    /**
     * Returns the list of results of the statement execution returned from QLDB.
     * @returns A list of Ion values which wrap the Ion values returned from the QLDB statement execution.
     */
    getResultList(): dom.Value[] {
        return this._resultList.slice();
    }

    /**
     * Returns the number of read IO request for the executed statement.
     * @returns IOUsage, containing number of read IOs.
     */
    getConsumedIOs(): IOUsage {
        return this._ioUsage;
    }

    /**
     * Returns server-side processing time for the executed statement.
     * @returns TimingInformation, containing processing time.
     */
    getTimingInformation(): TimingInformation {
        return this._timingInformation;
    }

    /**
     * Handle the unexpected Blob return type from QLDB.
     * @param ionBinary The IonBinary value returned from QLDB.
     * @returns The IonBinary value cast explicitly to one of the types that make up the IonBinary type. This will be
     *          either Buffer, Uint8Array, or string.
     * @throws {@linkcode ClientException} when the specific type of the IonBinary value is Blob.
     */
    static _handleBlob(ionBinary: IonBinary): Buffer|Uint8Array|string {
        if (ionBinary instanceof Buffer) {
            return <Buffer> ionBinary;
        }
        if (ionBinary instanceof Uint8Array) {
            return <Uint8Array> ionBinary;
        }
        if (typeof ionBinary === "string") {
            return <string> ionBinary;
        }
        throw new ClientException("Unexpected Blob returned from QLDB.");
    }

    /**
     * Fetches all subsequent Pages given an initial Page, places each value of each Page in an Ion value.
     * @param txnId The ID of the transaction the statement was executed in.
     * @param executeResult The returned result from the statement execution.
     * @param communicator The Communicator used for the statement execution.
     * @returns Promise which fulfills with a Result, containing a list of Ion values, representing all the returned
     * values of the result set, number of IOs for the request, and time the spent processing the request.
     */
    private static async _fetchResultPages(
        txnId: string,
        executeResult: ExecuteStatementResult,
        communicator: Communicator
    ): Promise<Result> {
        let currentPage: Page = executeResult.FirstPage;
        let ioUsage: IOUsageImp = <IOUsageImp>Result._getIOUsage(executeResult.ConsumedIOs);
        let timingInformation: TimingInformationImp = <TimingInformationImp>Result._getTimingInformation(executeResult.TimingInformation);
        const pageValuesArray: ValueHolder[][] = [];
        if (currentPage.Values && currentPage.Values.length > 0) {
            pageValuesArray.push(currentPage.Values);
        }
        while (currentPage.NextPageToken) {
            const fetchPageResult: FetchPageResult =
                await communicator.fetchPage(txnId, currentPage.NextPageToken);
            currentPage = fetchPageResult.Page;
            if (currentPage.Values && currentPage.Values.length > 0) {
                pageValuesArray.push(currentPage.Values);
            }

            if (ioUsage == null && fetchPageResult.ConsumedIOs != null) {
                ioUsage = new IOUsageImp(fetchPageResult.ConsumedIOs.ReadIOs)
            } else if (ioUsage != null) {
                ioUsage.accumulateIOUsage(fetchPageResult.ConsumedIOs);
            }

            if (timingInformation == null && fetchPageResult.TimingInformation != null) {
                timingInformation = new TimingInformationImp(fetchPageResult.TimingInformation.ProcessingTimeMilliseconds)
            } else if (timingInformation != null) {
                timingInformation.accumulateTimingInfo(fetchPageResult.TimingInformation);
            }
        }
        const ionValues: dom.Value[] = [];
        pageValuesArray.forEach((valueHolders: ValueHolder[]) => {
            valueHolders.forEach((valueHolder: ValueHolder) => {
                ionValues.push(dom.load(Result._handleBlob(valueHolder.IonBinary)));
            });
        });
        return new Result(ionValues, ioUsage, timingInformation);
    }

    /**
     * Helper method that reads a ResultStream and extracts the results, placing them in an array of Ion values.
     * @param resultStream The ResultStream to read.
     * @returns Promise which fulfills with a list of Ion values, representing all the returned values of the result set.
     */
    private static async _readResultStream(resultStream: ResultStream): Promise<dom.Value[]> {
        return new Promise(res => {
            let ionValues: dom.Value[] = [];
            resultStream.on("data", function(value) {
                ionValues.push(value);
            }).on("end", function() {
                res(ionValues);
            });
        });
    }

    /**
     * Creates IOUsage from server-side ConsumedIOs.
     * @param consumedIOs The server-side ConsumedIOs
     * @returns An instance of IOUsage
     */
    static _getIOUsage(consumedIOs: sdkIOUsage): IOUsage {
        if (consumedIOs == null) {
            return null;
        }
        return new IOUsageImp(consumedIOs.ReadIOs);
    }

    /**
     * Creates TimingInformation from server-side processing time information.
     * @param timingInfo The server-side processing time information.
     * @returns An instance of TimingInformation
     */
    static _getTimingInformation(timingInfo: sdkTimingInformation): TimingInformation {
        if (timingInfo == null) {
            return null;
        }
        return new TimingInformationImp(timingInfo.ProcessingTimeMilliseconds);
    }
}
