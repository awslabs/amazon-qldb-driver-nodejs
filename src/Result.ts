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
import { ExecuteStatementResult, FetchPageResult, Page, ValueHolder } from "@aws-sdk/client-qldb-session";
import { dom } from "ion-js";

import { Communicator } from "./Communicator";
import { ClientError } from "./errors/Errors"
import { ResultReadable } from "./ResultReadable";
import { IOUsage } from "./stats/IOUsage";
import { TimingInformation } from "./stats/TimingInformation";

interface Blob {}
export type IonBinary = Buffer|Uint8Array|Blob|string;

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
     * @param ioUsage Contains the number of consumed IO requests for the executed statement.
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
     * 
     * @internal
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
     * Static method that creates a Result object by reading and buffering the contents of a ResultReadable.
     * @param resultReadable A ResultReadable object to convert to a Result object.
     * @returns Promise which fulfills with a Result.
     */
    static async bufferResultReadable(resultReadable: ResultReadable): Promise<Result> {
        const resultList: dom.Value[] = await Result._readResultReadable(resultReadable);
        return new Result(resultList, resultReadable.getConsumedIOs(), resultReadable.getTimingInformation());
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
     * 
     * @internal
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
        throw new ClientError("Unexpected Blob returned from QLDB.");
    }

    /**
     * Fetches all subsequent Pages given an initial Page, places each value of each Page in an Ion value.
     * @param txnId The ID of the transaction the statement was executed in.
     * @param executeResult The returned result from the statement execution.
     * @param communicator The Communicator used for the statement execution.
     * @returns Promise which fulfills with a Result, containing a list of Ion values, representing all the returned
     * values of the result set, number of IOs for the request, and the time spent processing the request.
     */
    private static async _fetchResultPages(
        txnId: string,
        executeResult: ExecuteStatementResult,
        communicator: Communicator
    ): Promise<Result> {
        let currentPage: Page = executeResult.FirstPage;
        let readIO: number = executeResult.ConsumedIOs != null ? executeResult.ConsumedIOs.ReadIOs : null;
        let processingTime: number =
            executeResult.TimingInformation != null ? executeResult.TimingInformation.ProcessingTimeMilliseconds : null;

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

            if (fetchPageResult.ConsumedIOs != null) {
                readIO += fetchPageResult.ConsumedIOs.ReadIOs;
            }

            if (fetchPageResult.TimingInformation != null) {
                processingTime += fetchPageResult.TimingInformation.ProcessingTimeMilliseconds;
            }
        }
        const ionValues: dom.Value[] = [];
        pageValuesArray.forEach((valueHolders: ValueHolder[]) => {
            valueHolders.forEach((valueHolder: ValueHolder) => {
                ionValues.push(dom.load(Result._handleBlob(valueHolder.IonBinary)));
            });
        });
        const ioUsage: IOUsage = readIO != null ? new IOUsage(readIO) : null;
        const timingInformation = processingTime != null ? new TimingInformation(processingTime) : null;
        return new Result(ionValues, ioUsage, timingInformation);
    }

    /**
     * Helper method that reads a ResultReadable and extracts the results, placing them in an array of Ion values.
     * @param resultReadable The ResultReadable to read.
     * @returns Promise which fulfills with a list of Ion values, representing all the returned values of the result set.
     */
    private static async _readResultReadable(resultReadable: ResultReadable): Promise<dom.Value[]> {
        return new Promise(res => {
            const ionValues: dom.Value[] = [];
            resultReadable.on("data", function(value) {
                ionValues.push(value);
            }).on("end", function() {
                res(ionValues);
            });
        });
    }
}
