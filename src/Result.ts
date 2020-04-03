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

import { IonBinary, FetchPageResult, Page, ValueHolder } from "aws-sdk/clients/qldbsession";
import { dom } from "ion-js";

import { Communicator } from "./Communicator";
import { ClientException } from "./errors/Errors"
import { ResultStream } from "./ResultStream";

/**
 * A class representing a fully buffered set of results returned from QLDB.
 */
export class Result {
    private _resultList: dom.Value[];

    /**
     * Creates a Result.
     * @param resultList A list of Ion values containing the statement execution's result returned from QLDB.
     */
    private constructor(resultList: dom.Value[]) {
        this._resultList = resultList;
    }

    /**
     * Static factory method that creates a Result object, containing the results of a statement execution from QLDB.
     * @param txnId The ID of the transaction the statement was executed in.
     * @param page The initial page returned from the statement execution.
     * @param communicator The Communicator used for the statement execution.
     * @returns Promise which fulfills with a Result.
     */
    static async create(txnId: string, page: Page, communicator: Communicator): Promise<Result> {
        const resultList: dom.Value[] = await Result._fetchResultPages(txnId, page, communicator);
        return new Result(resultList);
    }

    /**
     * Static method that creates a Result object by reading and buffering the contents of a ResultStream.
     * @param resultStream A ResultStream object to convert to a Result object.
     * @returns Promise which fulfills with a Result.
     */
    static async bufferResultStream(resultStream: ResultStream): Promise<Result> {
        const resultList: dom.Value[] = await Result._readResultStream(resultStream);
        return new Result(resultList);
    }

    /**
     * Returns the list of results of the statement execution returned from QLDB.
     * @returns A list of Ion values which wrap the Ion values returned from the QLDB statement execution.
     */
    getResultList(): dom.Value[] {
        return this._resultList.slice();
    }

    /**
     * Handle the unexpected Blob return type from QLDB.
     * @param IonBinary The IonBinary value returned from QLDB.
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
     * @param page The initial page returned from the statement execution.
     * @param communicator The Communicator used for the statement execution.
     * @returns Promise which fulfills with a list of Ion values, representing all the returned values of the result set.
     */
    private static async _fetchResultPages(txnId: string, page: Page, communicator: Communicator): Promise<dom.Value[]> {
        let currentPage: Page = page;
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
        }
        const ionValues: dom.Value[] = [];
        pageValuesArray.forEach((valueHolders: ValueHolder[]) => {
            valueHolders.forEach((valueHolder: ValueHolder) => {
                ionValues.push(dom.load(Result._handleBlob(valueHolder.IonBinary)));
            });
        });
        return ionValues;
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
}
