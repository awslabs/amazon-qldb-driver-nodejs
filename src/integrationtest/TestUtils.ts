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

import { dom, load } from "ion-js";

import * as mocharc from './.mocharc.json'
import { isResourceNotFoundException } from "../errors/Errors";
import { Result } from "../Result";
import * as constants from "./TestConstants";
import { TransactionExecutor } from "../TransactionExecutor";
import { 
    CreateLedgerCommand,
    CreateLedgerRequest, 
    CreateLedgerResponse, 
    DeleteLedgerCommand, 
    DeleteLedgerRequest, 
    DescribeLedgerCommand, 
    DescribeLedgerRequest, 
    DescribeLedgerResponse, 
    QLDBClient, 
    QLDBClientConfig, 
    UpdateLedgerCommand, 
    UpdateLedgerRequest 
} from "@aws-sdk/client-qldb";

export class TestUtils {
    public ledgerName: string;
    public regionName: string;
    public clientConfig: QLDBClientConfig;
    public qldbClient: QLDBClient;

    constructor(ledgerName: string) {
        this.ledgerName = ledgerName;
        this.regionName = mocharc.region;
        this.clientConfig = this.createClientConfiguration();
        this.qldbClient = new QLDBClient(this.clientConfig);
    }
    
    createClientConfiguration() : QLDBClientConfig {
        const config: QLDBClientConfig = {};
        if (this.regionName != undefined) {
            config.region = this.regionName;
        }
        return config;
    }

    static sleep(ms: number): Promise<void> {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async runCreateLedger(): Promise<void> {
        console.log(`Creating a ledger named: ${this.ledgerName}...`);
        const request: CreateLedgerRequest = {
            Name: this.ledgerName,
            PermissionsMode: "ALLOW_ALL"
        }
        const command = new CreateLedgerCommand(request);
        const response: CreateLedgerResponse = await this.qldbClient.send(command);
        console.log(`Success. Ledger state: ${response.State}.`);
        await this.waitForActive();
    }

    async waitForActive(): Promise<void> {
        console.log(`Waiting for ledger ${this.ledgerName} to become active...`);
        const request: DescribeLedgerRequest = {
            Name: this.ledgerName
        }
        while (true) {
            const command = new DescribeLedgerCommand(request);
            const result: DescribeLedgerResponse = await this.qldbClient.send(command);
            if (result.State === "ACTIVE") {
                console.log("Success. Ledger is active and ready to be used.");
                return;
            }
            console.log("The ledger is still creating. Please wait...");
            await TestUtils.sleep(10000);
        }
    }

    async runDeleteLedger(): Promise<void> {
        await this.deleteLedger();
        await this.waitForDeletion();
    }

    async runForceDeleteLedger(): Promise<void> {
        try {
            await this.deleteLedger();
            await this.waitForDeletion();
        } catch (e) {
            if (isResourceNotFoundException(e)) {
                console.log("Ledger did not previously exist.");
                return;
            } else {
                throw e;
            }
        }
    }

    private async deleteLedger(): Promise<void> {
        console.log(`Attempting to delete the ledger with name: ${this.ledgerName}`);
        await this.disableDeletionProtection();
        const request: DeleteLedgerRequest = {
            Name: this.ledgerName
        };
        const command = new DeleteLedgerCommand(request);
        await this.qldbClient.send(command);
    }

    private async waitForDeletion(): Promise<void> {
        console.log("Waiting for the ledger to be deleted...");
        const request: DescribeLedgerRequest = {
            Name: this.ledgerName
        };
        while (true) {
            try {
                const command = new DescribeLedgerCommand(request);
                await this.qldbClient.send(command);
                console.log("The ledger is still being deleted. Please wait...");
                await TestUtils.sleep(10000);
            } catch (e) {
                if (isResourceNotFoundException(e)) {
                    console.log("Success. Ledger is deleted.");
                    break;
                } else {
                    throw e;
                }
            }
        }
    }

    private async disableDeletionProtection(): Promise<void> {
        const request: UpdateLedgerRequest = {
            Name: this.ledgerName,
            DeletionProtection: false
        }
        const command = new UpdateLedgerCommand(request);
        await this.qldbClient.send(command);
    }

    async readIonValue(txn: TransactionExecutor, value: dom.Value): Promise<dom.Value> {
        let result: Result;
        if (value.isNull()) {
            const searchQuery: string = `SELECT VALUE ${constants.COLUMN_NAME} FROM ${constants.TABLE_NAME}` +
                ` WHERE ${constants.COLUMN_NAME} IS NULL`;
            result = await txn.execute(searchQuery);
        } else {
            const searchQuery: string = `SELECT VALUE ${constants.COLUMN_NAME} FROM ${constants.TABLE_NAME}` +
                ` WHERE ${constants.COLUMN_NAME} = ?`;
            result = await txn.execute(searchQuery, value);
        }
        return  result.getResultList()[0];
    }

    static getLengthOfResultSet(result: Result): number {
        return result.getResultList().length;
    }

    static getIonTypes(): dom.Value[] {
        const values: dom.Value[] = [];

        const ionClob: dom.Value = load('{{"This is a CLOB of text."}}');
        values.push(ionClob);
        const ionBlob: dom.Value = load('{{aGVsbG8=}}');
        values.push(ionBlob);
        const ionBool: dom.Value = load('true');
        values.push(ionBool);
        const ionDecimal: dom.Value = load('0.1');
        values.push(ionDecimal);
        const ionFloat: dom.Value = load('0.2e0');
        values.push(ionFloat);
        const ionInt: dom.Value = load('1');
        values.push(ionInt);
        const ionList: dom.Value = load('[1,2]');
        values.push(ionList);
        const ionNull: dom.Value = load('null');
        values.push(ionNull);
        const ionSexp: dom.Value = load('(cons 1 2)');
        values.push(ionSexp);
        const ionString: dom.Value = load('"string"');
        values.push(ionString);
        const ionStruct: dom.Value = load('{a:1}');
        values.push(ionStruct);
        const ionSymbol: dom.Value = load('abc');
        values.push(ionSymbol);
        const ionTimestamp: dom.Value = load('2016-12-20T05:23:43.000000-00:00');
        values.push(ionTimestamp);

        const ionNullClob: dom.Value = load('null.clob');
        values.push(ionNullClob);
        const ionNullBlob: dom.Value = load('null.blob');
        values.push(ionNullBlob);
        const ionNullBool: dom.Value = load('null.bool');
        values.push(ionNullBool);
        const ionNullDecimal: dom.Value = load('null.decimal');
        values.push(ionNullDecimal);
        const ionNullFloat: dom.Value = load('null.float');
        values.push(ionNullFloat);
        const ionNullInt: dom.Value = load('null.int');
        values.push(ionNullInt);
        const ionNullList: dom.Value = load('null.list');
        values.push(ionNullList);
        const ionNullSexp: dom.Value = load('null.sexp');
        values.push(ionNullSexp);
        const ionNullString: dom.Value = load('null.string');
        values.push(ionNullString);
        const ionNullStruct: dom.Value = load('null.struct');
        values.push(ionNullStruct);
        const ionNullSymbol: dom.Value = load('null.symbol');
        values.push(ionNullSymbol);
        const ionNullTimestamp: dom.Value = load('null.timestamp');
        values.push(ionNullTimestamp);

        const ionClobWithAnnotation: dom.Value = load('annotation::{{"This is a CLOB of text."}}');
        values.push(ionClobWithAnnotation);
        const ionBlobWithAnnotation: dom.Value = load('annotation::{{aGVsbG8=}}');
        values.push(ionBlobWithAnnotation);
        const ionBoolWithAnnotation: dom.Value = load('annotation::true');
        values.push(ionBoolWithAnnotation);
        const ionDecimalWithAnnotation: dom.Value = load('annotation::0.1');
        values.push(ionDecimalWithAnnotation);
        const ionFloatWithAnnotation: dom.Value = load('annotation::0.2e0');
        values.push(ionFloatWithAnnotation);
        const ionIntWithAnnotation: dom.Value = load('annotation::1');
        values.push(ionIntWithAnnotation);
        const ionListWithAnnotation: dom.Value = load('annotation::[1,2]');
        values.push(ionListWithAnnotation);
        const ionNullWithAnnotation: dom.Value = load('annotation::null');
        values.push(ionNullWithAnnotation);
        const ionSexpWithAnnotation: dom.Value = load('annotation::(cons 1 2)');
        values.push(ionSexpWithAnnotation);
        const ionStringWithAnnotation: dom.Value = load('annotation::"string"');
        values.push(ionStringWithAnnotation);
        const ionStructWithAnnotation: dom.Value = load('annotation::{a:1}');
        values.push(ionStructWithAnnotation);
        const ionSymbolWithAnnotation: dom.Value = load('annotation::abc');
        values.push(ionSymbolWithAnnotation);
        const ionTimestampWithAnnotation: dom.Value = load('annotation::2016-12-20T05:23:43.000000-00:00');
        values.push(ionTimestampWithAnnotation);
 
        return values;
    }
}
