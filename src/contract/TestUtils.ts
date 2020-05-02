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

import { QLDB } from "aws-sdk";
import { ClientConfiguration } from "aws-sdk/clients/qldbsession";
import {
    CreateLedgerRequest,
    CreateLedgerResponse,
    DeleteLedgerRequest,
    DescribeLedgerRequest,
    DescribeLedgerResponse,
    UpdateLedgerRequest
} from "aws-sdk/clients/qldb";
import { dom, load } from "ion-js";

import { isResourceNotFoundException } from "../errors/Errors";

export class TestUtils {
    public ledgerName: string;
    public regionName: string;
    public clientConfig: ClientConfiguration;
    public qldbClient: QLDB;

    constructor(ledgerName: string) {
        this.ledgerName = ledgerName;
        if (process.argv.length == 6) {
            this.regionName = process.argv[5];
        }

        this.clientConfig = this.createClientConfiguration();
        this.qldbClient = new QLDB(this.clientConfig);
    }
    
    createClientConfiguration() : ClientConfiguration {
        const config: ClientConfiguration = {};
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
        const response: CreateLedgerResponse = await this.qldbClient.createLedger(request).promise();
        console.log(`Success. Ledger state: ${response.State}.`);
        await this.waitForActive();
    }

    async waitForActive(): Promise<void> {
        console.log(`Waiting for ledger ${this.ledgerName} to become active...`);
        const request: DescribeLedgerRequest = {
            Name: this.ledgerName
        }
        while (true) {
            const result: DescribeLedgerResponse = await this.qldbClient.describeLedger(request).promise();
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
        await this.qldbClient.deleteLedger(request).promise();
    }

    private async waitForDeletion(): Promise<void> {
        console.log("Waiting for the ledger to be deleted...");
        const request: DescribeLedgerRequest = {
            Name: this.ledgerName
        };
        while (true) {
            try {
                await this.qldbClient.describeLedger(request).promise();
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
        await this.qldbClient.updateLedger(request).promise();
    }

    static getIonTypes(): dom.Value[] {
        const values: dom.Value[] = [];
        values.push(load("null"));
        values.push(dom.Value.from(true));
        values.push(dom.Value.from(1));
        values.push(dom.Value.from(3.2));
        values.push(dom.load("5.5"));
        values.push(dom.load("2020-02-02"));
        values.push(dom.load("abc123"));
        values.push(dom.load("\"string\""));
        values.push(dom.load("{{ \"clob\" }}"));
        values.push(dom.load("{{ blob }}"));
        values.push(dom.load("(1 2 3)"));
        values.push(dom.load("[1, 2, 3]"));
        values.push(dom.load("{brand: ford}"));
        return values;
    }
}
