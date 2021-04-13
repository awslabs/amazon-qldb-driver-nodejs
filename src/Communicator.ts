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

import { QLDBSession } from "aws-sdk";
import {
    AbortTransactionResult,
    CommitDigest,
    CommitTransactionResult,
    EndSessionResult,
    ExecuteStatementResult,
    FetchPageResult,
    PageToken,
    SendCommandRequest,
    SendCommandResult,
    StartTransactionResult,
    ValueHolder
} from "aws-sdk/clients/qldbsession";
import { inspect } from "util";

import { debug, warn } from "./LogUtil";

/**
 * A class representing an independent session to a QLDB ledger that handles endpoint requests. This class is used in
 * {@linkcode QldbDriver} and {@linkcode QldbSession}. This class is not meant to be used directly by developers.
 */
export class Communicator {
    private _qldbClient: QLDBSession;
    private _sessionToken: string;

    /**
     * Creates a Communicator.
     * @param qldbClient The low level service client.
     * @param sessionToken The initial session token representing the session connection.
     */
    private constructor(qldbClient: QLDBSession, sessionToken: string) {
        this._qldbClient = qldbClient;
        this._sessionToken = sessionToken;
    }

    /**
     * Static factory method that creates a Communicator object.
     * @param qldbClient The low level client that communicates with QLDB.
     * @param ledgerName The QLDB ledger name.
     * @returns Promise which fulfills with a Communicator.
     */
    static async create(qldbClient: QLDBSession, ledgerName: string): Promise<Communicator> {
        const request: SendCommandRequest = {
            StartSession: {
                LedgerName: ledgerName
            }
        };
        const result: SendCommandResult = await qldbClient.sendCommand(request).promise();
        return new Communicator(qldbClient, result.StartSession.SessionToken);
    }

    /**
     * Send request to abort the currently active transaction.
     * @returns Promise which fulfills with the abort transaction response returned from QLDB.
     */
    async abortTransaction(): Promise<AbortTransactionResult> {
        const request: SendCommandRequest = {
            SessionToken: this._sessionToken,
            AbortTransaction: {}
        };
        const result: SendCommandResult = await this._sendCommand(request);
        return result.AbortTransaction;
    }

    /**
     * Send request to commit the currently active transaction.
     * @param txnId The ID of the transaction.
     * @param commitDigest The digest hash of the transaction to commit.
     * @returns Promise which fulfills with the commit transaction response returned from QLDB.
     */
    async commit(txnId: string, commitDigest: CommitDigest): Promise<CommitTransactionResult> {
        const request: SendCommandRequest = {
            SessionToken: this._sessionToken,
            CommitTransaction: {
                TransactionId: txnId,
                CommitDigest: commitDigest
            }
        };
        const result: SendCommandResult = await this._sendCommand(request);
        return result.CommitTransaction;
    }

    /**
     * Send an execute statement request with parameters to QLDB.
     * @param txnId The ID of the transaction.
     * @param statement The statement to execute.
     * @param parameters The parameters of the statement contained in ValueHolders.
     * @returns Promise which fulfills with the execute statement response returned from QLDB.
     */
    async executeStatement(
        txnId: string,
        statement: string,
        parameters: ValueHolder[]
    ): Promise<ExecuteStatementResult> {
        const request: SendCommandRequest = {
            SessionToken: this._sessionToken,
            ExecuteStatement: {
                Statement: statement,
                TransactionId: txnId,
                Parameters: parameters
            }
        };
        const result: SendCommandResult = await this._sendCommand(request);
        return result.ExecuteStatement;
    }

    /**
     * Send request to end the independent session represented by the instance of this class.
     * @returns Promise which fulfills with the end session response returned from QLDB.
     */
    async endSession(): Promise<EndSessionResult> {
        const request: SendCommandRequest = {
            SessionToken: this._sessionToken,
            EndSession: {}
        };
        const result: SendCommandResult = await this._sendCommand(request);
        return result.EndSession;
    }

    /**
     * Send fetch result request to QLDB, retrieving the next chunk of data for the result.
     * @param txnId The ID of the transaction.
     * @param pageToken The token to fetch the next page.
     * @returns Promise which fulfills with the fetch page response returned from QLDB.
     */
    async fetchPage(txnId: string, pageToken: PageToken): Promise<FetchPageResult> {
        const request: SendCommandRequest = {
            SessionToken: this._sessionToken,
            FetchPage: {
                TransactionId: txnId,
                NextPageToken: pageToken
            }
        };
        const result: SendCommandResult = await this._sendCommand(request);
        return result.FetchPage;
    }

    /**
     * Get the low-level service client that communicates with QLDB.
     * @returns The low-level service client.
     */
    getQldbClient(): QLDBSession {
        return this._qldbClient;
    }

    /**
     * Get the session token representing the session connection.
     * @returns The session token.
     */
    getSessionToken(): string {
        return this._sessionToken;
    }

    /**
     * Send a request to start a transaction.
     * @returns Promise which fulfills with the start transaction response returned from QLDB.
     */
    async startTransaction(): Promise<StartTransactionResult> {
        const request: SendCommandRequest = {
            SessionToken: this._sessionToken,
            StartTransaction: {}
        };
        const result: SendCommandResult = await this._sendCommand(request);
        return result.StartTransaction;
    }

    /**
     * Call the sendCommand method of the low level service client.
     * @param request A SendCommandRequest object containing the request information to be sent to QLDB.
     * @returns Promise which fulfills with a SendCommandResult object.
     */
    private async _sendCommand(request: SendCommandRequest): Promise<SendCommandResult> {
        try {
            const result = await this._qldbClient.sendCommand(request).promise();
            debug(`Received response: ${inspect(result, { depth: 2 })}`);
            return result;
        } catch (e) {
            warn(`Error sending a command: ${e}.`);
            throw e;
        }
    }
}
