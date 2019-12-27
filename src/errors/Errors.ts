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

import { AWSError } from "aws-sdk";

import { error } from "../LogUtil";

export class ClientException extends Error {
    constructor(message: string) {
        super(message);
        Object.setPrototypeOf(this, ClientException.prototype)
        this.message = message;
        this.name = "ClientException";
        error(message);
    }
}

export class DriverClosedError extends Error {
    constructor() {
        const message: string = "Cannot invoke methods on a closed driver. Please create a new driver and retry.";
        super(message);
        Object.setPrototypeOf(this, DriverClosedError.prototype)
        this.message = message;
        this.name = "DriverClosedError";
        error(message);
    }
}

export class LambdaAbortedError extends Error {
    constructor() {
        const message: string = "Abort called. Halting execution of lambda function.";
        super(message);
        Object.setPrototypeOf(this, LambdaAbortedError.prototype)
        this.message = message;
        this.name = "LambdaAbortedError";
        error(message);
    }
}

export class SessionClosedError extends Error {
    constructor() {
        const message: string = "Cannot invoke methods on a closed QldbSession. Please create a new session and retry.";
        super(message);
        Object.setPrototypeOf(this, SessionClosedError.prototype)
        this.message = message;
        this.name = "SessionClosedError";
        error(message);
    }
}

export class SessionPoolEmptyError extends Error {
    constructor(timeout: number) {
        const message: string =
            `Session pool is empty after waiting for ${timeout} milliseconds. Please close existing sessions first ` +
            "before retrying.";
        super(message);
        Object.setPrototypeOf(this, SessionPoolEmptyError.prototype)
        this.message = message;
        this.name = "SessionPoolEmptyError";
        error(message);
    }
}

export class TransactionClosedError extends Error {
    constructor() {
        const message: string =
            "Cannot invoke methods on a closed Transaction. Please create a new transaction and retry.";
        super(message);
        Object.setPrototypeOf(this, TransactionClosedError.prototype)
        this.message = message;
        this.name = "TransactionClosedError";
        error(message);
    }
}

/**
 * Is the exception an InvalidParameterException?
 * @param e The client error caught.
 * @returns True if the exception is an InvalidParameterException. False otherwise.
 */
export function isInvalidParameterException(e: AWSError): boolean {
    return e.code === "InvalidParameterException";
}

/**
 * Is the exception an InvalidSessionException?
 * @param e The client error caught.
 * @returns True if the exception is an InvalidSessionException. False otherwise.
 */
export function isInvalidSessionException(e: AWSError): boolean {
    return e.code === "InvalidSessionException";
}

/**
 * Is the exception an OccConflictException?
 * @param e The client error caught.
 * @returns True if the exception is an OccConflictException. False otherwise.
 */
export function isOccConflictException(e: AWSError): boolean {
    return e.code === "OccConflictException";
}

/**
 * Is the exception a ResourceNotFoundException?
 * @param e The client error to check to see if it is a ResourceNotFoundException.
 * @returns Whether or not the exception is a ResourceNotFoundException.
 */
export function isResourceNotFoundException(e: AWSError): boolean {
    return e.code === "ResourceNotFoundException";
}

/**
 * Is the exception a ResourcePreconditionNotMetException?
 * @param e The client error to check to see if it is a ResourcePreconditionNotMetException.
 * @returns Whether or not the exception is a ResourcePreconditionNotMetException.
 */
export function isResourcePreconditionNotMetException(e: AWSError): boolean {
    return e.code === "ResourcePreconditionNotMetException";
}

/**
 * Is the exception a retriable exception?
 * @param e The client error caught.
 * @returns True if the exception is a retriable exception. False otherwise.
 */
export function isRetriableException(e: AWSError): boolean {
    return (e.statusCode === 500) ||
           (e.statusCode === 503) ||
           (e.code === "NoHttpResponseException") ||
           (e.code === "SocketTimeoutException");
}
