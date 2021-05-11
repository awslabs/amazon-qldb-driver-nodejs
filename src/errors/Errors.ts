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

import { AWSError } from "aws-sdk";

import { error } from "../LogUtil";

const transactionExpiredPattern = RegExp("Transaction .* has expired");

export class ClientError extends Error {

    /**
     * @internal
     */
    constructor(message: string) {
        super(message);
        Object.setPrototypeOf(this, ClientError.prototype)
        this.message = message;
        this.name = "ClientError";
        error(message);
    }
}

export class DriverClosedError extends Error {

    /**
     * @internal
     */
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

    /**
     * @internal
     */
    constructor() {
        const message: string = "Abort called. Halting execution of lambda function.";
        super(message);
        Object.setPrototypeOf(this, LambdaAbortedError.prototype)
        this.message = message;
        this.name = "LambdaAbortedError";
        error(message);
    }
}

export class SessionPoolEmptyError extends Error {

    /**
     * @internal
     */
    constructor() {
        const message: string =
            "Session pool is empty. Please close existing sessions first before retrying.";
        super(message);
        Object.setPrototypeOf(this, SessionPoolEmptyError.prototype)
        this.message = message;
        this.name = "SessionPoolEmptyError";
        error(message);
    }
}

/**
 * @internal
 */
export class ExecuteError extends Error {
    cause: Error;
    isRetryable: boolean;
    isInvalidSessionException: boolean;
    transactionId: string;

    constructor(cause: Error, isRetryable: boolean, isInvalidSessionException: boolean, transactionId: string = null) {
        const message: string = "Error containing the context of a failure during Execute.";
        super(message);
        Object.setPrototypeOf(this, ExecuteError.prototype)
        this.cause = cause;
        this.isRetryable = isRetryable;
        this.isInvalidSessionException = isInvalidSessionException;
        this.transactionId = transactionId;
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
 * Is the exception because the transaction expired? The transaction expiry is a message wrapped
 * inside InvalidSessionException.
 * @param e The client error to check to see if it is an InvalidSessionException due to transaction expiry.
 * @returns Whether or not the exception is is an InvalidSessionException due to transaction expiry.
 */
export function isTransactionExpiredException(e: AWSError): boolean {
    return e.code === "InvalidSessionException" && transactionExpiredPattern.test(e.message);
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
 * Is the exception a BadRequestException?
 * @param e The client error to check to see if it is a BadRequestException.
 * @returns Whether or not the exception is a BadRequestException.
 */
export function isBadRequestException(e: AWSError): boolean {
    return e.code === "BadRequestException";
}

/**
 * Is the exception a retryable exception given the state of the session's transaction?
 * @param e The client error caught.
 * @returns True if the exception is a retryable exception. False otherwise.
 * 
 * @internal
 */
export function isRetryableException(e: AWSError, onCommit: boolean): boolean {
    const canRetryNetworkError: boolean = onCommit ? false : isNetworkingError(e);

    return isRetryableStatusCode(e) || isOccConflictException(e) || canRetryNetworkError ||
        (isInvalidSessionException(e) && !isTransactionExpiredException(e));
}

/**
 * Does the error have a retryable code or status code?
 * @param e The client error caught.
 * @returns True if the exception has a retryable code.
 */
function isRetryableStatusCode(e: AWSError): boolean {
    return (e.statusCode === 500) ||
           (e.statusCode === 503) ||
           (e.code === "NoHttpResponseException") ||
           (e.code === "SocketTimeoutException");
}

/**
 * Is the error caused by a network issue?
 * @param e The client error caught.
 * @returns True if the exception was caused by a network issue.
 */
function isNetworkingError(e: AWSError): boolean {
    if (!e.originalError) {
        return false;
    } else {
        const sourceError: AWSError = <AWSError> e.originalError;
        return (sourceError.code === "NetworkingError");
    }
}
