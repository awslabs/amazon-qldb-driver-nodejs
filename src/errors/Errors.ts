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

import { 
    InvalidParameterException, 
    ResourceNotFoundException, 
    ResourcePreconditionNotMetException 
} from "@aws-sdk/client-qldb";
import { 
    BadRequestException, 
    InvalidSessionException, 
    OccConflictException, 
    QLDBSessionServiceException 
} from "@aws-sdk/client-qldb-session";

import { error, log } from "../LogUtil";

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
export function isInvalidParameterException(e: Error): boolean {
    return e instanceof InvalidParameterException;
}

/**
 * Is the exception an InvalidSessionException?
 * @param e The client error caught.
 * @returns True if the exception is an InvalidSessionException. False otherwise.
 */
export function isInvalidSessionException(e: Error): boolean {
    return e instanceof InvalidSessionException;
}

/**
 * Is the exception because the transaction expired? The transaction expiry is a message wrapped
 * inside InvalidSessionException.
 * @param e The client error to check to see if it is an InvalidSessionException due to transaction expiry.
 * @returns Whether or not the exception is is an InvalidSessionException due to transaction expiry.
 */
export function isTransactionExpiredException(e: Error): boolean {
    return e instanceof InvalidSessionException  && transactionExpiredPattern.test(e.message);
}

/**
 * Is the exception an OccConflictException?
 * @param e The client error caught.
 * @returns True if the exception is an OccConflictException. False otherwise.
 */
export function isOccConflictException(e: Error): boolean {
    return e instanceof OccConflictException;
}

/**
 * Is the exception a ResourceNotFoundException?
 * @param e The client error to check to see if it is a ResourceNotFoundException.
 * @returns Whether or not the exception is a ResourceNotFoundException.
 */
export function isResourceNotFoundException(e: Error): boolean {
    return e instanceof ResourceNotFoundException;
}

/**
 * Is the exception a ResourcePreconditionNotMetException?
 * @param e The client error to check to see if it is a ResourcePreconditionNotMetException.
 * @returns Whether or not the exception is a ResourcePreconditionNotMetException.
 */
export function isResourcePreconditionNotMetException(e: Error): boolean {
    return e instanceof ResourcePreconditionNotMetException;
}

/**
 * Is the exception a BadRequestException?
 * @param e The client error to check to see if it is a BadRequestException.
 * @returns Whether or not the exception is a BadRequestException.
 */
export function isBadRequestException(e: Error): boolean {
    return e instanceof BadRequestException;
}

/**
 * Is the exception a retryable exception given the state of the session's transaction?
 * @param e The client error caught.
 * @param onCommit If the error caught was on a commit command.
 * @returns True if the exception is a retryable exception. False otherwise.
 * 
 * @internal
 */
export function isRetryableException(e: Error, onCommit: boolean): boolean {
    if (e instanceof QLDBSessionServiceException) {
        // TODO: is checking whether QLDBSessionServiceException is throttling retryable the same as checking whether AWSError is retryable?
        const canSdkRetry: boolean = onCommit ? false : e.$retryable && e.$retryable.throttling;
    
        return isRetryableStatusCode(e) || isOccConflictException(e) || canSdkRetry ||
            (isInvalidSessionException(e) && !isTransactionExpiredException(e));
    }
    log("Error is not an instance of QLDBSessionServiceException");
    return false;
}

/**
 * Does the error have a retryable code or status code?
 * @param e The client error caught.
 * @returns True if the exception has a retryable code.
 */
function isRetryableStatusCode(e: Error): boolean {
    if (e instanceof QLDBSessionServiceException) {
        // TODO: is it safe getting rid of NoHttpResponseException and SocketTimeoutException checks?
        return (e.$metadata.httpStatusCode === 500) ||
               (e.$metadata.httpStatusCode === 503);
    }
    log("Error is not an instance of QLDBSessionServiceException");
    return false;
}
