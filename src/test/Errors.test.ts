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

// Test environment imports
import "mocha";

import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";

import {
    ClientError,
    DriverClosedError,
    isInvalidParameterException,
    isInvalidSessionException,
    isOccConflictException,
    isResourceNotFoundException,
    isResourcePreconditionNotMetException,
    isRetryableException,
    LambdaAbortedError,
    SessionPoolEmptyError,
    isTransactionExpiredException,
    isBadRequestException
} from "../errors/Errors";
import * as LogUtil from "../LogUtil";
import { BadRequestException, InvalidSessionException, OccConflictException } from "@aws-sdk/client-qldb-session";
import { InvalidParameterException, ResourceNotFoundException, ResourcePreconditionNotMetException } from "@aws-sdk/client-qldb";
import { ServiceException } from "@aws-sdk/smithy-client";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testMessage: string = "foo";

describe("Errors", () => {

    afterEach(() => {
        sandbox.restore();
    });

    describe("#ClientError", () => {
        it("should be a ClientError when new ClientError created", () => {
            const logSpy = sandbox.spy(LogUtil, "error");
            const error = new ClientError(testMessage);
            chai.expect(error).to.be.instanceOf(ClientError);
            chai.assert.equal(error.name, "ClientError");
            chai.assert.equal(error.message, testMessage);
            sinon.assert.calledOnce(logSpy);
        });
    });

    describe("#DriverClosedError", () => {
        it("should be a DriverClosedError when new DriverClosedError created", () => {
            const logSpy = sandbox.spy(LogUtil, "error");
            const error = new DriverClosedError();
            chai.expect(error).to.be.instanceOf(DriverClosedError);
            chai.assert.equal(error.name, "DriverClosedError");
            sinon.assert.calledOnce(logSpy);
        });
    });

    describe("#LambdaAbortedError", () => {
        it("should be a LambdaAbortedError when new LambdaAbortedError created", () => {
            const logSpy = sandbox.spy(LogUtil, "error");
            const error = new LambdaAbortedError();
            chai.expect(error).to.be.instanceOf(LambdaAbortedError);
            chai.assert.equal(error.name, "LambdaAbortedError");
            sinon.assert.calledOnce(logSpy);
        });
    });

    describe("#SessionPoolEmptyError", () => {
        it("should be a SessionPoolEmptyError when new SessionPoolEmptyError created", () => {
            const logSpy = sandbox.spy(LogUtil, "error");
            const error = new SessionPoolEmptyError();
            chai.expect(error).to.be.instanceOf(SessionPoolEmptyError);
            chai.assert.equal(error.name, "SessionPoolEmptyError");
            sinon.assert.calledOnce(logSpy);
        });
    });

    describe("#isInvalidParameterException()", () => {
        it("should return true when error is an InvalidParameterException", () => {
            const mockError = new InvalidParameterException({ message: "", $metadata: {}});
            chai.assert.isTrue(isInvalidParameterException(mockError));
        });

        it("should return false when error is not an InvalidParameterException", () => {
            const mockError = new ServiceException({ $metadata: {}, name: "", $fault: "server" });
            chai.assert.isFalse(isInvalidParameterException(mockError));
        });
    });

    describe("#isInvalidSessionException()", () => {
        it("should return true when error is an InvalidSessionException", () => {
            const mockError = new InvalidSessionException({ message: "", $metadata: {}});
            chai.assert.isTrue(isInvalidSessionException(mockError));
        });

        it("should return false when error is not an InvalidSessionException", () => {
            const mockError = new ServiceException({ $metadata: {}, name: "", $fault: "server" });
            chai.assert.isFalse(isInvalidSessionException(mockError));
        });
    });

    describe("#isOccConflictException()", () => {
        it("should return true when error is an OccConflictException", () => {
            const mockError = new OccConflictException({ message: "", $metadata: {}});
            chai.assert.isTrue(isOccConflictException(mockError));
        });

        it("should return false when error is not an OccConflictException", () => {
            const mockError = new ServiceException({ $metadata: {}, name: "", $fault: "server" });
            chai.assert.isFalse(isOccConflictException(mockError));
        });
    });

    describe("#isResourceNotFoundException()", () => {
        it("should return true when error is a ResourceNotFoundException", () => {
            const mockError = new ResourceNotFoundException({ message: "", $metadata: {}});
            chai.assert.isTrue(isResourceNotFoundException(mockError));
        });

        it("should return false when error is not a ResourceNotFoundException", () => {
            const mockError = new ServiceException({ $metadata: {}, name: "", $fault: "server" });
            chai.assert.isFalse(isResourceNotFoundException(mockError));
        });
    });

    describe("#isResourcePreconditionNotMetException()", () => {
        it("should return true when error is a ResourcePreconditionNotMetException", () => {
            const mockError = new ResourcePreconditionNotMetException({ message: "", $metadata: {}});
            chai.assert.isTrue(isResourcePreconditionNotMetException(mockError));
        });

        it("should return false when error is not a ResourcePreconditionNotMetException", () => {
            const mockError = new ServiceException({ $metadata: {}, name: "", $fault: "server" });
            chai.assert.isFalse(isResourcePreconditionNotMetException(mockError));
        });
    });

    describe("#isRetryableException()", () => {
        it("should return true with statusCode 500", () => {
            const mockError = new ServiceException({ $metadata: { httpStatusCode: 500 }, name: "", $fault: "server" });
            chai.assert.isTrue(isRetryableException(mockError, false));
        });

        it("should return true with statusCode 503", () => {
            const mockError = new ServiceException({ $metadata: { httpStatusCode: 503 }, name: "", $fault: "server" });
            chai.assert.isTrue(isRetryableException(mockError, false));
        });
        
        it("should return true when error is NoHttpResponseException", () => {
            const mockError = new ServiceException({ $metadata: { }, name: "NoHttpResponseException", $fault: "client" })
            chai.assert.isTrue(isRetryableException(mockError, false));
        });

        it("should return true when error is SocketTimeoutException", () => {
            const mockError = new ServiceException({ $metadata: { }, name: "SocketTimeoutException", $fault: "client" })
            chai.assert.isTrue(isRetryableException(mockError, false));
        });

        it("should return false when not a retryable exception", () => {
            const mockError = new ServiceException({ $metadata: { httpStatusCode: 200 }, name: "", $fault: "server" });
            chai.assert.isFalse(isRetryableException(mockError, false));
        });

        it("should appropriately handle retryable errors from the SDK", () => {
            const awsError = new ServiceException({ $metadata: { httpStatusCode: 200 }, name: "", $fault: "server", });

            // Empty retryable causes false
            awsError.$retryable = undefined;
            chai.assert.isFalse(isRetryableException(awsError, false));
            chai.assert.isFalse(isRetryableException(awsError, true));
            
            // False retryable causes false
            awsError.$retryable =  { throttling: false };
            chai.assert.isFalse(isRetryableException(awsError, false));
            chai.assert.isFalse(isRetryableException(awsError, true));

            // True retryable causes true, but only if not on commit
            awsError.$retryable = { throttling: true };
            chai.assert.isTrue(isRetryableException(awsError, false));
            chai.assert.isFalse(isRetryableException(awsError, true));
        });
    });

    describe("#isTransactionExpiredException", () => {
        it("should return true when error is an InvalidSessionException and message is Tranaction <txId> has expired", () => {
            const mockError = new InvalidSessionException({ message: "", $metadata: {}});
            mockError.message = "Transaction ABC has expired"
            chai.assert.isTrue(isTransactionExpiredException(mockError));
        });

        it("should return false when error is an InvalidSessionException but message is different", () => {
            const mockError = new InvalidSessionException({ message: "", $metadata: {}});
            mockError.message = "SessionNotIdentified"
            chai.assert.isFalse(isTransactionExpiredException(mockError));
        });

        it("should return false when error is not an InvalidSessionException ", () => {
            const mockError = new ServiceException({ $metadata: {}, name: "", $fault: "server" });
            chai.assert.isFalse(isTransactionExpiredException(mockError));
        });
    });

    describe("#isBadRequestException()", () => {
        it("should return true when error is a BadRequestException", () => {
            const mockError = new BadRequestException({ message: "", $metadata: {} });
            chai.assert.isTrue(isBadRequestException(mockError));
        });

        it("should return false when error is not a BadRequestException", () => {
            const mockError = new ServiceException({ $metadata: {}, name: "", $fault: "server" });
            chai.assert.isFalse(isBadRequestException(mockError));
        });
    });
});
