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

import { AWSError } from "aws-sdk";
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

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

const testMessage: string = "foo";
const mockError: AWSError = <AWSError><any> sandbox.mock(Error);

describe("Errors", () => {

    afterEach(() => {
        mockError.code = undefined;
        mockError.statusCode = undefined;
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
            mockError.code = "InvalidParameterException";
            chai.assert.isTrue(isInvalidParameterException(mockError));
        });

        it("should return false when error is not an InvalidParameterException", () => {
            mockError.code = "NotInvalidParameterException";
            chai.assert.isFalse(isInvalidParameterException(mockError));
        });
    });

    describe("#isInvalidSessionException()", () => {
        it("should return true when error is an InvalidSessionException", () => {
            mockError.code = "InvalidSessionException";
            chai.assert.isTrue(isInvalidSessionException(mockError));
        });

        it("should return false when error is not an InvalidSessionException", () => {
            mockError.code = "NotInvalidSessionException";
            chai.assert.isFalse(isInvalidSessionException(mockError));
        });
    });

    describe("#isOccConflictException()", () => {
        it("should return true when error is an OccConflictException", () => {
            mockError.code = "OccConflictException";
            chai.assert.isTrue(isOccConflictException(mockError));
        });

        it("should return false when error is not an OccConflictException", () => {
            mockError.code = "NotOccConflictException";
            chai.assert.isFalse(isOccConflictException(mockError));
        });
    });

    describe("#isResourceNotFoundException()", () => {
        it("should return true when error is a ResourceNotFoundException", () => {
            mockError.code = "ResourceNotFoundException";
            chai.assert.isTrue(isResourceNotFoundException(mockError));
        });

        it("should return false when error is not a ResourceNotFoundException", () => {
            mockError.code = "NotResourceNotFoundException";
            chai.assert.isFalse(isResourceNotFoundException(mockError));
        });
    });

    describe("#isResourcePreconditionNotMetException()", () => {
        it("should return true when error is a ResourcePreconditionNotMetException", () => {
            mockError.code = "ResourcePreconditionNotMetException";
            chai.assert.isTrue(isResourcePreconditionNotMetException(mockError));
        });

        it("should return false when error is not a ResourcePreconditionNotMetException", () => {
            mockError.code = "NotResourcePreconditionNotMetException";
            chai.assert.isFalse(isResourcePreconditionNotMetException(mockError));
        });
    });

    describe("#isRetryableException()", () => {
        it("should return true with statusCode 500", () => {
            mockError.code = "NotRetryableException";
            mockError.statusCode = 500;
            chai.assert.isTrue(isRetryableException(mockError));
        });

        it("should reeturn true with statusCode 503", () => {
            mockError.code = "NotRetryableException";
            mockError.statusCode = 503;
            chai.assert.isTrue(isRetryableException(mockError));
        });

        it("should return true when error is NoHttpResponseException", () => {
            mockError.code = "NoHttpResponseException";
            mockError.statusCode = 200;
            chai.assert.isTrue(isRetryableException(mockError));
        });

        it("shoud return true when error is SocketTimeoutException", () => {
            mockError.code = "SocketTimeoutException";
            mockError.statusCode = 200;
            chai.assert.isTrue(isRetryableException(mockError));
        });

        it("should return false when not a retryable exception", () => {
            mockError.code = "NotRetryableException";
            mockError.statusCode = 200;
            chai.assert.isFalse(isRetryableException(mockError));
        });

        it("should appropriately handle networking errors", () => {
            const awsError: AWSError = <AWSError><any> sandbox.mock(Error);
            const networkingError: AWSError = <AWSError><any> sandbox.mock(Error);

            // Empty originalError causes false
            awsError.originalError = undefined;
            chai.assert.isFalse(isRetryableException(awsError));

            // Empty code in networking error causes false
            awsError.originalError = networkingError;
            networkingError.code = undefined;
            chai.assert.isFalse(isRetryableException(awsError));
            
            // Wrong code in networking error causes false
            networkingError.code = "NotNetworkingError";
            chai.assert.isFalse(isRetryableException(awsError));

            // Right code causes true
            networkingError.code = "NetworkingError";
            chai.assert.isTrue(isRetryableException(awsError));
        });
    });

    describe("#isTransactionExpiredException", () => {
        it("should return true when error is an InvalidSessionException and message is Tranaction <txId> has expired", () => {
            mockError.code = "InvalidSessionException";
            mockError.message = "Transaction ABC has expired"
            chai.assert.isTrue(isTransactionExpiredException(mockError));
        });

        it("should return false when error is an InvalidSessionException but message is different", () => {
            mockError.code = "InvalidSessionException";
            mockError.message = "SessionNotIdentified"
            chai.assert.isFalse(isTransactionExpiredException(mockError));
        });

        it("should return false when error is not an InvalidSessionException ", () => {
            mockError.code = "NotInvalidSessionException";
            chai.assert.isFalse(isTransactionExpiredException(mockError));
        });
    });

    describe("#isBadRequestException()", () => {
        it("should return true when error is a BadRequestException", () => {
            mockError.code = "BadRequestException";
            chai.assert.isTrue(isBadRequestException(mockError));
        });

        it("should return false when error is not a BadRequestException", () => {
            mockError.code = "NotBadRequestException";
            chai.assert.isFalse(isBadRequestException(mockError));
        });
    });
});
