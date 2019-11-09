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

// Test environment imports
import "mocha";

import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import * as sinon from "sinon";

import { createQldbWriter, QldbWriter } from "../QldbWriter";

chai.use(chaiAsPromised);
const sandbox = sinon.createSandbox();

describe("QldbWriter", () => {

    afterEach(() => {
        sandbox.restore();
    });

    describe("#createQldbWriter()", () => {
        it("should return a QldbWriter object when called", () => {
            let qldbWriter: QldbWriter = createQldbWriter();
            chai.assert.isTrue("getBytes" in qldbWriter);
        });
    });
});
