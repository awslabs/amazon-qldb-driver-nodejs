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

import * as chai from "chai";
import * as chaiAsPromised from "chai-as-promised";
import { dom, IonType } from "ion-js";

import { isOccConflictException } from "../errors/Errors";
import { QldbDriver } from "../QldbDriver";
import { Result } from "../Result";
import { ResultReadable } from "../ResultReadable";
import { RetryConfig } from "../retry/RetryConfig";
import { IOUsage } from "../stats/IOUsage";
import { TimingInformation } from "../stats/TimingInformation";
import { TransactionExecutor } from "../TransactionExecutor";
import * as constants from "./TestConstants";
import { TestUtils } from "./TestUtils";

const itParam = require("mocha-param");
chai.use(chaiAsPromised);

describe("StatementExecution", function() {
    this.timeout(0);
    let testUtils: TestUtils; 
    let driver: QldbDriver;

    before(async () => {
        testUtils = new TestUtils(constants.LEDGER_NAME);

        await testUtils.runForceDeleteLedger();
        await testUtils.runCreateLedger();

        driver = new QldbDriver(constants.LEDGER_NAME, testUtils.createClientConfiguration());

        // Create table
        const statement: string = `CREATE TABLE ${constants.TABLE_NAME}`;
        const count: number = await driver.executeLambda(async (txn: TransactionExecutor): Promise<number> => {
            const result: Result = await txn.execute(statement);
            const resultSet: dom.Value[] = result.getResultList();
            return resultSet.length;
        });
        chai.assert.equal(count, 1);
    });

    after(async () => {
        await testUtils.runDeleteLedger();
        driver.close();
    });

    this.afterEach(async () => {
        await driver.executeLambda(async (txn: TransactionExecutor) => {
            await txn.execute(`DELETE FROM ${constants.TABLE_NAME}`);
        });
    });

    it("Can create and drop a table", async () => {
        // Create table
        const createTableStatement: string = `CREATE TABLE ${constants.CREATE_TABLE_NAME}`;
        const createTableCount: number = await driver.executeLambda(async (txn: TransactionExecutor): Promise<number> => {
            const result: Result = await txn.execute(createTableStatement);
            const resultSet: dom.Value[] = result.getResultList();
            return resultSet.length;
        });
        chai.assert.equal(createTableCount, 1); 

        // List tables in ledger to ensure table is created
        const firstListTablesResult: string[] = await driver.getTableNames();
        chai.assert.isTrue(firstListTablesResult.includes(constants.CREATE_TABLE_NAME));

        // Drop table
        const dropTableStatement: string = `DROP TABLE ${constants.CREATE_TABLE_NAME}`;
        const dropTableCount: number = await driver.executeLambda(async (txn: TransactionExecutor): Promise<number> => {
            const result: Result = await txn.execute(dropTableStatement);
            const resultSet: dom.Value[] = result.getResultList();
            return resultSet.length;
        });
        chai.assert.equal(dropTableCount, 1); 

        // List tables in ledger to ensure table is dropped
        const secondListTablesResult: string[] = await driver.getTableNames();
        chai.assert.isFalse(secondListTablesResult.includes(constants.CREATE_TABLE_NAME));
    });

    it("Can return a list of table names", async () => {
        const tables: string[] = await driver.getTableNames();
        chai.assert.equal(tables[0], constants.TABLE_NAME);
    });

    it("Throws exception when creating table using the same name as an already-existing one", async () => {
        const statement: string = `CREATE TABLE ${constants.TABLE_NAME}`;
        const error = await chai.expect(driver.executeLambda(async (txn: TransactionExecutor) => {
            await txn.execute(statement);
        })).to.be.rejected;
        chai.assert.equal(error.name, "BadRequestException");
    });

    it("Can create an index", async () => {
        const createIndexStatement: string = `CREATE INDEX on ${constants.TABLE_NAME} (${constants.INDEX_ATTRIBUTE})`;

        const count: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return TestUtils.getLengthOfResultSet(await txn.execute(createIndexStatement));
        });
        chai.assert.equal(count, 1);

        const searchStatement = `SELECT VALUE indexes[0] FROM information_schema.user_tables WHERE status = 'ACTIVE'` +
            `AND name = '${constants.TABLE_NAME}'`;
        const indexResult: Result = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return await txn.execute(searchStatement);
        });
        // This gives:
        // {
        //    expr: "[MyColumn]"
        // }
        const indexColumn: string = indexResult.getResultList()[0].get("expr").stringValue();
        chai.assert.equal(indexColumn, "[" + constants.INDEX_ATTRIBUTE + "]");
    });

    it("Returns an empty result when querying table with no records", async () => {
        const statement: string = `SELECT * FROM ${constants.TABLE_NAME}`;
        const results: dom.Value[] = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return (await txn.execute(statement)).getResultList();
        });
        chai.assert.equal(results.length, 0);
    });

    it("Can insert a document", async () => {
        const struct: Record<string, string> = {
            [constants.COLUMN_NAME]: constants.SINGLE_DOCUMENT_VALUE
        };
        const insertStatement: string = `INSERT INTO ${constants.TABLE_NAME} ?`;
        const count: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return TestUtils.getLengthOfResultSet(await txn.execute(insertStatement, struct));
        });
        chai.assert.equal(count, 1);

        const searchQuery: string = `SELECT VALUE ${constants.COLUMN_NAME} FROM ${constants.TABLE_NAME} WHERE ` +
            `${constants.COLUMN_NAME} = ?`;
        const result: Result = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return await txn.execute(searchQuery, constants.SINGLE_DOCUMENT_VALUE);
        });
        const value: string = result.getResultList()[0].stringValue();
        chai.assert.equal(value, constants.SINGLE_DOCUMENT_VALUE);
    });

    it("Can query a table enclosed in quotes", async () => {
        const struct: Record<string, string> = {
            [constants.COLUMN_NAME]: constants.SINGLE_DOCUMENT_VALUE
        };
        const insertStatement: string = `INSERT INTO ${constants.TABLE_NAME} ?`;
        const count: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return TestUtils.getLengthOfResultSet(await txn.execute(insertStatement, struct));
        });
        chai.assert.equal(count, 1);
            
        const searchQuery: string = `SELECT VALUE ${constants.COLUMN_NAME} FROM "${constants.TABLE_NAME}" WHERE ` +
            `${constants.COLUMN_NAME} = ?`;
        const result: Result = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return (await txn.execute(searchQuery, constants.SINGLE_DOCUMENT_VALUE));
        });
        const value: string = result.getResultList()[0].stringValue();
        chai.assert.equal(value, constants.SINGLE_DOCUMENT_VALUE);
    });

    it("Can insert multiple documents", async () => {
        const struct1: Record<string, string> = {
            [constants.COLUMN_NAME]: constants.MULTI_DOC_VALUE_1
        };
        const struct2: Record<string, string> = {
            [constants.COLUMN_NAME]: constants.MULTI_DOC_VALUE_2
        };
        const insertStatement: string = `INSERT INTO ${constants.TABLE_NAME} <<?,?>>`;
        const count: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return TestUtils.getLengthOfResultSet(await txn.execute(insertStatement, struct1, struct2));
        });
        chai.assert.equal(count, 2);

        const searchQuery: string = `SELECT VALUE ${constants.COLUMN_NAME} FROM ${constants.TABLE_NAME} WHERE ` + 
            `${constants.COLUMN_NAME} IN (?,?)`;
        const domValues: dom.Value[] = await driver.executeLambda(async (txn: TransactionExecutor) => {
            const result: Result = await txn.execute(searchQuery, constants.MULTI_DOC_VALUE_1, constants.MULTI_DOC_VALUE_2);
            return result.getResultList();
        });
        const tables: string[] = domValues.map((value: dom.Value) => {
            return value.stringValue();
        });
        chai.assert.isTrue(tables.includes(constants.MULTI_DOC_VALUE_1));
        chai.assert.isTrue(tables.includes(constants.MULTI_DOC_VALUE_2));
    });
    
    it("Can delete a single document", async () => {
        const struct: Record<string, string> = {
            [constants.COLUMN_NAME]: constants.SINGLE_DOCUMENT_VALUE
        };
        const insertStatement: string = `INSERT INTO ${constants.TABLE_NAME} ?`;
        const count: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return TestUtils.getLengthOfResultSet(await txn.execute(insertStatement, struct));
        });
        chai.assert.equal(count, 1);

        const deleteStatement: string = `DELETE FROM ${constants.TABLE_NAME} WHERE ${constants.COLUMN_NAME} = ?`;
        const deleteCount: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return TestUtils.getLengthOfResultSet(await txn.execute(deleteStatement, constants.SINGLE_DOCUMENT_VALUE));
        });
        chai.assert.equal(deleteCount, 1);

        const searchQuery: string = `SELECT COUNT(*) FROM ${constants.TABLE_NAME}`;
        const result: Result = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return await txn.execute(searchQuery);
        });

        // This gives:
        // {
        //    _1: 1
        // }
        const searchCount: number = result.getResultList()[0].get("_1").numberValue();
        chai.assert.equal(searchCount, 0);
    });

    it("Can return metrics for executes", async () => {
        const insertStatement: string = `INSERT INTO ${constants.TABLE_NAME} << {'col': 1}, {'col': 2}, {'col': 3} >>`;
        await driver.executeLambda(async (txn: TransactionExecutor) => {
            await txn.execute(insertStatement);
        });

        const searchQuery: string = `SELECT * FROM ${constants.TABLE_NAME} as a, ${constants.TABLE_NAME} as b, ${constants.TABLE_NAME}` +
            ` as c, ${constants.TABLE_NAME} as d, ${constants.TABLE_NAME} as e, ${constants.TABLE_NAME} as f`;

        // execute()
        const result: Result = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return await txn.execute(searchQuery);
        });

        chai.assert.isNotNull(result.getConsumedIOs());
        chai.assert.isNotNull(result.getTimingInformation());
        chai.assert.equal(result.getConsumedIOs().getReadIOs(), 1092);
        chai.assert.isTrue(result.getTimingInformation().getProcessingTimeMilliseconds() > 0);

        // executeAndStreamResults
        await driver.executeLambda(async (txn: TransactionExecutor) => {
            const resultReadable: ResultReadable = await txn.executeAndStreamResults(searchQuery);

            const ioUsage: IOUsage = resultReadable.getConsumedIOs();
            const timingInformation: TimingInformation = resultReadable.getTimingInformation();

            chai.assert.isNotNull(ioUsage);
            chai.assert.isNotNull(timingInformation);
            chai.assert.isTrue(ioUsage.getReadIOs() > 0);
            chai.assert.isTrue(timingInformation.getProcessingTimeMilliseconds() > 0);
        });
    });

    it("Can delete all documents", async () => {
        const struct1: Record<string, string> = {
            [constants.COLUMN_NAME]: constants.MULTI_DOC_VALUE_1
        };
        const struct2: Record<string, string> = {
            [constants.COLUMN_NAME]: constants.MULTI_DOC_VALUE_2
        };
        const insertStatement: string = `INSERT INTO ${constants.TABLE_NAME} <<?,?>>`;
        const count: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return TestUtils.getLengthOfResultSet(await txn.execute(insertStatement, struct1, struct2));
        });
        chai.assert.equal(count, 2);

        const deleteStatement: string = `DELETE FROM ${constants.TABLE_NAME}`;
        const deleteCount: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return TestUtils.getLengthOfResultSet(await txn.execute(deleteStatement));
        });
        chai.assert.equal(deleteCount, 2);

        const searchQuery: string = `SELECT COUNT(*) FROM ${constants.TABLE_NAME}`;
        const result: Result = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return await txn.execute(searchQuery);
        });

        // This gives:
        // {
        //    _1: 1
        // }
        const searchCount: number = result.getResultList()[0].get("_1").numberValue();
        chai.assert.equal(searchCount, 0);
    });

    it("Throws OCC error if the same record is updated at the same time", async () => {
        const struct: Record<string, number> = {
            [constants.COLUMN_NAME]: 0
        };

        const result: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return TestUtils.getLengthOfResultSet(await txn.execute(`INSERT INTO ${constants.TABLE_NAME} ?`, struct));
        });
        chai.assert.equal(result, 1);
        
        // Create a driver that does not retry OCC errors
        const retryConfig: RetryConfig = new RetryConfig(0);
        const noRetryDriver: QldbDriver = new QldbDriver(constants.LEDGER_NAME, testUtils.createClientConfiguration(), {}, 3, retryConfig);
        async function updateField(driver: QldbDriver): Promise<void> {
            await driver.executeLambda(async (txn: TransactionExecutor) => {
                let currentValue: number;
                
                // Query table
                const result: Result = await txn.execute(`SELECT VALUE ${constants.COLUMN_NAME} from ${constants.TABLE_NAME}`);
                currentValue = result.getResultList()[0].numberValue();

                // Update document
                await txn.execute(`UPDATE ${constants.TABLE_NAME} SET ${constants.COLUMN_NAME} = ?`, currentValue + 5);
            });
        }

        let occFlag: boolean = false;
        try {   
            await Promise.all([updateField(noRetryDriver), updateField(noRetryDriver), updateField(noRetryDriver)]);
        } catch (e) {
            if (isOccConflictException(e)) {
                occFlag = true;
            }
        }
        chai.assert.isTrue(occFlag);
    });

    itParam("Can insert and read different Ion types", TestUtils.getIonTypes(), async (value: dom.Value) => {
        const struct: Record<string, dom.Value> = {
            [constants.COLUMN_NAME]: value
        };

        await driver.executeLambda(async (txn: TransactionExecutor) => {
            // Insert the Ion Value
            const numberOfInsertedDocs: number = TestUtils.getLengthOfResultSet(await txn.execute(`INSERT INTO ${constants.TABLE_NAME} ?`, struct));
            chai.assert.equal(numberOfInsertedDocs, 1);

            // Read the Ion Value
            const returnedValue: dom.Value = await testUtils.readIonValue(txn, value);
            chai.assert.deepEqual(returnedValue, value);
        });
    });

    itParam("Can update different Ion types", TestUtils.getIonTypes(), async (value: dom.Value) => {
        const struct: Record<string, dom.Value> = {
            [constants.COLUMN_NAME]: dom.load("null")
        };

        await driver.executeLambda(async (txn: TransactionExecutor) => {
            // Insert a base Ion Value
            const numberOfInsertedDocs: number = TestUtils.getLengthOfResultSet(await txn.execute(`INSERT INTO ${constants.TABLE_NAME} ?`, struct));
            chai.assert.equal(numberOfInsertedDocs, 1);

            // Update the Ion Value
            const updateQuery: string = `UPDATE ${constants.TABLE_NAME} SET ${constants.COLUMN_NAME} = ?`;
            const numberOfUpdatedDocs: number = TestUtils.getLengthOfResultSet(await txn.execute(updateQuery, value));
            chai.assert.equal(numberOfUpdatedDocs, 1);

            // Read the Ion Value
            const returnedValue: dom.Value = await testUtils.readIonValue(txn, value);
            chai.assert.deepEqual(returnedValue, value);
        });
    });

    it("Statements are executed without needing a returned value", async () => {
        const struct: Record<string, string> = {
            [constants.COLUMN_NAME]: constants.SINGLE_DOCUMENT_VALUE
        };
        const insertStatement: string = `INSERT INTO ${constants.TABLE_NAME} ?`;
        await driver.executeLambda(async (txn: TransactionExecutor) => {
            return TestUtils.getLengthOfResultSet(await txn.execute(insertStatement, struct));
        });

        const searchQuery: string = `SELECT VALUE ${constants.COLUMN_NAME} FROM ${constants.TABLE_NAME} WHERE ` +
            `${constants.COLUMN_NAME} = ?`;
        const result: Result = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return await txn.execute(searchQuery, constants.SINGLE_DOCUMENT_VALUE);
        });

        const value: string = result.getResultList()[0].stringValue();
        chai.assert.equal(value, constants.SINGLE_DOCUMENT_VALUE);
    });

    it("Throws exception when deleting from a table that doesn't exist", async () => {
        const statement: string = "DELETE FROM NonExistentTable";
        const error = await chai.expect(driver.executeLambda(async (txn: TransactionExecutor) => {
            await txn.execute(statement);
        })).to.be.rejected;
        chai.assert.equal(error.name, "BadRequestException");
    });
});
