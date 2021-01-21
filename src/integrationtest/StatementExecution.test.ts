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
        const firstListTablesResult: string[] = await driver.getTableNames()
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
        const secondListTablesResult: string[] = await driver.getTableNames()
        chai.assert.isFalse(secondListTablesResult.includes(constants.CREATE_TABLE_NAME));
    });

    it("Can return a list of table names", async () => {
        const tables: string[] = await driver.getTableNames();
        chai.assert.equal(tables[0], constants.TABLE_NAME);
    });

    it("Throws exception when creating table using the same name as an already-existing one", async () => {
        const statement: string = `CREATE TABLE ${constants.TABLE_NAME}`;
        const error: AWSError = await chai.expect(driver.executeLambda(async (txn: TransactionExecutor) => {
            await txn.execute(statement);
        })).to.be.rejected;
        chai.assert.equal(error.code, "BadRequestException");
    });

    it("Can create an index", async () => {
        const createIndexStatement: string = `CREATE INDEX on ${constants.TABLE_NAME} (${constants.INDEX_ATTRIBUTE})`;

        const count: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            const result: Result = await txn.execute(createIndexStatement);
            return result.getResultList().length;
        });
        chai.assert.equal(count, 1);

        const searchStatement = `SELECT VALUE indexes[0] FROM information_schema.user_tables WHERE status = 'ACTIVE'` +
            `AND name = '${constants.TABLE_NAME}'`;
        const indexColumn: string = await driver.executeLambda(async (txn: TransactionExecutor) => {
            const result: Result = await txn.execute(searchStatement);
            // This gives:
            // {
            //    expr: "[MyColumn]"
            // }
            const indexColumn: string = result.getResultList()[0].elements()[0].stringValue();
            return indexColumn;
        });
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
            return (await txn.execute(insertStatement, struct)).getResultList().length;
        });
        chai.assert.equal(count, 1);

        const searchQuery: string = `SELECT VALUE ${constants.COLUMN_NAME} FROM ${constants.TABLE_NAME} WHERE ` +
            `${constants.COLUMN_NAME} = ?`;
        const value: string = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return (await txn.execute(searchQuery, constants.SINGLE_DOCUMENT_VALUE)).getResultList()[0].stringValue();
        });
        chai.assert.equal(value, constants.SINGLE_DOCUMENT_VALUE);
    });

    it("Can query a table enclosed in quotes", async () => {
        const struct: Record<string, string> = {
            [constants.COLUMN_NAME]: constants.SINGLE_DOCUMENT_VALUE
        };
        const insertStatement: string = `INSERT INTO ${constants.TABLE_NAME} ?`;
        const count: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return (await txn.execute(insertStatement, struct)).getResultList().length;
        });
        chai.assert.equal(count, 1);
            
        const searchQuery: string = `SELECT VALUE ${constants.COLUMN_NAME} FROM "${constants.TABLE_NAME}" WHERE ` +
            `${constants.COLUMN_NAME} = ?`;
        const value: string = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return (await txn.execute(searchQuery, constants.SINGLE_DOCUMENT_VALUE)).getResultList()[0].stringValue();
        });
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
            return (await txn.execute(insertStatement, struct1, struct2)).getResultList().length;
        });
        chai.assert.equal(count, 2);

        const searchQuery: string = `SELECT VALUE ${constants.COLUMN_NAME} FROM ${constants.TABLE_NAME} WHERE ` + 
            `${constants.COLUMN_NAME} IN (?,?)`;
        const tables: string[] = await driver.executeLambda(async (txn: TransactionExecutor) => {
            const result: Result = await txn.execute(searchQuery, constants.MULTI_DOC_VALUE_1, constants.MULTI_DOC_VALUE_2);
            const resultSet: dom.Value[] = result.getResultList();
            return resultSet.map((value: dom.Value) => {
                return value.stringValue();
            });
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
            return (await txn.execute(insertStatement, struct)).getResultList().length;
        });
        chai.assert.equal(count, 1);

        const deleteStatement: string = `DELETE FROM ${constants.TABLE_NAME} WHERE ${constants.COLUMN_NAME} = ?`;
        const deleteCount: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return (await txn.execute(deleteStatement, constants.SINGLE_DOCUMENT_VALUE)).getResultList().length;
        });
        chai.assert.equal(deleteCount, 1);

        const searchQuery: string = `SELECT COUNT(*) FROM ${constants.TABLE_NAME}`;
        const searchCount: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            // This gives:
            // {
            //    _1: 1
            // }
            return (await txn.execute(searchQuery)).getResultList()[0].elements()[0].numberValue();
        });

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
            const result: ResultReadable = await txn.executeAndStreamResults(searchQuery);

            for await (const chunk of result) {
                // Iterate through result
            }

            const ioUsage: IOUsage = result.getConsumedIOs();
            const timingInformation: TimingInformation = result.getTimingInformation();

            chai.assert.isNotNull(ioUsage);
            chai.assert.isNotNull(timingInformation);
            chai.assert.equal(ioUsage.getReadIOs(), 1092);
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
            return (await txn.execute(insertStatement, struct1, struct2)).getResultList().length;
        });
        chai.assert.equal(count, 2);

        const deleteStatement: string = `DELETE FROM ${constants.TABLE_NAME}`;
        const deleteCount: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return (await txn.execute(deleteStatement)).getResultList().length;
        });
        chai.assert.equal(deleteCount, 2);

        const searchQuery: string = `SELECT COUNT(*) FROM ${constants.TABLE_NAME}`;
        const searchCount: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            // This gives:
            // {
            //    _1: 1
            // }
            return (await txn.execute(searchQuery)).getResultList()[0].elements()[0].numberValue();
        });
        chai.assert.equal(searchCount, 0);
    });

    it("Throws OCC error if the same record is updated at the same time", async () => {
        const struct: Record<string, number> = {
            [constants.COLUMN_NAME]: 0
        };

        const result: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return (await txn.execute(`INSERT INTO ${constants.TABLE_NAME} ?`, struct)).getResultList().length;
        });
        chai.assert.equal(result, 1);
        
        // Create a driver that does not retry OCC errors
        const retryConfig: RetryConfig = new RetryConfig(0);
        const noRetryDriver: QldbDriver = new QldbDriver(constants.LEDGER_NAME, testUtils.createClientConfiguration(), 3, retryConfig);
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
        // Insert the Ion Value
        const struct: Record<string, dom.Value> = {
            [constants.COLUMN_NAME]: value
        };

        const result: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return (await txn.execute(`INSERT INTO ${constants.TABLE_NAME} ?`, struct)).getResultList().length;
        });
        chai.assert.equal(result, 1);

        // Read the Ion Value
        if (value.isNull()) {
            const searchQuery: string = `SELECT VALUE ${constants.COLUMN_NAME} FROM ${constants.TABLE_NAME}` + 
                ` WHERE ${constants.COLUMN_NAME} IS NULL`;
            const returnedValue: IonType = await driver.executeLambda(async (txn: TransactionExecutor) => {
                const result: Result = await txn.execute(searchQuery);
                const resultSet: dom.Value[] = result.getResultList();
                return resultSet[0].getType();
            });
            chai.assert.equal(returnedValue, value.getType());

        } else {
            const searchQuery: string = `SELECT VALUE ${constants.COLUMN_NAME} FROM ${constants.TABLE_NAME}` + 
            ` WHERE ${constants.COLUMN_NAME} = ?`;
            const returnedValue: IonType = await driver.executeLambda(async (txn: TransactionExecutor) => {
                const result: Result = await txn.execute(searchQuery, value);
                const resultSet: dom.Value[] = result.getResultList();
                return resultSet[0].getType();
            });
            chai.assert.equal(returnedValue, value.getType());
        }
    });

    itParam("Can update different Ion types", TestUtils.getIonTypes(), async (value: dom.Value) => {
        // Insert a base Ion Value
        const struct: Record<string, dom.Value> = {
            [constants.COLUMN_NAME]: dom.load("null")
        };

        const result: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return (await txn.execute(`INSERT INTO ${constants.TABLE_NAME} ?`, struct)).getResultList().length;
        });
        chai.assert.equal(result, 1);

        // Update the Ion Value
        const updateQuery: string = `UPDATE ${constants.TABLE_NAME} SET ${constants.COLUMN_NAME} = ?`;
        const updateResult: number = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return (await txn.execute(updateQuery, value)).getResultList().length;
        });
        chai.assert.equal(updateResult, 1);
        
        // Read the Ion Value
        if (value.isNull()) {
            const searchQuery: string = `SELECT VALUE ${constants.COLUMN_NAME} FROM ${constants.TABLE_NAME}` + 
                ` WHERE ${constants.COLUMN_NAME} IS NULL`;
            const returnedValue: IonType = await driver.executeLambda(async (txn: TransactionExecutor) => {
                const result: Result = await txn.execute(searchQuery);
                const resultSet: dom.Value[] = result.getResultList();
                return resultSet[0].getType();
            });
            chai.assert.equal(returnedValue, value.getType());

        } else {
            const searchQuery: string = `SELECT VALUE ${constants.COLUMN_NAME} FROM ${constants.TABLE_NAME}` + 
                ` WHERE ${constants.COLUMN_NAME} = ?`;
            const returnedValue: IonType = await driver.executeLambda(async (txn: TransactionExecutor) => {
                const result: Result = await txn.execute(searchQuery, value);
                const resultSet: dom.Value[] = result.getResultList();
                return resultSet[0].getType();
            });
            chai.assert.equal(returnedValue, value.getType());
        }
    });

    it("Statements are executed without needing a returned value", async () => {
        const struct: Record<string, string> = {
            [constants.COLUMN_NAME]: constants.SINGLE_DOCUMENT_VALUE
        };
        const insertStatement: string = `INSERT INTO ${constants.TABLE_NAME} ?`;
        await driver.executeLambda(async (txn: TransactionExecutor) => {
            return (await txn.execute(insertStatement, struct)).getResultList().length;
        });

        const searchQuery: string = `SELECT VALUE ${constants.COLUMN_NAME} FROM ${constants.TABLE_NAME} WHERE ` + 
            `${constants.COLUMN_NAME} = ?`;
        const value: string = await driver.executeLambda(async (txn: TransactionExecutor) => {
            return (await txn.execute(searchQuery, constants.SINGLE_DOCUMENT_VALUE)).getResultList()[0].stringValue();
        });
        chai.assert.equal(value, constants.SINGLE_DOCUMENT_VALUE);
    });

    it("Throws exception when deleting from a table that doesn't exist", async () => {
        const statement: string = "DELETE FROM NonExistentTable";
        const error: AWSError = await chai.expect(driver.executeLambda(async (txn: TransactionExecutor) => {
            await txn.execute(statement);
        })).to.be.rejected;
        chai.assert.equal(error.code, "BadRequestException");
    });
});
