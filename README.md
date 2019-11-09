# AmazonQLDB Node.js Driver

This is the Node.js driver for Amazon Quantum Ledger Database (QLDB), which allows Node.js developers
to write software that makes use of AmazonQLDB.

**This is a preview release of the Amazon QLDB Driver for Node.js, and we do not recommend that it be used for production purposes.**

## Requirements

### Basic Configuration

You need to set up your AWS security credentials and config before the driver is able to connect to AWS. 

Set up credentials (in e.g. `~/.aws/credentials`):

```
[default]
aws_access_key_id = <your access key id>
aws_secret_access_key = <your secret key>
```

Set up a default region (in e.g. `~/.aws/config`):

```
[default]
region = us-east-1 <or other region>
```

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html#SettingUp.Q.GetCredentials) page for more information.

### TypeScript 3.5.x

The driver is written in, and requires, TypeScript 3.5.x. It will be automatically installed as a dependency. 
Please see the link below for more detail on TypeScript 3.5.x:

* [TypeScript 3.5.x](https://www.npmjs.com/package/typescript)

## Installing the Driver

To install the driver, run the following in the root directory of the project:

```npm install```

To build the driver, transpiling the TypeScript source code to JavaScript, run the following in the root directory:

```npm run build```

## Using the Driver as a Dependency

To use the driver, in your package that wishes to use the driver, run the following:

```npm install amazon-qldb-driver-nodejs```

The driver also has aws-sdk and ion-js as peer dependencies. Thus, they must also be dependencies of the package that
will be using the driver as a dependency.

```npm install aws-sdk```

```npm install ion-js```

Then from within your package, you can call the use the driver. This example shows usage in TypeScript specifying the 
ledger name:

```javascript
import { PooledQldbDriver, QldbSession } from "amazon-qldb-driver-nodejs";

const testServiceConfigOptions = {
    region: "us-east-1"
};

const qldbDriver: PooledQldbDriver = new PooledQldbDriver(testServiceConfigOptions, "testLedger");
const qldbSession: QldbSession = await qldbDriver.getSession();

for (const table of await qldbSession.getTableNames()) {
    console.log(table);
}
```

## Development

### Running Tests

You can run the unit tests with this command:

```
$ npm run testWithCoverage
```

The performance tests have a separate README.md within the performance folder.

### Documentation 

TypeDoc is used for documentation. You can generate HTML locally with the following:

```npm run doc```

## Release Notes

### Release 0.1.0-preview.1 (November 8, 2019)

Preview release of the driver.
