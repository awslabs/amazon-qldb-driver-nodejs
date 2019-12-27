# Amazon QLDB Node.js Driver

This is the Node.js driver for Amazon Quantum Ledger Database (QLDB), which allows Node.js developers
to write software that makes use of AmazonQLDB.

**This is a preview release of the Amazon QLDB Driver for Node.js, and we do not recommend that it be used for production purposes.**

## Requirements

### Basic Configuration

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html) for information on connecting to AWS.

The JavaScript AWS SDK needs to have AWS_SDK_LOAD_CONFIG environment variable set to a truthy value in order to read
from the ~./.aws/config file.

See [Setting Region](https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/setting-region.html) page for more information.

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

Then from within your package, you can now use the driver by importing it. This example shows usage in TypeScript 
specifying the QLDB ledger name and a specific region:

```javascript
import { PooledQldbDriver, QldbSession } from "amazon-qldb-driver-nodejs";

const testServiceConfigOptions = {
    region: "us-east-1"
};

const qldbDriver: PooledQldbDriver = new PooledQldbDriver("testLedger", testServiceConfigOptions));
const qldbSession: QldbSession = await qldbDriver.getSession();

for (const table of await qldbSession.getTableNames()) {
    console.log(table);
}
```

## Development

### Running Tests

You can run the unit tests with this command:

```npm test```

or

```npm run testWithCoverage```

### Documentation 

TypeDoc is used for documentation. You can generate HTML locally with the following:

```npm run doc```

## Release Notes

### Release 0.1.1-preview.2 (December 26, 2019)

* Fix "Digests don't match" bug #8
* Renamed src/logUtil.ts to src/LogUtil.ts to match PascalCase.

### Release 0.1.0-preview.2 (November 12, 2019)

* Fix a bug in the test command that caused unit tests to fail compilation.
* Small clarifications to the README.
* Addition of a valid `buildspec.yml` file for running unit tests via CodeBuild.

### Release 0.1.0-preview.1 (November 8, 2019)

Preview release of the driver.

## License

This library is licensed under the Apache 2.0 License.
