# Amazon QLDB Node.js Driver

[![NPM Version](https://img.shields.io/badge/npm-v1.0.0--rc.1-green)](https://www.npmjs.com/package/amazon-qldb-driver-nodejs)[![Documentation](https://img.shields.io/badge/docs-api-green.svg)](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.nodejs.html)

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



## Getting Started

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

### See Also

1. [Amazon QLDB Nodejs Driver Tutorial](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.nodejs.html): In this tutorial, you use the QLDB Driver for Node.js to create an Amazon QLDB ledger and populate it with tables and sample data.
2. [Amazon QLDB Nodejs Driver Samples](https://github.com/aws-samples/amazon-qldb-dmv-sample-nodejs): A DMV based example application which demonstrates how to use QLDB with the QLDB Driver for Node.js.
3. QLDB Nodejs driver accepts and returns [Amazon ION](http://amzn.github.io/ion-docs/) Documents. Amazon Ion is a richly-typed, self-describing, hierarchical data serialization format offering interchangeable binary and text representations. For more information read the [ION docs](http://amzn.github.io/ion-docs/docs.html).
4. [Amazon ION Cookbook](http://amzn.github.io/ion-docs/guides/cookbook.html): This cookbook provides code samples for some simple Amazon Ion use cases.
5. Amazon QLDB supports the [PartiQL](https://partiql.org/) query language. PartiQL provides SQL-compatible query access across multiple data stores containing structured data, semistructured data, and nested data. For more information read the [PartiQL docs](https://partiql.org/docs.html).
6. Refer the section [Common Errors while using the Amazon QLDB Drivers](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-errors.html) which describes runtime errors that can be thrown by the Amazon QLDB Driver when calling the qldb-session APIs.


## Development

### Setup

To install the driver, run the following in the root directory of the project:

```npm install```

To build the driver, transpiling the TypeScript source code to JavaScript, run the following in the root directory:

```npm run build```

### Running Tests

You can run the unit tests with this command:

```npm test```

or

```npm run testWithCoverage```

### Documentation 

TypeDoc is used for documentation. You can generate HTML locally with the following:

```npm run doc```

## License

This library is licensed under the Apache 2.0 License.
