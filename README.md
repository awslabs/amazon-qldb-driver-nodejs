# Amazon QLDB Node.js Driver

[![NPM Version](https://img.shields.io/badge/npm-v2.1.1-green)](https://www.npmjs.com/package/amazon-qldb-driver-nodejs)
[![Documentation](https://img.shields.io/badge/docs-api-green.svg)](https://docs.aws.amazon.com/qldb/latest/developerguide/getting-started.nodejs.html)
[![license](https://img.shields.io/badge/license-Apache%202.0-blue)](https://github.com/awslabs/amazon-qldb-driver-nodejs/blob/master/LICENSE)
[![AWS Provider](https://img.shields.io/badge/provider-AWS-orange?logo=amazon-aws&color=ff9900)](https://aws.amazon.com/qldb/)

This is the Node.js driver for Amazon Quantum Ledger Database (QLDB), which allows Node.js developers to write software that makes use of AmazonQLDB.

## Requirements

### Basic Configuration

See [Accessing Amazon QLDB](https://docs.aws.amazon.com/qldb/latest/developerguide/accessing.html) for information on connecting to AWS.

The JavaScript AWS SDK needs to have AWS_SDK_LOAD_CONFIG environment variable set to a truthy value in order to read
from the ~/.aws/config file.

See [Setting Region](https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/setting-region.html) page for more information.

### TypeScript 3.8.x

The driver is written in, and requires, TypeScript 3.8.x. It will be automatically installed as a dependency. 
Please see the link below for more detail on TypeScript 3.8.x:

* [TypeScript 3.8.x](https://www.npmjs.com/package/typescript)


## Getting Started

Please see the [Quickstart guide for the Amazon QLDB Driver for Node.js](https://docs.aws.amazon.com/qldb/latest/developerguide/driver-quickstart-nodejs.html).

To use the driver, in your package that wishes to use the driver, run the following:

```npm install amazon-qldb-driver-nodejs```

The driver also has aws-sdk, ion-js and jsbi as peer dependencies. Thus, they must also be dependencies of the package that will be using the driver as a dependency.

```npm install aws-sdk```

```npm install ion-js```

```npm install jsbi```

#### Note: For using version 2.1.0 and above of the driver, the version of the aws-sdk should be >= 2.815

Then from within your package, you can now use the driver by importing it. This example shows usage in TypeScript specifying the QLDB ledger name and a specific region:

```javascript
import { QldbDriver } from "amazon-qldb-driver-nodejs";

const testServiceConfigOptions = {
    region: "us-east-1"
};

const qldbDriver: QldbDriver = new QldbDriver("testLedger", testServiceConfigOptions);
qldbDriver.getTableNames().then(function(tableNames: string[]) {
    console.log(tableNames);
});
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

To install the dependencies for the driver, run the following in the root directory of the project:

```npm install```

To build the driver, transpiling the TypeScript source code to JavaScript, run the following in the root directory:

```npm run build```

### Running Tests

You can run the unit tests with this command:

```npm test```

or

```npm run testWithCoverage```

### Integration Tests

You can run the integration tests with this command:

```npm run integrationTest```

This command requires that credentials are pre-configured and it has the required permissions.

Additionally, a region can be specified in: `src/integrationtest/.mocharc.json`.

### Documentation 

TypeDoc is used for documentation. You can generate HTML locally with the following:

```npm run doc```

## Getting Help

Please use these community resources for getting help.
* Ask a question on StackOverflow and tag it with the [amazon-qldb](https://stackoverflow.com/questions/tagged/amazon-qldb) tag.
* Open a support ticket with [AWS Support](http://docs.aws.amazon.com/awssupport/latest/user/getting-started.html).
* If you think you may have found a bug, please open an [issue](https://github.com/awslabs/amazon-qldb-driver-nodejs/issues/new).

## Opening Issues

If you encounter a bug with the Amazon QLDB Node.js Driver, we would like to hear about it. Please search the [existing issues](https://github.com/awslabs/amazon-qldb-driver-nodejs/issues) and see if others are also experiencing the issue before opening a new issue. When opening a new issue, we will need the version of Amazon QLDB Node.js Driver, Node.js language version, and OS you’re using. Please also include reproduction case for the issue when appropriate.

The GitHub issues are intended for bug reports and feature requests. For help and questions with using AWS QLDB Node.js Driver please make use of the resources listed in the [Getting Help](https://github.com/awslabs/amazon-qldb-driver-nodejs#getting-help) section. Keeping the list of open issues lean will help us respond in a timely manner.

## License

This library is licensed under the Apache 2.0 License.
