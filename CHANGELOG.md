# 3.1.0 (2023-11-10)
No new features are introduced but [570](https://github.com/awslabs/amazon-qldb-driver-nodejs/pull/570) updates the peer dependency requirement `ion-js` from `^4.3.0` to `^5.2.0`.

# 3.0.1 (2022-11-04)
This is a minor release to incorporate a recent PR by the community: [245](https://github.com/awslabs/amazon-qldb-driver-nodejs/pull/245)

## :bug: Bug Fixes
* When the driver session is not live and a new one needs to be created and returned to the callee, the session is null resulting on an error: Cannot read properties of null (reading 'executeLambda'). The problem is that session var is not getting the new session recently created and it fails by returning null. It should re-up a connection and replace the driver object with the new session to the database.


# 3.0.0 (2022-09-26)
All the changes are introduced by SDK V3, please check [Migrating to the AWS SDK for JavaScript V3](https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/migrating-to-v3.html) to learn how to migrate to the AWS SDK for JavaScript V3 from AWS SDK for JavaScript V2.

## :tada: Enhancements
* Migrated to [AWS SDK for JavasScript V3](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/index.html).

## :bug: Bug Fixes
* Fixed a order of operations bug in `defaultBackoffFunction` which would add up-to 10s of sleep over 4 retries, versus less than 300 ms total sleep between 4 retries. The defaultBackoffFunction strategy is defaulted if users do not provide their own backoff strategy function for the `RetryConfig`.

## :boom: Breaking changes
* Changed driver constructor to take a new type of [qldbClientOptions](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-qldb-session/classes/qldbsessionclient.html#constructor) and added a new parameter [httpOptions](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-qldb-session/interfaces/nodehttphandleroptions.html) to configure the low level node http client separately. Application code needs to be modified for driver construction.
For example, the following:
```typescript
import { Agent } from 'https';
import { QldbDriver, RetryConfig  } from 'amazon-qldb-driver-nodejs';

const maxConcurrentTransactions: number = 10;

const agentForQldb: Agent = new Agent({
    keepAlive: true,
    maxSockets: maxConcurrentTransactions
});

const serviceConfigurationOptions = {
    region: "us-east-1",
    httpOptions: {
        agent: agentForQldb
    }
};

const qldbDriver: QldbDriver = new QldbDriver("testLedger", serviceConfigurationOptions, maxConcurrentTransactions);
```
Should be changed to

```typescript
import { Agent } from 'https';
import { QldbDriver, RetryConfig  } from 'amazon-qldb-driver-nodejs';
import { NodeHttpHandlerOptions } from "@aws-sdk/node-http-handler";

const maxConcurrentTransactions: number = 10;

const lowLevelClientHttpOptions: NodeHttpHandlerOptions = {
    httpAgent: new Agent({
      keepAlive: true,
      maxSockets: maxConcurrentTransactions
    })
};

const serviceConfigurationOptions = {
    region: "us-east-1"
};

const qldbDriver: QldbDriver = new QldbDriver("testLedger", serviceConfigurationOptions, lowLevelClientHttpOptions, maxConcurrentTransactions);
```
* Updated driver to comply with new [service exception class](https://aws.amazon.com/blogs/developer/service-error-handling-modular-aws-sdk-js/).

# 2.2.0
This release is focused on improving the retry logic, optimizing it and handling more possible failures, as well as more
strictly defining the API to help prevent misuse. These changes are potentially breaking if the driver is being used in
a way that isn't supported by the documentation.

## :tada: Enhancements
* Improved retry logic
   * Failures when starting a new session are now retried.
   * Dead sessions are immediately discarded, reducing latency when using the driver.
   * `ClientError`, `DriverClosedError`, `LambdaAbortedError`, and `SessionPoolEmptyError` are now exported.
   * Peer dependency `aws-sdk` bumped to `2.841.0` or greater, which gives visibility to `CapacityExceededException`.
 * Updated the exponential backoff algorithm to better align with the algorithm specified [here](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/).
 * Specify minimum Node.js version to 10.0 in [package.json](https://github.com/awslabs/amazon-qldb-driver-nodejs/blob/master/package.json#L13).

## :warning: API Clean-up

These changes either remove unintentionally exported modules or remove the use of `any`. Updating to 2.2.0 should not break any documented usage of the driver and should not require code changes. If something is now incompatible and it's believed that it should be, please open an issue.

* TypeScript: updated `executeLambda` signature

  * ```typescript
    // Old
    async ExecuteLambda(transactionLambda: (transactionExecutor: TransactionExecutor) => any,
        retryConfig?: RetryConfig): Promise<any>;
    // New
    async ExecuteLambda<Type>(transactionLambda: (transactionExecutor: TransactionExecutor) => Promise<Type>,
        retryConfig?: RetryConfig): Promise<Type>;
    ```

  * The returned value from the `transactionLambda` is what `ExecuteLambda` returns, so it's now strictly defined by the `Type`.

  * The `transactionLambda` must return a `Promise`, as any methods called on the `TransactionExecutor` must be awaited for the driver to properly function.

* JavaScript: removed `QldbDriver.getSession()`

  * This is unavailable to TypeScript users and was not intended to be called directly by JavaScript users.

* Module exports

  * Removed `Transaction` from the exports list.
  * Removed modules being accessible when importing from `amazon-qldb-driver-nodejs/src`.
  * TypeScript: Removed constructors in the API for some exported types.

## :bug: Bug Fixes

* Fixed a bug where `No open transaction` or `Transaction already open` errors would occur

# 2.1.1
* Export `ResultReadable`.
* Change the return type of `executeAndStreamResults()` from `Readable` to `ResultReadable` which extends `Readable`.
* `getConsumedIOs(): IOUsage` and `getTimingInformation(): TimingInformation` functions, are accessible through `ResultReadable`.

# 2.1.0
Add support for obtaining basic server-side statistics on individual statement executions.

## :tada: Enhancements
* Added `IOUsage` and `TimingInformation` interface to provide server-side execution statistics
   * IOUsage provides `getReadIOs(): number`
   * TimingInformation provides `getProcessingTimeMilliseconds(): number`
   * Added `getConsumedIOs(): IOUsage` and `getTimingInformation(): TimingInformation` to the `Result` and `ResultStream`
   * `getConsumedIOs(): IOUsage` and `getTimingInformation(): TimingInformation` methods are stateful, meaning the statistics returned by them reflect the state at the time of method execution

#### Note: For using version 2.1.0 and above of the driver, the version of the aws-sdk should be >= 2.815

# 2.0.0 (2020-08-27)

The release candidate 1 (v2.0.0-rc.1) has been selected as a final release of v2.0.0. No new changes are introduced between v2.0.0-rc.1 and v2.0.0.
Please check the [release notes](http://github.com/awslabs/amazon-qldb-driver-nodejs/releases/tag/v2.0.0)

# 2.0.0-rc.1 (2020-08-13)

***Note: This version is a release candidate. We might introduce some additional changes before releasing v2.0.0.***

## :tada: Enchancements

* Added support for defining customer retry config and backoffs.

## :boom: Breaking changes

* Renamed `QldbDriver` property `poolLimit` to `maxConcurrentTransactions`.
* Removed `QldbDriver` property `poolTimeout`.
* Removed `retryIndicator` from `QldbSession.executeLambda` method and replaced it with `retryConfig`.
* Moved `retryLimit` from `QldbDriver` constructor to `RetryConfig` constructor.

* The classes and methods marked deprecated in version v1.0.0 have now been removed. List of classes and methods:

  * `PooledQldbDriver` has been removed. Please use `QldbDriver` instead.
  * `QldbSession.getTableNames` method has been removed. Please use `QldbDriver.getTableNames` method instead.
  * `QldbSession.executeLambda` method has been removed. Please use `QldbDriver.executeLambda` method instead.

# 1.0.0 (2020-06-05)

The release candidate 2 (v1.0.0-rc.2) has been selected as a final release of v1.0.0. No new changes are introduced between v1.0.0-rc.2 and v1.0.0.
Please check the [release notes](http://github.com/awslabs/amazon-qldb-driver-nodejs/releases/tag/v1.0.0)

# 1.0.0-rc.2 (2020-05-29)

## :tada: Enhancements

* Session pooling functionality moved to QldbDriver.  More details can be found in the [release notes](http://github.com/awslabs/amazon-qldb-driver-nodejs/releases/tag/v1.0.0-rc.2)

## :bug: Fixes
* Fixed the delay calculation logic when retrying the transaction due to failure.

## :warning: Deprecated

* `PooledQldbDriver` has been deprecated and will be removed in future versions. Please use `QldbDriver` instead. Refer to the [release notes](https://github.com/awslabs/amazon-qldb-driver-nodejs/releases/tag/v1.0.0-rc.2)

* `QldbSession.getTableNames`  method has been deprecated and will be removed in future versions. Please use `QldbDriver.getTableNames` method instead.

* `QldbSession.executeLambda`  method has been deprecated and will be removed in future versions. Please use `QldbDriver.executeLambda` method instead.

# 1.0.0-rc.1 (2020-04-03)

## :boom: Breaking changes

* [(#22)](https://github.com/awslabs/amazon-qldb-driver-nodejs/issues/22) `executeInline` method renamed to `execute` and `executeStream` method renamed to `executeAndStreamResults`.
* [(#23)](https://github.com/awslabs/amazon-qldb-driver-nodejs/issues/23) `execute` and `executeAndStreamResults`  methods  accept JavaScript built-in data types(and [Ion Value data types](https://github.com/amzn/ion-js/blob/master/src/dom/README.md#iondom-data-types))  instead of `IonWriter` type.
* [(#24)](https://github.com/awslabs/amazon-qldb-driver-nodejs/issues/24) `execute` and `executeAndStreamResults` method accepts variable number of arguments instead of passing an array of arguments.
* [(#25)](https://github.com/awslabs/amazon-qldb-driver-nodejs/issues/25) Query results will be returned as an  [Ion Value](https://github.com/amzn/ion-js/blob/master/src/dom/Value.ts)  instead of  an `IonReader` when running the PartiQL query via `execute` and/or `executeAndStreamResults`
* Removed `executeStatement` method from Qldb Session.
* Target version changed to ES6

## :tada: Enhancements

* [(#5)](https://github.com/awslabs/amazon-qldb-driver-nodejs/issues/5) The Ion Value results returned by `execute` and `executeAndStreamResults` can be converted into JSON String via `JSON.stringify(result)`

* [(#26)](https://github.com/awslabs/amazon-qldb-driver-nodejs/issues/26) Introduced `executeLambda` method on Qldb Driver.



# 0.1.2-preview.1 (2020-03-06)

## :bug: Fixes

* "Error: stream.push() after EOF" bug [#7](https://github.com/awslabs/amazon-qldb-driver-nodejs/issues/7)
* On reading from ResultStream, potential event listeners might not have received an error. Error fixed by rightly calling the destroy method and passing the error to it.
* On starting a transaction, on consuming the resultstream, the last value could sometimes show up twice.

# 0.1.1-preview.2 (2019-12-26)

## :bug: Fix

* "Digests don't match" bug [#8](https://github.com/awslabs/amazon-qldb-driver-nodejs/issues/8)

## :nut_and_bolt: Other​

* Renamed src/logUtil.ts to src/LogUtil.ts to match PascalCase.

# 0.1.0-preview.2 (2019-11-12)

## :bug: Fix

* Fix a bug in the test command that caused unit tests to fail compilation.

## :tada: Enhancement

* Add a valid `buildspec.yml` file for running unit tests via CodeBuild.

## :book: Documentation

* Small clarifications to the README.

# 0.1.0-preview.1 (2019-11-08)

* Preview release of the driver.
