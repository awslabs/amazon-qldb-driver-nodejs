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

