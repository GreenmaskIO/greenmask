# Greenmask 0.1.9

This release introduces improvements and bug fixes

## Improvements

* Implemented tables scoring according to the table size and transformation costs. This correctly spread the tables
  dumping between the requested workers pool and reduces the execution time. Now greenmask introspects the table size,
  adds the transformation scoring using the formula
  `score = tableSizeInBytes + (tableSizeInBytes * 0.03 * tableTransformationsCount)`, and uses the strategy "Largest
  First". The problem is described [here](https://github.com/GreenmaskIO/greenmask/discussions/50)
* Introduced `no_verify_ssl` parameter for S3 storage
* Adjusted Dockerfile
    * Changed entrypoint to `greenmask` binary
    * The `greenmask` container now runs under `greenmask` user and groups
* Refactored storage config structure. Now it contains the `type` that is used for the storage type determination
* Most of the attributes may be overridden with environment variables where the letters are capitalized and the dots
  are replaced with underscores. For instance, the setting `storage.type` might be represented with the environment
  variable `STORAGE_TYPE`
* Parameter `--config` is not required anymore. This simplifies the greenmask utility user experience
* Directory storage set as the default
* Set the default temporary directory as `/tmp`
* Added environment variable section to the configuration docs

## Fixes

* Fixed `S3_REGION` environment variable usage. Tested cases where the S3 storage is set up using `S3` variables that
  uses by `github.com/aws/aws-sdk-go`
* Updated project dependencies to the latest version

## Assets

To download the Greenmask binary compatible with your system, see
the [release's assets list](https://github.com/GreenmaskIO/greenmask/releases/tag/v0.1.9).
