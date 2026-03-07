# Greenmask 0.1.0

We are excited to announce the release of Greenmask v0.1.0, marking the first production-ready version. This release addresses various bug fixes, introduces improvements, and includes documentation refactoring for enhanced clarity.

## New features

- Added positional arguments for the list-transformers command, allowing specific transformer information retrieval (e.g., `greenmask list-transformers RandomDate`).

- Added a version parameter `--version` that prints Greenmask version.

- Added numeric parameters support for `-Int` and `-Float` transformers.

## Improvements

- Improved verbosity in custom transformer interaction, accumulating `stderr` data and forwarding it in batches instead of writing it one by one.

- Updated dependencies to newer versions.

- Enhanced the stability of the JSON line interaction protocol by utilizing the stdlib JSON encoder/decoder.

- Modified the method for sending table metadata to custom transformers; now, it is sent via `stdin` in the first line in JSON format instead of providing it via command arguments.

- Refactored template functions naming.

- Refactored `NoiseDate` transformer implementation for improved stability and predictability.

- Changed the default value for the `Dict` transformer: `fail_not_matched parameter: true`.

- Refactored the `Hash` transformer to provide a salt parameter and receive a base64 encoded salt. If salt is not provided, it generates one randomly.

- Added validation for the truncate parameter of `NoiseDate` and `RandomDate` transformers that issues a warning if the provided value is invalid.

- Increased verbosity of parameter validation warnings, now properly forwarding warnings to `stdout`.

## Fixes

- Resolved `pgx` driver connection leakage issue.

- Fixed deletion failure of dumps for S3 storage.

- Corrected cobra autocompletion for the Greenmask utility.

- Fixed NOT NULL constraint validation.

- Addressed JSON API interaction issues that previously caused deadlocks and timeouts.

- Fixed encode-decoding for binary parameters, ensuring accurate forwarding of values to custom transformers.

- Fixed the `RandomChoice` transformer to correctly marshal and unmarshal values during validation.

- Introduced the nullable property for the `SetNull` transformer to enhance NOT NULL constraint validation.

- Resolved text wrapping issues for the `validate` command.

- Fixed build failures on Windows due to Linux platform dependencies.

- Corrected `stdout` readline buffer reading during interaction with custom transformers.

- Fixed integration tests.

## Ecosystem changes

- Implemented CI/CD pipelines for the entire project.

- Established a user-friendly playground in Docker compose, including:

  - Deployed Minio storage container.
  - PostgreSQL container containing both the original database (Adventure Works) and the transformed (empty DB).
  - Greenmask container itself.

- Refactored current readme files.

## Assets

To download the Greenmask binary compatible with your system, see the [release's assets list](https://github.com/GreenmaskIO/greenmask/releases/tag/v0.1.0).
