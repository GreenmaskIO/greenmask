# Greenmask 0.2.24

## Changes

* feat: add the [HashedPassword](../built_in_transformers/standard_transformers/hashed_password.md) transformer. It
  writes a bcrypt hash of one known password into a column, so test accounts can log in after the restore. The password
  is read from an environment variable via `resolve_env`, so it stays out of the config and the
  dump [#482](https://github.com/GreenmaskIO/greenmask/pull/482). Closes
  [#480](https://github.com/GreenmaskIO/greenmask/issues/480)
* feat: add server-side encryption for the S3 storage. The new `sse` parameter sets the encryption mode
  (`AES256`, `aws:kms` or `aws:kms:dsse`), `kms_key_arn` selects the KMS key for the KMS-backed modes, and
  `bucket_key_enabled` turns on [S3 Bucket Keys](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-key.html)
  to cut KMS request cost on large dumps. Encryption is applied to both single-part and multipart uploads, so dumps
  larger than `max_part_size` are covered as
  well [#473](https://github.com/GreenmaskIO/greenmask/pull/473) [#484](https://github.com/GreenmaskIO/greenmask/pull/484)
* feat: forward PostgreSQL notices to the log. Messages raised with `RAISE NOTICE` or `RAISE WARNING` were previously
  discarded at every log level. Notices from restore scripts are logged between the `executing script` and
  `script execution complete` entries: `WARNING` at `warn`, `NOTICE`, `INFO` and `LOG` at `info`, `DEBUG` at
  `debug` [#479](https://github.com/GreenmaskIO/greenmask/pull/479). Notices raised while dumping or restoring table
  data (for example by row-level triggers) are tagged with the worker id and logged at `debug`, except `WARNING`, which
  stays at `warn`, so a trigger raising one notice per row does not flood the
  log [#485](https://github.com/GreenmaskIO/greenmask/pull/485)
* fix: validate the S3 storage config before the dump starts. An unknown `sse` value now fails immediately instead of
  on the first upload, once the dump has already been produced. Setting `kms_key_arn` or `bucket_key_enabled` without a
  KMS-backed `sse` is rejected as well — previously the KMS key was silently dropped and the dump was encrypted with a
  key other than the configured one [#484](https://github.com/GreenmaskIO/greenmask/pull/484)
* fix: use the upstream Minio image for the integration test storage
  service [#483](https://github.com/GreenmaskIO/greenmask/pull/483)
* docs: extend the [supporting a new PostgreSQL version](../supporting_new_postgres.md)
  guide [#477](https://github.com/GreenmaskIO/greenmask/pull/477)

#### Full Changelog: [v0.2.23...v0.2.24](https://github.com/GreenmaskIO/greenmask/compare/v0.2.23...v0.2.24)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Discord](https://discord.gg/tAJegUKSTB)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
