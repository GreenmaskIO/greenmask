# Greenmask 0.2.24

## Changes

* feat: add server-side encryption for the S3 storage. The new `sse` parameter sets the encryption mode
  (`AES256`, `aws:kms` or `aws:kms:dsse`), `kms_key_arn` selects the KMS key for the KMS-backed modes, and
  `bucket_key_enabled` turns on [S3 Bucket Keys](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-key.html)
  to cut KMS request cost on large dumps. Encryption is applied to both single-part and multipart uploads, so dumps
  larger than `max_part_size` are covered as
  well [#473](https://github.com/GreenmaskIO/greenmask/pull/473)
* fix: validate the S3 storage config before the dump starts. An unknown `sse` value now fails immediately instead of
  on the first upload, once the dump has already been produced. Setting `kms_key_arn` or `bucket_key_enabled` without a
  KMS-backed `sse` is rejected as well — previously the KMS key was silently dropped and the dump was encrypted with a
  key other than the configured one
* fix: use the upstream Minio image for the integration test storage
  service [#483](https://github.com/GreenmaskIO/greenmask/pull/483)

#### Full Changelog: [v0.2.23...v0.2.24](https://github.com/GreenmaskIO/greenmask/compare/v0.2.23...v0.2.24)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Discord](https://discord.gg/tAJegUKSTB)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
