# Greenmask 0.3.0

Greenmask 0.3.0 moves the storage backends to the [greenmaskio/storages](https://github.com/greenmaskio/storages) library and the S3 backend from the unsupported `aws-sdk-go` v1 to `aws-sdk-go-v2`. The config file, the environment variables, the dump format and the dump IDs stay the same.

## Changes

* feat: `directory`, `s3`, `azure` and `ssh` storages now come from the [greenmaskio/storages](https://github.com/greenmaskio/storages) library. The S3 storage runs on `aws-sdk-go-v2`, because support for `aws-sdk-go` v1 ended on 2025-07-31
* feat: PostgreSQL 19 is supported and tested by the integration suite. The dump format of PostgreSQL 19 is the same as in PostgreSQL 18 (archive version 1.16). See [Supporting a New PostgreSQL Version](../supporting_new_postgres.md)
* feat: new `ssh.private_key_passphrase` key for encrypted private keys, and `ssh.connect_timeout` to limit the TCP connection and the SSH handshake (default `30s`). See [ssh storage](../configuration.md#storage-section)
* feat: new `azure.max_retries` key, the number of retries after a failed try (default `3`)
* fix: with `s3.role_arn`, Greenmask now works under the assumed role and renews its credentials before they expire. Before, the role was applied only when `access_key_id` and `secret_access_key` were also set, and `session_name` was required
* fix: `Hash`, `Replace` and `RegexpReplace` with `apply_for_references: true` are now applied to the referencing foreign-key columns. Before, the config passed validation, but the transformer was applied only to the primary key, and the referencing columns were dumped unchanged ([#571](https://github.com/GreenmaskIO/greenmask/issues/571))
* fix: `apply_for_references` now reaches every table that references a transformed column, at any depth. Before, below the first level a table got the transformer only when its foreign key was part of its own primary key, so the foreign key of the last table in a chain was dumped unchanged
* fix: a transformer with `apply_for_references` set by hand on a table in the middle of a chain is no longer applied twice to the tables below it
* fix: `validate --diff` now deletes its temporary directory after the run
* fix: storage keys are checked before use. `restore` and `show-dump` with a dump ID that leaves the storage, such as `../x`, report that the dump is not found
* docs: `s3.disable_ssl` is deprecated. It never had an effect: put `http://` in `endpoint` to use plain HTTP
* chore: Greenmask is built with Go 1.27.4
* chore: [greenmaskio/storages](https://github.com/greenmaskio/storages) is updated to v0.5.1, with a newer AWS SDK for the S3 storage

## Upgrade notes

The dump format and the storage layout have not changed. Dumps made by 0.2.x can be listed, restored and deleted by 0.3.0, and the other way round, so rolling back to 0.2.27 is safe.

* **S3 with `role_arn`.** If `role_arn` is set without `access_key_id` and `secret_access_key`, 0.2.x assumed the role but kept using the default credentials. 0.3.0 uses the role, so the role itself needs access to the bucket. The role is assumed with the default AWS credential chain on the first storage request, so a wrong or untrusted role fails there, not at startup.
* **S3 shared config.** The AWS SDK now always reads `~/.aws/config`, including the region and the credentials of the active profile. Before, it read the file only with `AWS_SDK_LOAD_CONFIG=1`. A `region` set in the Greenmask config takes precedence.
* **S3 endpoint and retries.** An `endpoint` without a scheme is still used as `https://<endpoint>`, and `max_retries` still counts retries after the first attempt.
* **Logs.** Debug output of the AWS and Azure SDKs and the wording of some storage errors have changed. Messages about invalid storage config are the same.
* **SSH.** The connection now times out after `30s` by default. Raise `ssh.connect_timeout` for hosts that are slower to connect.
* **Go 1.27.** The macOS binaries need macOS 13 Ventura or later. The legacy TLS settings `GODEBUG=tlsrsakex=1`, `tls3des=1` and `tls10server=1` are gone, so a PostgreSQL server or an S3-compatible storage that offers only RSA key exchange or 3DES can no longer be reached. On macOS and Windows, `SSL_CERT_FILE` and `SSL_CERT_DIR` now replace the system certificate store for TLS connections that rely on it, such as the S3 and Azure storages.
* **Interrupted uploads.** The `directory` and `ssh` storages write each file under a temporary `.copy-*` name and rename it when done. A dump killed in the middle can leave such a file in its directory. It does not affect `list-dumps` or `restore`, and `delete` removes it with the dump.

#### Full Changelog: [v0.2.27...v0.3.0](https://github.com/GreenmaskIO/greenmask/compare/v0.2.27...v0.3.0)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Discord](https://discord.gg/tAJegUKSTB)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
