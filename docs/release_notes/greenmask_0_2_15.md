# Greenmask 0.2.15

Greenmask 0.2.15 was released on 2025-12-13. It adds PostgreSQL 18 support, the `postcode` type of the `Masking` transformer and the `skip_not_exist` parameter for `Json` transformer operations, and fixes two schema dump bugs.

## Changes

* Supported PostgreSQL 18 [#380](https://github.com/GreenmaskIO/greenmask/pull/380)
* Added `postcode` type to `Masking` transformer for masking postcodes [#362](https://github.com/GreenmaskIO/greenmask/pull/362)
* Added `skip_not_exist` parameter to `Json` transformer operations. This allows skipping the operation if the key does not exist by the provided path [#356](https://github.com/GreenmaskIO/greenmask/pull/356)
* Fixed temp file cleanup order in schema-only dump [#379](https://github.com/GreenmaskIO/greenmask/pull/379)
* Fixed TOC entry duplication when merging schema and data sections [#378](https://github.com/GreenmaskIO/greenmask/pull/378)

#### Full Changelog: [v0.2.14...v0.2.15](https://github.com/GreenmaskIO/greenmask/compare/v0.2.14...v0.2.15)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Discord](https://discord.gg/tAJegUKSTB)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
