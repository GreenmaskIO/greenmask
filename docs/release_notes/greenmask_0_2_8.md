# Greenmask 0.2.8

This release includes a new feature and bug fixes.

## Changes

* Support `postgres://` schema connection URLs [#263](https://github.com/GreenmaskIO/greenmask/pull/263)
* Implemented `--blobs` and `--no-blobs` flags for the `greenmask dump` command. These flags allow inclusion or
  exclusion of large objects in the dump file. By default, `--blobs` is enabled. If `--no-blobs` is specified, large
  object data will be skipped, and only large object creation commands and ACLs will be included in the
  dump. [#265](https://github.com/GreenmaskIO/greenmask/pull/266)
* Implemented the `--no-blobs` flag for greenmask restore. If there are any large objects in your database, this will
  create an empty placeholder instead. [#265](https://github.com/GreenmaskIO/greenmask/pull/266)
* Enabled support for all textual data types in all transformers that have a text
  type [#267](https://github.com/GreenmaskIO/greenmask/pull/267).
  Closes [#260](https://github.com/GreenmaskIO/greenmask/issues/260)
* Fixed numerous introspection queries that have out of range errors caused by casting OID types to int4
  values [#264](https://github.com/GreenmaskIO/greenmask/pull/266).
  Closes [#265](https://github.com/GreenmaskIO/greenmask/issues/265)
* Refactored the logic of restorers and added test coverage to improve maintainability and
  stability [#268](https://github.com/GreenmaskIO/greenmask/pull/266)
* Fixed domain constraint introspection query [#266](https://github.com/GreenmaskIO/greenmask/pull/266)

#### Full Changelog: [v0.2.7...v0.2.8](https://github.com/GreenmaskIO/greenmask/compare/v0.2.7...v0.2.8)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Greenmask Roadmap](https://github.com/orgs/GreenmaskIO/projects/6)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram](https://t.me/greenmask_community)
* [Discord](https://discord.gg/tAJegUKSTB)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
