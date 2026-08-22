# Greenmask 0.2.23

## Changes

* feat: add `--exclude-extension` option to the `dump` command, mirroring `--exclude-schema` and
  `--exclude-table` (requires pg_dump 17 or later) [#470](https://github.com/GreenmaskIO/greenmask/pull/470)
* fix: fail the dump when sequence privileges are missing on PostgreSQL 18 and later. Since PostgreSQL 18
  `pg_sequence_last_value()` returns `NULL` instead of raising `permission denied for sequence`, so Greenmask silently
  dumped `setval(<start>, false)` and every sequence was reset on restore. Unreadable sequences are now reported
  together with a remediation hint and the dump
  fails [#471](https://github.com/GreenmaskIO/greenmask/pull/471). Fixes
  bug [#469](https://github.com/GreenmaskIO/greenmask/issues/469)
* fix: exclude generated columns from subset dump queries — subsetted tables were dumped with `SELECT "schema"."table".*`
  while restore omits generated columns, so every row was one field
  too wide [#472](https://github.com/GreenmaskIO/greenmask/pull/472)
* fix: `RandomEmail` no longer requires the source value to be a valid email when the configured templates do not
  reference `original_local_part` or `original_domain` [#468](https://github.com/GreenmaskIO/greenmask/pull/468). Fixes
  bug [#466](https://github.com/GreenmaskIO/greenmask/issues/466)
* fix: fix subset-related unit test and bump Go
  dependencies [#475](https://github.com/GreenmaskIO/greenmask/pull/475)

#### Full Changelog: [v0.2.22...v0.2.23](https://github.com/GreenmaskIO/greenmask/compare/v0.2.22...v0.2.23)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Discord](https://discord.gg/tAJegUKSTB)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
