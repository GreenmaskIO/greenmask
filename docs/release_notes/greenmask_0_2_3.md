# Greenmask 0.2.3

This release introduces bug fixes.

## Changes

* Fixed an issue where the partitioned table itself was executed in the restore worker, resulting in a "file not found"
  error in storage. Closes bug: restoring partitioned tables
  fails [#238](https://github.com/GreenmaskIO/greenmask/pull/238) [#242](https://github.com/GreenmaskIO/greenmask/pull/242).
* Fixed template function availability [#239](https://github.com/GreenmaskIO/greenmask/issues/239). Renamed methods
  according to the documentation: GetColumnRawValue is now GetRawColumnValue, and SetColumnRawValue is now
  SetRawColumnValue [#242](https://github.com/GreenmaskIO/greenmask/pull/242)
* Resolved an issue where Dump.createTocEntries processed partitioned tables as if they were physical entities, despite
  being logical [#241](https://github.com/GreenmaskIO/greenmask/pull/241)
* Corrected merging in the pre-data, data, and post-data sections, which previously caused a panic in dump command when
  the post-data section was excluded [#241](https://github.com/GreenmaskIO/greenmask/pull/241)
* Fixed an issue where dumps created with --load-via-partition-root did not use the root partition table in --inserts
  generation during restoration [#241](https://github.com/GreenmaskIO/greenmask/pull/241)

#### Full Changelog: [v0.2.2...v0.2.3](https://github.com/GreenmaskIO/greenmask/compare/v0.2.2...v0.2.3)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Greenmask Roadmap](https://github.com/orgs/GreenmaskIO/projects/6)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram](https://t.me/greenmask_community)
* [Discord](https://discord.gg/tAJegUKSTB)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
