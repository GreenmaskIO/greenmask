# Greenmask 0.2.7

Greenmask 0.2.7 was released on 2025-01-31. This release fixes several bugs. It fixes a lost semicolon in a directory dump converted to a single file with `pg_restore --file` and updates Go dependencies.

## Changes

* Fixed a case when converted directory dump into single file using
`pg_restore --file=1738263512345.sql` is corrupted due to lost semicolon [#255](https://github.com/GreenmaskIO/greenmask/issues/255)
* Updated go dependencies [#256](https://github.com/GreenmaskIO/greenmask/issues/256)


#### Full Changelog: [v0.2.6...v0.2.7](https://github.com/GreenmaskIO/greenmask/compare/v0.2.6...v0.2.7)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Greenmask Roadmap](https://github.com/orgs/GreenmaskIO/projects/6)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram](https://t.me/greenmask_community)
* [Discord](https://discord.gg/tAJegUKSTB)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
