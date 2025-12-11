# Greenmask 0.2.12

## Changes

* Added support for dynamic parameters in the Replace transformer, allowing values to be dynamically replaced based on
  column values. This feature enables spreading the same value across multiple columns. See
  the [documentation](https://docs.greenmask.io/latest/built_in_transformers/standard_transformers/replace/) for
  examples. [#293](https://github.com/GreenmaskIO/greenmask/pull/293)
* Updated the `--verbose` flag to a boolean type. It is now `true` if provided and `false`
  otherwise. [#282](https://github.com/GreenmaskIO/greenmask/pull/282)
* Fixed a bug in the `RandomDate` transformer where minutes were not truncated as
  expected. [#298](https://github.com/GreenmaskIO/greenmask/pull/298)
* Updated go dependencies to the latest. [#304](https://github.com/GreenmaskIO/greenmask/pull/304)

#### Full Changelog: [v0.2.11...v0.2.12](https://github.com/GreenmaskIO/greenmask/compare/v0.2.11...v0.2.12)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Discord](https://discord.gg/tAJegUKSTB)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
