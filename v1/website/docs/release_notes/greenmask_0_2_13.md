# Greenmask 0.2.13

## Changes

* Fixed a panic in the introspection function when virtual references were set on tables without primary keys 
  [#309](https://github.com/GreenmaskIO/greenmask/issues/309). Virtual references on such tables are still not 
  supported, but the function no longer panics. Related MR [#315](https://github.com/GreenmaskIO/greenmask/pull/315).
* Fixed a case when greenmask hash engine ignores GREENMASK_GLOBAL_SALT [#317](https://github.com/GreenmaskIO/greenmask/issues/317) 
  Related MR [#318](https://github.com/GreenmaskIO/greenmask/pull/318).

#### Full Changelog: [v0.2.12...v0.2.13](https://github.com/GreenmaskIO/greenmask/compare/v0.2.12...v0.2.13)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Discord](https://discord.gg/tAJegUKSTB)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
