# Greenmask 0.2.9

This release introduces a new transformer and fixes some bug.

## Changes

* Implemented [RandomCompany](https://docs.greenmask.io/latest/built_in_transformers/standard_transformers/random_company/) transformer - 
  it's a multi-column transformer, that generates a company data with attributes `CompanyName` and `CompanyName` 
  [#273](https://github.com/GreenmaskIO/greenmask/pull/273) [#274](https://github.com/GreenmaskIO/greenmask/pull/274).
* Fixed a case when transformers with column containers were not printed on `greenmask list-transformers` command
  call [#275](https://github.com/GreenmaskIO/greenmask/pull/275).
* Fixed `RandomEmail` transformer bug when an incorrect buffer size for hex-encoded symbols resulted in a `\0` 
  appearing in the string [#278](https://github.com/GreenmaskIO/greenmask/pull/278).
* Fixed typo in database_subset.md docs [#271](https://github.com/GreenmaskIO/greenmask/pull/271)
* Revised README.md [#280](https://github.com/GreenmaskIO/greenmask/pull/280)

#### Full Changelog: [v0.2.8...v0.2.9](https://github.com/GreenmaskIO/greenmask/compare/v0.2.8...v0.2.9)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Greenmask Roadmap](https://github.com/orgs/GreenmaskIO/projects/6)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [Discord](https://discord.gg/tAJegUKSTB)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
