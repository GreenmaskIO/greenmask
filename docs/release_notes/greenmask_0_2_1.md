# Greenmask 0.2.1

This release introduces two new features transformation conditions and transformation inheritance for primary and
foreign keys. It also includes several bug fixes and improvements.

## Changes

* Feat: [Transformation conditions](../built_in_transformers/transformation_condition.md)
  execute a defined transformation only if a specified condition is
  met. [#133](https://github.com/GreenmaskIO/greenmask/pull/133)
* Feat: [Transformation inheritance](../built_in_transformers/transformation_inheritance.md) - transformation 
  inheritance for partitioned tables and tables with foreign keys. Define once and apply to all.
* CI/CD: Add golangci-lint job to pull request check [#223](https://github.com/GreenmaskIO/greenmask/pull/223)
* CI/CD: Deploy development version of the documentation (main branch) and divided jobs into separate blocks and made them
  reusable [#212](https://github.com/GreenmaskIO/greenmask/pull/212)
* Fix: Fixed type in subset documentation [#211](https://github.com/GreenmaskIO/greenmask/pull/211)
* Fix: Bump go and python dependencies [#219](https://github.com/GreenmaskIO/greenmask/pull/219)
* Fix: Fatal validation error in playground [#224](https://github.com/GreenmaskIO/greenmask/pull/224)
* Fix: Code refactoring and golangci-lint warns fixes [#226](https://github.com/GreenmaskIO/greenmask/pull/226)
* Docs: Revised README.md - added badges, updated the description, added getting started section, added greenmask design
  schema [#216](https://github.com/GreenmaskIO/greenmask/pull/216) [#217](https://github.com/GreenmaskIO/greenmask/pull/217) [#218](https://github.com/GreenmaskIO/greenmask/pull/218)
* Docs: main page errors in docs [#221](https://github.com/GreenmaskIO/greenmask/pull/221)
* Docs: Revised README.md according to the latest changes [#225](https://github.com/GreenmaskIO/greenmask/pull/225)
* Docs: moved documentation to docs.greenmask.io, added feedback form and GA
  integration [#220](https://github.com/GreenmaskIO/greenmask/pull/220)

#### Full Changelog: [v0.2.0...v0.2.1](https://github.com/GreenmaskIO/greenmask/compare/v0.2.0...v0.2.1)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Greenmask Roadmap](https://github.com/orgs/GreenmaskIO/projects/6)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram](https://t.me/greenmask_community)
* [Discord](https://discord.gg/tAJegUKSTB)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
