# Greenmask 0.2.14

## Changes

* Fixed RandomPerson hash generation [#327](https://github.com/GreenmaskIO/greenmask/pull/327)
* Implemented `--quiet` flag for `list-dumps` command [#331](https://github.com/GreenmaskIO/greenmask/pull/331).
  This makes it easy to use list-dumps in shell pipelines like:
  ```bash
  greenmask list-dumps -q | xargs -I {} greenmask delete {}
  ```
* Implemented an official greenmask installation script [#334](https://github.com/GreenmaskIO/greenmask/pull/334). Now
greenmask can be installed with a single command:
  ```bash
  curl -fsSL https://greenmask.io/install.sh | bash
  ```
* Added a `--description` flag to the dump command, store it in metadata, and display it in `list-dumps` for 
  better context [#339](https://github.com/GreenmaskIO/greenmask/pull/339).
* Fixed logic in ExcludeSchema filter: now correctly returns false for excluded schemas, preventing them 
 from being restored [#343](https://github.com/GreenmaskIO/greenmask/pull/343)
* Fix: ensure SEQUENCE SET and BLOB entries are restored after topologically sorted tables when 
 using `--restore-in-order` [#340](https://github.com/GreenmaskIO/greenmask/pull/340)
* Fixed command links in index documentation [#337](https://github.com/GreenmaskIO/greenmask/pull/337)
* Fix: prevent panic when using `latest` dump id with `restore` command if no dumps exist in 
 storage [#346](https://github.com/GreenmaskIO/greenmask/pull/346)

#### Full Changelog: [v0.2.13...v0.2.14](https://github.com/GreenmaskIO/greenmask/compare/v0.2.13...v0.2.14)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Discord](https://discord.gg/tAJegUKSTB)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
