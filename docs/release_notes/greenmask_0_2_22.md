# Greenmask 0.2.22

## Changes

* feat(storage): add Azure Blob Storage backend (`storage.type: azure`) supporting shared key, SAS token, and default credential chain authentication, as well as sovereign clouds [#457](https://github.com/GreenmaskIO/greenmask/pull/457). Closes [#299](https://github.com/GreenmaskIO/greenmask/issues/299)
* feat(storage): add SSH/SFTP storage backend (`storage.type: ssh`) for storing dumps on a remote host over SFTP [#458](https://github.com/GreenmaskIO/greenmask/pull/458)
* feat: support providing the config file path via the `GREENMASK_CONFIG` environment variable when `--config` is not set [#459](https://github.com/GreenmaskIO/greenmask/pull/459). Closes [#455](https://github.com/GreenmaskIO/greenmask/issues/455)
* feat: add `validate --strict` parameter to exit with a non-zero status if there are unresolved warnings [#460](https://github.com/GreenmaskIO/greenmask/pull/460). Closes [#454](https://github.com/GreenmaskIO/greenmask/issues/454)

#### Full Changelog: [v0.2.21...v0.2.22](https://github.com/GreenmaskIO/greenmask/compare/v0.2.21...v0.2.22)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Discord](https://discord.gg/tAJegUKSTB)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
