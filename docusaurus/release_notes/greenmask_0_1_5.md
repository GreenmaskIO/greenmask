# Greenmask 0.1.5

This release introduces a new Greenmask command, improvements, bug fixes, and numerous documentation updates.

## New features

Added a new Greenmask CLI command—[show-transformer](../commands/show-transformer.md) that shows detailed information about a specified transformer.

## Improvements

- The [Hash transformer](../built_in_transformers/standard_transformers/hash.md) has been completely remastered and now has the `function` parameter to choose from some hash algorithm options and the `max_length` parameter to truncate the hash tail.
- Split information about transformers between the `list-transformers` and new `show-transformer` CLI commands, which allows for more comprehensible and useful outputs for both commands
- Added error severity for the `Cmd` parameter validator
- Improved UX for the Greenmask release binaries

## Fixes

- Fixed metadata enrichment for validation warnings caused by `RawValueValidator`
- Fixed a typo in the `credit_card` value for the `type` parameter of the `Masking` transformer
- Fixed Greenmask Playground environment variables and the `cleanup` command
- Fixed `list-dump`, `list-transformers`, and `restore` commands exit code on error

## Assets

To download the Greenmask binary compatible with your system, see the [release's assets list](https://github.com/GreenmaskIO/greenmask/releases/tag/v0.1.5).
