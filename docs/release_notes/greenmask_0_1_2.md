# Greenmask 0.1.2

Greenmask 0.1.2 was released on 2024-01-30. This release introduces bug fixes. It fixes parsing of raw COPY lines, the `--version` parameter and URI connection strings in `--dbname`.

## Fixes

- Fixed bug when raw COPY lines were parsed incorrectly
- Fixed `--version` parameter behavior
- Fixed `--dbname` parameter - now it correctly works with PostgreSQL connection string in URI format `postgresql:///`

## Assets

To download the Greenmask binary compatible with your system, see the [release's assets list](https://github.com/GreenmaskIO/greenmask/releases/tag/v0.1.2).
