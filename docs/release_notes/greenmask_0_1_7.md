# Greenmask 0.1.7

This release introduces a new Greenmask command, improvements, bug fixes, and documentation update.

## New features

- Added restoration filtering by `--table`, `--schema` and `--exclude-schema` parameters

## Improvements

- Improved Hash transformer 
  - Added salt parameter that can be set via config or via `GREENMASK_GLOBAL_SALT`
  - Added sha3 functions support in different modes (sha3-224, sha3-256, sha3-384, sha3-512)
- Refactored `Cmd` transformer logic
  - Json API: Now it allows to use of column names instead of column indexes in JSON format
  - Csv API: Now it can use the column order from config via column remapping
- The `validate` command was rewritten almost from scratch.
  - New option `--transformed-only` - displays only columns that are transformed with primary key (if exists). This 
    allows to reduce the output data and make it more readable
  - Implemented `json` format for output
  - Added the `--table-format` parameter which is responsible for the `vertical` and `horizontal` table orientation. 
    This works only when `--format=text`
  - Added the `--warnings` parameter, if it is specified then not only fatal-warnings will be displayed, but also 
    those with a lower severity

## Fixes

- Fixed `--use-list` option - now it applies toc entries according to the order in list file
- Fixed `--use-list` option behaviour together with `--list-format` option (`json` or `text`). Now it 
  generates list file in text format for providing it to the pg_restore call



## Assets

To download the Greenmask binary compatible with your system, see
the [release's assets list](https://github.com/GreenmaskIO/greenmask/releases/tag/v0.1.7).
