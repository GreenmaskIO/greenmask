# Greenmask 0.1.8

This release introduces improvements and bug fixes

## Improvements

* Implemented `--exit-on-error` parameter for `pg_restore` run. But it does not play for "data" section restoration now. If any error is caused in `data` section greenmask exits with the error whether `--exit-on-error` was provided or not. This might be fixed later

## Fixes

* Fixed dependent objects dropping when running with the `restore` command with the `--clean` parameter. Useful when restoring and overriding only required tables
* Fixed `show-dump` command output in text mode
* Disabled CGO. Fixes problem when downloaded binary from repo cannot run
* Fixed `delete` dump operation


## Assets

To download the Greenmask binary compatible with your system, see
the [release's assets list](https://github.com/GreenmaskIO/greenmask/releases/tag/v0.1.8).
