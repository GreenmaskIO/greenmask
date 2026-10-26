# Greenmask 0.1.11

Greenmask 0.1.11 was released on 2024-04-28. This release introduces improvements and bug fixes. It adds support for generated columns and fixes transformer parameter encoding, table scoring and the connection idle timeout in `restore`.

## Changes

* Added support for generated columns in the table
* Fixed transformer parameters encoding issue caused by spf13/viper
* Fixed table scoring for transformed table
* Refactored connection management logic in restore command - fixes connection idle timeout

## Assets

To download the Greenmask binary compatible with your system, see
the [release's assets list](https://github.com/GreenmaskIO/greenmask/releases/tag/v0.1.11).
