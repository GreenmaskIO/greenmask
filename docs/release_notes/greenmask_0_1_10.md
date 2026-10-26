# Greenmask 0.1.10

Greenmask 0.1.10 was released on 2024-04-17. This release introduces improvements and bug fixes. It fixes a panic in the `RandomString` transformer and the table size calculation, which now includes the TOAST table, and removes the bucket name from the object path prefix.

## Changes

* Fixed panic caused in `RandomString` transformer
* Fixed wrong table size calculation. Now the table size includes TOAST table size
* Added custom transformer interaction API defaults if not set
* Changed docker workdir to greenmask home
* Removed bucket name from object path prefix

## Assets

To download the Greenmask binary compatible with your system, see
the [release's assets list](https://github.com/GreenmaskIO/greenmask/releases/tag/v0.1.10).
