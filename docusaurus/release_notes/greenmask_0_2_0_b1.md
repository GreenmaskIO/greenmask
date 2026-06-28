# Greenmask 0.2.0b1 (pre-release)

This **major beta** release introduces new features and refactored transformers, significantly enhancing Greenmask's
flexibility to better meet business needs.

## Changes overview

* [Introduced dynamic parameters in the transformers](../built_in_transformers/dynamic_parameters.md)
    * Most transformers now support dynamic parameters where applicable.
    * Dynamic parameters are strictly enforced. If you need to cast values to another type, Greenmask provides templates
      and predefined cast functions accessible via `cast_to`. These functions cover frequent operations such as
      `UnixTimestampToDate` and `IntToBool`.
* The transformation logic has been significantly refactored, making transformers more customizable and flexible than
  before.
* [Introduced transformation engines](../built_in_transformers/transformation_engines.md)
    * `random` - generates transformer values based on pseudo-random algorithms.
    * `hash` - generates transformer values using hash functions. Currently, it utilizes `sha3` hash functions, which
      are secure but perform slowly. In the stable release, there will be an option to choose between `sha3` and
      `SipHash`.

* [Introduced static parameters value template](../built_in_transformers/parameters_templating.md)

## Notable changes

### Core

* Introduced the `Parametrizer` interface, now implemented for both dynamic and static parameters.
* Renamed most of the toolkit types for enhanced clarity and comprehensive documentation coverage.
* Refactored the `Driver` initialization logic.
* Added validation warnings for overridden types in the `Driver`.
* Migrated existing built-in transformers to utilize the new `Parametrizer` interface.
* Implemented a new abstraction, `TransformationContext`, as the first step towards enabling new feature transformation
  conditions (#34).
* Optimized most transformers for performance in both dynamic and static modes. While dynamic mode offers flexibility,
  static mode ensures performance remains high. Using only the necessary transformation features helps keep
  transformation time predictable.

### Documentation

Documentation has been significantly refactored. New information about features and updates to transformer descriptions
have been added.

### Transformers

* [RandomEmail](../built_in_transformers/standard_transformers/random_email.md) - Introduces a new transformer that
  supports both random and deterministic engines. It allows for flexible email value generation; you can use column
  values in the template and choose to keep the original domain or select any from the `domains` parameter.

* [NoiseDate](../built_in_transformers/standard_transformers/noise_date.md), [NoiseFloat](../built_in_transformers/standard_transformers/noise_float.md), [NoiseInt](../built_in_transformers/standard_transformers/noise_int.md) -
  These transformers support both random and deterministic engines, offering dynamic mode parameters that control the
  noise thresholds within the `min` and `max` range. Unlike previous implementations which used a single `ratio`
  parameter, the new release features `min_ratio` and `max_ratio` parameters to define noise values more precisely.
  Utilizing the `hash` engine in these transformers enhances security by complicating statistical analysis for
  attackers, especially when the same salt is used consistently over long periods.

* [NoiseNumeric](../built_in_transformers/standard_transformers/noise_numeric.md) - A newly implemented transformer,
  sharing features with `NoiseInt` and `NoiseFloat`, but specifically designed for numeric values (large integers or
  floats). It provides a `decimal` parameter to handle values with fractions.

* [RandomChoice](../built_in_transformers/standard_transformers/random_choice.md) - Now supports the `hash` engine

* [RandomDate](../built_in_transformers/standard_transformers/random_date.md), [RandomFloat](../built_in_transformers/standard_transformers/random_float.md), [RandomInt](../built_in_transformers/standard_transformers/random_int.md) -
  Now enhanced with hash engine support. Threshold parameters `min` and `max` have been updated to support dynamic mode,
  allowing for more flexible configurations.

* [RandomNumeric](../built_in_transformers/standard_transformers/random_numeric.md) - A new transformer specifically
  designed for numeric types (large integers or floats), sharing similar features with `RandomInt` and `RandomFloat`,
  but tailored for handling huge numeric values.

* [RandomString](../built_in_transformers/standard_transformers/random_string.md) - Now supports hash engine mode

* [RandomUnixTimestamp](../built_in_transformers/standard_transformers/random_unix_timestamp.md) - This new transformer
  generates Unix timestamps with selectable units (`second`, `millisecond`, `microsecond`, `nanosecond`). Similar in
  function to `RandomDate`, it supports the hash engine and dynamic parameters for `min` and `max` thresholds, with the
  ability to override these units using `min_unit` and `max_unit` parameters.

* [RandomUuid](../built_in_transformers/standard_transformers/random_uuid.md) - Added hash engine support

* [RandomPerson](../built_in_transformers/standard_transformers/random_person.md) - Implemented a new transformer that
  replaces `RandomName`, `RandomLastName`, `RandomFirstName`, `RandomFirstNameMale`, `RandomFirstNameFemale`,
  `RandomTitleMale`, and `RandomTitleFemale`. This new transformer offers enhanced customizability while providing
  similar functionalities as the previous versions. It generates personal data such as `FirstName`, `LastName`, and
  `Title`, based on the provided `gender` parameter, which now supports dynamic mode. Future minor versions will allow
  for overriding the default names database.

* Added [tsModify](../built_in_transformers/advanced_transformers/custom_functions/core_functions.md#tsmodify) - a new
  template function for time.Time objects modification

* Introduced a new [RandomIp](../built_in_transformers/standard_transformers/random_ip.md) transformer capable of
  generating a random IP address based on the specified netmask.

* Added a new [RandomMac](../built_in_transformers/standard_transformers/random_mac.md) transformer for generating
  random Mac addresses.

* Deleted transformers include `RandomMacAddress`, `RandomIPv4`, `RandomIPv6`, `RandomUnixTime`, `RandomTitleMale`,
  `RandomTitleFemale`, `RandomFirstName`, `RandomFirstNameMale`, `RandomFirstNameFemale`, `RandomLastName`, and
  `RandomName` due to the introduction of more flexible and unified options.

#### Full Changelog: [v0.1.14...v0.2.0b1](https://github.com/GreenmaskIO/greenmask/compare/v0.1.14...v0.2.0b1)

## Playground usage for beta version

If you want to run a Greenmask [playground](../playground.md) for the beta version v0.2.0b1 execute:

```
git checkout tags/v0.2.0b1 -b v0.2.0b1
docker-compose run greenmask-from-source
```

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Greenmask Roadmap](https://github.com/orgs/GreenmaskIO/projects/6)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram](https://t.me/greenmask_community)
* [Discord](https://discord.gg/tAJegUKSTB)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
