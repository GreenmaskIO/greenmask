# Greenmask 0.2.0

This is one of the biggest releases since Greenmask was founded. We've been in close contact with our users, gathering
feedback, and working hard to make Greenmask more flexible, reliable, and user-friendly.

This major release introduces exciting new features such as database subsetting, pgzip support, restoration in
topological order, and refactored transformers, significantly enhancing Greenmask's flexibility to better meet business
needs. It also includes several fixes and improvements.

## Preface

This release is a major milestone that significantly expands Greenmask's functionality, transforming it into a simple,
extensible, and reliable solution for database security, data anonymization, and everyday operations. Our goal is to
create a core system that can serve as a foundation for comprehensive dynamic staging environments and robust data
security.

## Notable changes

* PostgreSQL 17 support - revised ported library to support PostgreSQL 17

* [**Database Subset**](../database_subset.md) - a new feature that allows you to define a subset of the database,
  allowing you to scale down the dump size ([#110](https://github.com/GreenmaskIO/greenmask/issues/110)). This is
  robust for multipurpose and especially useful for testing and development environments. It supports:

    * References with [NULL values](../database_subset.md/#references-with-null-values) - generate the LEFT JOIN query
      for the FK reference with NULL values to include them in the subset.
    * Supports [virtual references](../database_subset.md/#virtual-references) (virtual foreign keys) - create a logical
      FK in Greenmask that will be used for subset dependencies graph. The virtual reference can be defined for a column
      or an expression, allowing you to get the value from JSON and similar.
    * Supports [circular references](../database_subset.md/#circular-reference) - Greenmask will automatically resolve
      circular dependencies in the subset by generating a recursive query. The query is generated with integrity checks
      of the subset ensuring that the data gathered from circular dependencies is consistent.
    * Fully covered with documentation including [troubleshooting](../database_subset.md/#troubleshooting)
      and [examples](../database_subset.md/#example-dump-a-subset-of-the-database).
    * Supports FK and PK that have more than one column (or expression).
    * **Multi-cycles resolution in one strong connected component (SCC)** is supported - Greenmask will generate a
      recursive query for the SCC whether it is a single cycle or multiple cycles, making the subset system universal
      for any database schema.
  * **Supports polymorphic relationships** - You can define
    a [virtual reference for a table with polymorphic references]((../database_subset.md/#troubleshooting))
    using `polymorphic_exprs` attribute and use greenmask to generate a subset for such tables.

* **pgzip** support for faster [compression](../commands/dump.md/#pgzip-compression)
  and [decompression](../commands/restore.md/#pgzip-decompression) — setting `--pgzip` can speed up the dump and
  restoration processes through parallel compression. In some tests, it shows up to 5x faster dump and restore
  operations.
* [**Restoration in topological order**](../commands/restore.md/#restoration-in-topological-order) - This flag ensures
  that dependent tables are not restored until the tables they depend on have been restored. This is useful when you
  want to be notified of errors as immediately as possible without waiting for the entire table to be restored.
* **[Insert format](../commands/restore.md/#inserts-and-error-handling)** restoration - For a flexible restoration
  process, Greenmask now supports data restoration in the `INSERT` format. It generates the insert statements based on
  `COPY` records from the dump. You do not need to re-dump your data to use this feature; it can be defined in the
  `restore` command. The list of new features related to the `INSERT` format:

    * Generate `INSERT` statements with the `**ON CONFLICT DO NOTHING**` clause if the flag `--on-conflict-do-nothing`
      is set.
    * **[Error exclusion list](../configuration.md/#restoration-error-exclusion)** in the config to skip
      certain errors and continue inserting subsequent rows from the dump.
    * Use cases - **incremental dump and restoration** for logical data. For example, if you have a database, and you
      want to insert data periodically from another source, this can be used together with the database subset and
      transformations to catch up the target database.

* [Restore data batching](../commands/restore.md/#restore-data-batching) ([#173](https://github.com/GreenmaskIO/greenmask/pull/174)) -
  By default, the COPY protocol returns the error only on transaction commit. To override this behavior, use the
  `--batch-size` flag to specify the number of rows to insert in a single batch during the COPY command. This is useful
  when you want to control the transaction size and commit.
* [Introduced](https://github.com/GreenmaskIO/greenmask/pull/162) `keep_null` parameter for `RandomPerson` transformer.

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

* [Dumps retention management](../commands/delete.md) - Introduced retention
  parameters ([#201](https://github.com/GreenmaskIO/greenmask/pull/201)) for the delete command. Introduced two new
  statuses: failed and in progress. A dump is considered failed if it lacks a "done" heartbeat or
  if the last heartbeat timestamp exceeds 30 minutes. The delete command now supports the following retention
  parameters:
    * `--dry-run`: Runs the deletion operation in test mode with verbose output, without actually deleting anything.
    * `--before-date 2024-08-27T23:50:54+00:00`: Deletes dumps older than the specified date. The date must be provided
      in RFC3339Nano format, for example: `2021-01-01T00:00:00Z`.
    * `--retain-recent 10`: Retains the N most recent dumps, where N is specified by the user.
    * `--retain-for 1w2d3h4m5s6ms7us8ns`: Retains dumps for the specified duration. The format supports weeks (w),
      days (d), hours (h), minutes (m), seconds (s), milliseconds (ms), microseconds (us), and nanoseconds (ns).
    * `--prune-failed`: Prunes (removes) all dumps that have failed.
    * `--prune-unsafe`: Prunes dumps with "unknown-or-failed" statuses. This option only works in conjunction with
      `--prune-failed`.
      

* Docker image mirroring into the GitHub Container Registry

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

### Fixes and improvements

* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/140) `validate` command with the `--table` flag, which had the
  wrong order of the table name representation `{{ table_name }}.{{ schema }}` instead of
  `{{ schema }}.{{ table_name }}`.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/137/commits/d421d6df2b55019235c81bdd22e341aa2509400b#diff-7a8b28dfeb9522d6af581535cbf61f3d2a744a68d4558515644d746fc9d43a2bL114)
  `Row.SetColumn` out of range validation.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/137/commits/d421d6df2b55019235c81bdd22e341aa2509400b#diff-ef03875763278adee04b936cae57bb51d57c4ec8e55816f73e98c0af479a2441L543)
  `restoreWorker` panic caused when the worker received an error from pgx.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/157/commits/03d7d7af3c569d629f44b29114caa74c14a47826) error
  handling in the `restore` command.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/157/commits/03d7d7af3c569d629f44b29114caa74c14a47826) restore
  jobs now start a transaction for each table restoration and commit it after the table restoration is done.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/157/commits/03d7d7af3c569d629f44b29114caa74c14a47826)
  `--exit-on-error` works incorrectly in the `restore` command. Now, the `--exit-on-error` flag works correctly with the
  `data` section.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/159) transaction rollback in the `validate` command.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/143) typo in documentation.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/136) a CI/CD bug related to retrieving current tags.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/141) the Docker image tag for `latest` to exclude specific
  keywords.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/161) a case where the hashing value was not set for each column
  in the `RandomPerson` transformer.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/165) original email value parsing conditions.
* [Subset docs revision](https://github.com/GreenmaskIO/greenmask/pull/169/files).
* [Fixes](https://github.com/GreenmaskIO/greenmask/pull/171) a case where data entries were excluded by exclusion
  parameters such as `--exclude-table`, `--table`, etc.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/172) zero bytes that were written in the buffer due to the wrong
  buffer limit in the `Email` transformer.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/175) a case where the overridden type of column via
  `columns_type_override` did not work.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/177) a case where an unknown option provided in the config was
  just ignored instead of throwing an error.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/178) a case where `min` and `max` parameter values were ignored
  in transformers `NoiseDate`, `NoiseNumeric`, `NoiseFloat`, `NoiseInt`, `RandomNumeric`, `RandomFloat`, and
  `RandomInt`.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/180) TOC entry COPY restoration statement - added missing
  newline and semicolon. Now backward pg_dump call `pg_restore 1724504511561 --file 1724504511561.sql` is backward
  compatible and works as expected.
* [Fixed](https://github.com/GreenmaskIO/greenmask/pull/184) a case where dump/restore fails when masking tables with a
  generated column.
* [Updated go version (v1.22) and dependencies](https://github.com/GreenmaskIO/greenmask/pull/188)
* [Revised installation section of doc](https://github.com/GreenmaskIO/greenmask/pull/187)
* [PostgreSQL 17 support](https://github.com/GreenmaskIO/greenmask/pull/207) - revised ported library to support PostgreSQL 17
* [Fixed integration tests](https://github.com/GreenmaskIO/greenmask/pull/208) - reset the go test cache on each iteration
* [Push docker images to ghcr.io registry](https://github.com/GreenmaskIO/greenmask/pull/203)
* A bunch of refactoring and code cleanup to make the codebase more maintainable and readable.

#### Full Changelog: [v0.1.14...v0.2.0](https://github.com/GreenmaskIO/greenmask/compare/v0.1.14...v0.2.0)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Greenmask Roadmap](https://github.com/orgs/GreenmaskIO/projects/6)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram](https://t.me/greenmask_community)
* [Discord](https://discord.gg/tAJegUKSTB)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
