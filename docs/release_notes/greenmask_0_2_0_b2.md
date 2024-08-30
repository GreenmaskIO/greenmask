# Greenmask 0.2.0b2 (pre-release)

This **major beta** release introduces new features such as the database subset, pgzip support, restoration in
topological and many more. It also includes fixes and improvements.

## Preface

This release is a major milestone that significantly expands Greenmask's functionality, transforming it into a simple,
extensible, and reliable solution for database security, data anonymization, and everyday operations. Our goal is to
create a core system that can serve as a foundation for comprehensive dynamic staging environments and robust data
security.

## Notable changes

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
* A bunch of refactoring and code cleanup to make the codebase more maintainable and readable.

#### Full Changelog: [v0.2.0b1...v0.2.0b2](https://github.com/GreenmaskIO/greenmask/compare/v0.2.0b1...v0.2.0b2)

## Playground usage for beta version

If you want to run a Greenmask [playground](../playground.md) for the beta version v0.2.0b2 execute:

```bash
git checkout tags/v0.2.0b2 -b v0.2.0b2
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
