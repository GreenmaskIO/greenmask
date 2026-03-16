---
hide:
  - feedback
---

# About Greenmask

## Dump anonymization and synthetic data generation tool

**Greenmask** is a powerful open-source utility that is designed for logical database backup dumping,
anonymization, synthetic data generation and restoration. It has ported PostgreSQL libraries, making it reliable.
It is stateless and does not require any changes to your database schema. It is designed to be highly customizable and
backward-compatible with existing PostgreSQL utilities, fast and reliable.


## Key features

* **[Deterministic transformers](built_in_transformers/transformation_engines.md/#hash-engine)**
  — deterministic approach to data transformation based on the hash
  functions. This ensures that the same input data will always produce the same output data. Almost each transformer
  supports either `random` or `hash` engine making it universal for any use case.
* **[Dynamic parameters](built_in_transformers/dynamic_parameters.md)** — almost each
  transformer supports dynamic parameters, allowing to parametrize the
  transformer dynamically from the table column value. This is helpful for resolving the functional dependencies
  between columns and satisfying the constraints.
* **[Transformation validation and easy maintainable](commands/validate.md)** - During
  configuration process, Greenmask provides validation
  warnings, data transformation diff and schema diff features, allowing you to monitor and maintain transformations
  effectively
  throughout the software lifecycle. Schema diff helps to avoid data leakage when schema changed.
* **[Partitioned tables transformation inheritance](configuration.md/?h=partition#dump-section)**
  — Define transformation configurations once and apply them to all
  partitions within partitioned tables (using `apply_for_inherited` parameter), simplifying the anonymization process.
* **Stateless** - Greenmask operates as a logical dump and does not impact your existing database schema.
* **Cross-platform** - Can be easily built and executed on any platform, thanks to its Go-based architecture,
  which eliminates platform dependencies.
* **Database type safe** - Ensures data integrity by validating data and utilizing the database driver for
  encoding and decoding operations. This approach guarantees the preservation of data formats.
* **Backward compatible** - It fully supports the same features and protocols as existing vanilla PostgreSQL utilities.
  Dumps created by Greenmask can be successfully restored using the pg_restore utility.
* **Extensible** - Users have the flexibility
  to [implement domain-based transformations](built_in_transformers/standard_transformers/cmd.md/)
  in any programming language or
  use [predefined templates](built_in_transformers/advanced_transformers/index.md).
* **Integrable** - Integrate seamlessly into your CI/CD system for automated database anonymization and
  restoration.
* **Parallel execution** - Take advantage of parallel dumping and restoration, significantly reducing the time required
  to deliver results.
* **Provide variety of storages** - offers a variety of storage options for local and remote data storage,
  including directories and S3-like storage solutions.
* **[Pgzip support for faster compression](commands/dump.md/?h=pgzip#pgzip-compression)** — by
  setting `--pgzip`, it can speeds up the dump and restoration
  processes through parallel compression.


## Use cases

Greenmask is ideal for various scenarios, including:

* **Backup and restoration**. Use Greenmask for your daily routines involving logical backup dumping and restoration. It
  seamlessly handles tasks like table restoration after truncation. Its functionality closely mirrors that of pg_dump
  and pg_restore, making it a straightforward replacement.
* **Anonymization, transformation, and data masking**. Employ Greenmask for anonymizing, transforming, and masking
  backups, especially when setting up a staging environment or for analytical purposes. It simplifies the deployment of
  a pre-production environment with consistently anonymized data, facilitating faster time-to-market in the development
  lifecycle.

## Links

* [Greenmask Roadmap](https://github.com/orgs/GreenmaskIO/projects/6)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram](https://t.me/greenmask_community)
* [Discord](https://discord.gg/tAJegUKSTB)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
