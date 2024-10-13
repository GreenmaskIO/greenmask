# [Greenmask](https://greenmask.io)

## Dump anonymization and synthetic data generation tool

**Greenmask** is a powerful open-source utility that is designed for logical database backup dumping,
anonymization, synthetic data generation and restoration. It has ported PostgreSQL libraries, making it reliable.
It is stateless and does not require any changes to your database schema. It is designed to be highly customizable and
backward-compatible with existing PostgreSQL utilities, fast and reliable.

![Build status](https://github.com/greenmaskio/greenmask/workflows/ci/badge.svg)
[![License](https://img.shields.io/github/license/greenmaskio/greenmask)](https://github.com/greenmaskio/greenmask/blob/main/LICENSE)
![GitHub Release](https://img.shields.io/github/v/release/greenmaskio/greenmask)
![GitHub Downloads (all assets, all releases)](https://img.shields.io/github/downloads/greenmaskio/greenmask/total)
[![Docker pulls](https://img.shields.io/docker/pulls/greenmask/greenmask)](https://hub.docker.com/r/greenmask/greenmask)
[![Go Report Card](https://goreportcard.com/badge/github.com/greenmaskio/greenmask)](https://goreportcard.com/report/github.com/greenmaskio/greenmask)

![schema.png](docs/assets/schema.png)

# Features

* **[Deterministic transformers](https://greenmask.io/latest/built_in_transformers/transformation_engines/#hash-engine)**
  — deterministic approach to data transformation based on the hash
  functions. This ensures that the same input data will always produce the same output data. Almost each transformer
  supports either `random` or `hash` engine making it universal for any use case.
* **[Dynamic parameters](https://greenmask.io/latest/built_in_transformers/dynamic_parameters/)** — almost each
  transformer supports dynamic parameters, allowing to parametrize the
  transformer dynamically from the table column value. This is helpful for resolving the functional dependencies
  between columns and satisfying the constraints.
* **[Transformation validation and easy maintainable](https://greenmask.io/latest/commands/validate/)** - During
  configuration process, Greenmask provides validation
  warnings, data transformation diff and schema diff features, allowing you to monitor and maintain transformations
  effectively
  throughout the software lifecycle. Schema diff helps to avoid data leakage when schema changed.
* **[Partitioned tables transformation inheritance](https://greenmask.io/latest/configuration/?h=partition#dump-section)**
  — Define transformation configurations once and apply them to all
  partitions within partitioned tables (using `apply_for_inherited` parameter), simplifying the obfuscation process.
* **Stateless** - Greenmask operates as a logical dump and does not impact your existing database schema.
* **Cross-platform** - Can be easily built and executed on any platform, thanks to its Go-based architecture,
  which eliminates platform dependencies.
* **Database type safe** - Ensures data integrity by validating data and utilizing the database driver for
  encoding and decoding operations. This approach guarantees the preservation of data formats.
* **Backward compatible** - It fully supports the same features and protocols as existing vanilla PostgreSQL utilities.
  Dumps created by Greenmask can be successfully restored using the pg_restore utility.
* **Extensible** - Users have the flexibility
  to [implement domain-based transformations](https://greenmask.io/latest/built_in_transformers/standard_transformers/cmd/)
  in any programming language or
  use [predefined templates](https://greenmask.io/latest/built_in_transformers/advanced_transformers/).
* **Integrable** - Integrate seamlessly into your CI/CD system for automated database obfuscation and
  restoration.
* **Parallel execution** - Take advantage of parallel dumping and restoration, significantly reducing the time required
  to deliver results.
* **Provide variety of storages** - offers a variety of storage options for local and remote data storage,
  including directories and S3-like storage solutions.
* **[Pgzip support for faster compression](https://greenmask.io/latest/commands/dump/?h=pgzip#pgzip-compression)** — by
  setting `--pgzip`, it can speeds up the dump and restoration
  processes through parallel compression.

## Getting started

Greenmask has a [Playground](https://greenmask.io/latest/playground/) - it is a sandbox environment in Docker with
sample databases included to help you try Greenmask without any additional actions

1. Clone the `greenmask` repository and navigate to its directory by running the following commands:

    ```shell
    git clone git@github.com:GreenmaskIO/greenmask.git && cd greenmask
    ```

2. Once you have cloned the repository, start the environment by running Docker Compose:

    ```shell
    docker-compose run greenmask
    ```

## Use Cases

Greenmask is ideal for various scenarios, including:

* **Backup and Restoration**. Use Greenmask for your daily routines involving logical backup dumping and restoration. It
  seamlessly handles tasks like table restoration after truncation. Its functionality closely mirrors that of pg_dump
  and pg_restore, making it a straightforward replacement.
* **Anonymization, Transformation, and Data Masking**. Employ Greenmask for anonymizing, transforming, and masking
  backups, especially when setting up a staging environment or for analytical purposes. It simplifies the deployment of
  a pre-production environment with consistently anonymized data, facilitating faster time-to-market in the development
  lifecycle.

### General Information

It is evident that the most appropriate approach for executing logical backup dumping and restoration is by leveraging
the core PostgreSQL utilities, specifically pg_dump and pg_restore. **Greenmask** has been purposefully designed to
align with PostgreSQL's native utilities, ensuring compatibility. Greenmask primarily handles data dumping
operations independently and delegates the responsibilities of schema dumping and restoration to pg_dump and pg_restore,
maintaining seamless integration with PostgreSQL's standard tools.

#### Backup and Process

Greenmask uses the **directory format** of _pg_dump_ and _pg_restore_. This format is particularly suitable for
parallel execution and partial restoration, and it includes clear metadata files that aid in determining the backup and
restoration steps. Greenmask has been optimized to work seamlessly with remote storage systems and obfuscation
procedures.

#### Storage Options

* **s3** - This option supports any S3-like storage system, including AWS S3, making it versatile and adaptable to
  various cloud-based storage solutions.
* **directory** - This is the standard choice, representing the ordinary filesystem directory for local storage.

## Data Obfuscation and Validation

Greenmask works with **COPY lines**, collects schema metadata using the Golang driver, and employs this driver in the
encoding and decoding process. The **validate command** offers a way to assess the impact on both schema
(**validation warnings**) and data (**transformation and displaying differences**). This command allows you to validate
the schema and data transformations, ensuring the desired outcomes during the obfuscation process.

## Customization

If your table schema relies on functional dependencies between columns, you can address this challenge using the
[Dynamic parameters](https://greenmask.io/latest/built_in_transformers/dynamic_parameters/). By setting dynamic
parameters, you can resolve such as created_at and updated_at cases, where the
updated_at must be greater or equal than the created_at.

If you need to implement custom logic imperatively use
[TemplateRecord](https://greenmask.io/latest/built_in_transformers/advanced_transformers/template_record/) or
[Template](https://greenmask.io/latest/built_in_transformers/advanced_transformers/template/) transformers.

Greenmask provides a framework for creating your custom transformers, which can be reused efficiently. These
transformers can be seamlessly integrated without requiring recompilation, thanks to the PIPE (stdin/stdout)
interaction.

Furthermore, Greenmask's architecture is designed to be highly extensible, making it possible to introduce other
interaction protocols, such as HTTP or Socket, for conducting obfuscation procedures.

## PostgreSQL Version Compatibility

**Greenmask** is compatible with PostgreSQL versions **11 and higher**.

## References

* Utilized the  [Demo database](https://postgrespro.com/community/demodb), provided by PostgresPro, for integration
  testing purposes.
* Employed the [adventureworks database](https://github.com/morenoh149/postgresDBSamples) created
  by `morenoh149/postgresDBSamples`, in the Docker Compose playground.

## Links

* [Documentation](https://docs.greenmask.io)
* Email: **support@greenmask.io**
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram](https://t.me/greenmask_community)
* [Discord](https://discord.com/invite/rKBKvDECfd)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
