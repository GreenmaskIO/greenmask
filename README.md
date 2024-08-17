# Greenmask - dump obfuscation tool

## Preface

**Greenmask** is a powerful open-source utility that is designed for logical database backup dumping,
obfuscation, and restoration. It offers extensive functionality for backup, anonymization, and data masking. Greenmask
is written entirely in pure Go and includes ported PostgreSQL libraries, making it platform-independent. This tool is
stateless and does not require any changes to your database schema. It is designed to be highly customizable and
backward-compatible with existing PostgreSQL utilities.

# Features

* **Deterministic transformers** — deterministic approach to data transformation based on the hash
  functions. This ensures that the same input data will always produce the same output data. Almost each transformer
  supports either `random` or `hash` engine making it universal for any use case.
* **Dynamic parameters** — almost each transformer supports dynamic parameters, allowing to parametrize the
  transformer dynamically from the table column value. This is helpful for resolving the functional dependencies
  between columns and satisfying the constraints.
* **Cross-platform** - Can be easily built and executed on any platform, thanks to its Go-based architecture,
  which eliminates platform dependencies.
* **Database type safe** - Ensures data integrity by validating data and utilizing the database driver for
  encoding and decoding operations. This approach guarantees the preservation of data formats.
* **Transformation validation and easy maintainable** - During obfuscation development, Greenmask provides validation
  warnings and a transformation diff feature, allowing you to monitor and maintain transformations effectively
  throughout the software lifecycle.
* **Partitioned tables transformation inheritance** - Define transformation configurations once and apply them to all
  partitions within partitioned tables, simplifying the obfuscation process.
* **Stateless** - Greenmask operates as a logical dump and does not impact your existing database schema.
* **Backward compatible** - It fully supports the same features and protocols as existing vanilla PostgreSQL utilities.
  Dumps created by Greenmask can be successfully restored using the pg_restore utility.
* **Extensible** - Users have the flexibility to implement domain-based transformations in any programming language or
  use predefined templates.
* **Declarative** - Greenmask allows you to define configurations in a structured, easily parsed, and recognizable
  format.
* **Integrable** - Integrate Greenmask seamlessly into your CI/CD system for automated database obfuscation and
  restoration.
* **Parallel execution** - Take advantage of parallel dumping and restoration, significantly reducing the time required
  to deliver results.
* **Provide variety of storages** - Greenmask offers a variety of storage options for local and remote data storage,
  including directories and S3-like storage solutions.
* **Pgzip support for faster compression** — by setting `--pgzip`, greenmask can speeds up the dump and restoration
processes through parallel compression.

## Use Cases

Greenmask is ideal for various scenarios, including:

* **Backup and Restoration**. Use Greenmask for your daily routines involving logical backup dumping and restoration. It
  seamlessly handles tasks like table restoration after truncation. Its functionality closely mirrors that of pg_dump
  and pg_restore, making it a straightforward replacement.
* **Anonymization, Transformation, and Data Masking**. Employ Greenmask for anonymizing, transforming, and masking
  backups, especially when setting up a staging environment or for analytical purposes. It simplifies the deployment of
  a pre-production environment with consistently anonymized data, facilitating faster time-to-market in the development
  lifecycle.

## Our purpose

The Greenmask utility plays a central role in the Greenmask ecosystem. Our goal is to develop a comprehensive, UI-based
solution for managing obfuscation procedures. We recognize the challenges of maintaining obfuscation consistency
throughout the software lifecycle. Greenmask is dedicated to providing valuable tools and features that ensure the
obfuscation process remains fresh, predictable, and transparent.

### General Information

It is evident that the most appropriate approach for executing logical backup dumping and restoration is by leveraging
the core PostgreSQL utilities, specifically pg_dump and pg_restore. **Greenmask** has been purposefully designed to
align with PostgreSQL's native utilities, ensuring compatibility. Greenmask primarily handles data dumping
operations independently and delegates the responsibilities of schema dumping and restoration to pg_dump and pg_restore,
maintaining seamless integration with PostgreSQL's standard tools.

### Backup Process

The process of backing up PostgreSQL databases is divided into three distinct sections:

* **Pre-data** - This section encompasses the raw schema of tables, excluding primary keys (PK) and foreign keys (FK).
* **Data** - The data section contains the actual table data in COPY format, including information about sequence
  current
  values and Large Objects data.
* **Post-data** - In this section, you'll find the definitions of indexes, triggers, rules, and constraints (such as PK
  and
  FK).

Greenmask focuses exclusively on the data section during runtime. It delegates the handling of the _pre-data_ and
_post-data_ sections to the core PostgreSQL utilities, _pg_dump_ and _pg_restore_.

Greenmask employs the **directory format** of _pg_dump_ and _pg_restore_. This format is particularly suitable for
parallel execution and partial restoration, and it includes clear metadata files that aid in determining the backup and
restoration steps. Greenmask has been optimized to work seamlessly with remote storage systems and obfuscation
procedures.

When performing data dumping, Greenmask utilizes the COPY command in TEXT format, maintaining reliability and
compatibility with the vanilla PostgreSQL utilities.

Additionally, Greenmask supports parallel execution, significantly reducing the time required for the dumping process.

## Storage Options

The core PostgreSQL utilities, _pg_dump_ and _pg_restore_, traditionally operate with files in a directory format,
offering no alternative methods. To meet **modern backup requirements** and provide flexible approaches,
Greenmask introduces the concept of **Storages**.

* **s3** - This option supports any S3-like storage system, including AWS S3, making it versatile and adaptable to
  various cloud-based storage solutions.
* **directory** - This is the standard choice, representing the ordinary filesystem directory for local storage.

## Restoration Process

In the restoration process, Greenmask combines the capabilities of different tools:

* **Schema Restoration** - Greenmask utilizes _pg_restore_ to restore the database schema. This ensures that the schema
  is accurately reconstructed.
* **Data Restoration** - For data restoration, Greenmask independently applies the data using the COPY protocol.
  This allows Greenmask to handle the data efficiently, especially when working with various storage solutions.
  Greenmask is aware of the restoration metadata, which enables it to download only the necessary data. This feature
  is particularly useful for partial restoration scenarios, such as restoring a single table from a complete backup.

Greenmask also **supports parallel restoration**, which can significantly reduce the time required to complete the
restoration process. This parallel execution enhances the efficiency of restoring large datasets.

## Data Obfuscation and Validation

Greenmask works with **COPY lines**, collects schema metadata using the Golang driver, and employs this driver in the
encoding and decoding process. The **validate command** offers a way to assess the impact on both schema
(**validation warnings**) and data (**transformation and displaying differences**). This command allows you to validate
the schema and data transformations, ensuring the desired outcomes during the obfuscation process.

## Customization

If your table schema relies on functional dependencies between columns, you can address this challenge using the
**TemplateRecord** transformer. This transformer enables you to define transformation logic for entire tables,
offering type-safe operations when assigning new values.

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

* [Documentation](https://greenmask.io)
* Email: **support@greenmask.io**
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram](https://t.me/greenmask_community)
* [Discord](https://discord.com/invite/rKBKvDECfd)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
