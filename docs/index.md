# About Greenmask

**Greenmask** is a powerful open-source utility that is designed for logical database backup dumping,
obfuscation, and restoration. It offers extensive functionality for backup, anonymization, and data masking.

Greenmask is written in pure Go and includes ported PostgreSQL libraries that allows for platform independence. This
tool is stateless and does not require any changes to your database schema. It is designed to be highly customizable and
backward-compatible with existing PostgreSQL utilities.

## Purpose

The Greenmask utility plays a central role in the Greenmask ecosystem. Our goal is to develop a comprehensive, UI-based
solution for managing obfuscation procedures. We recognize the challenges of maintaining obfuscation consistency
throughout the software lifecycle. Greenmask is dedicated to providing valuable tools and features that ensure the
obfuscation process remains fresh, predictable, and transparent.

## Key features

* **Database subset** - Dumps only the necessary data consistently based on the subset condition, reducing the size 
  of the dump and speeding up the restoration process.
* **Deterministic transformers** — deterministic approach to data transformation based on the hash
  functions. This ensures that the same input data will always produce the same output data. Almost each transformer
  supports either `random` or `hash` engine making it universal for any use case.
* **Dynamic parameters** — almost each transformer supports dynamic parameters, allowing to parametrize the
  transformer dynamically from the table column value. This is helpful for resolving the functional dependencies
  between columns and satisfying the constraints.
* **Cross-platform** — can be easily built and executed on any platform, thanks to its Go-based architecture,
  which eliminates platform dependencies.
* **Database type safe** — ensures data integrity by validating data and utilizing the database driver for
  encoding and decoding operations. This approach guarantees the preservation of data formats.
* **Transformation validation and easy maintainable** — during obfuscation development, Greenmask provides validation
  warnings and a transformation diff feature, allowing you to monitor and maintain transformations effectively
  throughout the software lifecycle.
* **Partitioned tables transformation inheritance** — define transformation configurations once and apply them to all
  partitions within partitioned tables, simplifying the obfuscation process.
* **Stateless** — Greenmask operates as a logical dump and does not impact your existing database schema.
* **Backward compatible** — it fully supports the same features and protocols as existing vanilla PostgreSQL utilities.
  Dumps created by Greenmask can be successfully restored using the pg_restore utility.
* **Extensible** — users have the flexibility to implement domain-based transformations in any programming language or
  use predefined templates.
* **Declarative** — Greenmask allows you to define configurations in a structured, easily parsed, and recognizable
  format.
* **Integrable** — integrate Greenmask seamlessly into your CI/CD system for automated database obfuscation and
  restoration.
* **Parallel execution** — take advantage of parallel dumping and restoration, significantly reducing the time required
  to deliver results.
* **Provide variety of storages** — Greenmask offers a variety of storage options for local and remote data storage,
  including directories and S3-like storage solutions.
* **Pgzip support for faster compression** — by setting `--pgzip`, greenmask can speeds up the dump and restoration
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
