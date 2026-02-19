# [Greenmask](https://greenmask.io)

**Greenmask** is a powerful open-source utility for logical database dumping, anonymization, synthetic data generation, and restoration. Designed for **PostgreSQL** and **MySQL** (in progress), it is stateless and creates logical backups compatible with standard tools like `pg_restore` or `mysqldump`.

[![Discord](https://img.shields.io/discord/1179422525294399488?label=Discord&logo=discord)](https://discord.com/invite/rKBKvDECfd)
[![Telegram](https://img.shields.io/badge/Telegram-Join%20Chat-blue.svg?logo=telegram)](https://t.me/greenmask_ru)
[![X (formerly Twitter) Follow](https://img.shields.io/twitter/follow/GreenmaskIO)](https://twitter.com/GreenmaskIO)
[![Documentation](https://img.shields.io/badge/docs-latest-blue)](https://docs.greenmask.io)
[![License](https://img.shields.io/github/license/greenmaskio/greenmask)](https://github.com/greenmaskio/greenmask/blob/main/LICENSE)
[![GitHub Release](https://img.shields.io/github/v/release/greenmaskio/greenmask)](https://github.com/greenmaskio/greenmask/releases/latest)
[![GitHub Downloads (all assets, all releases)](https://img.shields.io/github/downloads/greenmaskio/greenmask/total)](https://somsubhra.github.io/github-release-stats/?username=greenmaskio&repository=greenmask&page=1&per_page=5)
[![Docker pulls](https://img.shields.io/docker/pulls/greenmask/greenmask)](https://hub.docker.com/r/greenmask/greenmask)
[![Go Report Card](https://goreportcard.com/badge/github.com/greenmaskio/greenmask)](https://goreportcard.com/report/github.com/greenmaskio/greenmask)


![Demo](docs/assets/tapes/playground.gif)

## Supported Databases

*   **PostgreSQL**: Fully supported (Production Ready).
*   **MySQL**: [Work In Progress (Beta)](https://github.com/GreenmaskIO/greenmask/issues/222).

## Key Features

*   **[Database Subsetting](https://docs.greenmask.io/latest/database_subset/)**: Create smaller, referentially intact development databases with support for cyclic and polymorphic references.
*   **Storage Agnostic**: Supports local directories and S3-compatible storage (AWS S3, MinIO, GCS, Azure, etc.) for flexible backup management.
*   **[Deterministic Transformation](https://docs.greenmask.io/latest/built_in_transformers/transformation_engines/#hash-engine)**: Reproducible data masking using hash functions, ensuring consistent output for the same input.
*   **[Dynamic Parameters](https://docs.greenmask.io/latest/built_in_transformers/dynamic_parameters/)**: Transformers can adapt based on other column values to maintain logical consistency (e.g., `created_at < updated_at`).
*   **[Transformation Condition](https://docs.greenmask.io/latest/built_in_transformers/transformation_condition/)**: Apply transformations only when specific criteria are met, allowing for conditional logic at the table or transformer scope.
*   **Stateless & Compatible**: Operates as a logical dump proxy. Dumps are compatible with `pg_restore`.
*   **[Transformation Inheritance](https://docs.greenmask.io/latest/built_in_transformers/transformation_inheritance/)**: Eliminate redundancy by automatically applying transformations to partitioned tables and foreign key references.
*   **Database Type Safety**: Ensures data integrity by using the native database driver for all encoding and decoding operations.
*   **[Extensible](https://docs.greenmask.io/latest/built_in_transformers/standard_transformers/cmd/)**: Implement domain-specific transformations in any programming language or use [predefined templates](https://docs.greenmask.io/latest/built_in_transformers/advanced_transformers/).
*   **[Cross-Platform](https://github.com/GreenmaskIO/greenmask/releases)**: Single binary, runs anywhere.

## Use Cases

*   **Sensitive Data Sanitization**: Anonymize, transform, and mask PII for staging, analytics, and testing environments, ensuring compliance and security.
*   **Backup & Restore**: A robust, drop-in replacement for `pg_dump`/`pg_restore` and `mysqldump`, handling schema and data with ease.
*   **Local Development**: Quickly spin up lightweight, referentially intact subsets of production databases for developers.
*   **Synthetic Data Generation**: Generate realistic test data from scratch to populate empty environments using the [CMD transformer](https://docs.greenmask.io/latest/built_in_transformers/standard_transformers/cmd/) and [custom transformations](https://docs.greenmask.io/latest/built_in_transformers/standard_transformers/cmd/).

## Quick Start

Try the sandbox environment with a sample database and pre-configured transformations:

```bash
git clone git@github.com:GreenmaskIO/greenmask.git && cd greenmask
docker-compose run greenmask
```

For more details, visit the [Documentation](https://docs.greenmask.io) or the [Playground](https://docs.greenmask.io/latest/playground/).

## Sponsors

<a href="https://www.testmuai.com/?utm_medium=sponsor&utm_source=greenmask" target="_blank">
    <img src="https://assets.testmu.ai/resources/images/logos/black-logo.png" style="vertical-align: middle;" width="250" height="100" />
</a>

## Powered by

[![JetBrains logo.](https://resources.jetbrains.com/storage/products/company/brand/logos/jetbrains.svg)](https://jb.gg/OpenSource)
