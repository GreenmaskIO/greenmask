# [Greenmask](https://greenmask.io)

**Greenmask** is a powerful open-source utility for logical database dumping, anonymization, synthetic data generation, and restoration. It is stateless, creating logical backups that can be restored using standard tools like `pg_restore`.

[![Discord](https://img.shields.io/discord/1179422525294399488?label=Discord&logo=discord)](https://discord.com/invite/rKBKvDECfd)
[![Telegram](https://img.shields.io/badge/Telegram-Join%20Chat-blue.svg?logo=telegram)](https://t.me/greenmask_ru)
[![X (formerly Twitter) Follow](https://img.shields.io/twitter/follow/GreenmaskIO)](https://twitter.com/GreenmaskIO)
[![Documentation](https://img.shields.io/badge/docs-latest-blue)](https://docs.greenmask.io)
[![License](https://img.shields.io/github/license/greenmaskio/greenmask)](https://github.com/greenmaskio/greenmask/blob/main/LICENSE)
[![GitHub Release](https://img.shields.io/github/v/release/greenmaskio/greenmask)](https://github.com/greenmaskio/greenmask/releases/latest)
[![Go Report Card](https://goreportcard.com/badge/github.com/greenmaskio/greenmask)](https://goreportcard.com/report/github.com/greenmaskio/greenmask)

![Demo](playground.gif)

## Key Features

*   **[Database Subsetting](https://docs.greenmask.io/latest/database_subset/)**: Create smaller, referentially intact development databases with support for cyclic and polymorphic references.
*   **[Deterministic Transformation](https://docs.greenmask.io/latest/built_in_transformers/transformation_engines/#hash-engine)**: Reproducible data masking using hash functions, ensuring consistent output for the same input.
*   **[Dynamic Parameters](https://docs.greenmask.io/latest/built_in_transformers/dynamic_parameters/)**: Transformers can adapt based on other column values to maintain logical consistency (e.g., `created_at < updated_at`).
*   **Stateless & Compatible**: Operates as a logical dump proxy. Dumps are compatible with `pg_restore`.
*   **Storage Agnostic**: Supports local directories and S3-compatible storage (AWS S3, MinIO, etc.).
*   **[Cross-Platform](https://github.com/GreenmaskIO/greenmask/releases)**: Single binary, runs anywhere.

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
