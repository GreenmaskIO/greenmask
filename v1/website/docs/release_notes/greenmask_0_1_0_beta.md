# Greenmask 0.0.1 Beta

We are excited to announce the beta release of Greenmask, a versatile and open-source utility for PostgreSQL logical backup dumping, anonymization, and restoration. Greenmask is perfect for routine backup and restoration tasks. It facilitates anonymization and data masking for staging environments and analytics.

This release introduces a range of features aimed at enhancing database management and security.

## Key features

- Cross-platform support — fully written in Go without platform dependencies.
- Type-safe database operations — validates and encodes data, maintaining integrity.
- Transformation validation — ensures data transformations are correct and maintainable.
- Partitioned table support — simplifies configuration for partitioned tables.
- Stateless and backward compatible — works alongside standard PostgreSQL utilities.
- Parallel execution — enhances efficiency in dumping and restoration processes.
- Multiple storage options — supports both local (directory) and remote (S3-like) storage solutions.

## Download

To download the Greenmask binary compatible with your system, see the [release's assets list](https://github.com/GreenmaskIO/greenmask/releases/tag/v0.1.0-beta).
