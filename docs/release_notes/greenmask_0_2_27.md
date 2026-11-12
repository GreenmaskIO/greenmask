# Greenmask 0.2.27

Greenmask 0.2.27 updates pgx to 5.12.0, which fixes a security vulnerability, and adds the `common.timezone` setting that fixes the time zone in which `timestamptz` values are read from the data.

## Security

* fix: update pgx to 5.12.0 to fix [GHSA-95fj-3fxw-g62g](https://github.com/jackc/pgx/security/advisories/GHSA-95fj-3fxw-g62g). Greenmask 0.2.26 is built with pgx 5.10.0, which can accept a server that impersonates PostgreSQL and skips authentication. It affects you if the connection relies on `require_auth` or `channel_binding=require` without verifying the server identity with `sslmode=verify-full`. pgx 5.12.0 also fixes [GHSA-j38q-h3ph-jjpq](https://github.com/jackc/pgx/security/advisories/GHSA-j38q-h3ph-jjpq) in simple-protocol batches, which greenmask does not use

## Changes

* feat: add the [`common.timezone`](../configuration.md#common-section) setting, also `COMMON_TIMEZONE`. It is the time zone in which `dump` and `validate` read `timestamptz` values from the data. It takes a case-sensitive IANA name, such as `UTC` or `Europe/Berlin`, and defaults to `UTC`. An unknown name stops the command with an error. Greenmask passes it to external transformers in the metadata, and those built with the new `pkg/toolkit` decode `timestamptz` in it; the new `Meta.TimestamptzLocation` and `WithTimestamptzLocation` driver option are available to code that uses `pkg/toolkit` directly
* change: `timestamptz` values are now read in `common.timezone` (`UTC` by default) instead of the offset sent by the server. If both the PostgreSQL session and the machine that runs greenmask were in UTC, results do not change. If the session was not in UTC, `truncate` in [NoiseDate](../built_in_transformers/standard_transformers/noise_date.md), `truncateDate` and other template functions that read the date or time of a value now work in `common.timezone`, taking DST into account for a named zone. NoiseDate with `engine: hash` produces different values once if either the session or the machine was not in UTC, because its seed included the zone name. The seed is now the instant in UTC, so it no longer depends on `common.timezone`. Setting `common.timezone` to the server time zone does not fully restore the old results, because 0.2.26 used the fixed offset of each value
* change: with pgx 5.11 and later, connection strings in URI format (`postgres://...`) are parsed exactly as libpq parses them: `+` stays `+`, malformed percent-encoding is an error, `#` is an ordinary character, the last occurrence of a repeated parameter wins and the ports of multiple hosts are matched by position. See the [pgx changelog](https://github.com/jackc/pgx/blob/master/CHANGELOG.md) for the full list

:::note

Reading `timestamptz` values no longer depends on the time zone of the machine that runs greenmask. With pgx 5.12.0 alone, they would have been read in the local time zone of that machine.

:::

#### Full Changelog: [v0.2.26...v0.2.27](https://github.com/GreenmaskIO/greenmask/compare/v0.2.26...v0.2.27)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Discord](https://discord.gg/tAJegUKSTB)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
