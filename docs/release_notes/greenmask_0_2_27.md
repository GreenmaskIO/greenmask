# Greenmask 0.2.27

Greenmask 0.2.27 was released on 2026-11-12. It fixes security vulnerabilities in pgx and other dependencies, adds the `common.timezone` setting, and fixes two transformers that could leave columns unmasked, a data race with custom transformers and more than twenty transformer bugs. Some transformers produce different values for the same input, see [Changed output](#changed-output).

## Security

* fix: update pgx to 5.12.0 to fix [GHSA-95fj-3fxw-g62g](https://github.com/jackc/pgx/security/advisories/GHSA-95fj-3fxw-g62g). Greenmask 0.2.26 is built with pgx 5.10.0, which can accept a server that impersonates PostgreSQL and skips authentication. It affects you if the connection relies on `require_auth` or `channel_binding=require` without verifying the server identity with `sslmode=verify-full`. pgx 5.12.0 also fixes [GHSA-j38q-h3ph-jjpq](https://github.com/jackc/pgx/security/advisories/GHSA-j38q-h3ph-jjpq) in simple-protocol batches, which greenmask does not use [#551](https://github.com/GreenmaskIO/greenmask/pull/551)
* fix: update `golang.org/x/crypto` to 0.58.0, which fixes two denial-of-service issues in `ssh` used by the [SSH storage](../configuration.md#storage-section) (CVE-2026-78662, CVE-2026-56855), and `golang.org/x/net` to 0.62.0, which fixes HTTP/2 issues reachable through the Azure storage client [#557](https://github.com/GreenmaskIO/greenmask/pull/557)
* fix: binaries and Docker images are built with Go 1.26.11, which distrusts compromised intermediate certificates in `crypto/x509` (CVE-2026-111943)

## Changes

### Columns that could be left unmasked

* fix: [RealAddress](../built_in_transformers/standard_transformers/real_address.md) skipped the remaining columns when one column with `keep_null` was NULL, so the columns after it were dumped unmasked. Only that column is skipped now [#509](https://github.com/GreenmaskIO/greenmask/pull/509)
* fix: [Dict](../built_in_transformers/standard_transformers/dict.md) with `fail_not_matched: false` and no default replaced an unmatched value with the original data, discarding the value an earlier transformer had written to the same column. The current value is kept now [#510](https://github.com/GreenmaskIO/greenmask/pull/510)
* fix: a data race in tables with a custom transformer and more than one transformer. The `when` condition of a transformer was evaluated while the previous transformer in the same window was writing the record, so the condition usually saw the value before that transformer and the transformer could run on the wrong rows. A transformer with a `when` condition now starts a new window, and columns read by dynamic parameters count as conflicts. Such tables can produce different, now correct, values and can dump more slowly [#523](https://github.com/GreenmaskIO/greenmask/pull/523)

### Features

* feat: add the [`common.timezone`](../configuration.md#common-section) setting, also `COMMON_TIMEZONE`. It is the time zone in which `dump` and `validate` read `timestamptz` values from the data. It takes a case-sensitive IANA name, such as `UTC` or `Europe/Berlin`, and defaults to `UTC`. An unknown name stops the command with an error. Greenmask passes it to external transformers in the metadata, and those built with the new `pkg/toolkit` decode `timestamptz` in it; the new `Meta.TimestamptzLocation` and `WithTimestamptzLocation` driver option are available to code that uses `pkg/toolkit` directly [#551](https://github.com/GreenmaskIO/greenmask/pull/551)
* feat: `validate` reports a column used in a table or transformer `when` condition that does not exist as a "column does not exist" error. Previously it passed validation and the dump failed on the first row with `cannot call nil`. The error for a non-boolean condition result now prints the result [#524](https://github.com/GreenmaskIO/greenmask/pull/524)
* feat: [RandomMac](../built_in_transformers/standard_transformers/random_mac.md) gets `keep_null` (default `false`, which keeps the current output) [#512](https://github.com/GreenmaskIO/greenmask/pull/512)

### Fixes

* fix: `timestamptz` values are now read in `common.timezone` (`UTC` by default) instead of the offset sent by the server, see [Changed output](#changed-output) [#551](https://github.com/GreenmaskIO/greenmask/pull/551)
* fix: values of `numeric` columns with more than 10000 digits after the decimal point failed to decode after the `shopspring/decimal` update. The limit is raised to 16383, the largest scale PostgreSQL allows
* fix: [RandomDate](../built_in_transformers/standard_transformers/random_date.md) and [RandomUnixTimestamp](../built_in_transformers/standard_transformers/random_unix_timestamp.md): values were generated in the local time zone, so the output depended on the machine and could leave the range; nanoseconds at the min second could go below `min`; `truncate` to `millisecond` and `microsecond` did nothing; RandomUnixTimestamp read microseconds as milliseconds, converted `max` with `min_unit` and generated nanoseconds with `% 999999999`; a dynamic `min` accepted no compatible column types [#532](https://github.com/GreenmaskIO/greenmask/pull/532)
* fix: [NoiseDate](../built_in_transformers/standard_transformers/noise_date.md): a static `min` was ignored, and so was `max` alone; a dynamic bound without the other one clamped to year 1; the microseconds of an interval were multiplied by a millisecond, so the time part of ratios, `tsModify` and `noiseDatePgInterval` was 1000 times larger; `min_ratio` equal to `max_ratio` was rejected [#530](https://github.com/GreenmaskIO/greenmask/pull/530)
* fix: template functions: `randomString` used the global source with a biased index, and `randomString 3 3` produced only the first 3 symbols; `randomInt` and the `randomString` length excluded `max` although the documentation says inclusive; `randomInt 5 5` and `randomDate t t` panicked; empty symbols are an error [#520](https://github.com/GreenmaskIO/greenmask/pull/520)
* fix: [Template](../built_in_transformers/advanced_transformers/template.md) and [TemplateRecord](../built_in_transformers/advanced_transformers/template_record.md): TemplateRecord with an unknown column panicked; the `validate` parameter was never read; `.DecodeValue` passed the type name where a column name is expected [#515](https://github.com/GreenmaskIO/greenmask/pull/515)
* fix: [Json](../built_in_transformers/advanced_transformers/json.md): `error_not_exist` was checked only for `value_template`, so a static `value` silently created a missing path; NULL input with only delete operations wrote an empty string, which is invalid JSON, and now stays NULL; static numbers lost precision through float64; template output that is not valid JSON was written silently [#519](https://github.com/GreenmaskIO/greenmask/pull/519)
* fix: [RandomCompany](../built_in_transformers/standard_transformers/random_company.md) with `engine: hash` hashed a buffer that grew every row, so the same value got different companies and the cost grew quadratically; `keep_null` was ignored with the random engine [#514](https://github.com/GreenmaskIO/greenmask/pull/514)
* fix: [Masking](../built_in_transformers/standard_transformers/masking.md) wrote one star per byte (`Иван` gave 8 stars), and the postcode masker could cut a multi-byte character and write invalid UTF-8 that failed on restore [#518](https://github.com/GreenmaskIO/greenmask/pull/518)
* fix: [RandomIp](../built_in_transformers/standard_transformers/random_ip.md) produced addresses outside a subnet written with host bits (`192.168.1.200/24`), and invalid addresses for subnets with a leading zero byte (`0.0.0.0/8`) and IPv4-mapped IPv6 [#517](https://github.com/GreenmaskIO/greenmask/pull/517)
* fix: [RandomEmail](../built_in_transformers/standard_transformers/random_email.md) rejected original values with RFC 5322 characters such as ``!#$%&'*/=?^`{|}~`` in the local part when `keep_original_domain` or the `original_local_part` and `original_domain` template variables were used [#537](https://github.com/GreenmaskIO/greenmask/pull/537). Closes [#536](https://github.com/GreenmaskIO/greenmask/issues/536)
* fix: [RandomString](../built_in_transformers/standard_transformers/random_string.md) panicked with more than 256 symbols and long strings, and with a negative `min_length`, `max_length` below `min_length` or empty `symbols`, which are now validation errors; generator errors were turned into an empty string [#511](https://github.com/GreenmaskIO/greenmask/pull/511)
* fix: [RandomChoice](../built_in_transformers/standard_transformers/random_choice.md) with an empty list, or one where every value was skipped, panicked with integer divide by zero on the first row. It is now a validation error [#513](https://github.com/GreenmaskIO/greenmask/pull/513)
* fix: [RandomMac](../built_in_transformers/standard_transformers/random_mac.md) panicked on NULL with `keep_original_vendor` and swallowed scan errors of `cast_type` and `management_type` [#512](https://github.com/GreenmaskIO/greenmask/pull/512)
* fix: [RandomCCNumber](../built_in_transformers/standard_transformers/random_cc_number.md) no longer accepts `int4` columns: the generated 16-digit numbers do not fit and failed on restore [#527](https://github.com/GreenmaskIO/greenmask/pull/527)
* fix: [Hash](../built_in_transformers/standard_transformers/hash.md) registered SHA3-256 as `sha3-254` and rejected `sha3-256`. Both are accepted and the output is unchanged [#529](https://github.com/GreenmaskIO/greenmask/pull/529)
* fix: [Replace](../built_in_transformers/standard_transformers/replace.md) and [RegexpReplace](../built_in_transformers/standard_transformers/regexp_replace.md) required an `engine: hash` parameter they do not define, so `apply_for_references` never passed validation [#526](https://github.com/GreenmaskIO/greenmask/pull/526)
* fix: [HashedPassword](../built_in_transformers/standard_transformers/hashed_password.md) was declared nullable and gave a false warning on `NOT NULL` columns; Dict and RandomChoice can write NULL but did not declare it [#528](https://github.com/GreenmaskIO/greenmask/pull/528)
* fix: dynamic parameters: the default value for NULL was cached as the caller's pointer, so later NULL rows read whatever that variable became; the "column does not exist" warning put the parameter name into `ColumnName` [#525](https://github.com/GreenmaskIO/greenmask/pull/525)
* fix: [RandomPerson](../built_in_transformers/standard_transformers/random_person.md) requested 12 generator bytes and read 13. The output is unchanged [#516](https://github.com/GreenmaskIO/greenmask/pull/516)
* fix: subset with polymorphic virtual references that share a foreign-key column could drop valid rows when independent paths were combined with `AND`. A polymorphic scope is now bypassed when its discriminator does not match [#538](https://github.com/GreenmaskIO/greenmask/pull/538). Closes [#396](https://github.com/GreenmaskIO/greenmask/issues/396)
* change: with pgx 5.11 and later, connection strings in URI format (`postgres://...`) are parsed exactly as libpq parses them: `+` stays `+`, malformed percent-encoding is an error, `#` is an ordinary character, the last occurrence of a repeated parameter wins and the ports of multiple hosts are matched by position. See the [pgx changelog](https://github.com/jackc/pgx/blob/master/CHANGELOG.md) for the full list [#551](https://github.com/GreenmaskIO/greenmask/pull/551)

### Documentation and build

* docs: every release note starts with the version, release date and a summary; document `install.sh` flags and manual verification of a pinned release, validation severities, `when` condition exceptions, `keep_null` defaults and a full `dump.transformation` example [#508](https://github.com/GreenmaskIO/greenmask/pull/508)
* chore: configure Dependabot for Go modules and GitHub Actions, update GitHub Actions to their current major versions and Go dependencies to their latest minor versions [#539](https://github.com/GreenmaskIO/greenmask/pull/539) [#540](https://github.com/GreenmaskIO/greenmask/pull/540) [#541](https://github.com/GreenmaskIO/greenmask/pull/541) [#542](https://github.com/GreenmaskIO/greenmask/pull/542) [#543](https://github.com/GreenmaskIO/greenmask/pull/543) [#544](https://github.com/GreenmaskIO/greenmask/pull/544) [#545](https://github.com/GreenmaskIO/greenmask/pull/545) [#553](https://github.com/GreenmaskIO/greenmask/pull/553) [#554](https://github.com/GreenmaskIO/greenmask/pull/554) [#555](https://github.com/GreenmaskIO/greenmask/pull/555) [#556](https://github.com/GreenmaskIO/greenmask/pull/556) [#557](https://github.com/GreenmaskIO/greenmask/pull/557)
* ci: create the release draft before uploading the binaries [#507](https://github.com/GreenmaskIO/greenmask/pull/507)

## Changed output

Upgrading changes the output for the same input, including with `engine: hash`, in these cases. Configurations that do not use them produce the same values as 0.2.26.

* `timestamptz` values when the PostgreSQL session or the machine that runs greenmask was not in UTC: `truncate` in NoiseDate, `truncateDate` and other template functions that read the date or time of a value now work in `common.timezone`, taking DST into account for a named zone, and NoiseDate with `engine: hash` produces different values once because its seed included the zone name. The seed is now the instant in UTC, so it no longer depends on `common.timezone`. Setting `common.timezone` to the server time zone does not fully restore the old results, because 0.2.26 used the fixed offset of each value
* RandomDate and RandomUnixTimestamp on a machine that is not in UTC, and with `truncate` to `millisecond` or `microsecond`
* NoiseDate with a static `min`, with `max` alone, or with ratios that have a time part
* the template functions `randomInt` and `randomString`
* Json with static numbers that do not fit float64 exactly
* RandomCompany with `engine: hash`
* Masking of values with multi-byte characters
* RandomIp with a subnet written with host bits or with a leading zero byte
* tables with a custom transformer, more than one transformer and a `when` condition or dynamic parameters
* RealAddress and Dict in the cases listed under [Columns that could be left unmasked](#columns-that-could-be-left-unmasked): these columns are now masked

These configurations passed validation in 0.2.26 and are now validation errors. Each of them panicked or wrote data that failed on restore: RandomString with a negative `min_length`, `max_length` below `min_length` or empty `symbols`; RandomChoice with an empty list; RandomCCNumber on an `int4` column; a `when` condition that uses a column that does not exist.

#### Full Changelog: [v0.2.26...v0.2.27](https://github.com/GreenmaskIO/greenmask/compare/v0.2.26...v0.2.27)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Discord](https://discord.gg/tAJegUKSTB)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
