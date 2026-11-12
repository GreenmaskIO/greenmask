# Greenmask 0.2.27

Greenmask 0.2.27 was released on 2026-11-12. It fixes security vulnerabilities in dependencies, adds the `common.timezone` setting, and fixes transformers that could leave columns unmasked and more than twenty other transformer bugs. Some transformers now produce different values for the same input, see [Changed output](#changed-output).

## Security

* fix: update pgx to 5.12.0 for [GHSA-95fj-3fxw-g62g](https://github.com/jackc/pgx/security/advisories/GHSA-95fj-3fxw-g62g): a connection that relies on `require_auth` or `channel_binding=require` without `sslmode=verify-full` could accept a server that skips authentication [#551](https://github.com/GreenmaskIO/greenmask/pull/551)
* fix: update `golang.org/x/crypto` to 0.58.0 (denial of service in `ssh`, used by the SSH storage) and `golang.org/x/net` to 0.62.0 (HTTP/2, used by the Azure storage) [#557](https://github.com/GreenmaskIO/greenmask/pull/557)
* fix: build with Go 1.26.11, which distrusts compromised intermediate certificates in `crypto/x509`

## Changes

### Columns that could be left unmasked

* fix: [RealAddress](../built_in_transformers/standard_transformers/real_address.md) dumped the columns after a NULL `keep_null` column unmasked [#509](https://github.com/GreenmaskIO/greenmask/pull/509)
* fix: [Dict](../built_in_transformers/standard_transformers/dict.md) with `fail_not_matched: false` and no default replaced an unmatched value with the original data [#510](https://github.com/GreenmaskIO/greenmask/pull/510)
* fix: in a table with a custom transformer, a `when` condition was evaluated while the previous transformer was still writing the row, so a transformer could run on the wrong rows [#523](https://github.com/GreenmaskIO/greenmask/pull/523)

### Features

* feat: add [`common.timezone`](../configuration.md#common-section) (`COMMON_TIMEZONE`), the time zone in which `dump` and `validate` read `timestamptz` values, `UTC` by default. pgx 5.11 and later would otherwise read them in the time zone of the machine. External transformers built with the new `pkg/toolkit` use it too [#551](https://github.com/GreenmaskIO/greenmask/pull/551)
* feat: `validate` reports a missing column in a `when` condition; the dump used to fail on the first row [#524](https://github.com/GreenmaskIO/greenmask/pull/524)
* feat: [RandomMac](../built_in_transformers/standard_transformers/random_mac.md) gets `keep_null` [#512](https://github.com/GreenmaskIO/greenmask/pull/512)

### Fixes

* fix: `numeric` values with more than 10000 digits after the decimal point failed to decode after the `shopspring/decimal` update
* fix: [RandomDate](../built_in_transformers/standard_transformers/random_date.md) and [RandomUnixTimestamp](../built_in_transformers/standard_transformers/random_unix_timestamp.md) depended on the time zone of the machine and could leave the range; `truncate` to milliseconds and microseconds did nothing; RandomUnixTimestamp mixed up units [#532](https://github.com/GreenmaskIO/greenmask/pull/532)
* fix: [NoiseDate](../built_in_transformers/standard_transformers/noise_date.md) ignored a static `min` and `max` alone, made the time part of ratios 1000 times larger and rejected equal ratios [#530](https://github.com/GreenmaskIO/greenmask/pull/530)
* fix: template functions `randomInt` and `randomString` excluded `max`, `randomString` used only part of the symbols, and equal bounds panicked [#520](https://github.com/GreenmaskIO/greenmask/pull/520)
* fix: [Template](../built_in_transformers/advanced_transformers/template.md) and [TemplateRecord](../built_in_transformers/advanced_transformers/template_record.md) panicked on an unknown column, ignored `validate` and broke `.DecodeValue` [#515](https://github.com/GreenmaskIO/greenmask/pull/515)
* fix: [Json](../built_in_transformers/advanced_transformers/json.md) ignored `error_not_exist` for a static `value`, wrote invalid JSON for NULL input and lost precision of static numbers [#519](https://github.com/GreenmaskIO/greenmask/pull/519)
* fix: [RandomCompany](../built_in_transformers/standard_transformers/random_company.md) with `engine: hash` gave different companies for the same value; `keep_null` was ignored with the random engine [#514](https://github.com/GreenmaskIO/greenmask/pull/514)
* fix: [Masking](../built_in_transformers/standard_transformers/masking.md) counted bytes instead of characters and could write invalid UTF-8 [#518](https://github.com/GreenmaskIO/greenmask/pull/518)
* fix: [RandomIp](../built_in_transformers/standard_transformers/random_ip.md) generated addresses outside a subnet with host bits or a leading zero byte [#517](https://github.com/GreenmaskIO/greenmask/pull/517)
* fix: [RandomEmail](../built_in_transformers/standard_transformers/random_email.md) rejected RFC 5322 characters in the original local part [#537](https://github.com/GreenmaskIO/greenmask/pull/537). Closes [#536](https://github.com/GreenmaskIO/greenmask/issues/536)
* fix: [RandomString](../built_in_transformers/standard_transformers/random_string.md), [RandomChoice](../built_in_transformers/standard_transformers/random_choice.md) and [RandomMac](../built_in_transformers/standard_transformers/random_mac.md) panicked on invalid parameters or NULL [#511](https://github.com/GreenmaskIO/greenmask/pull/511) [#513](https://github.com/GreenmaskIO/greenmask/pull/513) [#512](https://github.com/GreenmaskIO/greenmask/pull/512)
* fix: [RandomCCNumber](../built_in_transformers/standard_transformers/random_cc_number.md) no longer accepts `int4`, which cannot hold its output [#527](https://github.com/GreenmaskIO/greenmask/pull/527)
* fix: [Hash](../built_in_transformers/standard_transformers/hash.md) accepts `sha3-256` [#529](https://github.com/GreenmaskIO/greenmask/pull/529)
* fix: [Replace](../built_in_transformers/standard_transformers/replace.md) and [RegexpReplace](../built_in_transformers/standard_transformers/regexp_replace.md) can be used with `apply_for_references` [#526](https://github.com/GreenmaskIO/greenmask/pull/526)
* fix: correct NOT NULL warnings for HashedPassword, Dict and RandomChoice [#528](https://github.com/GreenmaskIO/greenmask/pull/528)
* fix: a dynamic parameter default for NULL could change between rows [#525](https://github.com/GreenmaskIO/greenmask/pull/525)
* fix: RandomPerson read one generator byte more than it requested; output is unchanged [#516](https://github.com/GreenmaskIO/greenmask/pull/516)
* fix: subset with polymorphic virtual references could drop valid rows [#538](https://github.com/GreenmaskIO/greenmask/pull/538). Closes [#396](https://github.com/GreenmaskIO/greenmask/issues/396)
* change: pgx 5.11 parses `postgres://` connection URIs exactly as libpq does, see the [pgx changelog](https://github.com/jackc/pgx/blob/master/CHANGELOG.md) [#551](https://github.com/GreenmaskIO/greenmask/pull/551)

### Documentation and build

* docs: document `install.sh`, validation severities, `when` exceptions and `keep_null` defaults [#508](https://github.com/GreenmaskIO/greenmask/pull/508)
* chore: configure Dependabot and update GitHub Actions and Go dependencies [#539](https://github.com/GreenmaskIO/greenmask/pull/539) [#540](https://github.com/GreenmaskIO/greenmask/pull/540) [#541](https://github.com/GreenmaskIO/greenmask/pull/541) [#542](https://github.com/GreenmaskIO/greenmask/pull/542) [#543](https://github.com/GreenmaskIO/greenmask/pull/543) [#544](https://github.com/GreenmaskIO/greenmask/pull/544) [#545](https://github.com/GreenmaskIO/greenmask/pull/545) [#553](https://github.com/GreenmaskIO/greenmask/pull/553) [#554](https://github.com/GreenmaskIO/greenmask/pull/554) [#555](https://github.com/GreenmaskIO/greenmask/pull/555) [#556](https://github.com/GreenmaskIO/greenmask/pull/556) [#557](https://github.com/GreenmaskIO/greenmask/pull/557)
* ci: create the release draft before uploading the binaries [#507](https://github.com/GreenmaskIO/greenmask/pull/507)

## Changed output

The same input gives a different value, also with `engine: hash`, in these cases:

* `timestamptz` values when the PostgreSQL session or the machine was not in UTC: `truncate` in NoiseDate and template date functions now work in `common.timezone`, and NoiseDate with `engine: hash` changes once
* RandomDate and RandomUnixTimestamp on a machine not in UTC
* NoiseDate with a static `min`, `max` alone or ratios with a time part
* template functions `randomInt` and `randomString`
* RandomCompany with `engine: hash`, Masking of multi-byte text, RandomIp with a non-aligned subnet, Json with large static numbers
* the unmasked-column fixes above

RandomString, RandomChoice and RandomCCNumber configurations and `when` conditions that used to panic or fail on restore are now validation errors.

#### Full Changelog: [v0.2.26...v0.2.27](https://github.com/GreenmaskIO/greenmask/compare/v0.2.26...v0.2.27)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Discord](https://discord.gg/tAJegUKSTB)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
