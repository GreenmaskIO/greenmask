# Greenmask 0.2.25

This is a bugfix release. It fixes a data corruption issue present in every earlier release, starting from 0.1.0-beta. Check the [Am I affected?](#am-i-affected) section below.

## Changes

* fix: keep the byte following `\.` or `\N` in values written by transformers. The COPY encoder escaped these sequences and then skipped one byte too many, so the next byte was dropped from the dump. Depending on the dropped byte, the value was silently altered, or the restore of the whole table failed with `invalid input syntax for type json` or `invalid byte sequence for encoding "UTF8"` [#490](https://github.com/GreenmaskIO/greenmask/pull/490). Closes [#489](https://github.com/GreenmaskIO/greenmask/issues/489)
* fix: write ASCII control characters without a named escape (for example `0x01`) once. Previously they were written twice, so the restored value contained the character twice [#490](https://github.com/GreenmaskIO/greenmask/pull/490)
* fix: clean the S3 integration test prefix before and after the suites, so objects left by an interrupted run no longer fail the next one, and add `make rebuild-images`, `rebuild-greenmask-image` and `rebuild-integration-image` targets that rebuild the local images without cache [#490](https://github.com/GreenmaskIO/greenmask/pull/490)

## Am I affected?

!!! warning

    Dumps made by an earlier release are not repaired by upgrading: the byte was lost when the dump was made. If you are affected, make a new dump with 0.2.25.

A table is affected only if all of the following are true:

1. The dump was made with Greenmask 0.2.24 or earlier.
2. A transformer targets the column, and the row was not skipped by a `when` condition. Columns no transformer targets and rows skipped by `when` are copied byte for byte and are not affected.
3. The value the transformer writes back contains one of:
    * `\.` or `\N` followed by at least one more character, for example `a\.b`, `\.php$` or `C:\Nightly`. In a JSON value this includes a string ending in `\.`, since the closing quote follows it.
    * an ASCII control character other than tab, newline, carriage return, backspace, form feed and vertical tab.

In practice, this affects transformers that keep part of the original value:

* [Template](../built_in_transformers/advanced_transformers/template.md) and [TemplateRecord](../built_in_transformers/advanced_transformers/template_record.md) that output the original value or part of it, including the `jsonSet`, `jsonSetRaw` and `jsonDelete` functions
* [Json](../built_in_transformers/advanced_transformers/json.md) — all keys of the document are written back, including the keys the operations do not touch
* [RegexpReplace](../built_in_transformers/standard_transformers/regexp_replace.md), and [Masking](../built_in_transformers/standard_transformers/masking.md) types that keep part of the value, such as `email`, `url` or `addr`
* [Replace](../built_in_transformers/standard_transformers/replace.md), [Dict](../built_in_transformers/standard_transformers/dict.md) and [RandomChoice](../built_in_transformers/standard_transformers/random_choice.md) when the configured values contain these sequences
* custom transformers that return the original value or part of it

Transformers that generate a new value from scratch, such as random numbers, dates, UUIDs or hashes, do not produce these sequences.

Typical data that triggers the issue: regular expressions (`\.php$`), Windows paths (`C:\Nightly\build`) and web server configuration stored in `text` or `json`/`jsonb` columns.

To check a column in the source database, run the query below for each transformed column. A non-zero count means dumps of this column made before 0.2.25 may be affected:

```sql
SELECT count(*)
FROM my_schema.my_table
WHERE my_column::text ~ '\\[.N].'
   OR my_column::text ~ '[\x01-\x07\x0e-\x1f]';
```

#### Full Changelog: [v0.2.24...v0.2.25](https://github.com/GreenmaskIO/greenmask/compare/v0.2.24...v0.2.25)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Discord](https://discord.gg/tAJegUKSTB)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
