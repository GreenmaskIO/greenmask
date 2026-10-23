# Greenmask 0.2.26

## Changes

* feat: add `--diff-mode=summary` to the [validate](../commands/validate.md#summary-diff) command. It compares the same rows as `--diff` but prints only counts per table and column — changed, unchanged and NULLs kept — with no original or transformed value and no primary key. This makes `validate` usable where production data may be read but must not be shown, such as kept CI logs, a shared terminal or an AI agent's transcript. Text and `--format=json` output are supported [#495](https://github.com/GreenmaskIO/greenmask/pull/495) [#503](https://github.com/GreenmaskIO/greenmask/pull/503)
* feat: the summary warns when a transformed column keeps more than `--diff-unchanged-threshold` percent of its non-NULL values (default `90`, `100` turns it off), which usually means a no-op transformer, and when a column changed although no transformer affects it. These are validation warnings: they print with `--warnings` or `--strict`, have a hash that can go into `resolved_warnings`, and `--strict` exits non-zero on an unresolved one. The hash ignores row counts, so it stays the same when `--rows-limit`, the data or the threshold change [#503](https://github.com/GreenmaskIO/greenmask/pull/503)
* fix: make the gender chosen by [RandomPerson](../built_in_transformers/standard_transformers/random_person.md) reproducible across runs. The list of genders was built from a map, so its order changed between runs and the deterministic engine could pick a different gender, and so a different name, for the same input [#502](https://github.com/GreenmaskIO/greenmask/pull/502). Closes [#501](https://github.com/GreenmaskIO/greenmask/issues/501)
* docs: move the documentation site to Docusaurus and drop MkDocs [#498](https://github.com/GreenmaskIO/greenmask/pull/498) [#500](https://github.com/GreenmaskIO/greenmask/pull/500) [#504](https://github.com/GreenmaskIO/greenmask/pull/504)
* docs: add the [Save and reset test environments with arcave](../arcave.md) page: restore a masked dump into a Docker Compose project once, save it as an arcave savepoint and reset the whole environment in one command [#499](https://github.com/GreenmaskIO/greenmask/pull/499)
* docs: add `make docs-serve` and `make docs-build` to preview the documentation locally with the stock Docusaurus theme, and a [CONTRIBUTING.md](https://github.com/GreenmaskIO/greenmask/blob/main/CONTRIBUTING.md) guide for contributors [#505](https://github.com/GreenmaskIO/greenmask/pull/505)
* docs: correct the documented defaults of Hash `function`, RandomNumeric `decimal`, RandomString `symbols` and RandomPerson `gender_mapping` to match the code [#493](https://github.com/GreenmaskIO/greenmask/pull/493)

:::note

`--diff-mode=values` stays the default, so the existing `validate --diff` output is unchanged. `--data` with `--diff-mode=summary` and without `--diff` is an error, because plain `--data` prints every value. Without `--data` the mode is ignored, so `diff_mode: summary` can stay in a shared config.

:::

#### Full Changelog: [v0.2.25...v0.2.26](https://github.com/GreenmaskIO/greenmask/compare/v0.2.25...v0.2.26)

## Links

Feel free to reach out to us if you have any questions or need assistance:

* [Discord](https://discord.gg/tAJegUKSTB)
* [Email](mailto:support@greenmask.io)
* [Twitter](https://twitter.com/GreenmaskIO)
* [Telegram [RU]](https://t.me/greenmask_ru)
* [DockerHub](https://hub.docker.com/r/greenmask/greenmask)
