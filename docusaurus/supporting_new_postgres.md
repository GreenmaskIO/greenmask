---
title: Supporting a New PostgreSQL Version
description: Step-by-step guide for adding support for a new PostgreSQL major version in Greenmask, including how to update the test suite and verify compatibility.
keywords: ["postgresql version support", "greenmask compatibility", "postgres upgrade", "Enterprise support", "Open-Source", "PostgreSQL anonymization", "test data management", "compliance", "security", "agentic pipeline", "development cycle"]
---

# Supporting a New PostgreSQL Version

## Support policy

**Greenmask supports only PostgreSQL major versions that are still supported upstream (non-EOL).**

We deliberately do not claim support for EOL versions. Every version we list is exercised by the
integration suite on every release, so we can actually guarantee it works — dump, transform, restore,
and native `pg_restore` interoperability. Versions past their upstream EOL date are dropped from the
matrix at the first release after that date. Greenmask will very likely keep working against them,
but it is untested and unsupported.

| Version | Upstream EOL  | Status in greenmask       |
|---------|---------------|---------------------------|
| 19      | ~Nov 2031     | beta — support in progress |
| 18      | Nov 14, 2030  | supported                 |
| 17      | Nov 8, 2029   | supported                 |
| 16      | Nov 9, 2028   | supported                 |
| 15      | Nov 11, 2027  | supported                 |
| 14      | Nov 12, 2026  | supported                 |
| 13      | Nov 13, 2025  | **EOL — to be dropped**   |

Authoritative source: [PostgreSQL Versioning Policy](https://www.postgresql.org/support/versioning/).

Keep this table, `docker-compose-integration.yml`, `docker/integration/tests/Dockerfile`, and the
`PG_VERSIONS_CHECK` environment variables in sync. They are the single definition of "supported".

## Where greenmask is coupled to PostgreSQL

Three independent coupling points. A new major version can break any one of them, so all three are
checked separately.

1. **The ported TOC archive library** — `internal/db/postgres/toc` is a Go re-implementation of
   pg_dump's `toc.dat` reader/writer. It must stay byte-compatible with the C implementation.
2. **The `pg_dump` / `pg_restore` CLI contract** — greenmask shells out to the real binaries for the
   pre-data and post-data sections (`internal/db/postgres/pgdump`, `internal/db/postgres/pgrestore`).
3. **Catalog introspection** — greenmask runs its own catalog queries instead of reusing pg_dump's
   (`internal/db/postgres/context`).

---

## Pre-release checklist

Run this before **every** release, not only when adding a major version. Upstream changes the archive
format and pg_dump behaviour in minor releases far less often than in majors, but catalog and
client-tool behaviour does move.

### 1. Refresh the local PostgreSQL checkout

```bash
cd /path/to/postgres
git fetch origin --tags
git tag -l 'REL_1*'          # find the newest tag of each supported branch
```

### 2. Check the archive format version

This is the single most important check. If `K_VERS_MINOR` was bumped, the ported library **must** be
updated before the release ships.

```bash
git diff REL_18_0 REL_19_BETA3 -- src/bin/pg_dump/pg_backup_archiver.h
```

Compare against `MaxVersion` in [`internal/db/postgres/toc/utils.go`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/utils.go).
As of PostgreSQL 19 beta 3 the format is still **1.16**, unchanged since PostgreSQL 18.

### 3. Diff the TOC read/write functions

Walk the watchlist in [TOC archive format watchlist](#toc-archive-format-watchlist) below, function by
function. Any change in field order, field count, or version gating must be mirrored in
`reader.go` / `writer.go`.

```bash
git diff REL_18_0 REL_19_BETA3 -- src/bin/pg_dump/pg_backup_archiver.c \
                                  src/bin/pg_dump/pg_backup_directory.c
```

### 4. Diff the `pg_dump` / `pg_restore` option surface

Any option greenmask passes that was removed upstream is an immediate runtime failure. Any new option
is a candidate feature.

```bash
git diff REL_18_0 REL_19_BETA3 -- src/bin/pg_dump/pg_dump.c \
                                  src/bin/pg_dump/pg_restore.c \
                                  src/bin/pg_dump/pg_backup.h
```

Cross-check every field of `Options` in
[`internal/db/postgres/pgdump/pgdump.go`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/pgdump/pgdump.go) and
[`internal/db/postgres/pgrestore/pgrestore.go`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/pgrestore/pgrestore.go).

### 5. Diff the TOC entry `desc` strings

New object types add new `desc` values (`STATISTICS DATA`, `EXTENDED STATISTICS DATA`,
`SUBSCRIPTION TABLE`, …). Greenmask passes unknown descs through, but the ones it handles explicitly
in `internal/db/postgres/cmd/restore.go` and `internal/db/postgres/toc/common.go` must stay correct.

```bash
git show REL_19_BETA3:src/bin/pg_dump/pg_dump.c \
  | grep -oE '\.description = "[A-Z][A-Z /]*"' | sort -u
```

### 6. Diff the catalogs greenmask queries

Greenmask reads `pg_class`, `pg_attribute`, `pg_type`, `pg_constraint`, `pg_sequence`, `pg_namespace`,
`pg_inherits`, `pg_largeobject_metadata`, `pg_foreign_table`, `pg_foreign_server`, and `pg_roles`.

```bash
git diff REL_18_0 REL_19_BETA3 -- src/include/catalog/
```

Pay attention to:

- **New `relkind` values** ([`pg_class.h`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/include/catalog/pg_class.h#L171)).
  PostgreSQL 19 adds `'g'` (property graph). Every introspection query filters `relkind`, so new kinds
  are excluded by default — verify that is still the behaviour you want, and that the
  `unknown relkind` branch in `internal/db/postgres/context/pg_catalog.go` cannot be reached.
- **New `contype` values** — the `switch` in
  [`internal/db/postgres/context/tables_introspection.go`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/context/tables_introspection.go)
  must cover them or log the unknown-constraint warning.
- **New built-in types** in `pg_type.dat`. These are not "custom types", so they are invisible to
  `CustomTypesWithTypeChainQuery` and depend on `pgx` knowing their OID. If `pgx` does not, columns of
  that type are marked unsupported in [`pkg/toolkit/driver.go`](https://github.com/GreenmaskIO/greenmask/blob/main/pkg/toolkit/driver.go) and no
  transformer can be attached. PostgreSQL 19 adds `oid8` (6437) and `regdatabase` (6490).
- **Behaviour changes in functions greenmask calls** — `pg_sequence_last_value`,
  `has_sequence_privilege`, `pg_get_constraintdef`, `aclexplode`, `acldefault`, `pg_partition_ancestors`.

### 7. Read the release notes migration section

```bash
sed -n '/<sect2 .*id="release-19-migration"/,/^  <\/sect2>/p' doc/src/sgml/release-19.sgml
```

Anything under "Migration to Version N" that mentions `pg_dump`, `pg_restore`, dump/restore
compatibility, or a GUC default change is a candidate for a release-note caveat on our side.

### 8. Drop versions that went EOL

If a supported version passed its EOL date since the last release, remove it now — see
[Dropping an EOL version](#dropping-an-eol-version).

### 9. Run the integration suite against every supported version

```bash
docker compose -f docker-compose-integration.yml --profile all up \
  --renew-anon-volumes --force-recreate \
  --exit-code-from greenmask --abort-on-container-exit greenmask
```

The suite must be green for every version in the support table. Exit code 2 means at least one version
failed.

### 10. Update the support table

Update the table at the top of this file, and note the tested version range in the release notes.

---

## TOC archive format watchlist

These are the upstream functions that define the `toc.dat` binary layout. **Diff every one of them on
every revision.** Links are pinned to `REL_19_BETA3`; swap the tag when a new major branches.

### Format version constants

| Upstream | Greenmask counterpart | What to check |
|---|---|---|
| [`K_VERS_1_16` / `K_VERS_MAJOR` / `K_VERS_MINOR` / `K_VERS_REV`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_archiver.h#L71-L79) | [`MaxVersion`, `BackupVersions`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/utils.go) | A bumped `K_VERS_MINOR` means new fields were added to the entry or header layout. Add the new `1.N` key and gate the new fields identically. |
| [`K_VERS_MAX`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_archiver.h#L82) | version guard in [`readHeader`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/reader.go) | Upper bound accepted by pg_restore; ours must not exceed it. |
| [`archUnknown` / `archCustom` / `archTar` / `archNull` / `archDirectory`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup.h#L39-L46) | [`ArchTar` and friends](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/utils.go) | Format byte values. |
| [`pg_compress_algorithm`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/include/common/compression.h#L21) | [`PgCompressionNone`…`PgCompressionZSTD`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/utils.go) | Ordinal values of the compression algorithm byte written since format 1.15. |

### Header read/write

| Upstream | Greenmask counterpart | What to check |
|---|---|---|
| [`WriteHead()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_archiver.c#L4158) | [`Writer.writeHeader`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/writer.go) | Exact field order: magic `PGDMP`, major, minor, rev, `intSize`, `offSize`, format, compression algorithm, seven `tm` ints, db name, remote version, dump version. |
| [`ReadHead()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_archiver.c#L4184) | [`Reader.readHeader`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/reader.go) | Same order, plus the version gates (`>= 1.7` offset size, `>= 1.15` compression algorithm, `>= 1.4` timestamp and db name, `>= 1.10` version strings). |

### TOC entry read/write

| Upstream | Greenmask counterpart | What to check |
|---|---|---|
| [`WriteToc()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_archiver.c#L2611) | [`Writer.writeEntries`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/writer.go) | Per-entry field order: `dumpId`, `hadDumper`, tableoid, oid, tag, desc, section, defn, dropStmt, copyStmt, namespace, tablespace, tableam, **relkind**, owner, literal `"false"`, dependency list, `NULL` terminator, then the format's extra TOC. |
| [`ReadToc()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_archiver.c#L2709) | [`Reader.readEntries`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/reader.go) | Same order and the same version gates (`>= 1.8` tableoid, `>= 1.11` section, `>= 1.3` copyStmt, `>= 1.6` namespace, `>= 1.10` tablespace, `>= 1.14` tableam, `>= 1.16` relkind, `>= 1.5` dependencies). |
| [`ArchiveEntry()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_archiver.c#L1242) | [`Entry` struct](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/entry.go) | New members on `_tocEntry` are the early warning that a format bump is coming. |

### Primitive encoders

| Upstream | Greenmask counterpart | What to check |
|---|---|---|
| [`WriteInt()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_archiver.c#L2135) / [`ReadInt()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_archiver.c#L2166) | [`writeInt`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/writer.go) / [`readInt`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/reader.go) | Sign byte then `intSize` little-endian bytes. Only `intSize == 4` is supported by our port. |
| [`WriteStr()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_archiver.c#L2193) / [`ReadStr()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_archiver.c#L2212) | [`writeStr`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/writer.go) / [`readStr`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/reader.go) | Length-prefixed, `-1` encodes `NULL`. |
| [`WriteOffset()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_archiver.c#L2054) / [`ReadOffset()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_archiver.c#L2071) | not ported | Only used by the custom format. If we ever support `-Fc`, this is required. |

### Directory format specifics

| Upstream | Greenmask counterpart | What to check |
|---|---|---|
| [`_CloseArchive()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_directory.c#L531) | format check in [`Reader.readHeader`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/reader.go) | The directory format deliberately writes `archTar` (3) into the format byte of `toc.dat`. Our reader rejects anything else. |
| [`_ArchiveEntry()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_directory.c#L198) | [`entries/table.go`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/entries/table.go), [`entries/large_object.go`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/entries/large_object.go) | Data file naming: `%d.dat` for table data, `blobs_%d.toc` for large objects. |
| [`_WriteExtraToc()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_directory.c#L228) / [`_ReadExtraToc()`](https://github.com/postgres/postgres/blob/REL_19_BETA3/src/bin/pg_dump/pg_backup_directory.c#L249) | trailing `FileName` in [`writeEntries`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/writer.go) / [`readEntries`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/toc/reader.go) | For the directory format the "extra TOC" is a single string — the data file name. If upstream ever writes more than one field here, our entry loop desynchronises. |

### Regression coverage

`tests/integration/greenmask/toc_readwriter_test.go` round-trips a real `toc.dat` produced by the
version under test. It is the practical guard for everything in this section — if the layout changed
and we did not follow, this test fails against the new version.

---

## Adding a new major version

### Step 1: Run the pre-release checklist against the new branch

Steps 1–7 above, diffing the new tag against the newest currently supported one. Fix whatever it
surfaces before touching the test matrix.

### Step 2: Add the database service

In `docker-compose-integration.yml`:

```yaml
db-19:
  profiles: ["pg19", "all"]
  volumes:
    - "/var/lib/postgresql/19/data"
  image: postgres:19beta3
  ports:
    - "54319:5432"
  restart: always
  environment:
    POSTGRES_PASSWORD: example
  healthcheck:
    test: ["CMD", "psql", "-U", "postgres"]
    interval: 5s
    timeout: 1s
    retries: 3
```

Port convention is `543<major>`. Beta releases are published as `postgres:19beta3`; switch to
`postgres:19` at GA.

### Step 3: Wire the new profile into the shared services

In the same file, add `"pg19"` to the `profiles` list of `storage`, `test-dbs-filler`, and `greenmask`,
add the `db-19` dependency to `test-dbs-filler`, and extend `PG_VERSIONS_CHECK` on both
`test-dbs-filler` and `greenmask`:

```yaml
test-dbs-filler:
  profiles: ["pg14", "pg15", "pg16", "pg17", "pg18", "pg19", "all"]
  environment:
    PG_VERSIONS_CHECK: "14,15,16,17,18,19"
  depends_on:
    db-19:
      condition: service_healthy
      required: false

greenmask:
  profiles: ["pg14", "pg15", "pg16", "pg17", "pg18", "pg19", "all"]
  environment:
    PG_VERSIONS_CHECK: "14,15,16,17,18,19"
```

`filldb.sh` needs no change — it derives the host name `db-$pgver` from `PG_VERSIONS_CHECK`.

### Step 4: Install the client binaries in the test image

In `docker/integration/tests/Dockerfile`, add `postgresql-19` to the apt install list. Greenmask
resolves `pg_dump`/`pg_restore` from `/usr/lib/postgresql/${pg_version}/bin/`, so the package must be
present for the version to be testable.

Beta packages are **not** in the default PGDG repository — they live in `-pgdg-testing`. Until GA, add:

```dockerfile
RUN echo "deb https://apt.postgresql.org/pub/repos/apt $(lsb_release -sc)-pgdg-testing main 19" \
      > /etc/apt/sources.list.d/pgdg-testing.list
```

### Step 5: Run the tests for the new version alone

```bash
docker compose -f docker-compose-integration.yml --profile pg19 up
```

### Step 6: Update this document and the release notes

Add the version to the support table with its upstream EOL date, and record any behavioural caveat
found in step 7 of the checklist.

---

## Dropping an EOL version

Do this at the first release after the upstream EOL date.

1. Remove the `db-<major>` service from `docker-compose-integration.yml`.
2. Remove `pg<major>` from every `profiles` list and from both `PG_VERSIONS_CHECK` variables.
3. Remove `postgresql-<major>` from `docker/integration/tests/Dockerfile`.
4. Remove the row from the support table in this document.
5. Remove now-dead version gates. Introspection queries are templated on `server_version_num`
   (`{{ if ge .Version NNNNNN }}` in [`internal/db/postgres/context/queries.go`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/context/queries.go)
   and the `version >=` checks in
   [`tables_introspection.go`](https://github.com/GreenmaskIO/greenmask/blob/main/internal/db/postgres/context/tables_introspection.go)); gates below
   the new floor can go.
6. Note the drop in the release notes.

---

## Historical reference

Commits that show the shape of a typical version bump:

- [PostgreSQL 17 support](https://github.com/GreenmaskIO/greenmask/commit/194a08dc7f10d7a44e37919a706a36cfa8e9d3c6)
- [PostgreSQL 18 support](https://github.com/GreenmaskIO/greenmask/commit/3717afcfbb71f6bcd9f9820cfeda72d0507b2d4c)
- [PostgreSQL 18 sequence privileges](https://github.com/GreenmaskIO/greenmask/pull/471) — an example
  of a behaviour change found by step 6 of the checklist rather than by a format diff

The main integration test is
[`tests/integration/greenmask/backward_compatibility_test.go`](https://github.com/GreenmaskIO/greenmask/blob/main/tests/integration/greenmask/backward_compatibility_test.go).
