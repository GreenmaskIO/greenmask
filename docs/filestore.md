# Filestore configuration

Greenmask can optionally dump and restore a filestore alongside database data. Instead of maintaining a separate script to archive binary files, you can reuse the same database connection and storage configuration (for example, S3 settings) to back up and restore binaries together with the database, without extra scripts or duplicate credential management. You can also limit the filestore to an explicit file list to apply "anonymization by reduction": after restore, a post-restore script can replace selected binary references with a placeholder.

## `dump.filestore` section

In the `dump` section, `filestore` controls how a filesystem directory is packaged and uploaded to storage.

### Parameters

* `enabled` — enables or disables filestore dumping.
* `root_path` — **required**. Root directory of the filestore on the source filesystem.
* `include_list_file` — path to a file containing relative paths to include. Each line is a path relative to `root_path`.
* `include_list_query` — SQL query (inline) that returns a list of relative paths to include.
* `include_list_query_file` — path to a file with the SQL query to execute.
* `subdir` — storage subdirectory for filestore artifacts. Default: `filestore`.
* `archive_name` — name of the tar archive produced. Default: `filestore.tar.gz`.
* `metadata_name` — name of the metadata JSON file. Default: `filestore.json`.
* `use_pgzip` — optionally overrides the default compression behavior. Inherits dump `--pgzip`, which is `false` by default.
* `fail_on_missing` — if true, missing files cause the dump to fail. Default: `false`.
* `split.max_size_bytes` — enables archive splitting by maximum size. Default: `0` (disabled).
* `split.max_files` — enables archive splitting by maximum number of files. Default: `0` (disabled).

!!! note
    Filestore archives and metadata are uploaded into the configured storage under the `subdir` path.

!!! warning
    Only one include-list source can be configured at a time:
    `include_list_file` **or** `include_list_query` **or** `include_list_query_file`.
    If no include list is configured, all files under `root_path` are included recursively.

### Example

```yaml title="filestore dump config example"
dump:
  filestore:
    enabled: true
    root_path: "/var/lib/odoo/filestore"
    subdir: "filestore"
    archive_name: "filestore.tar.gz"
    metadata_name: "filestore.json"

    # choose exactly one source of paths:
    include_list_file: "/etc/greenmask/filestore-files.txt"
    # include_list_query: "SELECT DISTINCT store_fname FROM ir_attachment WHERE mimetype != 'application/pdf'"
    # include_list_query_file: "/etc/greenmask/filestore_query.sql"

    fail_on_missing: true
    use_pgzip: true

    split:
      max_size_bytes: 1073741824   # 1 GiB
      max_files: 100000
```

## `restore.filestore` section

In the `restore` section, `filestore` controls how stored filestore archives are fetched and unpacked.

### Parameters

* `enabled` — enables or disables filestore restoration.
* `target_path` — **required**. Destination directory on the target filesystem.
* `subdir` — storage subdirectory where filestore artifacts are stored. Default: `filestore`.
* `metadata_name` — metadata file name. Default: `filestore.json`.
* `use_pgzip` — optionally overrides compression behavior from metadata.
* `clean_target` — if true, removes the target directory before extraction. Default: `false`.
* `skip_existing` — if true, existing files are left untouched. Default: `false`.

!!! note
    If `use_pgzip` is set, it overrides the `use_pgzip` value stored in the filestore metadata.

!!! warning
    If `clean_target` is enabled, the entire `target_path` directory will be removed before restore.

### Example

```yaml title="filestore restore config example"
restore:
  filestore:
    enabled: true
    target_path: "/var/lib/odoo/filestore"
    subdir: "filestore"
    metadata_name: "filestore.json"
    clean_target: false
    skip_existing: true
    use_pgzip: true
```

## Include list sources

When dumping a filestore, you can limit which files are packed using one of the include-list mechanisms:

* **File list** (`include_list_file`) — a text file with one relative path per line.
* **SQL query** (`include_list_query` / `include_list_query_file`) — a query that returns relative paths.

All paths are resolved relative to `root_path`.

!!! tip
    Use an SQL query when paths are stored in the database and you want the filestore selection to follow the dataset.

### Why include lists are useful

Restricting the filestore to an explicit list lets you implement "anonymization by reduction". Instead of copying
all binaries, you can keep only the necessary files and then use a post-restore script to replace references to
missing binaries with a placeholder or a generic asset (for example, `invoice_placeholder.pdf`). This approach
reduces storage, shortens transfer time, and keeps access credentials and storage handling centralized in Greenmask.

### Odoo example query

The following Odoo query excludes all PDF and ZIP attachments from the filestore selection:

```sql
SELECT DISTINCT store_fname
FROM ir_attachment
WHERE store_fname IS NOT NULL
  AND (NOT ((COALESCE(mimetype, '') = 'application/pdf') OR (COALESCE(mimetype, '') = 'application/zip')))
ORDER BY store_fname
```

## Archive splitting

If `split.max_size_bytes` or `split.max_files` is set, the filestore is split into multiple archives. Each archive is
stored separately, and metadata contains the archive list and statistics.

Splitting is useful when:
* individual archives must stay below storage limits,
* large filestores should be processed in smaller parts.

Splitting is not tied to `jobs`: filestore dump/restore does not use multi-threaded workers and processes archives sequentially.
