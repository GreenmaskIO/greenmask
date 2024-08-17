## dump command

The `dump` command operates in the following way:

1. Dumps the data from the source database.
2. Validates the data for potential issues.
3. Applies the defined transformations.
4. Stores the transformed data in the specified storage location.

Note that the `dump` command shares the same parameters and environment variables as `pg_dump`,
allowing you to configure the restoration process as needed.

Mostly it supports the same flags as the `pg_dump` utility, with some extra flags for Greenmask-specific features.

```text title="Supported flags"
  -b, --blobs                           include large objects in dump
  -c, --clean                           clean (drop) database objects before recreating
  -Z, --compress int                    compression level for compressed formats (default -1)
  -C, --create                          include commands to create database in dump
  -a, --data-only                       dump only the data, not the schema
  -d, --dbname string                   database to dump (default "postgres")
      --disable-dollar-quoting          disable dollar quoting, use SQL standard quoting
      --disable-triggers                disable triggers during data-only restore
      --enable-row-security             enable row security (dump only content user has access to)
  -E, --encoding string                 dump the data in encoding ENCODING
  -N, --exclude-schema strings          dump the specified schema(s) only
  -T, --exclude-table strings           do NOT dump the specified table(s)
      --exclude-table-data strings      do NOT dump data for the specified table(s)
  -e, --extension strings               dump the specified extension(s) only
      --extra-float-digits string       override default setting for extra_float_digits
  -f, --file string                     output file or directory name
  -h, --host string                     database server host or socket directory (default "/var/run/postgres")
      --if-exists                       use IF EXISTS when dropping objects
      --include-foreign-data strings    use IF EXISTS when dropping objects
  -j, --jobs int                        use this many parallel jobs to dump (default 1)
      --load-via-partition-root         load partitions via the root table
      --lock-wait-timeout int           fail after waiting TIMEOUT for a table lock (default -1)
  -B, --no-blobs                        exclude large objects in dump
      --no-comments                     do not dump comments
  -O, --no-owner string                 skip restoration of object ownership in plain-text format
  -X, --no-privileges                   do not dump privileges (grant/revoke)
      --no-publications                 do not dump publications
      --no-security-labels              do not dump security label assignments
      --no-subscriptions                do not dump subscriptions
      --no-sync                         do not wait for changes to be written safely to dis
      --no-synchronized-snapshots       do not use synchronized snapshots in parallel jobs
      --no-tablespaces                  do not dump tablespace assignments
      --no-toast-compression            do not dump TOAST compression methods
      --no-unlogged-table-data          do not dump unlogged table data
      --pgzip                           use pgzip compression instead of gzip
  -p, --port int                        database server port number (default 5432)
      --quote-all-identifiers           quote all identifiers, even if not key words
  -n, --schema strings                  dump the specified schema(s) only
  -s, --schema-only string              dump only the schema, no data
      --section string                  dump named section (pre-data, data, or post-data)
      --serializable-deferrable         wait until the dump can run without anomalies
      --snapshot string                 use given snapshot for the dump
      --strict-names                    require table and/or schema include patterns to match at least one entity each
  -S, --superuser string                superuser user name to use in plain-text format
  -t, --table strings                   dump the specified table(s) only
      --test string                     connect as specified database user (default "postgres")
      --use-set-session-authorization   use SET SESSION AUTHORIZATION commands instead of ALTER OWNER commands to set ownership
  -U, --username string                 connect as specified database user (default "postgres")
  -v, --verbose string                  verbose mode
```

### Pgzip compression

By default, Greenmask uses gzip compression to restore data. In mist cases it is quite slow and does not utilize all
available resources and is a bootleneck for IO operations. To speed up the restoration process, you can use
the `--pgzip` flag to use pgzip compression instead of gzip. This method splits the data into blocks, which are
compressed in parallel, making it ideal for handling large volumes of data. The output remains a standard gzip file.
