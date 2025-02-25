## restore command

The `restore` command is used to restore a database from a previously created dump. You can specify the dump to restore
by providing the dump ID or use the `latest` keyword to restore the latest completed dump.

```shell
greenmask --config=config.yml restore DUMP_ID
```

Alternatively, to restore the latest completed dump, use the following command:

```shell
greenmask --config=config.yml restore latest
```

Note that the `restore` command shares the same parameters and environment variables as `pg_restore`,
allowing you to configure the restoration process as needed.

Mostly it supports the same flags as the `pg_restore` utility, with some extra flags for Greenmask-specific features.

```text title="Supported flags"
      --batch-size int                         the number of rows to insert in a single batch during the COPY command (0 - all rows will be inserted in a single batch)
  -c, --clean                                  clean (drop) database objects before recreating
  -C, --create                                 create the target database
  -a, --data-only                              restore only the data, no schema
  -d, --dbname string                          connect to database name (default "postgres")
      --disable-triggers                       disable triggers during data section restore
      --enable-row-security                    enable row security
  -N, --exclude-schema strings                 do not restore objects in this schema
  -e, --exit-on-error                          exit on error, default is to continue
  -f, --file string                            output file name (- for stdout)
  -P, --function strings                       restore named function
  -h, --host string                            database server host or socket directory (default "/var/run/postgres")
      --if-exists                              use IF EXISTS when dropping objects
  -i, --index strings                          restore named index
      --inserts                                restore data as INSERT commands, rather than COPY
  -j, --jobs int                               use this many parallel jobs to restore (default 1)
      --list-format string                     use table of contents in format of text, json or yaml (default "text")
  -B, --no-blobs                               exclude large objects from restoration (large objects will be created as empty placeholders)
      --no-comments                            do not restore comments
      --no-data-for-failed-tables              do not restore data of tables that could not be created
  -O, --no-owner                               skip restoration of object ownership
  -X, --no-privileges                          skip restoration of access privileges (grant/revoke)
      --no-publications                        do not restore publications
      --no-security-labels                     do not restore security labels
      --no-subscriptions                       ddo not restore subscriptions
      --no-table-access-method                 do not restore table access methods
      --no-tablespaces                         do not restore tablespace assignments
      --on-conflict-do-nothing                 add ON CONFLICT DO NOTHING to INSERT commands
      --overriding-system-value                use OVERRIDING SYSTEM VALUE clause for INSERTs
      --pgzip                                  use pgzip decompression instead of gzip
  -p, --port int                               database server port number (default 5432)
      --restore-in-order                       restore tables in topological order, ensuring that dependent tables are not restored until the tables they depend on have been restored
  -n, --schema strings                         restore only objects in this schema
  -s, --schema-only                            restore only the schema, no data
      --section string                         restore named section (pre-data, data, or post-data)
  -1, --single-transaction                     restore as a single transaction
      --strict-names                           restore named section (pre-data, data, or post-data) match at least one entity each
  -S, --superuser string                       superuser user name to use for disabling triggers
  -t, --table strings                          restore named relation (table, view, etc.)
  -T, --trigger strings                        restore named trigger
  -L, --use-list string                        use table of contents from this file for selecting/ordering output
      --use-session-replication-role-replica   use SET session_replication_role = 'replica' to disable triggers during data section restore (alternative for --disable-triggers)
      --use-set-session-authorization          use SET SESSION AUTHORIZATION commands instead of ALTER OWNER commands to set ownership
  -U, --username string                        connect as specified database user (default "postgres")
  -v, --verbose string                         verbose mode
```

## Extra features

### Inserts and error handling

!!! warning

    Insert commands are a lot slower than `COPY` commands. Use this feature only when necessary.

By default, Greenmask restores data using the `COPY` command. If you prefer to restore data using `INSERT` commands, you
can
use the `--inserts` flag. This flag allows you to manage errors that occur during the execution of INSERT commands. By
configuring an error and constraint [exclusion list in the config](../configuration.md#restoration-error-exclusion),
you can skip certain errors and continue inserting subsequent rows from the dump.

This can be useful when adding new records to an existing dump, but you don't want the process to stop if some records
already exist in the database or violate certain constraints.

By adding the `--on-conflict-do-nothing` flag, it generates `INSERT` statements with the ON `CONFLICT DO NOTHING`
clause, similar to the original pg_dump option. However, this approach only works for unique or exclusion constraints.
If a foreign key is missing in the referenced table or any other constraint is violated, the insertion will still fail.
To handle these issues, you can define
an[exclusion list in the config](../configuration.md#restoration-error-exclusion).

```shell title="example with inserts and error handling"

```shell title="example with inserts and on conflict do nothing"
greenmask --config=config.yml restore DUMP_ID --inserts --on-conflict-do-nothing
```

By adding the `--overriding-system-value` flag, it generates `INSERT` statements with the `OVERRIDING SYSTEM VALUE`
clause, which allows you to insert data into identity columns. 

```postgresql title="example of GENERATED ALWAYS AS IDENTITY column"
CREATE TABLE people (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    generated text GENERATED ALWAYS AS (id || first_name) STORED,
    first_name text
);
```

```shell title="example with inserts"
greenmask --config=config.yml restore DUMP_ID --inserts --overriding-system-value
```

### Restoration in topological order

By default, Greenmask restores tables in the order they are listed in the dump file. To restore tables in topological
order, use the `--restore-in-order` flag. This flag ensures that dependent tables are not restored until the tables they
depend on have been restored.

This is useful when you have the schema already created with foreign keys and other constraints, and you want to insert
data into the tables in the correct order or catch-up the target database with the new data.

!!! warning

    Greenmask cannot guarantee restoration in topological order when the schema contains cycles. The only way to restore
    tables with cyclic dependencies is to temporarily remove the foreign key constraint (to break the cycle), restore the
    data, and then re-add the foreign key constraint once the data restoration is complete.

If your database has cyclic dependencies you will be notified about it but the restoration will continue.

```text
2024-08-16T21:39:50+03:00 WRN cycle between tables is detected: cannot guarantee the order of restoration within cycle cycle=["public.employees","public.departments","public.projects","public.employees"]
```

### Pgzip decompression

By default, Greenmask uses gzip decompression to restore data. In mist cases it is quite slow and does not utilize all
available resources and is a bootleneck for IO operations. To speed up the restoration process, you can use
the `--pgzip` flag to use pgzip decompression instead of gzip. This method splits the data into blocks, which are
decompressed in parallel, making it ideal for handling large volumes of data.

```shell title="example with pgzip decompression"
greenmask --config=config.yml restore latest --pgzip
```

### Restore data batching

The COPY command returns the error only on transaction commit. This means that if you have a large dump and an error
occurs, you will have to wait until the end of the transaction to see the error message. To avoid this, you can use the
`--batch-size` flag to specify the number of rows to insert in a single batch during the COPY command. If an error
occurs during the batch insertion, the error message will be displayed immediately. The data will be committed **only 
if all batches are inserted successfully**.

This is useful when you want to be notified of errors as immediately as possible without waiting for the entire
table to be restored.

!!! warning

    The batch size should be chosen carefully. If the batch size is too small, the restoration process will be slow. If
    the batch size is too large, you may not be able to identify the error row.

In the example below, the batch size is set to 1000 rows. This means that 1000 rows will be inserted in a single batch,
so you will be notified of any errors immediately after each batch is inserted.

```shell title="example with batch size" 
greenmask --config=config.yml restore latest --batch-size 1000
```
