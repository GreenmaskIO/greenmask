## restore command

To perform a dump restoration with the provided dump ID, use the following command:

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
Flags:
  -c, --clean                           clean (drop) database objects before recreating
  -C, --create                          create the target database
  -a, --data-only                       restore only the data, no schema
  -d, --dbname string                   connect to database name (default "postgres")
      --disable-triggers                disable triggers during data-only restore
      --enable-row-security             enable row security
  -N, --exclude-schema strings          do not restore objects in this schema
  -e, --exit-on-error                   exit on error, default is to continue
  -f, --file string                     output file name (- for stdout)
  -P, --function strings                restore named function
  -h, --host string                     database server host or socket directory (default "/var/run/postgres")
      --if-exists                       use IF EXISTS when dropping objects
  -i, --index strings                   restore named index
      --inserts                         restore data as INSERT commands, rather than COPY
  -j, --jobs int                        use this many parallel jobs to restore (default 1)
      --list-format string              use table of contents in format of text, json or yaml (default "text")
      --no-comments                     do not restore comments
      --no-data-for-failed-tables       do not restore data of tables that could not be created
  -O, --no-owner string                 skip restoration of object ownership
  -X, --no-privileges                   skip restoration of access privileges (grant/revoke)
      --no-publications                 do not restore publications
      --no-security-labels              do not restore security labels
      --no-subscriptions                ddo not restore subscriptions
      --no-table-access-method          do not restore table access methods
      --no-tablespaces                  do not restore tablespace assignments
      --on-conflict-do-nothing          add ON CONFLICT DO NOTHING to INSERT commands
  -p, --port int                        database server port number (default 5432)
      --restore-in-order                restore tables in topological order, ensuring that dependent tables are not restored until the tables they depend on have been restored
  -n, --schema strings                  restore only objects in this schema
  -s, --schema-only                     restore only the schema, no data
      --section string                  restore named section (pre-data, data, or post-data)
  -1, --single-transaction              restore as a single transaction
      --strict-names                    restore named section (pre-data, data, or post-data) match at least one entity each
  -S, --superuser string                superuser user name to use for disabling triggers
  -t, --table strings                   restore named relation (table, view, etc.)
  -T, --trigger strings                 restore named trigger
  -L, --use-list string                 use table of contents from this file for selecting/ordering output
      --use-set-session-authorization   use SET SESSION AUTHORIZATION commands instead of ALTER OWNER commands to set ownership
  -U, --username string                 connect as specified database user (default "postgres")
  -v, --verbose string                  verbose mode
```

## Extra features

### Inserts and error handling

!!! warning

    Insert commands are a lot slower than `COPY` commands. Use this feature only when necessary.

By default, Greenmask restores data using the `COPY` command. If you prefer to restore data using `INSERT` commands, you can
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

```shell title="example with inserts and on conflict do nothing"
greenmask --config=config.yml restore DUMP_ID --inserts --on-conflict-do-nothing
```

### Restoration in topological order

By default, Greenmask restores tables in the order they are listed in the dump file. To restore tables in topological
order, use the `--restore-in-order` flag. This is particularly useful when your schema includes foreign key references and
you need to insert data in the correct order. Without this flag, you may encounter errors when inserting data into
tables with foreign key constraints.

!!! warning

    Greenmask cannot guarantee restoration in topological order when the schema contains cycles. The only way to restore
    tables with cyclic dependencies is to temporarily remove the foreign key constraint (to break the cycle), restore the
    data, and then re-add the foreign key constraint once the data restoration is complete.


If your database has cyclic dependencies you will be notified about it but the restoration will continue.

```text
2024-08-16T21:39:50+03:00 WRN cycle between tables is detected: cannot guarantee the order of restoration within cycle cycle=["public.employees","public.departments","public.projects","public.employees"]
```
