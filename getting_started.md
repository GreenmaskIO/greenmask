# Getting started

## Installation

**Requirements**:

* Preinstalled PostgreSQL utils with **the same major** version as a destination server
* `make` utility if building from source

From source:

* Clone greenmask repository `git clone git@github.com:GreenmaskIO/greenmask.git`
* Run `make build`
* You will find the binary called `greenmask` in the repository root

## Commands

`greenmask --log-format=[json|text] --log-level=[debug|info|error] --config=config.yml [dump | list-dumps | delete | list-transformers | restore | show-dump | validate]`

* list-transformers - list of the allowed transformers with documentation
* validate - perform validation procedure and data diff of transformation
* dump - perform a logical dump, transform data, and store it in storage
* list-dumps - list all dumps in the storage
* show-dump - shows metadata info about the dump (the same as pg_restore -l ./)
* restore - restore dump with ID or the latest to the target database
* delete - delete dump from the storage with a specific ID

## Building config

### Initialize demo database

Create two test databases

`psql -U postgres -c 'CREATE DATABASE testdb'`

`psql -U postgres -c 'CREATE DATABASE testdb_transformed'`

Initialize with test data

`psql -U postgres -d testdb -f tests/demodb/dump.sql`

After that, the `testdb` has table with structure:

```
                        Table "public.flights"
  Column   |           Type           | Collation | Nullable | Default 
-----------+--------------------------+-----------+----------+---------
 id        | integer                  |           | not null | 
 flight_no | text                     |           | not null | 
 departure | timestamp with time zone |           | not null | 
 arrival   | timestamp with time zone |           | not null | 
Indexes:
    "flights_pkey" PRIMARY KEY, btree (id)
    "flights_flight_no_key" UNIQUE CONSTRAINT, btree (flight_no)
Check constraints:
    "flights_check" CHECK (arrival > departure)

```

### Building simple config

The list of the currently available transformers can be received using the command

`./greenmask --config config.yml list-transformers`

![img.png](docs/resources/list-transformers-example.png)

For building config, you should fill in all the required attributes such as:

* common
* storage
* dump
* restore

The minimal config example:

```yaml
common:
  pg_bin_path: "/usr/bin/"
  tmp_dir: "/tmp"

storage:
  directory:
    path: "/tmp/pg_dump_test"

validate:
#  resolved_warnings:
#    - "8d436fae67b2b82b36bd3afeb0c93f30"

dump:
  pg_dump_options: # pg_dump option that will be provided
    dbname: "host=/run/postgresql user=postgres dbname=testdb"
    jobs: 10

  transformation: # List of tables to transform
    - schema: "public" # Table schema
      name: "flights"  # Table name
      # columns_type_override: # type of column that was overridden. It is required when the transformer is not working with column type
      #   post_code: "int4"
      transformers: # List of transformers to apply
        - name: "RandomDate" # name of transformers
          params: # Transformer parameters
            min: "2023-01-01 00:00:00.0+03"
            max: "2023-01-02 00:00:00.0+03"
            column: "departure" # Column parameter - this transformer affects scheduled_departure column

restore:
  pg_restore_options: # pg_restore option (you can use the same options as pg_restore has)
    jobs: 10
    dbname: "host=/run/postgresql user=postgres dbname=testdb_transformed"

  scripts: # List of scripts to apply after or before each section restoration
    pre-data: # Dump section name - One of [pre-data, data, post-data]
      - name: "pre-flight test script [1]" # Name if script
        when: "before" # When condition - One of [before, after] #
        query: "create table script_test(stage text)" # - query
        # query_file: "pre-data-after.sql" # path to SQL file with query to apply
        # command: test.sh # Path to executable cmd

```

### Run validation procedure

Use the command below to run a validation procedure

`./greenmask --config demo-config.yml validate --data --diff --format vertical --rows-limit=2`

Validation result:

![img.png](docs/resources/validate-result.png)

There is one warning, let's discover it:

```yaml
{
  "hash": "8d436fae67b2b82b36bd3afeb0c93f30",
  "meta": {
    "ColumnName": "departure",
    "ConstraintDef": "CHECK (arrival > departure)",
    "ConstraintName": "public",
    "ConstraintSchema": "public",
    "ConstraintType": "Check",
    "ParameterName": "column",
    "SchemaName": "public",
    "TableName": "flights",
    "TransformerName": "RandomDate"
  },
  "msg": "possible constraint violation: column has Check constraint",
  "severity": "warning"
}
```

The validation warnings contain:

* hash - the unique value of each validation warning that could be used for excluding this validation warning later.
  For instance - if you've made a transformation config and think that all warnings are resolved, you can add this
  hash into config `validate.resolved_warnings`
* meta - contains all required fields that allow you to determine the place in config or the potentially violated
  constraint
* msg - a detailed message that describes the warning reason
* severity - the severity of the warning. Possible warning or error. In error the *Greenmask* exited immediately with
  non-zero
  exit code

The next part of the validation procedure - is to check the difference before the transformation and after.
It is printed pretty in table format. The red column background means that the column is affected. The green value is
original value before transformation the red is the value after transformation.

If you want to exclude the warning from the next runs uncomment the `resolved_warning` attribute in the file

```yaml
validate:
  resolved_warnings:
    - "8d436fae67b2b82b36bd3afeb0c93f30"
```

And then this warning will not be shown in the next validate runs `./greenmask --config demo-config.yml validate`

### Dumping procedure

To perform the dump procedure you need to call

`./greenmask --config demo-config.yml dump`

Once the dump is completed the dump with an appropriate ID will be found in the storage

`./greenmask --config demo-config.yml list-dumps`
![img.png](docs/resources/list-dumps.png)

You can check the data that is going to be restored via `show dump command` for doing that provide you `dumpId` in the
call.

```shell
$ ./greenmask --config demo-config.yml show-dump 1701263538549
;
; Archive created at 2023-11-29 15:12:18 UTC
;     dbname: testdb
;     TOC Entries: 8
;     Compression: -1
;     Dump Version: 15.4
;     Format: DIRECTORY
;     Integer: 4 bytes
;     Offset: 8 bytes
;     Dumped from database version: 15.4
;     Dumped by pg_dump version: 15.4
;
;
; Selected TOC Entries:
;
3345; 0 0 ENCODING - ENCODING 
3346; 0 0 STDSTRINGS - STDSTRINGS 
3347; 0 0 SEARCHPATH - SEARCHPATH 
3348; 1262 44794 DATABASE - testdb postgres
214; 1259 44806 TABLE public flights postgres
3350; 0 44806 TABLE DATA public flights postgres
3198; 2606 44815 CONSTRAINT public flights flights_flight_no_key postgres
3200; 2606 44813 CONSTRAINT public flights flights_pkey postgres
```

### Restoration

For restoring data to the target database you can simply call `restore` command with an appropriate `dumpId` or restore
latest using the reserved word `latest`.

`./greenmask --config demo-config.yml restore latest`

Let's check the restored data

`psql -U postgres -d testdb_transformed -c 'select * from flights limit 2;'`

```
 id | flight_no |           departure           |            arrival            
----+-----------+-------------------------------+-------------------------------
  1 | ABCD1     | 2023-01-01 17:38:38.392952+02 | 2023-11-29 14:47:19.274407+02
  2 | ABCD2     | 2023-01-01 09:50:39.762323+02 | 2023-11-29 14:47:19.274407+02
```

### Delete dump

For deleting dump from the storage apply `delete` command with an appropriate `dumpId`

`./greenmask --config demo-config.yml delete 1701263538549`

The result

`./greenmask --config demo-config.yml list-dumps`

```
+----+------+----------+------+-----------------+----------+-------------+--------+
| ID | DATE | DATABASE | SIZE | COMPRESSED SIZE | DURATION | TRANSFORMED | STATUS |
+----+------+----------+------+-----------------+----------+-------------+--------+
+----+------+----------+------+-----------------+----------+-------------+--------+
```

## Conclusion

It is a simple case of usage. Greenmask has a variety of transformation cases if you want to make a deep dive refer to
the documentation
