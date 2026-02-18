    # Configuration

The configuration is organized into six sections:

* `common` — settings that can be used for both the `dump` and `restore` commands
* `log` — settings for the logging subsystem
* `storage` — settings for the storage locations where dumps are stored
* `dump` — settings for the `dump` command. This section includes `pg_dump` options and transformation parameters.
* `restore` — settings for the `restore` command. It contains `pg_restore` options and additional restoration
  scripts.
* `custom_transformers` — definitions of the custom transformers that interact through `stdin` and `stdout`. Once a custom transformer is configured, it becomes accessible via the `greenmask list-transformers` command.

## `common` section

In the `common` section of the configuration, you can specify the following settings:

* `pg_bin_path` — path to the PostgreSQL binaries. Note that the PostgreSQL server version must match the provided binaries.
* `tmp_dir` — temporary directory for storing the table of contents files. Default value is `/tmp`

!!! note

    Greenmask exclusively manages data dumping and data restoration processes, delegating schema dumping to the `pg_dump `utility and schema restoration to the `pg_restore` utility. Both `pg_dump` and `pg_restore` rely on a `toc.dat` file located in a specific directory, which contains metadata and object definitions. Therefore, the `tmp_dir` parameter is essential for storing the `toc.dat` file during the dumping or restoration procedure. It is important to note that all artifacts in this directory will be automatically deleted once the Greenmask command is completed.

## `log` section

In the `log` section of the configuration, you can specify the following settings:

* `level` — specifies the level of logging, which can be one of the following: `debug`, `info`, or `warn`. The default level is `info`.
* `format` — defines the logging format, which can be either `json` or `text`. The default format is `text`.

## `storage` section

In the `storage` section, you can configure the storage driver for storing the dumped data. Currently,
two storage `type` options are supported: `directory` and `s3`.

=== "`directory` option"

    The directory storage option refers to a filesystem directory where the dump data will be stored.

    Parameters include `path` which specifies the path to the directory in the filesystem where the dumps will be stored.

    ``` yaml title="directory storage config example"
    storage:
      type: "directory"
      directory:
        path: "/home/user_name/storage_dir" # (1)
    ```

=== "`s3` option"

    By choosing the `s3` storage option, you can store dump data in an S3-like remote storage service,
    such as Amazon S3 or Azure Blob Storage. Here are the parameters you can configure for S3 storage:

    * `endpoint` — overrides the default AWS endpoint to a custom one for making requests
    * `bucket` — the name of the bucket where the dump data will be stored
    * `prefix` — a prefix for objects in the bucket, specified in path format
    * `region` — the S3 service region
    * `storage_class` — the storage class for performing object requests
    * `no_verify_ssl` — disable SSL certificate verification
    * `access_key_id` — access key for authentication
    * `secret_access_key` — secret access key for authentication
    * `session_token` — session token for authentication
    * `role_arn` — Amazon resource name for role-based authentication
    * `session_name` — role session name to uniquely identify a session
    * `max_retries` — the number of retries on request failures
    * `cert_file` — the path to the SSL certificate for making requests
    * `max_part_size` — the maximum part length for one request
    * `concurrency` — the number of goroutines to use in parallel for each upload call when sending parts
    * `use_list_objects_v1` — use the old v1 `ListObjects` request instead of v2 one
    * `force_path_style` — force the request to use path-style addressing (e. g., `http://s3.amazonaws.com/BUCKET/KEY`) instead of virtual hosted bucket addressing (e. g., `http://BUCKET.s3.amazonaws.com/KEY`)
    * `use_accelerate` — enable S3 Accelerate feature

    ```yaml title="s3 storage config example for Minio running in Docker"
    storage:  
      type: "s3"
      s3:
        endpoint: "http://localhost:9000"
        bucket: "testbucket"
        region: "us-east-1"
        access_key_id: "Q3AM3UQ867SPQQA43P2F"
        secret_access_key: "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
    ```

    !!! tip "Cloudflare R2 Compatibility"

        Greenmask is compatible with Cloudflare R2 storage. To ensure it works correctly, you must use the following configuration:

        *   **Endpoint**: Use the R2 S3 API endpoint (e.g., `https://<account_id>.r2.cloudflarestorage.com`). Do **not** include the bucket name in the endpoint URL.
        *   **Force Path Style**: You must set `force_path_style` to `true`.
        *   **Region**: Set it to `us-east-1` (R2 is regionless, but this value is often required by S3 clients).

        ```yaml title="S3 storage config example for Cloudflare R2"
        storage:
          type: "s3"
          s3:
            endpoint: "https://<account_id>.r2.cloudflarestorage.com"
            bucket: "my-bucket"
            prefix: "backups"
            region: "us-east-1"
            force_path_style: true
            access_key_id: "<access_key>"
            secret_access_key: "<secret_key>"
        ```

## `dump` section

In the `dump` section of the configuration, you configure the `greenmask dump` command. It includes the following parameters:

* `pg_dump_options` — a map of `pg_dump` options to configure the behavior of the command itself. You can refer to the list of supported `pg_dump` options in the [Greenmask dump command documentation](commands/dump.md).
* `transformation` — this section contains configuration for applying transformations to table columns during the dump operation. It includes the following sub-parameters:

    * `schema` — the schema name of the table
    * `name` — the name of the table
    * `subset_conds` - list of the conditions to filter the rows to be dumped. The conditions are combined with `AND` operator. For details read [Database subset](database_subset.md)
    * `query` — an optional parameter for specifying a custom query to be used in the COPY command. By default, the entire table is dumped, but you can use this parameter to set a custom query.
        
        !!! warning

            Be cautious when using the `query` parameter, as it may lead to constraint violation errors during restoration, and Greenmask currently cannot handle query validation.

    * `columns_type_override` — allows you to override the column types explicitly. You can associate a column with another type that is supported by your transformer. This is useful when the transformer works strictly with specific types of columns. For example, if a column named `post_code` is of the TEXT type, but the `RandomInt` transformer works only with INT family types, you can override it as shown in the example provided.
      ``` yaml title="column type overridden example"
        columns_type_override:
          post_code: "int4"  # (1)
      ```
      { .annotate }

           1. Change the data type of the post_code column to `INT4` (`INTEGER`)

    * `apply_for_inherited` — an optional parameter to apply the same transformation to all partitions if the table is partitioned. This can save you from defining the transformation for each partition manually.

        !!! warning

            It is recommended to use the `--load-via-partition-root` parameter when dealing with partitioned tables, as the partition key value might change.

    * `transformers` — a list of transformers to apply to the table, along with their parameters. Each transformation item includes the following sub-parameters:

        * `name` — the name of the transformer
        * `params` — a map of the provided transformer parameters

        ```yaml title="transformers config example"
           transformers:
            - name: "RandomDate"
              params:
                min: "2023-01-01 00:00:00.0+03"
                max: "2023-01-02 00:00:00.0+03"
                column: "scheduled_departure"

            - name: "NoiseDate"
              params:
                ratio: "01:00:00"
                column: "scheduled_arrival"
        ```

Here is an example configuration for the `dump` section:

```yaml title="dump section config example"
dump:
  pg_dump_options:
    dbname: "host=/run/postgresql user=postgres dbname=demo"
    jobs: 10
    exclude-schema: "(\"teSt\"*|test*)"
    table: "bookings.flights"
    load-via-partition-root: true

  transformation:
    - schema: "bookings"
      name: "flights"
      query: "select * from bookings.flights3 limit 1000000"
      columns_type_override:
        post_code: "int4" # (1)
      transformers:
        - name: "RandomDate"
          params:
            min: "2023-01-01 00:00:00.0+03"
            max: "2023-01-02 00:00:00.0+03"
            column: "scheduled_departure"

        - name: "NoiseDate"
          params:
            ratio: "01:00:00"
            column: "scheduled_arrival"

        - name: "RegexpReplace"
          params:
            column: "status"
            regexp: "On Time"
            replace: "Delayed"

        - name: "RandomInt" # (2)
          params:
            column: "post_code"
            min: "11"
            max: "99"

    - schema: "bookings"
      name: "aircrafts_data"
      subset_conds: # (3)
        - "bookings.aircrafts_data.model = 'Boeing 777-300-2023'"
      transformers:
        - name: "Json"
          params:
            column: "model"
            operations:
              - operation: "set"
                path: "en"
                value: "Boeing 777-300-2023"
              - operation: "set"
                path: "crewSize"
                value: 10

        - name: "NoiseInt"
          params:
            ratio: 0.9
            column: "range"
```
{ .annotate }

1. Override the `post_code` column type to `int4` (INTEGER). This is necessary because the `post_code` column
   originally has a `TEXT` type, but it contains values that resemble integers. By explicitly overriding the type to `int4`, we ensure compatibility with transformers that work with integer types, such as `RandomInt`.
2. After the type is overridden, we can apply a compatible transformer.
3. Database subset condition applied to the `aircrafts_data` table. The subset condition filters the data based on the `model` column.

## `validate` section

In the `validate` section of the configuration, you can specify parameters for the `greenmask validate`
command. Here is an example of the validate section configuration:

```yaml title="validate section config example"
validate:
  tables: # (1)
    - "orders"
    - "public.cart"
  data: true # (2)
  diff: true # (3)
  rows_limit: 10 # (4)
  resolved_warnings: # (5)
    - "8d436fae67b2b82b36bd3afeb0c93f30"
  table_format: "horizontal" # (7)
  format: "text" # (6)
  schema: true # (8)
  transformed_only: true # (9)
  warnings: true # (10)
```
{ .annotate }

1. A list of tables to validate. If this list is not empty, the validation operation will only be performed for the specified tables. Tables can be written with or without the schema name (e.g., `"public.cart"` or `"orders"`).
2. Specifies whether to perform data transformation for a limited set of rows. If set to `true`, data transformation will be performed, and the number of rows transformed will be limited to the value specified in the `rows_limit` parameter (default is `10`).
3. Specifies whether to perform diff operations for the transformed data. If set to `true`, the validation process will **find the differences between the original and transformed data**. See more details in the [validate command documentation](commands/validate.md).
4. Limits the number of rows to be transformed during validation. The default limit is `10` rows, but you can change it by modifying this parameter.
5. A hash list of resolved warnings. These warnings have been addressed and resolved in a previous validation run.
6. Specifies the format of the transformation output. Possible values are `[horizontal|vertical]`. The default format is `horizontal`. You can choose the format that suits your needs. See more details in the [validate command documentation](commands/validate.md).
7. The output format (json or text)
8. Specifies whether to validate the current schema with the previous and print the differences if any.
9. If set to `true`, transformation output will be only with the transformed columns and primary keys
10. If set to then all the warnings be printed

## `restore` section

In the `restore` section of the configuration, you can specify parameters for the `greenmask restore` command. It contains `pg_restore` settings and custom script execution settings. Below you can find the available parameters:

* `pg_restore_options` — a map of `pg_restore` options that are used to configure the behavior of
  the `pg_restore` utility during the restoration process. You can refer to the list of supported `pg_restore` options in the [Greenmask restore command documentation](commands/restore.md).
* `scripts` — a map of custom scripts to be executed during different restoration stages. Each script is associated with a specific restoration stage and includes the following attributes:
    * `[pre-data|data|post-data]` — the name of the restoration stage when the script should be executed; has the following parameters:
        * `name` — the name of the script
        * `when` — specifies when to execute the script, which can be either `"before"` or `"after"` the
          specified restoration stage
        * `query` — an SQL query string to be executed
        * `query_file` — the path to an SQL query file to be executed
        * `command` — a command with parameters to be executed. It is provided as a list, where the first item is the command name.
* `insert_error_exclusions` — a list of error codes that should be ignored during the restoration process. This is 
useful when you want to skip specific errors that are not critical for the restoration process.

As mentioned in [the architecture](architecture.md/#backup-process), a backup contains three sections: pre-data, data, and post-data. The custom script execution allows you to customize and control the restoration process by executing scripts or commands at specific stages. The available restoration stages and their corresponding execution conditions are as follows:

* `pre-data` — scripts or commands can be executed before or after restoring the pre-data section
* `data` — scripts or commands can be executed before or after restoring the data section
* `post-data` — scripts or commands can be executed before or after restoring the post-data section

Each stage can have a `"when"` condition with one of the following possible values:

* `before` — execute the script or SQL command before the mentioned restoration stage
* `after` — execute the script or SQL command after the mentioned restoration stage

Below you can find one of the possible versions for the `scripts` part of the `restore` section:

``` yaml title="scripts definition example"
scripts:
  pre-data: # (1)
    - name: "pre-data before script [1] with query"
      when: "before"
      query: "create table script_test(stage text)"
    - name: "pre-data before script [2]"
      when: "before"
      query: "insert into script_test values('pre-data before')"
    - name: "pre-data after test script [1]"
      when: "after"
      query: "insert into script_test values('pre-data after')"
    - name: "pre-data after script with query_file [1]"
      when: "after"
      query_file: "pre-data-after.sql"
  data: # (2)
    - name: "data before script with command [1]"
      when: "before"
      command: # (4)
        - "data-after.sh"
        - "param1"
        - "param2"
    - name: "data after script [1]"
      when: "after"
      query_file: "data-after.sql"
  post-data: # (3)
    - name: "post-data before script [1]"
      when: "before"
      query: "insert into script_test values('post-data before')"
    - name: "post-data after script with query_file [1]"
      when: "after"
      query_file: "post-data-after.sql"
```
{ .annotate }

1. **List of pre-data stage scripts**. This section contains scripts that are executed before or after the restoration of the pre-data section. The scripts include SQL queries and query files.
2. **List of data stage scripts**. This section contains scripts that are executed before or after the restoration of the data section. The scripts include shell commands with parameters and SQL query files.
3. **List of post-data stage scripts**. This section contains scripts that are executed before or after the restoration of the post-data section. The scripts include SQL queries and query files.
4. **Command in the first argument and the parameters in the rest of the list**. When specifying a command to be executed in the scripts section, you provide the command name as the first item in a list, followed by any parameters or arguments for that command. The command and its parameters are provided as a list within the script configuration.

### restoration error exclusion

You can configure which errors to ignore during the restoration process by setting the insert_error_exclusions
parameter. This parameter can be applied globally or per table. If both global and table-specific settings are defined,
the table-specific settings will take precedence. Below is an example of how to configure the insert_error_exclusions
parameter. You can specify constraint names from your database schema or the error codes returned by PostgreSQL.
[codes in the PostgreSQL documentation](https://www.postgresql.org/docs/docs/current/errcodes-appendix.html).

```yaml title="parameter definition"
insert_error_exclusions:

  global:
    error_codes: ["23505"] # (1)
    constraints: ["PK_ProductReview_ProductReviewID"] # (2)
  tables: # (3)
    - schema: "production"
      name: "productreview"
      constraints: ["PK_ProductReview_ProductReviewID"]
      error_codes: ["23505"]

```

1. List of strings that contains postgresql error codes
2. List of strings that contains constraint names (globally)
3. List of tables with their schema, name, constraints, and error codes


Here is an example configuration for the `restore` section:

```yaml
restore:
  scripts:
      pre-data: # (1)
        - name: "pre-data before script [1] with query"
          when: "before"
          query: "create table script_test(stage text)"
  
  insert_error_exclusions:
    tables:
      - schema: "production"
        name: "productreview"
        constraints:
          - "PK_ProductReview_ProductReviewID"
        error_codes:
          - "23505"
    global:
      error_codes:
        - "23505"

  pg_restore_options:
    jobs: 10
    exit-on-error: false
    dbname: "postgresql://postgres:example@localhost:54316/transformed"
    table: 
      - "productreview"
    pgzip: true
    inserts: true
    on-conflict-do-nothing: true
    restore-in-order: true
    
```

## Environment variable configuration

It's also possible to configure Greenmask through environment variables. 

Greenmask will automatically parse any environment variable that matches the configuration in the config file by substituting the dot (`.`) separator for an underscore (`_`) and uppercasing it. As an example, the config file below would apply the same configuration as defining the `LOG_LEVEL=debug` environment variable

```yaml title="config.yaml"
log:
  level: debug
```

### Global configuration variables

* `GREENMASK_GLOBAL_SALT` - global salt value hex encoded with variadic length, used for the `hash` engine. For details
  read [Transformation engines](built_in_transformers/transformation_engines.md) section.

### Postgres connection variables

Additionally, there are some environment variables exposed by the `dump` and `restore` commands to facilitate the connection configuration with a Postgres database

* `PGHOST` - host used to connect to the postgres database
* `PGPORT` - port where postgres is exposed
* `PGDATABASE` - name of the database to dump/restore
* `PGUSER` - username used to connect to the postgres database
* `PGPASSWORD` - password used to authenticate to the postgres database