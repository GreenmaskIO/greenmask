# validate command

The `validate` command allows you to perform a validation procedure and compare transformed data.

Below is a list of all supported flags for the `validate` command:

```text title="Supported flags"
Usage:
  greenmask validate [flags]

Flags:
      --data                  Perform test dump for --rows-limit rows and print it pretty
      --diff                  Find difference between original and transformed data
      --format string         Format of output. possible values [text|json] (default "text")
      --rows-limit uint       Check tables dump only for specific tables (default 10)
      --schema                Make a schema diff between previous dump and the current state
      --table strings         Check tables dump only for specific tables
      --table-format string   Format of table output (only for --format=text). Possible values [vertical|horizontal] (default "vertical")
      --transformed-only      Print only transformed column and primary key
      --warnings              Print warnings
```

Validate command can exit with non-zero code when:

* Any error occurred
* Validate was called with `--warnings` flag and there are warnings
* Validate was called with `--schema` flag and there are schema differences

All of those cases may be used for CI/CD pipelines to stop the process when something went wrong. This is especially
useful when `--schema` flag is used - this allows to avoid data leakage when schema changed.

You can use the `--table` flag multiple times to specify the tables you want to check. Tables can be written with
or without schema names (e.g., `public.table_name` or `table_name`). If you specify multiple tables from different
schemas, an error will be thrown.

To start validation, use the following command:

```shell
greenmask --config=config.yml validate \
  --warnings \
  --data \
  --diff \
  --schema \
  --format=text \
  --table-format=vertical \
  --transformed-only \
  --rows-limit=1
```

```text title="Validation output example"
2024-03-15T19:46:12+02:00 WRN ValidationWarning={"hash":"aa808fb574a1359c6606e464833feceb","meta":{"ColumnName":"birthdate","ConstraintDef":"CHECK (birthdate \u003e= '1930-01-01'::date AND birthdate \u003c= (now() - '18 years'::interval))","ConstraintName":"humanresources","ConstraintSchema":"humanresources","ConstraintType":"Check","ParameterName":"column","SchemaName":"humanresources","TableName":"employee","TransformerName":"NoiseDate"},"msg":"possible constraint violation: column has Check constraint","severity":"warning"}
```

The validation output will provide detailed information about potential constraint violations and schema issues. Each
line contains nested JSON data under the `ValidationWarning` key, offering insights into the affected part of the
configuration and potential constraint violations.

```json title="Pretty formatted validation warning"
{ 
  "hash": "aa808fb574a1359c6606e464833feceb", // (13)
  "meta": { // (1)
    "ColumnName": "birthdate", // (2)
    "ConstraintDef": "CHECK (birthdate >= '1930-01-01'::date AND birthdate <= (now() - '18 years'::interval))", // (3)
    "ConstraintName": "humanresources", // (4)
    "ConstraintSchema": "humanresources", // (5)
    "ConstraintType": "Check", // (6)
    "ParameterName": "column", // (7)
    "SchemaName": "humanresources", // (8)
    "TableName": "employee", // (9)
    "TransformerName": "NoiseDate" // (10)
  },
  "msg": "possible constraint violation: column has Check constraint", // (11)
  "severity": "warning" // (12)
}
```

1. **Detailed metadata**. The validation output provides comprehensive metadata to pinpoint the source of problems.
2. **Column name** indicates the name of the affected column.
3. **Constraint definition** specifies the definition of the constraint that may be violated.
4. **Constraint name** identifies the name of the constraint that is potentially violated.
5. **Constraint schema name** indicates the schema in which the constraint is defined.
6. **Type of constraint** represents the type of constraint and can be one of the following:
   ```
   * ForeignKey
   * Check
   * NotNull
   * PrimaryKey
   * PrimaryKeyReferences
   * Unique
   * Length
   * Exclusion
   * TriggerConstraint
   ```
7. **Table schema name** specifies the schema name of the affected table.
8. **Table name** identifies the name of the table where the problem occurs.
9. **Transformer name** indicates the name of the transformer responsible for the transformation.
10. **Name of affected parameter** typically, this is the name of the column parameter that is relevant to the
    validation warning.
11. **Validation warning description** provides a detailed description of the validation warning and the reason behind
    it.
12. **Severity of validation warning** indicates the severity level of the validation warning and can be one of the
    following:
    ```
    * error
  	* warning
  	* info
  	* debug
    ```
13. **Hash** is a unique identifier of the validation warning. It is used to resolve the warning in the config file

:::note

A validation warning with a severity level of `"error"` is considered critical and must be addressed before the dump operation can proceed. Failure to resolve such warnings will prevent the dump operation from being executed.

:::
```text title="Schema diff changed output example"
2024-03-15T19:46:12+02:00 WRN Database schema has been changed Hint="Check schema changes before making new dump" PreviousDumpId=1710520855501
2024-03-15T19:46:12+02:00 WRN Column renamed Event=ColumnRenamed Signature={"CurrentColumnName":"id1","PreviousColumnName":"id","TableName":"test","TableSchema":"public"}
2024-03-15T19:46:12+02:00 WRN Column type changed Event=ColumnTypeChanged Signature={"ColumnName":"id","CurrentColumnType":"bigint","CurrentColumnTypeOid":"20","PreviousColumnType":"integer","PreviousColumnTypeOid":"23","TableName":"test","TableSchema":"public"}
2024-03-15T19:46:12+02:00 WRN Column created Event=ColumnCreated Signature={"ColumnName":"name","ColumnType":"text","TableName":"test","TableSchema":"public"}
2024-03-15T19:46:12+02:00 WRN Table created Event=TableCreated Signature={"SchemaName":"public","TableName":"test1","TableOid":"20563"}
```

Example of validation diff:

![img.png](../assets/validate_horizontal_diff.png)

The validation diff is presented in a neatly formatted table. In this table:

* Columns that are affected by the transformation are highlighted with a red background.
* The pre-transformation values are displayed in green.
* The post-transformation values are shown in red.
* The result in `--format=text` can be displayed in either horizontal (`--table-format=horizontal`) or 
  vertical (`--table-format=vertical`) format, making it easy to visualize and understand the 
  differences between the original and transformed data.

The whole validate command may be run in json format including logging making easy to parse the structure. 

```shell
greenmask --config=config.yml validate \
  --warnings \
  --data \
  --diff \
  --schema \
  --format=json \
  --table-format=vertical \
  --transformed-only \
  --rows-limit=1 \
  --log-format=json
```

The json object result

**The validation warning**

```json
{
  "level": "warn",
  "ValidationWarning": {
    "msg": "possible constraint violation: column has Check constraint",
    "severity": "warning",
    "meta": {
      "ColumnName": "birthdate",
      "ConstraintDef": "CHECK (birthdate >= '1930-01-01'::date AND birthdate <= (now() - '18 years'::interval))",
      "ConstraintName": "humanresources",
      "ConstraintSchema": "humanresources",
      "ConstraintType": "Check",
      "ParameterName": "column",
      "SchemaName": "humanresources",
      "TableName": "employee",
      "TransformerName": "NoiseDate"
    },
    "hash": "aa808fb574a1359c6606e464833feceb"
  },
  "time": "2024-03-15T20:01:51+02:00"
}
```

**Schema diff events**

```json
{
  "level": "warn",
  "PreviousDumpId": "1710520855501",
  "Diff": [
    {
      "event": "ColumnRenamed",
      "signature": {
        "CurrentColumnName": "id1",
        "PreviousColumnName": "id",
        "TableName": "test",
        "TableSchema": "public"
      }
    },
    {
      "event": "ColumnTypeChanged",
      "signature": {
        "ColumnName": "id",
        "CurrentColumnType": "bigint",
        "CurrentColumnTypeOid": "20",
        "PreviousColumnType": "integer",
        "PreviousColumnTypeOid": "23",
        "TableName": "test",
        "TableSchema": "public"
      }
    },
    {
      "event": "ColumnCreated",
      "signature": {
        "ColumnName": "name",
        "ColumnType": "text",
        "TableName": "test",
        "TableSchema": "public"
      }
    },
    {
      "event": "TableCreated",
      "signature": {
        "SchemaName": "public",
        "TableName": "test1",
        "TableOid": "20563"
      }
    }
  ],
  "Hint": "Check schema changes before making new dump",
  "time": "2024-03-15T20:01:51+02:00",
  "message": "Database schema has been changed"
}
```

**Transformation diff line**

```json
{
  "schema": "humanresources",
  "name": "employee",
  "primary_key_columns": [
    "businessentityid"
  ],
  "with_diff": true,
  "transformed_only": true,
  "records": [
    {
      "birthdate": {
        "original": "1969-01-29",
        "transformed": "1964-10-20",
        "equal": false,
        "implicit": true
      },
      "businessentityid": {
        "original": "1",
        "transformed": "1",
        "equal": true,
        "implicit": true
      }
    }
  ]
}
```
