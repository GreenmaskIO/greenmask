Set `NULL` value to a column.

## Parameters

| Name   | Description                                         | Default | Required | Supported DB types |
|--------|-----------------------------------------------------|---------|----------|--------------------|
| column | The name of the column to be affected |         | Yes      | any                |

## Description

The `SetNull` transformer assigns `NULL` value to a column. This transformer generates warning if the affected column has `NOT NULL` constraint.

```json title="NULL constraint violation warning"
{
  "hash": "5a229ee964a4ba674a41a4d63dab5a8c",
  "meta": {
    "ColumnName": "jobtitle",
    "ConstraintType": "NotNull",
    "ParameterName": "column",
    "SchemaName": "humanresources",
    "TableName": "employee",
    "TransformerName": "SetNull"
  },
  "msg": "transformer may produce NULL values but column has NOT NULL constraint",
  "severity": "warning"
}
```

## Example: Set NULL value to `updated_at` column

``` yaml title="SetNull transformer example"
- schema: "humanresources"
  name: "employee"
  transformation:
    - name: "SetNull"
      params:
        column: "jobtitle"
```

```bash title="Expected result"

| column name | original value          | transformed |
|-------------|-------------------------|-------------|
| jobtitle    | Chief Executive Officer | NULL        |
```
