---
title: "SetNull"
description: "The SetNull transformer assigns NULL value to a column. This transformer generates warning if the affected column has NOT NULL constraint."
keywords: ["SetNull", "NULL value", "nullification", "greenmask transformer", "data anonymization", "postgresql", "mysql", "oracle", "Enterprise support", "Open-Source", "PostgreSQL anonymization", "test data management", "compliance", "security", "agentic pipeline", "development cycle"]
---

Set `NULL` value to a column.

## Parameters

| Name   | Description                                         | Default | Required | Supported DB types |
|--------|-----------------------------------------------------|---------|----------|--------------------|
| column | The name of the column to be affected |         | Yes      | any                |

## Description

The `SetNull` transformer assigns `NULL` value to a column. It has no other parameters. This transformer generates warning if the affected column has `NOT NULL` constraint.

:::note

There is no `Nullify` transformer. Use `SetNull` to replace values with `NULL`.

:::

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

## Example: Set NULL value to `jobtitle` column

```yaml title="SetNull transformer example"
dump:
  transformation:
    - schema: "humanresources"
      name: "employee"
      transformers:
        - name: "SetNull"
          params:
            column: "jobtitle"
```

```bash title="Expected result"

| column name | original value          | transformed |
|-------------|-------------------------|-------------|
| jobtitle    | Chief Executive Officer | NULL        |
```
