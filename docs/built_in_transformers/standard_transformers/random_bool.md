Generate random boolean values.

## Parameters

| Name      | Description                                                                  | Default | Required | Supported DB types |
|-----------|------------------------------------------------------------------------------|---------|----------|--------------------|
| column    | The name of the column to be affected                          |         | Yes      | bool               |
| keep_null | Indicates whether NULL values should be replaced with transformed values or not | `true`  | No       | -                  |

## Description

The `RandomBool` transformer generates a random boolean value. The behaviour for NULL values can be
configured using the `keep_null` parameter.

## Example: Generate a random boolean for a column

In the following example, the `RandomBool` transformer generates a random boolean value for the `salariedflag` column.

``` yaml title="RandomBool transformer example"
- schema: "humanresources"
  name: "employee"
  transformers:
    - name: "RandomBool"
      params:
        column: "salariedflag"
```
