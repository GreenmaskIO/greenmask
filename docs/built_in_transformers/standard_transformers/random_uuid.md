Generate random unique user ID using version 4.

## Parameters

| Name      | Description                                                                  | Default | Required | Supported DB types  |
|-----------|------------------------------------------------------------------------------|---------|----------|---------------------|
| column    | The name of the column to be affected                          |         | Yes      | text, varchar, uuid |
| keep_null | Indicates whether NULL values should be replaced with transformed values or not | `true`  | No       | -                   |

## Description

The `RandomUuid` transformer generates a random UUID. The behaviour for NULL values can be configured using the `keep_null` parameter.

## Example: Updating the `rowguid` column

The following example replaces original UUID values of the `rowguid` column to randomly generated ones.

``` yaml title="RandomUuid transformer example"
- schema: "humanresources"
  name: "employee"
  transformers:
  - name: "RandomUuid"
    params:
      column: "rowguid"
      keep_null: false
```

```bash title="Expected result"

| column name | original value                       | transformed                          |
|-------------|--------------------------------------|--------------------------------------|
| rowguid     | f01251e5-96a3-448d-981e-0f99d789110d | 0211629f-d197-4187-8a87-095ec4f51977 |
```
