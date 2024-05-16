Generate random unique user ID using version 4.

## Parameters

| Name      | Description                                                                                     | Default  | Required | Supported DB types  |
|-----------|-------------------------------------------------------------------------------------------------|----------|----------|---------------------|
| column    | The name of the column to be affected                                                           |          | Yes      | text, varchar, uuid |
| keep_null | Indicates whether NULL values should be replaced with transformed values or not                 | `true`   | No       | -                   |
| engine    | The engine used for generating the values [random, hash]. Use hash for deterministic generation | `random` | No       | -                   |

## Description

The `RandomUuid` transformer generates a random UUID. The behaviour for NULL values can be configured using
the `keep_null` parameter.

The `engine` parameter allows you to choose between random and hash engines for generating values. Read more about the
engines in the [Transformation engines](../transformation_engines.md) section.

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

Result

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>rowguid</td><td><span style="color:green">f01251e5-96a3-448d-981e-0f99d789110d</span></td><td><span style="color:red">8ed8c4b2-7e7a-1e8d-f0f0-768e0e8ed0d0</span></td>
</tr>
</table>
