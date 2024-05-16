Generate random boolean values.

## Parameters

| Name      | Description                                                                                     | Default  | Required | Supported DB types |
|-----------|-------------------------------------------------------------------------------------------------|----------|----------|--------------------|
| column    | The name of the column to be affected                                                           |          | Yes      | bool               |
| keep_null | Indicates whether NULL values should be replaced with transformed values or not                 | `true`   | No       | -                  |
| engine    | The engine used for generating the values [random, hash]. Use hash for deterministic generation | `random` | No       | -                  |

## Description

The `RandomBool` transformer generates a random boolean value. The behaviour for NULL values can be
configured using the `keep_null` parameter. The `engine` parameter allows you to choose between random and hash engines
for generating values. Read more about the engines in the [Transformation engines](../transformation_engines.md)
section.

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

Result

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>salariedflag</td><td><span style="color:green">t</span></td><td><span style="color:red">f</span></td>
</tr>
</table>
