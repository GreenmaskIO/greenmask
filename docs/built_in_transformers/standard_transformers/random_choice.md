Replace values randomly chosen from a provided list.

## Parameters

| Name      | Description                                                                                                           | Default  | Required | Supported DB types |
|-----------|-----------------------------------------------------------------------------------------------------------------------|----------|----------|--------------------|
| column    | The name of the column to be affected                                                                                 |          | Yes      | any                |
| values    | A list of values in any format. The string with value `\N` is considered NULL.                                        |          | Yes      | -                  |
| validate  | Performs a decoding procedure via the PostgreSQL driver using the column type to ensure that values have correct type | `true`   | No       |                    |
| keep_null | Indicates whether NULL values should be replaced with transformed values or not                                       | `true`   | No       |                    |
| engine    | The engine used for generating the values [`random`, `hash`]. Use hash for deterministic generation                   | `random` | No       | -                  |

## Description

The `RandomChoice` transformer replaces one randomly chosen value from the list provided in the `values` parameter. You
can use the `validate` parameter to ensure that values are correct before applying the transformation. The behaviour for
NULL values can be configured using the `keep_null` parameter.

The `engine` parameter allows you to choose between random and hash engines for generating values. Read more about the
engines in the [Transformation engines](../transformation_engines.md) section.

## Example: Choosing randomly from provided dates

In this example, the provided values undergo validation through PostgreSQL driver decoding, and one value is randomly
chosen from the list.

```yaml title="RandomChoice transformer example"
- schema: "humanresources"
  name: "jobcandidate"
  transformers:
    - name: "RandomChoice"
      params:
        column: "modifieddate"
        validate: true
        engine: hash
        values:
          - "2023-12-21 07:41:06.891"
          - "2023-12-21 07:41:06.896"
```

Result

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>modifieddate</td><td><span style="color:green">2007-06-23 00:00:00</span></td><td><span style="color:red">2023-12-21 07:41:06.891</span></td>
</tr>
</table>

