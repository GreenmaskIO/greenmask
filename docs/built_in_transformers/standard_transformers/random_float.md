Generate a random float within the provided interval.

## Parameters

| Name      | Description                                                                                         | Default  | Required | Supported DB types |
|-----------|-----------------------------------------------------------------------------------------------------|----------|----------|--------------------|
| column    | The name of the column to be affected                                                               |          | Yes      | float4, float8     |
| min       | The minimum threshold for the random value. The value range depends on the column type.             |          | Yes      | -                  |
| max       | The maximum threshold for the random value. The value range depends on the column type.             |          | Yes      | -                  |
| decimal   | The decimal of the random float value (number of digits after the decimal point)                    | `4`      | No       | -                  |
| keep_null | Indicates whether NULL values should be replaced with transformed values or not                     | `true`   | No       | -                  |
| engine    | The engine used for generating the values [`random`, `hash`]. Use hash for deterministic generation | `random` | No       | -                  |

## Dynamic parameters

| Parameter | Supported types |
|-----------|-----------------|
| min       | float4, float8  |
| max       | float4, float8  |

## Description

The `RandomFloat` transformer generates a random float value within the provided interval, starting from `min` to
`max`, with the option to specify the number of decimal digits by using the `decimal` parameter. The behaviour for
NULL values can be configured using the `keep_null` parameter.

The `engine` parameter allows you to choose between random and hash engines for generating values. Read more about the
engines in the [Transformation engines](../transformation_engines.md) section.

## Example: Generate random price

In this example, the `RandomFloat` transformer generates random prices in the range from `0.1` to `7000` while
maintaining a decimal of up to 2 digits.

``` yaml title="RandomFloat transformer example"
- schema: "sales"
  name: "salesorderdetail"
  columns_type_override:  # (1)
    "unitprice": "float8"
  transformers:
    - name: "RandomFloat"
      params:
        column: "unitprice"
        min: 0.1
        max: 7000
        decimal: 2
```

1. The type overrides applied for example because the playground database does not contain any tables with float
   columns.

Result:

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>unitprice</td><td><span style="color:green">2024.994</span></td><td><span style="color:red">4449.7</span></td>
</tr>
</table>
