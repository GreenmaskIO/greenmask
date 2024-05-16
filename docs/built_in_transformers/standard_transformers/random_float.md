Generate a random float within the provided interval.

## Parameters

| Name      | Description                                                                             | Default | Required | Supported DB types                                |
|-----------|-----------------------------------------------------------------------------------------|---------|----------|---------------------------------------------------|
| column    | The name of the column to be affected                                                   |         | Yes      | float4 (real), float8 (double precision), numeric |
| min       | The minimum threshold for the random value. The value range depends on the column type. |         | Yes      | -                                                 |
| max       | The maximum threshold for the random value. The value range depends on the column type. |         | Yes      | -                                                 |
| decimal   | The decimal of the random float value (number of digits after the decimal point)        | `4`     | No       | -                                                 |
| keep_null | Indicates whether NULL values should be replaced with transformed values or not         | `true`  | No       | -                                                 |

## Description

The `RandomFloat` transformer generates a random float value within the provided interval, starting from `min` to
`max`, with the option to specify the number of decimal digits by using the `decimal` parameter. The behaviour for
NULL values can be configured using the `keep_null` parameter.

## Example: Generate random price

In this example, the `RandomFloat` transformer generates random prices in the range from `0.1` to `7000` while
maintaining a decimal of up to 2 digits.

``` yaml title="RandomFloat transformer example"
- schema: "sales"
  name: "salesorderdetail"
  transformers:
    - name: "RandomFloat"
      params:
        column: "unitprice"
        min: 0.1
        max: 7000
        decimal: 2
```

```bash title="Expected result"

| column name | original value | transformed |
|-------------|----------------|-------------|
| unitprice   | 2024.994       | 6806.5      |
```
