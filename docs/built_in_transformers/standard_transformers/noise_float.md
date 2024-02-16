Add or subtract a random fraction to the original float value.

## Parameters

| Name      | Description                                                                                              | Default | Required | Supported DB types                                |
|-----------|----------------------------------------------------------------------------------------------------------|---------|----------|---------------------------------------------------|
| column    | The name of the column to be affected                                                      |         | Yes      | float4 (real), float8 (double precision), numeric |
| ratio     | The maximum random percentage for noise, from `0` to `1`, e. g. `0.1` means "add noise up to 10%" |         | Yes      | -                                                 |
| precision | The precision of the noised float value (number of digits after the decimal point)                       | `4`     | No       | -                                                 |

## Description

The `NoiseFloat` transformer multiplies the original float value by a provided random value that is not higher than
the `ratio` parameter and adds it to or subtracts it from the original value. Additionally, you can specify the number of decimal digits by using the `precision` parameter.

## Example: Adding noise to the purchase price

In this example, the original value of `standardprice` will be noised up to `50%` and rounded up to `2` decimals.

``` yaml title="NoiseFloat transformer example"
- schema: "purchasing"
  name: "productvendor"
  transformers:
    - name: "NoiseFloat"
      params:
        column: "standardprice"
        ratio: 0.5
        precision: 2
```
