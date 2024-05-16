Add or subtract a random fraction to the original float value.

## Parameters

| Name      | Description                                                                                       | Default  | Required | Supported DB types |
|-----------|---------------------------------------------------------------------------------------------------|----------|----------|--------------------|
| column    | The name of the column to be affected                                                             |          | Yes      | float4, float8     |
| decimal   | The decimal of the noised float value (number of digits after the decimal point)                  | `4`      | No       | -                  |
| min_ratio | The minimum random percentage for noise, from `0` to `1`, e. g. `0.1` means "add noise up to 10%" | `0.05`   | No       | -                  |
| max_ratio | The maximum random percentage for noise, from `0` to `1`, e. g. `0.1` means "add noise up to 10%" |          | Yes      | -                  |
| min       | Min threshold of noised value                                                                     |          | No       | -                  |
| max       | Max threshold of noised value                                                                     |          | No       | -                  |
| engine    | The engine used for generating the values [random, hash]. Use hash for deterministic generation   | `random` | No       | -                  |

## Dynamic parameters

| Parameter | Supported types                  |
|-----------|----------------------------------|
| min       | float4, float8, int2, int4, int8 |
| max       | float4, float8, int2, int4, int8 |

## Description

The `NoiseFloat` transformer multiplies the original float value by randomly generated value that is not higher than
the `max_ratio` parameter and not less that `max_ratio` parameter and adds it to or subtracts it from the original
value. Additionally, you can specify the number of decimal digits by using the `decimal` parameter.

In case you have constraints on the float range, you can set the `min` and `max` parameters to specify the threshold
values. The values for `min` and `max` must have the same format as the `column` parameter. Parameters min and max
support dynamic mode. Engine parameter allows you to choose between random and hash engines for generating values. Read
more about the engines

!!! info

    If the noised value exceeds the `max` threshold, the transformer will set the value to `max`. If the noised value
    is lower than the `min` threshold, the transformer will set the value to `min`.

The `engine` parameter allows you to choose between random and hash engines for generating values. Read more about the
engines in the [Transformation engines](../transformation_engines.md) section.

## Example: Adding noise to the purchase price

In this example, the original value of `standardprice` will be noised up to `50%` and rounded up to `2` decimals.

``` yaml title="NoiseFloat transformer example"
- schema: "purchasing"
  name: "productvendor"
  transformers:
    - name: "NoiseFloat"    
      columns_type_override: # (1)
        lastreceiptcost: "numeric"
        standardprice: "numeric"
      params:
        column: "lastreceiptcost"
        max_ratio: 0.15
        decimal: 2
      dynamic_params:
        min:
          column: "standardprice"
```

1. The type overrides perfomed for example because the playground database does not contains any tables with float
   columns.
