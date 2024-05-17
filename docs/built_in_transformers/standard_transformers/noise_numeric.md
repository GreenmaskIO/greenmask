Add or subtract a random fraction to the original numeric value.

## Parameters

| Name      | Description                                                                                         | Default  | Required | Supported DB types |
|-----------|-----------------------------------------------------------------------------------------------------|----------|----------|--------------------|
| column    | The name of the column to be affected                                                               |          | Yes      | numeric, decimal   |
| decimal   | The decimal of the noised float value (number of digits after the decimal point)                    | `4`      | No       | -                  |
| min_ratio | The minimum random percentage for noise, from `0` to `1`, e. g. `0.1` means "add noise up to 10%"   | `0.05`   | No       | -                  |
| max_ratio | The maximum random percentage for noise, from `0` to `1`, e. g. `0.1` means "add noise up to 10%"   |          | Yes      | -                  |
| min       | Min threshold of noised value                                                                       |          | No       | -                  |
| max       | Max threshold of noised value                                                                       |          | No       | -                  |
| engine    | The engine used for generating the values [`random`, `hash`]. Use hash for deterministic generation | `random` | No       | -                  |

## Dynamic parameters

| Parameter | Supported types                                    |
|-----------|----------------------------------------------------|
| min       | numeric, decimal, float4, float8, int2, int4, int8 |
| max       | numeric, decimal, float4, float8, int2, int4, int8 |

## Description

The `NoiseNumeric` transformer multiplies the original numeric (or decimal) value by randomly generated value that is
not higher than the `max_ratio` parameter and not less that `max_ratio` parameter and adds it to or subtracts it from
the original value. Additionally, you can specify the number of decimal digits by using the `decimal` parameter.

In case you have constraints on the numeric range, you can set the `min` and `max` parameters to specify the threshold
values. The values for `min` and `max` must have the same format as the `column` parameter. Parameters min and max
support dynamic mode. Engine parameter allows you to choose between random and hash engines for generating values. Read
more about the engines

!!! info

    If the noised value exceeds the `max` threshold, the transformer will set the value to `max`. If the noised value
    is lower than the `min` threshold, the transformer will set the value to `min`.

The `engine` parameter allows you to choose between random and hash engines for generating values. Read more about the
engines in the [Transformation engines](../transformation_engines.md) section.

!!! warning

    Greenmask cannot parse the `numeric` type sitteng. For instance `NUMERIC(10, 2)`. You should set `min` and `max` treshholds
    manually as well as allowed `decimal`. This behaviour will be changed in the later versions. Grenmask will be able
    to determine the decimal and scale of the column and set the min and max treshholds automatically if were not set.

## Example: Adding noise to the purchase price

In this example, the original value of `standardprice` will be noised up to `50%` and rounded up to `2` decimals.

``` yaml title="NoiseNumeric transformer example"
- schema: "purchasing"
  name: "productvendor"
  transformers:
    - name: "NoiseNumeric"
      params:
        column: "lastreceiptcost"
        max_ratio: 0.15
        decimal: 2
        max: 10000
      dynamic_params:
        min:
          column: "standardprice"
```

Result

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>lastreceiptcost</td><td><span style="color:green">50.2635</span></td><td><span style="color:red">57.33</span></td>
</tr>
</table>
