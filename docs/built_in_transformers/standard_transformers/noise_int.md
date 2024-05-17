Add or subtract a random fraction to the original integer value.

## Parameters

| Name      | Description                                                                                         | Default  | Required | Supported DB types |
|-----------|-----------------------------------------------------------------------------------------------------|----------|----------|--------------------|
| column    | The name of the column to be affected                                                               |          | Yes      | int2, int4, int8   |
| min_ratio | The minimum random percentage for noise, from `0` to `1`, e. g. `0.1` means "add noise up to 10%"   | `0.05`   | No       | -                  |
| max_ratio | The maximum random percentage for noise, from `0` to `1`, e. g. `0.1` means "add noise up to 10%"   |          | Yes      | -                  |
| min       | Min threshold of noised value                                                                       |          | No       | -                  |
| max       | Min threshold of noised value                                                                       |          | No       | -                  |
| engine    | The engine used for generating the values [`random`, `hash`]. Use hash for deterministic generation | `random` | No       | -                  |

## Dynamic parameters

| Parameter | Supported types  |
|-----------|------------------|
| min       | int2, int4, int8 |
| max       | int2, int4, int8 |

## Description

The `NoiseInt` transformer multiplies the original integer value by randomly generated value that is not higher than
the `max_ratio` parameter and not less that `max_ratio` parameter and adds it to or subtracts it from the original
value.

In case you have constraints on the integer range, you can set the `min` and `max` parameters to specify the
threshold values. The values for `min` and `max` must have the same format as the `column` parameter. Parameters min and
max support dynamic mode.

!!! info

    If the noised value exceeds the `max` threshold, the transformer will set the value to `max`. If the noised value
    is lower than the `min` threshold, the transformer will set the value to `min`.

The `engine` parameter allows you to choose between random and hash engines for generating values. Read more about the
engines in the [Transformation engines](../transformation_engines.md) section.

## Example: Noise vacation hours of an employee

In the following example, the original value of `vacationhours` will be noised up to 40%. The transformer will set the
value to `10` if the noised value is lower than `10` and to `1000` if the noised value exceeds `1000`.

``` yaml title="NoiseInt transformer example"
- schema: "humanresources"
  name: "employee"
  transformers:
    - name: "NoiseInt"
      params:
        column: "vacationhours"
        max_ratio: 0.4
        min: 10
        max: 1000
```

Result

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>vacationhours</td><td><span style="color:green">99</span></td><td><span style="color:red">69</span></td>
</tr>
</table>
