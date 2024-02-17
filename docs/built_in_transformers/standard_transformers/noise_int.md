Add or subtract a random fraction to the original integer value.

## Parameters

| Name   | Description                                                      | Default | Required | Supported DB types |
|--------|------------------------------------------------------------------|---------|----------|--------------------|
| column | The name of the column to be affected              |         | Yes      | int2, int4, int8   |
| ratio  | The maximum random percentage for noise, from `0` to `1` |         | Yes      | -                  |

## Description

The `NoiseInt` transformer multiplies the original integer value by a provided random value that is not higher than the
`ratio` parameter and adds it to or subtracts it from the original value.

## Example: Noise vacation hours of an employee

In the following example, the original value of `vacationhours` will be noised up to 40%.

``` yaml title="NoiseInt transformer example"
- schema: "humanresources"
  name: "employee"
  transformers:
    - name: "NoiseInt"
      params:
        column: "vacationhours"
        ratio: 0.4
```
