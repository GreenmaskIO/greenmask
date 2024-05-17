Generate a random integer within the provided interval.

## Parameters

| Name      | Description                                                                                         | Default  | Required | Supported DB types |
|-----------|-----------------------------------------------------------------------------------------------------|----------|----------|--------------------|
| column    | The name of the column to be affected                                                               |          | Yes      | int2, int4, int8   |
| min       | The minimum threshold for the random value                                                          |          | Yes      | -                  |
| max       | The maximum threshold for the random value                                                          |          | Yes      | -                  |
| keep_null | Indicates whether NULL values should be replaced with transformed values or not                     | `true`   | No       | -                  |
| engine    | The engine used for generating the values [`random`, `hash`]. Use hash for deterministic generation | `random` | No       | -                  |

## Dynamic parameters

| Parameter | Supported types  |
|-----------|------------------|
| min       | int2, int4, int8 |
| max       | int2, int4, int8 |

## Description

The `RandomInt` transformer generates a random integer within the specified `min` and `max` thresholds. The behaviour
for NULL values can be configured using the `keep_null` parameter.

The `engine` parameter allows you to choose between random and hash engines for generating values. Read more about the
engines in the [Transformation engines](../transformation_engines.md) section.

## Example: Generate random item quantity

In the following example, the `RandomInt` transformer generates a random value in the range from `1` to `30` and assigns
it to
the `orderqty` column.

``` yaml title="generate random orderqty in the range from 1 to 30"
- schema: "sales"
  name: "salesorderdetail"
  transformers:
    - name: "RandomInt"
      params:
        column: "orderqty"
        min: 1
        max: 30
```

Result

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>orderqty</td><td><span style="color:green">1</span></td><td><span style="color:red">29</span></td>
</tr>
</table>

## Example: Generate random sick leave hours based on vacation hours

In the following example, the `RandomInt` transformer generates a random value in the range from `1` to the value of the
`vacationhours` column and assigns it to the `sickleavehours` column. This configuration allows for the simulation of
sick leave hours based on the number of vacation hours.

``` yaml title="RandomInt transformer example"
- schema: "humanresources"
  name: "employee"
  transformers:
    - name: "RandomInt"
      params:
        column: "sickleavehours"
        max: 100
      dynamic_params:
        min:
          column: "vacationhours"
```

Result

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>sickleavehours</td><td><span style="color:green">69</span></td><td><span style="color:red">99</span></td>
</tr>
</table>

