Generate a random date in a specified interval.

## Parameters

| Name      | Description                                                                                                                                                                                 | Default  | Required | Supported DB types           |
|-----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|----------|------------------------------|
| column    | Name of the column to be affected                                                                                                                                                           |          | Yes      | date, timestamp, timestamptz |
| min       | The minimum threshold date for the random value. The format depends on the column type.                                                                                                     |          | Yes      | -                            |
| max       | The maximum threshold date for the random value. The format depends on the column type.                                                                                                     |          | Yes      | -                            |
| truncate  | Truncate the date to the specified part (`nanosecond`, `microsecond`, `millisecond`, `second`, `minute`, `hour`, `day`, `month`, `year`). The truncate operation is not applied by default. |          | No       | -                            |
| keep_null | Indicates whether NULL values should be replaced with transformed values or not                                                                                                             | `true`   | No       | -                            |
| engine    | The engine used for generating the values [`random`, `hash`]. Use hash for deterministic generation                                                                                         | `random` | No       | -                            |

## Dynamic parameters

| Parameter | Supported types              |
|-----------|------------------------------|
| min       | date, timestamp, timestamptz |
| max       | date, timestamp, timestamptz |

## Description

The `RandomDate` transformer generates a random date within the provided interval, starting from `min` to `max`. It
can also perform date truncation up to the specified part of the date. The format of dates in the `min` and `max`
parameters must adhere to PostgreSQL types, including `DATE`, `TIMESTAMP WITHOUT TIMEZONE`,
or `TIMESTAMP WITH TIMEZONE`.

:::note

The value of `min` and `max` parameters depends on the column type. For example, for the `date` column, the value 
should be in the format `YYYY-MM-DD`, while for the `timestamp` column, the value should be in the format
`YYYY-MM-DD HH:MM:SS` or `YYYY-MM-DD HH:MM:SS.SSSSSS`. The `timestamptz` column requires the value to be in the
format `YYYY-MM-DD HH:MM:SS.SSSSSS+HH:MM`. Read more about date/time formats in 
the [PostgreSQL documentation](https://www.postgresql.org/docs/current/datatype-datetime.html).

:::
The behaviour for `NULL` values can be configured using the `keep_null` parameter. The `engine` parameter allows you to
choose between random and hash engines for generating values. Read more about the engines in
the [Transformation engines](../transformation_engines.md) section.

## Example: Generate `modifieddate`

In the following example, a random timestamp without timezone is generated for the `modifieddate` column within the
range from `2011-05-31 00:00:00` to `2013-05-31 00:00:00`, and the part of the random value after `day` is truncated.

``` yaml title="RandomDate transformer example"
- schema: "sales"
  name: "salesorderdetail"
  transformers:
    - name: "RandomDate"
      params:
        column: "modifieddate"
        keep_null: false
        min: "2011-05-31 00:00:00"
        max: "2013-05-31 00:00:00"
        truncate: "day"
```

Result

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>modifieddate</td><td><span>2014-06-30 00:00:00</span></td><td><span>2012-07-27 00:00:00</span></td>
</tr>
</table>

## Example: Generate `hiredate` based on `birthdate` using two transformations

In this example, the `RandomDate` transformer generates a random date for the `birthdate` column within the
range `now - 50 years` to `now - 18 years`. The hire date is generated based on the `birthdate`, ensuring that the
employee is at least 18 years old when hired.

```yaml
- schema: "humanresources"
  name: "employee"
  transformers:
    - name: "RandomDate"
      params:
        column: "birthdate"
        min: '{{ now | tsModify "-50 years" | .EncodeValue }}' # 1994
        max: '{{ now | tsModify "-18 years" | .EncodeValue }}' # 2006

    - name: "RandomDate"
      params:
        column: "hiredate"
        truncate: "month"
        max: "{{ now | .EncodeValue }}"
      dynamic_params:
        min:
          column: "birthdate"
          template: '{{ .GetValue | tsModify "18 years" | .EncodeValue }}' # min age 18 years
```

Result:

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>birthdate</td><td><span>1969-01-29</span></td><td><span>1985-10-29</span></td>
</tr>
<tr>
<td>hiredate</td><td><span>2009-01-14</span></td><td><span>2023-01-01</span></td>
</tr>
</table>


