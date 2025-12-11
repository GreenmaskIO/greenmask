Randomly add or subtract a duration within the provided `ratio` interval to the original date value.

## Parameters

| Name      | Description                                                                                                                                                                                 | Default                      | Required | Supported DB types           |
|-----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------|----------|------------------------------|
| column    | The name of the column to be affected                                                                                                                                                       |                              | Yes      | date, timestamp, timestamptz |
| min_ratio | The minimum random value for noise. The value must be in PostgreSQL interval format, e. g. `1 year 2 mons 3 day 04:05:06.07`                                                                | 5% from max_ration parameter | No       | -                            |
| max_ratio | The maximum random value for noise. The value must be in PostgreSQL interval format, e. g. `1 year 2 mons 3 day 04:05:06.07`                                                                |                              | Yes      | -                            |
| min       | Min threshold date (and/or time) of value. The value has the same format as `column` parameter                                                                                              |                              | No       | -                            |
| max       | Max threshold date (and/or time) of value. The value has the same format as `column` parameter                                                                                              |                              | No       | -                            |
| truncate  | Truncate the date to the specified part (`nanosecond`, `microsecond`, `millisecond`, `second`, `minute`, `hour`, `day`, `month`, `year`). The truncate operation is not applied by default. |                              | No       | -                            |
| engine    | The engine used for generating the values [`random`, `hash`]. Use hash for deterministic generation                                                                                         | `random`                     | No       | -                            |

## Dynamic parameters

| Parameter | Supported types              |
|-----------|------------------------------|
| min       | date, timestamp, timestamptz |
| max       | date, timestamp, timestamptz |

## Description

The `NoiseDate` transformer randomly generates duration between `min_ratio` and `max_ratio` parameter and adds it to or
subtracts it from the original date value. The `min_ratio` or `max_ratio` parameters must be written in the
[PostgreSQL interval format](https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT).
You can also truncate the resulted date up to a specified part by setting the `truncate` parameter.

In case you have constraints on the date range, you can set the `min` and `max` parameters to specify the threshold
values. The values for `min` and `max` must have the same format as the `column` parameter. Parameters min and max
support dynamic mode.

:::info

If the noised value exceeds the `max` threshold, the transformer will set the value to `max`. If the noised value
is lower than the `min` threshold, the transformer will set the value to `min`.

:::
The `engine` parameter allows you to choose between random and hash engines for generating values. Read more about the
engines in the [Transformation engines](../transformation_engines.md) section.

## Example: Adding noise to the modified date

In the following example, the original `timestamp` value of `modifieddate` will be noised up
to `1 year 2 months 3 days 4 hours 5 minutes 6 seconds and 7 milliseconds` with truncation up to the `month` part.

``` yaml title="NoiseDate transformer example"
- schema: "humanresources"
  name: "jobcandidate"
  transformers:
    - name: "NoiseDate"
      params:
        column: "hiredate"
        max_ratio: "1 year 2 mons 3 day 04:05:06.07"
        truncate: "month"
        max: "2020-01-01 00:00:00"
```

## Example: Adding noise to the modified date with dynamic min parameter with hash engine

In the following example, the original `timestamp` value of `hiredate` will be noised up
to `1 year 2 months 3 days 4 hours 5 minutes 6 seconds and 7 milliseconds` with truncation up to the `month` part.
The `max` threshold is set to `2020-01-01 00:00:00`, and the `min` threshold is set to the `birthdate` column. If the
`birthdate` column is `NULL`, the default value `1990-01-01` will be used. The hash engine is used for deterministic
generation - the same input will always produce the same output.

``` yaml title="NoiseDate transformer example"
- schema: "humanresources"
  name: "employee"
  transformers:
    - name: "NoiseDate"
      params:
        column: "hiredate"
        max_ratio: "1 year 2 mons 3 day 04:05:06.07"
        truncate: "month"
        max: "2020-01-01 00:00:00"
        engine: "hash"
      dynamic_params:
        min:
          column: "birthdate"
          default: "1990-01-01"
```

Result

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>hiredate</td><td><span>2009-01-14</span></td><td><span>2010-08-01</span></td>
</tr>
</table>

