Randomly add or subtract a duration within the provided `ratio` interval to the original date value.

## Parameters

| Name     | Description                                                                                                                                     | Default | Required | Supported DB types           |
|----------|-------------------------------------------------------------------------------------------------------------------------------------------------|---------|----------|------------------------------|
| column   | The name of the column to be affected                                                                                             |         | Yes      | date, timestamp, timestamptz |
| ratio    | The maximum random duration for noise. The value must be in PostgreSQL interval format, e. g. 1 year 2 mons 3 day — `04:05:06.07`                |         | Yes      | -                            |
| truncate | Truncate the date to the specified part (`nano`, `second`, `minute`, `hour`, `day`, `month`, `year`). The truncate operation is not applied by default. |         | No       | -                            |

## Description

The `NoiseDate` transformer randomly generates duration within the specified `ratio` parameter and adds it to or
subtracts it from the original date value. The `ratio` parameter must be written in
the [PostgreSQL interval format](https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT). You can also truncate the date up to a specified part by setting the `truncate` parameter.

## Example: Adding noise to the modified date

In the following example, the original `timestamp` value of `modifieddate` will be noised up to `1 year 2 months 3 days 4 hours 5
minutes 6 seconds and 7 milliseconds` with truncation up to the `nano` part.

``` yaml title="NoiseDate transformer example"
- schema: "humanresources"
  name: "jobcandidate"
  transformers:
    - name: "NoiseDate"
      params:
        column: "modifieddate"
        ratio: "1 year 2 mons 3 day 04:05:06.07"
        truncate: "nano"
```
