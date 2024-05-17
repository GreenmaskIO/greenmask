The `RandomUnixTimestamp` transformer generates random Unix time values (timestamps) for specified database columns. It
is
particularly useful for populating columns with timestamp data, simulating time-related data, or anonymizing actual
timestamps in a dataset.

## Parameters

| Name      | Description                                                                                                                                                                                 | Default  | Required | Supported DB types |
|-----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|----------|--------------------|
| column    | The name of the column to be affected                                                                                                                                                       |          | Yes      | int2, int4, int8   |
| min       | The minimum threshold date for the random value in unix timestamp format (integer) with `sec` unit by default                                                                               |          | Yes      | -                  |
| max       | The maximum threshold date for the random value in unix timestamp format (integer) with `sec` unit by default                                                                               |          | Yes      | -                  |
| unit      | Generated unix timestamp value unit. Possible values [`second`, `millisecond`, `microsecond`, `nanosecond`]                                                                                 | `second` | Yes      | -                  |
| min_unit  | Min unix timestamp threshold date unit. Possible values [`second`, `millisecond`, `microsecond`, `nanosecond`]                                                                              | `second` | Yes      | -                  |
| max_unit  | Min unix timestamp threshold date unit. Possible values [`second`, `millisecond`, `microsecond`, `nanosecond`]                                                                              | `second` | Yes      | -                  |
| keep_null | Indicates whether NULL values should be preserved                                                                                                                                           | `false`  | No       | -                  |
| truncate  | Truncate the date to the specified part (`nanosecond`, `microsecond`, `millisecond`, `second`, `minute`, `hour`, `day`, `month`, `year`). The truncate operation is not applied by default. |          | No       | -                  |
| engine    | The engine used for generating the values [`random`, `hash`]. Use hash for deterministic generation                                                                                         | `random` | No       | -                  |

## Description

The `RandomUnixTimestamp` transformer generates random Unix timestamps within the provided interval, starting from `min`
to `max`. The `min` and `max` parameters are expected to be in Unix timestamp format. The `min_unit` and `max_unit`
parameters specify the unit of the Unix timestamp threshold date. The `truncate` parameter allows you to truncate the
date to the specified part of the date. The keep_null parameter allows you to specify whether NULL values should be
preserved or replaced with transformed values.

The `engine` parameter allows you to choose between random and hash engines for generating values. Read more about the
engines in the [Transformation engines](../transformation_engines.md) section.

## Example: Generate random Unix timestamps with dynamic parameters

In this example, the `RandomUnixTimestamp` transformer generates random Unix timestamps using dynamic parameters. The
`min` parameter is set to the `created_at` column, which is converted to Unix seconds using the `TimestampToUnixSec`.
The `max` parameter is set to a fixed value. The `paid_at` column is populated with random Unix timestamps in the
range from `created_at` to `1715934239` (Unix timestamp for `2024-05-17 12:03:59`). The `unit` parameter is set to
`millisecond` because the `paid_at` column stores timestamps in milliseconds.

```sql
CREATE TABLE transactions
(
    id         SERIAL PRIMARY KEY,
    kind       VARCHAR(255),
    total      DECIMAL(10, 2),
    created_at TIMESTAMP,
    paid_at    BIGINT -- stores milliseconds since the epoch
);

-- Inserting data with milliseconds timestamp
INSERT INTO transactions (kind, total, created_at, paid_at)
VALUES ('Sale', 199.99, '2023-05-17 12:00:00', (EXTRACT(EPOCH FROM TIMESTAMP '2023-05-17 12:05:00') * 1000)),
       ('Refund', 50.00, '2023-05-18 15:00:00', (EXTRACT(EPOCH FROM TIMESTAMP '2023-05-18 15:10:00') * 1000)),
       ('Sale', 129.99, '2023-05-19 10:30:00', (EXTRACT(EPOCH FROM TIMESTAMP '2023-05-19 10:35:00') * 1000));
```

```yaml title="RandomUnixTimestamp transformer example"
- schema: "public"
  name: "transactions"
  transformers:
    - name: "RandomUnixTimestamp"
      params:
        column: "paid_at"
        max: 1715934239
        unit: "millisecond"
        min_unit: "second"
        max_unit: "second"
      dynamic_params:
        min:
          column: "created_at"
          cast_to: "TimestampToUnixSec"
```

Result:

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>paid_at</td><td><span style="color:green">1684325100000</span></td><td><span style="color:red">1708919030732</span></td>
</tr>
</table>

## Example: Generate simple random Unix timestamps

In this example, the `RandomUnixTimestamp` transformer generates random Unix timestamps for the `paid_at` column in the
range from `1615934239` (Unix timestamp for `2021-03-16 12:03:59`) to `1715934239` (Unix timestamp
for `2024-05-17 12:03:59`). The `unit` parameter is set to `millisecond` because the `paid_at` column stores timestamps
in milliseconds.

``` yaml
- schema: "public"
  name: "transactions"
  transformers:
    - name: "RandomUnixTimestamp"
      params:
        column: "paid_at"
        min: 1615934239
        max: 1715934239
        unit: "millisecond"
```

Result:

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>paid_at</td><td><span style="color:green">1684325100000</span></td><td><span style="color:red">1655768292548</span></td>
</tr>
</table>
