The `RandomUnixTime` transformer generates random Unix time values (timestamps) for specified database columns. It is particularly useful for populating columns with timestamp data, simulating time-related data, or anonymizing actual timestamps in a dataset.

## Parameters

| Name      | Description                                          | Default | Required | Supported DB types |
|-----------|------------------------------------------------------|---------|----------|--------------------|
| column    | The name of the column to be affected               |         | Yes      | int4, int8, numeric |
| keep_null | Indicates whether NULL values should be preserved  | `false` | No       | -                  |

## Description

The `RandomUnixTime` transformer uses the `faker` library to generate random Unix timestamps. Unix time, also known as POSIX time or Epoch time, is a system for describing points in time, defined as the number of seconds elapsed since midnight Coordinated Universal Time (UTC) of January 1, 1970, not counting leap seconds. This transformer allows for the generation of timestamps that can represent any moment from the Epoch to the present or even into the future, depending on the range of the `faker` library's implementation.

## Example: Populate random timestamps for the `registration_dates` table

This example configures the `RandomUnixTime` transformer to apply random Unix timestamps to the `registration_date` column in a `users` table, simulating user registration times.

```yaml title="RandomUnixTime transformer example"
- schema: "public"
  name: "users"
  transformers:
    - name: "RandomUnixTime"
      params:
        column: "registration_date"
        keep_null: false
```

In this configuration, every entry in the `registration_date` column is assigned a random Unix timestamp, replacing any existing non-NULL values. Setting `keep_null` to `true` would ensure that NULL values in the column are left unchanged.
