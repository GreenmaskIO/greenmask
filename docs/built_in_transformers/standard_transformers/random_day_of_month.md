The `RandomDayOfMonth` transformer is designed to populate specified database columns with random day-of-the-month values. It is particularly useful for scenarios requiring the simulation of dates, such as generating random event dates, user sign-up dates, or any situation where the specific day of the month is needed without reference to the actual month or year.

## Parameters

| Name      | Description                                          | Default | Required | Supported DB types |
|-----------|------------------------------------------------------|---------|----------|--------------------|
| column    | The name of the column to be affected               |         | Yes      | text, varchar, int2, int4, int8, numeric |
| keep_null | Indicates whether NULL values should be preserved  | `false` | No       | -                  |

## Description

Utilizing the `faker` library, the `RandomDayOfMonth` transformer generates random numerical values representing days of the month, ranging from 1 to 31. This allows for the easy insertion of random but plausible day-of-the-month data into a database, enhancing realism or anonymizing actual dates.

## Example: Populate random days of the month for the `events` table

This example illustrates how to configure the `RandomDayOfMonth` transformer to fill the `event_day` column in the `events` table with random day-of-the-month values, facilitating the simulation of varied event scheduling.

```yaml title="RandomDayOfMonth transformer example"
- schema: "public"
  name: "events"
  transformers:
    - name: "RandomDayOfMonth"
      params:
        column: "event_day"
        keep_null: false
```

With this setup, the `event_day` column will be updated with random day-of-the-month values, replacing any existing non-NULL values. Setting `keep_null` to `true` ensures that NULL values in the column are left unchanged, maintaining any existing gaps in the data.
