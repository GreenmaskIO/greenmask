The `RandomDayOfWeek` transformer is specifically designed to fill specified database columns with random day-of-the-week names. It is particularly useful for applications that require simulated weekly schedules, random event planning, or any scenario where the day of the week is relevant but the specific date is not.

## Parameters

| Name      | Description                                          | Default | Required | Supported DB types |
|-----------|------------------------------------------------------|---------|----------|--------------------|
| column    | The name of the column to be affected               |         | Yes      | text, varchar      |
| keep_null | Indicates whether NULL values should be preserved  | `false` | No       | -                  |

## Description

Utilizing the `faker` library, the `RandomDayOfWeek` transformer generates names of days (e. g., Monday, Tuesday) at random. This transformer can be applied to any text or varchar column in a database, introducing variability and realism into data sets that need to represent days of the week in a non-specific manner.

## Example: Populate random days of the week for the `work_schedule` table

This example demonstrates configuring the `RandomDayOfWeek` transformer to populate the `work_day` column in the `work_schedule` table with random days of the week. This setup can help simulate a diverse range of work schedules without tying them to specific dates.

```yaml title="RandomDayOfWeek transformer example"
- schema: "public"
  name: "work_schedule"
  transformers:
    - name: "RandomDayOfWeek"
      params:
        column: "work_day"
        keep_null: false
```

In this configuration, every entry in the `work_day` column will be updated with a random day of the week, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, then existing NULL values within the column will remain unchanged.
