The `RandomMonthName` transformer is crafted to populate specified database columns with random month names. This transformer is especially useful for scenarios requiring the simulation of time-related data, such as user birth months or event months, without relying on specific date values.

## Parameters

| Name      | Description                                          | Default | Required | Supported DB types |
|-----------|------------------------------------------------------|---------|----------|--------------------|
| column    | The name of the column to be affected               |         | Yes      | text, varchar      |
| keep_null | Indicates whether NULL values should be preserved  | `false` | No       | -                  |

## Description

The `RandomMonthName` transformer utilizes the `faker` library to generate the names of months at random. It can be applied to any textual column in a database to introduce variety and realism into data sets that require representations of months without the need for specific calendar dates.

## Example: Populate random month names for the `user_profiles` table

This example demonstrates how to configure the `RandomMonthName` transformer to fill the `birth_month` column in the `user_profiles` table with random month names, adding a layer of diversity to user data without using actual birthdates.

```yaml title="RandomMonthName transformer example"
- schema: "public"
  name: "user_profiles"
  transformers:
    - name: "RandomMonthName"
      params:
        column: "birth_month"
        keep_null: false
```

With this setup, the `birth_month` column will be updated with random month names, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, then existing NULL values within the column will remain untouched.
