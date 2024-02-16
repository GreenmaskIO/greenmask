The `RandomName` transformer is designed to populate specified database columns with random full names, including both first names and last names. This tool is indispensable for applications requiring the simulation of user profiles, testing user registration systems, or anonymizing user data in datasets.

## Parameters

| Name       | Description                                          | Default | Required | Supported DB types |
|------------|------------------------------------------------------|---------|----------|--------------------|
| column     | The name of the column to be affected               |         | Yes      | text, varchar      |
| keep_null  | Indicates whether NULL values should be preserved  | `false` | No       | -                  |

## Description

The `RandomName` transformer utilizes a comprehensive list of first names and last names to inject random full names into the designated database column. This feature allows for the creation of diverse and realistic user profiles by simulating a variety of full names without using real user data.

## Example: Populate random full names for the `user_profiles` table

This example demonstrates configuring the `RandomName` transformer to populate the name column in the `user_profiles` table with random full names. It is an effective method for simulating a variety of user profiles with diverse full names.

```yaml title="RandomName transformer example"
- schema: "public"
  name: "user_profiles"
  transformers:
    - name: "RandomName"
      params:
        column: "name"
        keep_null: false
```

In this configuration, the `name` column will be updated with random full names for each user profile entry, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, existing NULL values in the column will be preserved, ensuring the integrity of records where full name information is not applicable or provided.
