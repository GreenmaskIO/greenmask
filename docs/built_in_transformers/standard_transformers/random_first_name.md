The `RandomFirstName` transformer is designed to populate specified database columns with random first names. This tool is indispensable for applications requiring the simulation of user profiles, testing user registration systems, or anonymizing user data in datasets.

## Parameters

| Name       | Description                                          | Default | Required | Supported DB types |
|------------|------------------------------------------------------|---------|----------|--------------------|
| column     | The name of the column to be affected               |         | Yes      | text, varchar      |
| keep_null  | Indicates whether NULL values should be preserved  | `false` | No       | -                  |

## Description

The `RandomFirstName` transformer utilizes a comprehensive list of first names to inject random first names into the designated database column. This feature allows for the creation of diverse and realistic user profiles by simulating a variety of first names without using real user data.

## Example: Populate random first names for the `user_profiles` table

This example demonstrates configuring the `RandomFirstName` transformer to populate the `first_name` column in the `user_profiles` table with random first names. It is an effective method for simulating a variety of user profiles with diverse first names.

```yaml title="RandomFirstName transformer example"
- schema: "public"
  name: "user_profiles"
  transformers:
    - name: "RandomFirstName"
      params:
        column: "first_name"
        keep_null: false
```

In this configuration, the `first_name` column will be updated with random first names for each user profile entry, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, existing NULL values in the column will be preserved, ensuring the integrity of records where first name information is not applicable or provided.
