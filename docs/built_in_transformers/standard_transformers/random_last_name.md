The `RandomLastName` transformer is developed to populate specified database columns with random last names. This tool is essential for applications requiring the simulation of user profiles, testing user registration systems, or anonymizing user data in datasets.

## Parameters

| Name       | Description                                          | Default | Required | Supported DB types |
|------------|------------------------------------------------------|---------|----------|--------------------|
| column     | The name of the column to be affected               |         | Yes      | text, varchar      |
| keep_null  | Indicates whether NULL values should be preserved  | `false` | No       | -                  |

## Description

The `RandomLastName` transformer utilizes a comprehensive list of last names to inject random last names into the designated database column. This feature allows for the creation of diverse and realistic user profiles by simulating a variety of last names without using real user data.

## Example: Populate random last names for the `user_profiles` table

This example demonstrates configuring the `RandomLastName` transformer to populate the `last_name` column in the `user_profiles` table with random last names. It is an effective method for simulating a variety of user profiles with diverse last names.

```yaml title="RandomLastName transformer example"
- schema: "public"
  name: "user_profiles"
  transformers:
    - name: "RandomLastName"
      params:
        column: "last_name"
        keep_null: false
```

In this configuration, the `last_name` column will be updated with random last names for each user profile entry, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, existing NULL values in the column will be preserved, ensuring the integrity of records where last name information is not applicable or provided.
