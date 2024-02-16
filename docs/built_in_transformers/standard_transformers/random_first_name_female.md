The `RandomFirstNameFemale` transformer is designed to populate specified database columns with random female first names. This tool is crucial for applications requiring the simulation of user profiles, testing gender-specific features, or anonymizing user data in datasets while focusing on female names.

## Parameters

| Name       | Description                                          | Default | Required | Supported DB types |
|------------|------------------------------------------------------|---------|----------|--------------------|
| column     | The name of the column to be affected               |         | Yes      | text, varchar      |
| keep_null  | Indicates whether NULL values should be preserved  | `false` | No       | -                  |

## Description

The `RandomFirstNameFemale` transformer utilizes a comprehensive list of female first names to inject random female first names into the designated database column. This feature allows for the creation of diverse and realistic user profiles with a focus on female names without using real user data.

## Example: Populate random female first names for the `user_profiles` table

This example demonstrates configuring the `RandomFirstNameFemale` transformer to populate the `first_name` column in the `user_profiles` table with random female first names. It is an effective method for simulating a variety of user profiles with diverse female first names.

```yaml title="RandomFirstNameFemale transformer example"
- schema: "public"
  name: "user_profiles"
  transformers:
    - name: "RandomFirstNameFemale"
      params:
        column: "first_name"
        keep_null: false
```

In this configuration, the `first_name` column will be updated with random female first names for each user profile entry, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, existing NULL values in the column will be preserved, ensuring the integrity of records where female first name information is not applicable or provided.
