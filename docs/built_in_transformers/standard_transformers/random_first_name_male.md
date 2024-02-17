The `RandomFirstNameMale` transformer is developed to populate specified database columns with random male first names. This tool is essential for applications requiring the simulation of user profiles, testing gender-specific features, or anonymizing user data in datasets while focusing on male names.

## Parameters

| Name       | Description                                          | Default | Required | Supported DB types  |
|------------|------------------------------------------------------|---------|----------|---------------------|
| column     | The name of the column to be affected               |         | Yes      | text, varchar       |
| keep_null  | Indicates whether NULL values should be preserved  | `false` | No       | -                   |

## Description

The `RandomFirstNameMale` transformer utilizes a comprehensive list of male first names to inject random male first names into the designated database column. This feature allows for the creation of diverse and realistic user profiles with a focus on male names without using real user data.

## Example: Populate random male first names for the `user_profiles` table

This example demonstrates configuring the `RandomFirstNameMale` transformer to populate the `first_name` column in the `user_profiles` table with random male first names. It is an effective method for simulating a variety of user profiles with diverse male first names.

```yaml title="RandomFirstNameMale transformer example"
- schema: "public"
  name: "user_profiles"
  transformers:
    - name: "RandomFirstNameMale"
      params:
        column: "first_name"
        keep_null: false
```

In this configuration, the `first_name` column will be updated with random male first names for each user profile entry, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, existing NULL values in the column will be preserved, ensuring the integrity of records where male first name information is not applicable or provided.
