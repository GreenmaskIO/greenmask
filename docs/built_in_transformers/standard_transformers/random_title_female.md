The `RandomTitleFemale` transformer is designed to populate specified database columns with random female titles. This tool is crucial for applications that require the simulation of user profiles, testing gender-specific features, or anonymizing user data in datasets.

## Parameters

| Name       | Description                                           | Default | Required | Supported DB types |
|------------|-------------------------------------------------------|---------|----------|--------------------|
| column     | The name of the column to be affected                |         | Yes      | text, varchar      |
| keep_null  | Indicates whether NULL values should be preserved   | `false` | No       | -                  |

## Description

The `RandomTitleFemale` transformer utilizes a predefined list of female titles (e. g., Mrs., Dr., Prof.) to inject random female titles into the designated database column. This feature allows for the creation of diverse and realistic user profiles by simulating a variety of female titles without using real user data.

## Example: Populate random female titles for the `user_profiles` table

This example demonstrates configuring the `RandomTitleFemale` transformer to populate the `title` column in the `user_profiles` table with random female titles. It is an effective method for simulating a variety of user profiles with female titles.

```yaml title="RandomTitleFemale transformer example"
- schema: "public"
  name: "user_profiles"
  transformers:
    - name: "RandomTitleFemale"
      params:
        column: "title"
        keep_null: false
```

In this configuration, the `title` column will be updated with random female titles for each user profile entry, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, existing NULL values in the column will be preserved, ensuring the integrity of records where title information is not applicable or provided.
