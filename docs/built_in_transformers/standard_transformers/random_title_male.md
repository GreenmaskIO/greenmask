The `RandomTitleMale` transformer is developed to populate specified database columns with random male titles. This tool is essential for applications that require the simulation of user profiles, testing gender-specific features, or anonymizing user data in datasets.

## Parameters

| Name       | Description                                           | Default | Required | Supported DB types |
|------------|-------------------------------------------------------|---------|----------|--------------------|
| column     | The name of the column to be affected                |         | Yes      | text, varchar      |
| keep_null  | Indicates whether NULL values should be preserved   | `false` | No       | -                  |

## Description

The `RandomTitleMale` transformer utilizes a predefined list of male titles (e. g., Mr., Dr., Prof.) to inject random male titles into the designated database column. This feature allows for the creation of diverse and realistic user profiles by simulating a variety of male titles without using real user data.

## Example: Populate random male titles for the `user_profile` table

This example outlines configuring the `RandomTitleMale` transformer to populate the `title` column in a `user_profiles` table with random male titles. It is a straightforward method for simulating a variety of user profiles with male titles.

```yaml title="RandomTitleMale transformer example"
- schema: "public"
  name: "user_profiles"
  transformers:
    - name: "RandomTitleMale"
      params:
        column: "title"
        keep_null: false
```

In this configuration, the `title` column will be updated with random male titles for each user profile entry, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, existing NULL values in the column will be preserved, ensuring the integrity of records where title information is not applicable or provided.
