The `RandomUsername` transformer is crafted to populate specified database columns with random usernames. This utility is crucial for applications that require the simulation of user data, testing systems with user login functionality, or anonymizing real usernames in datasets.

## Parameters

| Name      | Description                                          | Default | Required | Supported DB types |
|-----------|------------------------------------------------------|---------|----------|--------------------|
| column    | The name of the column to be affected               |         | Yes      | text, varchar      |
| keep_null | Indicates whether NULL values should be preserved  | `false` | No       | -                  |

## Description

By employing sophisticated algorithms or libraries capable of generating believable usernames, the `RandomUsername` transformer introduces random usernames into the specified database column. Each generated username is designed to be unique and plausible, incorporating a mix of letters, numbers, and possibly special characters, depending on the generation logic used.

## Example: Populate random usernames for the `user_accounts` table

This example demonstrates configuring the `RandomUsername` transformer to populate the `username` column in a `user_accounts` table with random usernames. This setup is ideal for creating a diverse and realistic user base for development, testing, or demonstration purposes.

```yaml title="RandomUsername transformer example"
- schema: "public"
  name: "user_accounts"
  transformers:
    - name: "RandomUsername"
      params:
        column: "username"
        keep_null: false
```

In this configuration, every entry in the `username` column will be updated with a random username, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, then the transformer will preserve existing NULL values within the column, maintaining data integrity where usernames are not applicable or available.
