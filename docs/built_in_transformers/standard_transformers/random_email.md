The `RandomEmail` transformer is designed to populate specified database columns with random email addresses. This transformer is especially useful for applications requiring the simulation of user contact data, testing email functionalities, or anonymizing real user email addresses in datasets.

## Parameters

| Name      | Description                                          | Default | Required | Supported DB types |
|-----------|------------------------------------------------------|---------|----------|--------------------|
| column    | The name of the column to be affected               |         | Yes      | text, varchar      |
| keep_null | Indicates whether NULL values should be preserved  | `false` | No       | -                  |

## Description

Leveraging a method or library capable of generating plausible email address strings, the `RandomEmail` transformer injects random email addresses into the specified database column. It generates email addresses with varied domains and user names, offering a realistic range of email patterns suitable for filling user tables, contact lists, or any other dataset requiring email addresses without utilizing real user data.

## Example: Populate random email addresses for the `users` table

This example illustrates configuring the `RandomEmail` transformer to populate the `email` column in the `users` table with random email addresses, thereby simulating a diverse user base without exposing real contact information.

```yaml title="RandomEmail transformer example"
- schema: "public"
  name: "users"
  transformers:
    - name: "RandomEmail"
      params:
        column: "email"
        keep_null: false
```

In this setup, the `email` column will receive random email addresses for each entry, replacing any existing non-NULL values. If `keep_null` is set to `true`, then the transformer will preserve existing NULL values, maintaining the integrity of the original dataset where email information is absent.
