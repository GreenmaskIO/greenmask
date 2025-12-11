The `RandomPassword` transformer is designed to populate specified database columns with random passwords. This utility
is vital for applications that require the simulation of secure user data, testing systems with authentication
mechanisms, or anonymizing real passwords in datasets.

## Parameters

| Name      | Description                                       | Default | Required | Supported DB types                  |
|-----------|---------------------------------------------------|---------|----------|-------------------------------------|
| column    | The name of the column to be affected             |         | Yes      | text, varchar, char, bpchar, citext |
| keep_null | Indicates whether NULL values should be preserved | `false` | No       | -                                   |

## Description

Employing sophisticated password generation algorithms or libraries, the `RandomPassword` transformer injects random
passwords into the designated database column. This feature is particularly useful for creating realistic and secure
user password datasets for development, testing, or demonstration purposes.

## Example: Populate random passwords for the `user_accounts` table

This example demonstrates how to configure the `RandomPassword` transformer to populate the `password` column in the
`user_accounts` table with random passwords.

```yaml title="RandomPassword transformer example"
- schema: "public"
  name: "user_accounts"
  transformers:
    - name: "RandomPassword"
      params:
        column: "password"
        keep_null: false
```

In this configuration, every entry in the `password` column will be updated with a random password. Setting the
`keep_null` parameter to `true` will preserve existing NULL values in the column, accommodating scenarios where password
data may not be applicable.
