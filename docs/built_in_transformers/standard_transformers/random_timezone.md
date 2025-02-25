The `RandomTimezone` transformer is designed to populate specified database columns with random timezone strings. This
transformer is particularly useful for applications that require the simulation of global user data, testing of
timezone-related functionalities, or anonymizing real user timezone information in datasets.

## Parameters

| Name      | Description                                       | Default | Required | Supported DB types                  |
|-----------|---------------------------------------------------|---------|----------|-------------------------------------|
| column    | The name of the column to be affected             |         | Yes      | text, varchar, char, bpchar, citext |
| keep_null | Indicates whether NULL values should be preserved | `false` | No       | -                                   |

## Description

Utilizing a comprehensive library or algorithm for generating timezone data, the `RandomTimezone` transformer provides
random timezone strings (e. g., "America/New_York", "Europe/London") for database columns. This feature enables the
creation of diverse and realistic datasets by simulating timezone information for user profiles, event timings, or any
other data requiring timezone context.

## Example: Populate random timezone strings for  the `user_accounts` table

This example demonstrates how to configure the `RandomTimezone` transformer to populate the `timezone` column in the
`user_accounts` table with random timezone strings, enhancing the dataset with varied global user representations.

```yaml title="RandomTimezone transformer example"
- schema: "public"
  name: "user_accounts"
  transformers:
    - name: "RandomTimezone"
      params:
        column: "timezone"
        keep_null: false
```

With this configuration, every entry in the `timezone` column will be updated with a random timezone string, replacing
any existing non-NULL values. If the `keep_null` parameter is set to `true`, existing NULL values within the column will
remain unchanged, preserving the integrity of rows without specified timezone data.
