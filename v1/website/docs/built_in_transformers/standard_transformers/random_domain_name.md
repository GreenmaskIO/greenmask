The `RandomDomainName` transformer is designed to populate specified database columns with random domain names. This
tool is invaluable for simulating web data, testing applications that interact with domain names, or anonymizing real
domain information in datasets.

## Parameters

| Name      | Description                                       | Default | Required | Supported DB types                  |
|-----------|---------------------------------------------------|---------|----------|-------------------------------------|
| column    | The name of the column to be affected             |         | Yes      | text, varchar, char, bpchar, citext |
| keep_null | Indicates whether NULL values should be preserved | `false` | No       | -                                   |

## Description

By leveraging an algorithm or library capable of generating believable domain names, the `RandomDomainName` transformer
introduces random domain names into the specified database column. Each generated domain name includes a second-level
domain (SLD) and a top-level domain (TLD), such as "example.com" or "website.org," providing a wide range of plausible
web addresses for database enrichment.

## Example: Populate random domain names for the `websites` table

This example demonstrates configuring the `RandomDomainName` transformer to populate the `domain` column in the
`websites` table with random domain names. This approach facilitates the creation of a diverse and realistic set of web
addresses for testing, simulation, or data anonymization purposes.

```yaml title="RandomDomainName transformer example"
- schema: "public"
  name: "websites"
  transformers:
    - name: "RandomDomainName"
      params:
        column: "domain"
        keep_null: false
```

In this setup, the `domain` column will be updated with random domain names for each entry, replacing any existing
non-NULL values. If `keep_null` is set to `true`, the transformer will preserve existing NULL values in the column,
maintaining the integrity of data where domain information is not applicable.
