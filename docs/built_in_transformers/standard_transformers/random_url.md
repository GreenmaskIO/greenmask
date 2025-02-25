The `RandomURL` transformer is designed to populate specified database columns with random URL (Uniform Resource
Locator) addresses. This tool is highly beneficial for simulating web content, testing applications that require URL
input, or anonymizing real web addresses in datasets.

## Parameters

| Name      | Description                                       | Default | Required | Supported DB types                  |
|-----------|---------------------------------------------------|---------|----------|-------------------------------------|
| column    | The name of the column to be affected             |         | Yes      | text, varchar, char, bpchar, citext |
| keep_null | Indicates whether NULL values should be preserved | `false` | No       | -                                   |

## Description

Utilizing advanced algorithms or libraries for generating URL strings, the `RandomURL` transformer injects random,
plausible URLs into the designated database column. Each generated URL is structured to include the protocol (e. g., "
http://", "https://"), domain name, and path, offering a realistic range of web addresses for various applications.

## Example: Populate random URLs for the `webpages` table

This example illustrates how to configure the `RandomURL` transformer to populate the `page_url` column in a `webpages`
table with random URLs, providing a broad spectrum of web addresses for testing or data simulation purposes.

```yaml title="RandomURL transformer example"
- schema: "public"
  name: "webpages"
  transformers:
    - name: "RandomURL"
      params:
        column: "page_url"
        keep_null: false
```

With this configuration, the `page_url` column will be filled with random URLs for each entry, replacing any existing
non-NULL values. Setting the `keep_null` parameter to `true` allows for the preservation of existing NULL values within
the column, accommodating scenarios where URL data may be intentionally omitted.
