The `RandomParagraph` transformer is crafted to populate specified database columns with random paragraphs. This utility
is indispensable for applications that require the generation of extensive textual content, such as simulating articles,
enhancing textual datasets for NLP systems, or anonymizing textual content in databases.

## Parameters

| Name      | Description                                       | Default | Required | Supported DB types                  |
|-----------|---------------------------------------------------|---------|----------|-------------------------------------|
| column    | The name of the column to be affected             |         | Yes      | text, varchar, char, bpchar, citext |
| keep_null | Indicates whether NULL values should be preserved | `false` | No       | -                                   |

## Description

Employing sophisticated text generation algorithms or libraries, the `RandomParagraph` transformer generates random
paragraphs, injecting them into the designated database column. This transformer is designed to create varied and
plausible paragraphs that simulate real-world textual content, providing a valuable tool for database enrichment,
testing, and anonymization.

## Example: Populate random paragraphs for the `articles` table

This example illustrates configuring the `RandomParagraph` transformer to populate the `body` column in an `articles`
table with random paragraphs. It is an effective way to simulate diverse article content for development, testing, or
demonstration purposes.

```yaml title="RandomParagraph transformer example"
- schema: "public"
  name: "articles"
  transformers:
    - name: "RandomParagraph"
      params:
        column: "body"
        keep_null: false
```

With this setup, the `body` column will receive random paragraphs for each entry, replacing any existing non-NULL
values. Setting the `keep_null` parameter to `true` allows for the preservation of existing NULL values within the
column, maintaining the integrity of records where article content is not applicable or provided.
