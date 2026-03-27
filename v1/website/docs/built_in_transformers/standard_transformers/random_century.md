The `RandomCentury` transformer is crafted to populate specified database columns with random century values. It is
ideal for applications that require historical data simulation, such as generating random years within specific
centuries for historical databases, testing datasets with temporal dimensions, or anonymizing dates in historical
research data.

## Parameters

| Name      | Description                                       | Default | Required | Supported DB types                  |
|-----------|---------------------------------------------------|---------|----------|-------------------------------------|
| column    | The name of the column to be affected             |         | Yes      | text, varchar, char, bpchar, citext |
| keep_null | Indicates whether NULL values should be preserved | `false` | No       | -                                   |

## Description

The `RandomCentury` transformer utilizes an algorithm or a library function (hypothetical in this context) to generate
random century values. Each value represents a century (e.g., `19th`, `20th`, `21st`), providing a broad temporal range
that can be used to enhance datasets requiring a distribution across different historical periods without the need for
precise date information.

## Example: Populate random centuries for the `historical_artifacts` table

This example shows how to configure the `RandomCentury` transformer to populate the `century` column in a
`historical_artifacts` table with random century values, adding an element of variability and historical context to the
dataset.

```yaml title="RandomCentury transformer example"
- schema: "public"
  name: "historical_artifacts"
  transformers:
    - name: "RandomCentury"
      params:
        column: "century"
        keep_null: false
```

In this setup, the `century` column will be filled with random century values, replacing any existing non-NULL values.
If the `keep_null` parameter is set to `true`, then existing NULL values in the column will remain untouched, preserving
the original dataset's integrity where no temporal data is available.
