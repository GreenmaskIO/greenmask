The `RandomYearString` transformer is designed to populate specified database columns with random year strings. It is
ideal for scenarios that require the representation of years without specific dates, such as manufacturing years of
products, birth years of users, or any other context where only the year is relevant.

## Parameters

| Name      | Description                                       | Default | Required | Supported DB types                                             |
|-----------|---------------------------------------------------|---------|----------|----------------------------------------------------------------|
| column    | The name of the column to be affected             |         | Yes      | text, varchar, char, bpchar, citext, int2, int4, int8, numeric |
| keep_null | Indicates whether NULL values should be preserved | `false` | No       | -                                                              |

## Description

The `RandomYearString` transformer leverages the `faker` library to generate strings representing random years. This
allows for the easy generation of year data in a string format, adding versatility and realism to datasets that need to
simulate or anonymize year-related information.

## Example: Populate random year strings for the `products` table

This example shows how to use the `RandomYearString` transformer to fill the `manufacturing_year` column in the
`products` table with random year strings, simulating the diversity of manufacturing dates.

```yaml title="RandomYearString transformer example"
- schema: "public"
  name: "products"
  transformers:
    - name: "RandomYearString"
      params:
        column: "manufacturing_year"
        keep_null: false
```

In this configuration, the `manufacturing_year` column will be populated with random year strings, replacing any
existing non-NULL values. If `keep_null` is set to `true`, then existing NULL values in the column will be preserved.
