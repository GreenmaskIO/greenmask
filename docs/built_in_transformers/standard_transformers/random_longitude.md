The `RandomLongitude` transformer is designed to generate random longitude values for specified database columns, enhancing datasets with realistic geographic coordinates suitable for a wide range of applications, from testing location-based services to anonymizing real geographic data.

## Parameters

| Name      | Description                                          | Default | Required | Supported DB types |
|-----------|------------------------------------------------------|---------|----------|--------------------|
| column    | The name of the column to be affected               |         | Yes      | float4, float8, numeric |
| keep_null | Indicates whether NULL values should be preserved  | `false` | No       | -                  |

## Description

The `RandomLongitude` transformer leverages the `faker` library to produce random longitude values within the globally accepted range of -180 to +180 degrees. This flexibility allows the transformer to be applied to any column intended for storing longitude data, providing a simple yet powerful tool for introducing randomized longitude coordinates into a database.

## Example: Populate random longitude for the `locations` table

This example shows how to use the `RandomLongitude` transformer to fill the `longitude` column in the `locations` table with random longitude values.

```yaml title="RandomLongitude transformer example"
- schema: "public"
  name: "locations"
  transformers:
    - name: "RandomLongitude"
      params:
        column: "longitude"
        keep_null: false
```

This setup ensures that all entries in the `longitude` column receive a random longitude value, replacing any existing non-NULL values. If `keep_null` is set to `true`, then existing NULL values in the column will remain unchanged.
