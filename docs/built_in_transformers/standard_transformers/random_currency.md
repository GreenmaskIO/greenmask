The `RandomCurrency` transformer is tailored to populate specified database columns with random currency codes. This tool is highly beneficial for applications involving the simulation of international financial data, testing currency conversion features, or anonymizing currency information in datasets.

## Parameters

| Name       | Description                                           | Default | Required | Supported DB types |
|------------|-------------------------------------------------------|---------|----------|--------------------|
| column     | The name of the column to be affected                |         | Yes      | text, varchar      |
| keep_null  | Indicates whether NULL values should be preserved   | `false` | No       | -                  |

## Description

Utilizing a comprehensive list of global currency codes (e.g., USD, EUR, JPY), the `RandomCurrency` transformer injects random currency codes into the designated database column. This feature allows for the creation of diverse and realistic financial transaction datasets by simulating a variety of currencies without relying on actual financial data.

## Example: Populate random currency codes for the `transactions` table

This example outlines configuring the `RandomCurrency` transformer to populate the `currency_code` column in a `transactions` table with random currency codes. It is an effective way to simulate international transactions across multiple currencies.

```yaml title="RandomCurrency transformer example"
- schema: "public"
  name: "transactions"
  transformers:
    - name: "RandomCurrency"
      params:
        column: "currency_code"
        keep_null: false
```

In this configuration, the `currency_code` column will be updated with random currency codes for each entry, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, existing NULL values in the column will be preserved, ensuring the integrity of records where currency data may not be applicable.
