The `RandomAmountWithCurrency` transformer is specifically designed to populate specified database columns with random financial amounts accompanied by currency codes. Ideal for applications requiring the simulation of financial transactions, this utility enhances the realism of financial datasets by introducing variability in amounts and currencies.

## Parameters

| Name       | Description                                        | Default | Required | Supported DB types |
|------------|----------------------------------------------------|---------|----------|--------------------|
| column     | The name of the column to be affected              |         | Yes      | text, varchar      |
| keep_null  | Indicates whether NULL values should be preserved  | `false` | No       | -                  |

## Description

This transformer automatically generates random financial amounts along with corresponding global currency codes (e.g., `250.00 USD`, `300.00 EUR`), injecting them into the designated database column. It provides a straightforward solution for populating financial records with varied and realistic data, suitable for testing payment systems, data anonymization, and simulation of economic models.

## Example: Populate the `payments` table with random amounts and currencies

This example shows how to configure the `RandomAmountWithCurrency` transformer to populate the `payment_details` column in the `payments` table with random amounts and currencies. It is an effective approach to simulating a diverse range of payment transactions.

```yaml title="RandomAmountWithCurrency transformer example"
- schema: "public"
  name: "payments"
  transformers:
    - name: "RandomAmountWithCurrency"
      params:
        column: "payment_details"
        keep_null: false
```

In this setup, the `payment_details` column will be updated with random financial amounts and currency codes for each entry, replacing any existing non-NULL values. The `keep_null` parameter, when set to `true`, ensures that existing NULL values in the column remain unchanged, preserving the integrity of records without specified payment details.
