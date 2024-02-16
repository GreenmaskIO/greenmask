The `RandomCCNumber` transformer is specifically designed to populate specified database columns with random credit card numbers. This utility is crucial for applications that involve simulating financial data, testing payment systems, or anonymizing real credit card numbers in datasets.

## Parameters

| Name       | Description                                           | Default | Required | Supported DB types |
|------------|-------------------------------------------------------|---------|----------|--------------------|
| column     | The name of the column to be affected                |         | Yes      | text, varchar      |
| keep_null  | Indicates whether NULL values should be preserved   | `false` | No       | -                  |

## Description

By leveraging algorithms capable of generating plausible credit card numbers that adhere to standard credit card validation rules (such as the Luhn algorithm), the `RandomCCNumber` transformer injects random credit card numbers into the designated database column. This approach ensures the generation of credit card numbers that are realistic for testing and development purposes, without compromising real-world applicability and security.

## Example: Populate random credit card numbers for the `payment_information` table

This example demonstrates configuring the `RandomCCNumber` transformer to populate the `cc_number` column in the `payment_information` table with random credit card numbers. It is an effective strategy for creating a realistic set of payment data for application testing or data anonymization.

```yaml title="RandomCCNumber transformer example"
- schema: "public"
  name: "payment_information"
  transformers:
    - name: "RandomCCNumber"
      params:
        column: "cc_number"
        keep_null: false
```

With this setup, the `cc_number` column will be updated with random credit card numbers for each entry, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, it will ensure that existing NULL values in the column are preserved, maintaining the integrity of records where credit card information is not applicable or available.
