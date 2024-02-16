The `RandomTollFreePhoneNumber` transformer is designed to populate specified database columns with random toll-free phone numbers. This tool is essential for applications requiring the simulation of contact information, testing phone number validation systems, or anonymizing phone number data in datasets while focusing on toll-free numbers.

## Parameters

| Name       | Description                                          | Default | Required | Supported DB types |
|------------|------------------------------------------------------|---------|----------|--------------------|
| column     | The name of the column to be affected               |         | Yes      | text, varchar      |
| keep_null  | Indicates whether NULL values should be preserved  | `false` | No       | -                  |

## Description

The `RandomTollFreePhoneNumber` transformer utilizes algorithms capable of generating random toll-free phone numbers with various formats and injects them into the designated database column. This feature allows for the creation of diverse and realistic toll-free contact information in datasets for development, testing, or data anonymization purposes.

## Example: Populate random toll-free phone numbers for the `contact_information` table

This example demonstrates configuring the `RandomTollFreePhoneNumber` transformer to populate the `phone_number` column in the `contact_information` table with random toll-free phone numbers. It is an effective method for simulating a variety of contact information entries with toll-free numbers.

```yaml title="RandomTollFreePhoneNumber transformer example"
- schema: "public"
  name: "contact_information"
  transformers:
    - name: "RandomTollFreePhoneNumber"
      params:
        column: "phone_number"
        keep_null: false
```

In this configuration, the `phone_number` column will be updated with random toll-free phone numbers for each contact information entry, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, existing NULL values in the column will be preserved, ensuring the integrity of records where toll-free phone number information is not applicable or provided.
