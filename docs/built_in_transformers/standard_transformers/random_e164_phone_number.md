The `RandomE164PhoneNumber` transformer is developed to populate specified database columns with random E.164 phone numbers. This tool is essential for applications requiring the simulation of contact information, testing phone number validation systems, or anonymizing phone number data in datasets while focusing on E.164 numbers.

## Parameters

| Name       | Description                                          | Default | Required | Supported DB types |
|------------|------------------------------------------------------|---------|----------|--------------------|
| column     | The name of the column to be affected               |         | Yes      | text, varchar      |
| keep_null  | Indicates whether NULL values should be preserved  | `false` | No       | -                  |

## Description

The `RandomE164PhoneNumber` transformer utilizes algorithms capable of generating random E.164 phone numbers with the standard international format and injects them into the designated database column. This feature allows for the creation of diverse and realistic contact information in datasets for development, testing, or data anonymization purposes.

## Example: Populate random E.164 phone numbers for the `contact_information` table

This example demonstrates configuring the `RandomE164PhoneNumber` transformer to populate the `phone_number` column in the `contact_information` table with random E.164 phone numbers. It is an effective method for simulating a variety of contact information entries with E.164 numbers.

```yaml title="RandomE164PhoneNumber transformer example"
- schema: "public"
  name: "contact_information"
  transformers:
    - name: "RandomE164PhoneNumber"
      params:
        column: "phone_number"
        keep_null: false
```

In this configuration, the `phone_number` column will be updated with random E.164 phone numbers for each contact information entry, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, existing NULL values in the column will be preserved, ensuring the integrity of records where E.164 phone number information is not applicable or provided.
