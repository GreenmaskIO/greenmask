Generates real addresses for specified database columns using the `faker` library. It supports customization of the generated address format through Go templates.

## Parameters

| Name    | Properties | Description                                                                          | Default | Required | Supported DB types |
|---------|------------|--------------------------------------------------------------------------------------|---------|----------|--------------------|
| columns |            | Specifies the affected column names along with additional properties for each column |         | Yes      | Various            |
| ∟       | name       | The name of the column to be affected                                                |         | Yes      | string             |
| ∟       | template   | A Go template string for formatting real address attributes                          |         | Yes      | string             |
| ∟       | keep_null  | Indicates whether NULL values should be preserved                                    |         | No       | bool               |

### Template value descriptions

The `template` parameter allows for the injection of real address attributes into a customizable template. The following values can be included in your template:

- `{{.Address}}` — street address or equivalent
- `{{.City}}` — city name
- `{{.State}}` — state, province, or equivalent region name
- `{{.PostalCode}}` — postal or ZIP code
- `{{.Latitude}}` — geographic latitude
- `{{.Longitude}}` — geographic longitude

These placeholders can be combined and formatted as desired within the template string to generate custom address formats.

## Description

The `RealAddress` transformer uses the `faker` library to generate realistic addresses, which can then be formatted according to a specified template and applied to selected columns in a database. It allows for the generated addresses to replace existing values or to preserve NULL values, based on the transformer's configuration.

## Example: Generate Real addresses for the `employee` table

This example shows how to configure the `RealAddress` transformer to generate real addresses for the `address` column in the `employee` table, using a custom format.

```yaml title="RealAddress transformer example"
- schema: "humanresources"
  name: "employee"
  transformers:
    - name: "RealAddress"
      params:
        columns:
          - name: "address"
            template: "{{.Address}}, {{.City}}, {{.State}} {{.PostalCode}}"
            keep_null: false
```

This configuration will generate real addresses with the format "Street address, city, state postal code" and apply them to the `address` column, replacing any existing non-NULL values.
