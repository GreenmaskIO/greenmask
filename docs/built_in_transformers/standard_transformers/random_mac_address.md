The `RandomMacAddress` transformer is developed to populate specified database columns with random MAC (Media Access Control) addresses. This transformer is particularly useful for simulating network hardware data, testing applications that process MAC addresses, or anonymizing real network device identifiers in datasets.

## Parameters

| Name      | Description                                        | Default | Required | Supported DB types               |
|-----------|----------------------------------------------------|---------|----------|----------------------------------|
| column    | The name of the column to be affected             |         | Yes      | text, varchar, macaddr, macaddr8 |
| keep_null | Indicates whether NULL values should be preserved | `false` | No       | -                                |

## Description

Utilizing a sophisticated algorithm or library for generating MAC address strings, the `RandomMacAddress` transformer injects random MAC addresses into the designated database column. Each generated MAC address follows the standard format of six groups of two hexadecimal digits, separated by colons (e. g., "01:23:45:67:89:ab"), ensuring plausible values for network device simulations.

## Example: Populate random MAC addresses for the `network_devices` table

This example shows how to configure the `RandomMacAddress` transformer to populate the `mac_address` column in
a `network_devices` table with random MAC addresses, enhancing the realism of simulated network device data.

```yaml title="RandomMacAddress transformer example"
- schema: "public"
  name: "network_devices"
  transformers:
    - name: "RandomMacAddress"
      params:
        column: "mac_address"
        keep_null: false
```

With this configuration, every entry in the `mac_address` column will be assigned a random MAC address, replacing any existing non-NULL values. Setting the `keep_null` parameter to `true` allows the preservation of existing NULL values within the column, accommodating scenarios where MAC address data may be intentionally absent.
