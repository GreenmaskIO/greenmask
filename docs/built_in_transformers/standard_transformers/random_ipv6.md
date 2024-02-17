The `RandomIPv6` transformer is engineered to populate specified database columns with random IPv6 addresses. This tool is particularly useful for simulating modern network environments, testing systems that operate with IPv6 addresses, or anonymizing datasets containing real IPv6 addresses.

## Parameters

| Name      | Description                                          | Default | Required | Supported DB types  |
|-----------|------------------------------------------------------|---------|----------|---------------------|
| column    | The name of the column to be affected               |         | Yes      | text, varchar, inet |
| keep_null | Indicates whether NULL values should be preserved  | `false` | No       | -                   |

## Description

Employing advanced algorithms or libraries capable of generating IPv6 address strings, the `RandomIPv6` transformer introduces random IPv6 addresses into the specified database column. IPv6 addresses, represented as eight groups of four hexadecimal digits separated by colons (e. g., "2001:0db8:85a3:0000:0000:8a2e:0370:7334"), provide a vast range of possible addresses, reflecting the extensive addressing capacity of the IPv6 standard.

## Example: Populate random IPv6 addresses for the `devices` table

This example illustrates configuring the `RandomIPv6` transformer to populate the `device_ip` column in the `devices` table with random IPv6 addresses, enhancing the dataset with a broad spectrum of network addresses for development, testing, or data protection purposes.

```yaml title="RandomIPv6 transformer example"
- schema: "public"
  name: "devices"
  transformers:
    - name: "RandomIPv6"
      params:
        column: "device_ip"
        keep_null: false
```

This configuration ensures that the `device_ip` column receives random IPv6 addresses for each entry, replacing any existing non-NULL values. Setting the `keep_null` parameter to `true` allows for the preservation of existing NULL values within the column, maintaining the integrity of records where IP address information is not applicable or available.
