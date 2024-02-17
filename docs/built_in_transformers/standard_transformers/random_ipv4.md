The `RandomIPv4` transformer is designed to populate specified database columns with random IPv4 addresses. This utility is essential for applications requiring the simulation of network data, testing systems that utilize IP addresses, or anonymizing real IP addresses in datasets.

## Parameters

| Name      | Description                                          | Default  | Required | Supported DB types  |
|-----------|------------------------------------------------------|----------|----------|---------------------|
| column    | The name of the column to be affected               |          | Yes      | text, varchar, inet |
| keep_null | Indicates whether NULL values should be preserved  | `false`  | No       | -                   |

## Description

Utilizing a robust algorithm or library for generating IPv4 address strings, the `RandomIPv4` transformer injects random IPv4 addresses into the designated database column. Each generated address follows the standard IPv4 format, consisting of four octets separated by dots (e. g., "192.168.1.1"), ensuring a wide range of plausible network addresses for various use cases.

## Example: Populate random IPv4 addresses for the `network_logs` table

This example shows how to configure the `RandomIPv4` transformer to populate the `source_ip` column in the `network_logs` table with random IPv4 addresses, simulating diverse network traffic sources for analysis or testing purposes.

```yaml title="RandomIPv4 transformer example"
- schema: "public"
  name: "network_logs"
  transformers:
    - name: "RandomIPv4"
      params:
        column: "source_ip"
        keep_null: false
```

With this setup, the `source_ip` column will be updated with random IPv4 addresses for each entry, replacing any existing non-NULL values. If the `keep_null` parameter is set to `true`, it will ensure that existing NULL values in the column are preserved, accommodating scenarios where IP address data may be intentionally omitted.
