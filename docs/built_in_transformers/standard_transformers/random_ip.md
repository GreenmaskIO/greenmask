The `RandomIp` transformer is designed to populate specified database columns with random IP v4 or V6 addresses.
This utility is essential for applications requiring the simulation of network data, testing systems that utilize IP
addresses, or anonymizing real IP addresses in datasets.

## Parameters

| Name   | Description                                                                                         | Default  | Required | Supported DB types  |
|--------|-----------------------------------------------------------------------------------------------------|----------|----------|---------------------|
| column | The name of the column to be affected                                                               |          | Yes      | text, varchar, inet |
| subnet | Subnet for generating random ip in V4 or V6 format                                                  |          | Yes      | -                   |
| engine | The engine used for generating the values [`random`, `hash`]. Use hash for deterministic generation | `random` | No       | -                   |

## Dynamic parameters

| Name   | Supported types     |
|--------|---------------------|
| subnet | cidr, text, varchar |

## Description

Utilizing a robust algorithm or library for generating IP addresses, the `RandomIp` transformer injects random IPv4
or IPv6 addresses into the designated database column, depending on the provided subnet. The transformer automatically
detects whether to generate an IPv4 or IPv6 address based on the subnet version specified.

## Example: Generate a Random IPv4 Address for a 192.168.1.0/24 Subnet

This example demonstrates how to configure the RandomIp transformer to inject a random IPv4 address into the
ip_address column for entries in the `192.168.1.0/24` subnet:

```sql title="Create table ip_networks and insert data"
CREATE TABLE ip_networks
(
    id         SERIAL PRIMARY KEY,
    ip_address INET,
    network    CIDR
);

INSERT INTO ip_networks (ip_address, network)
VALUES ('192.168.1.10', '192.168.1.0/24'),
       ('10.0.0.5', '10.0.0.0/16'),
       ('172.16.254.3', '172.16.0.0/12'),
       ('192.168.100.14', '192.168.100.0/24'),
       ('2001:0db8:85a3:0000:0000:8a2e:0370:7334', '2001:0db8:85a3::/64'); -- An IPv6 address and network

```

```yaml title="RandomPerson transformer example"
- schema: public
  name: ip_networks
  transformers:
    - name: "RandomIp"
      params:
        subnet: "192.168.1.0/24"
        column: "ip_address"
        engine: "random"
```

Result:

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>ip_address</td><td><span style="color:green">192.168.1.10</span></td><td><span style="color:red">192.168.1.28</span></td>
</tr>
</table>

## Example: Generate a Random IP Based on the Dynamic Subnet Parameter

This configuration illustrates how to use the RandomIp transformer dynamically, where it reads the subnet information
from the network column of the database and generates a corresponding random IP address:

```yaml title="RandomPerson transformer example with dynamic mode"
- schema: public
  name: ip_networks
  transformers:
    - name: "RandomIp"
      params:
        column: "ip_address"
        engine: "random"
      dynamic_params:
        subnet:
          column: "network"
```

Result:

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>ip_address</td><td><span style="color:green">192.168.1.10</span></td><td><span style="color:red">192.168.1.111</span></td>
</tr>
</table>
