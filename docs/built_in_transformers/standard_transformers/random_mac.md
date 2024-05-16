The `RandomMac` transformer is designed to populate specified database columns with random MAC addresses.

## Parameters

| Name      | Description                                                                                                                                                                                                                                                                                                                 | Default  | Required | Supported DB types  |
|-----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|----------|---------------------|
| column    | The name of the column to be affected                                                                                                                                                                                                                                                                                       |          | Yes      | text, varchar, inet |
| keep_original_vendor | Should the Individual/Group (I/G) and Universal/Local (U/L) bits be preserved from the original MAC address.                                                                                                                                                                                                                | `false`  | No       | -                   |
| cast_type       | Param wich allow to set Individual/Group (I/G) bit in MAC Address. Allowed values [any, individual, group]. If this value is `individual`, the address is meant for a single device (unicast). If it is `group`, the address is for a group of devices, which can include multicast and broadcast addresses.                | any      | No       |                     |
| management_type | Param wich allow to set Universal/Local (U/L) bit in MAC Address. Allowed values [any, universal, local]. If this bit is `universal`, the address is universally administered (globally unique). If it is `local`, the address is locally administered (such as when set manually or programmatically on a network device). | any      | No       |                     |
| engine | The engine used for generating the values [random, hash]. Use hash for deterministic generation                                                                                                                                                                                                                             | `random` | No       |-                    |

## Description

TODO


## Example: Generate a Random MAC Address

This example demonstrates how to configure the RandomMac transformer to inject a random MAC address into the
mac_address column:

```sql title="Create table mac_addresses and insert data"
CREATE TABLE mac_addresses (
   id SERIAL PRIMARY KEY,
   device_name VARCHAR(50),
   mac_address MACADDR,
   description TEXT
);

INSERT INTO mac_addresses (device_name, mac_address, description) VALUES
  ('Device A', '00:1A:2B:3C:4D:5E', 'Description for Device A'),
  ('Device B', '01:2B:3C:4D:5E:6F', 'Description for Device B'),
  ('Device C', '02:3C:4D:5E:6F:70', 'Description for Device C'),
  ('Device D', '03:4D:5E:6F:70:71', 'Description for Device D'),
  ('Device E', '04:5E:6F:70:71:72', 'Description for Device E');

```

```yaml title="RandomPerson transformer example"
- schema: public
  name: mac_addresses
  transformers:
    - name: "RandomMac"
      params:
        column: "mac_address"
        engine: "random"
        cast_type: "any"
        management_type: "any"
```