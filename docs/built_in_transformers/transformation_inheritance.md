# Transformation Inheritance

## Description

If you have partitioned tables or want to apply a transformation to a primary key and propagate it to all tables
referencing that column, you can do so with Greenmask.

## Apply for inherited

Using `apply_for_inherited`, you can apply transformations to all partitions of a partitioned table, including any
subpartitions.

### Configuration conflicts

When a partition has a transformation defined manually via config, and `apply_for_inherited` is set on the parent table,
Greenmask will merge both the inherited and manually defined configurations. The manually defined transformation will
execute last, giving it higher priority.

If this situation occurs, you will see the following information in the log:

```json
{
  "level": "info",
  "ParentTableSchema": "public",
  "ParentTableName": "sales",
  "ChildTableSchema": "public",
  "ChildTableName": "sales_2022_feb",
  "ChildTableConfig": [
    {
      "name": "RandomDate",
      "params": {
        "column": "sale_date",
        "engine": "random",
        "max": "2005-01-01",
        "min": "2001-01-01"
      }
    }
  ],
  "time": "2024-11-03T22:14:01+02:00",
  "message": "config will be merged: found manually defined transformers on the partitioned table"
}
```

## Apply for references

Using `apply_for_references`, you can apply transformations to columns involved in a primary key or in tables with a
foreign key that references that column. This simplifies the transformation process by requiring you to define the
transformation only on the primary key column, which will then be applied to all tables referencing that column.

The transformer must be deterministic or support `hash` engine and the `hash` engin must be set in the
configuration file.

List of transformers that supports `apply_for_references`:

* Hash
* NoiseDate
* NoiseFloat
* NoiseInt
* NoiseNumeric
* RandomBool
* RandomDate
* RandomEmail
* RandomFloat
* RandomInt
* RandomIp
* RandomMac
* RandomNumeric
* RandomString
* RandomUuid
* RandomUnixTimestamp

### End-to-End Identifiers

End-to-end identifiers in databases are unique identifiers that are consistently used across multiple tables in a
relational database schema, allowing for a seamless chain of references from one table to another. These identifiers
typically serve as primary keys in one table and are propagated as foreign keys in other tables, creating a direct,
traceable link from one end of a data relationship to the other.

Greenmask can detect end-to-end identifiers and apply transformations across the entire sequence of tables. These
identifiers are detected when the following condition is met: the foreign key serves as both a primary key and a foreign
key in the referenced table.

### Configuration conflicts

When on the referenced column a transformation is manually defined via config, and the `apply_for_references` is set on
parent table, the transformation defined will be chosen and the inherited transformation will be ignored. You will
receive a `INFO` message in the logs.

```json
{
  "level": "info",
  "TransformerName": "RandomInt",
  "ParentTableSchema": "public",
  "ParentTableName": "tablea",
  "ChildTableSchema": "public",
  "ChildTableName": "tablec",
  "ChildColumnName": "id2",
  "TransformerConfig": {
    "name": "RandomInt",
    "apply_for_references": true
  },
  "time": "2024-11-03T21:28:10+02:00",
  "message": "skipping apply transformer for reference: found manually configured transformer"
}
```

### Limitations

- The transformation must be deterministic.
- The transformation condition will not be applied to the referenced column.
- Not all transformers support `apply_for_references`

!!! warning

    We do not recommend using `apply_for_references` with transformation conditions, as these conditions are not 
    inherited by transformers on the referenced columns. This may lead to inconsistencies in the data.

## Example 1. Partitioned tables

In this example, we have a partitioned table `sales` that is partitioned by year and then by month. Each partition
contains a subset of data based on the year and month of the sale. The `sales` table has a primary key `sale_id` and is
partitioned by `sale_date`. The `sale_date` column is transformed using the `RandomDate` transformer.

```sql
CREATE TABLE sales
(
    sale_id   SERIAL         NOT NULL,
    sale_date DATE           NOT NULL,
    amount    NUMERIC(10, 2) NOT NULL
) PARTITION BY RANGE (EXTRACT(YEAR FROM sale_date));

-- Step 2: Create first-level partitions by year
CREATE TABLE sales_2022 PARTITION OF sales
    FOR VALUES FROM (2022) TO (2023)
    PARTITION BY LIST (EXTRACT(MONTH FROM sale_date));

CREATE TABLE sales_2023 PARTITION OF sales
    FOR VALUES FROM (2023) TO (2024)
    PARTITION BY LIST (EXTRACT(MONTH FROM sale_date));

-- Step 3: Create second-level partitions by month for each year, adding PRIMARY KEY on each partition

-- Monthly partitions for 2022
CREATE TABLE sales_2022_jan PARTITION OF sales_2022 FOR VALUES IN (1)
    WITH (fillfactor = 70);
CREATE TABLE sales_2022_feb PARTITION OF sales_2022 FOR VALUES IN (2);
CREATE TABLE sales_2022_mar PARTITION OF sales_2022 FOR VALUES IN (3);
-- Continue adding monthly partitions for 2022...

-- Monthly partitions for 2023
CREATE TABLE sales_2023_jan PARTITION OF sales_2023 FOR VALUES IN (1);
CREATE TABLE sales_2023_feb PARTITION OF sales_2023 FOR VALUES IN (2);
CREATE TABLE sales_2023_mar PARTITION OF sales_2023 FOR VALUES IN (3);
-- Continue adding monthly partitions for 2023...

-- Step 4: Insert sample data
INSERT INTO sales (sale_date, amount)
VALUES ('2022-01-15', 100.00);
INSERT INTO sales (sale_date, amount)
VALUES ('2022-02-20', 150.00);
INSERT INTO sales (sale_date, amount)
VALUES ('2023-03-10', 200.00);
```

To transform the `sale_date` column in the `sales` table and all its partitions, you can use the following
configuration:

```yaml
- schema: public
  name: sales
  apply_for_inherited: true
  transformers:
    - name: RandomDate
      params:
        min: "2022-01-01"
        max: "2022-03-01"
        column: "sale_date"
        engine: "random"
```

## Example 2. Simple table references

This is ordinary table references where the primary key of the `users` table is referenced in the `orders` table.

```sql
-- Enable the extension for UUID generation (if not enabled)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE users
(
    user_id  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(50) NOT NULL
);

CREATE TABLE orders
(
    order_id   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id    UUID REFERENCES users (user_id),
    order_date DATE NOT NULL
);

INSERT INTO users (username)
VALUES ('john_doe');
INSERT INTO users (username)
VALUES ('jane_smith');

INSERT INTO orders (user_id, order_date)
VALUES ((SELECT user_id FROM users WHERE username = 'john_doe'), '2024-10-31'),
       ((SELECT user_id FROM users WHERE username = 'jane_smith'), '2024-10-30');
```

To transform the `username` column in the `users` table, you can use the following configuration:

```yaml
- schema: public
  name: users
  apply_for_inherited: true
  transformers:
    - name: RandomUuid
      apply_for_references: true
      params:
        column: "user_id"
        engine: "hash"
```

This will apply the `RandomUuid` transformation to the `user_id` column in the `orders` table automatically.

## Example 3. References on tables with end-to-end identifiers

In this example, we have three tables: `tablea`, `tableb`, and `tablec`. All tables have a composite primary key.
In the tables `tableb` and `tablec`, the primary key is also a foreign key that references the primary key of `tablea`.
This means that all PKs are end-to-end identifiers.

```sql
CREATE TABLE tablea
(
    id1  INT,
    id2  INT,
    data VARCHAR(50),
    PRIMARY KEY (id1, id2)
);

CREATE TABLE tableb
(
    id1    INT,
    id2    INT,
    detail VARCHAR(50),
    PRIMARY KEY (id1, id2),
    FOREIGN KEY (id1, id2) REFERENCES tablea (id1, id2) ON DELETE CASCADE
);

CREATE TABLE tablec
(
    id1         INT,
    id2         INT,
    description VARCHAR(50),
    PRIMARY KEY (id1, id2),
    FOREIGN KEY (id1, id2) REFERENCES tableb (id1, id2) ON DELETE CASCADE
);

INSERT INTO tablea (id1, id2, data)
VALUES (1, 1, 'Data A1'),
       (2, 1, 'Data A2'),
       (3, 1, 'Data A3');

INSERT INTO tableb (id1, id2, detail)
VALUES (1, 1, 'Detail B1'),
       (2, 1, 'Detail B2'),
       (3, 1, 'Detail B3');

INSERT INTO tablec (id1, id2, description)
VALUES (1, 1, 'Description C1'),
       (2, 1, 'Description C2'),
       (3, 1, 'Description C3');
```

To transform the `data` column in `tablea`, you can use the following configuration:

```yaml
- schema: public
  name: "tablea"
  apply_for_inherited: true
  transformers:
    - name: RandomInt
      apply_for_references: true
      params:
        min: 0
        max: 100
        column: "id1"
        engine: "hash"
    - name: RandomInt
      apply_for_references: true
      params:
        min: 0
        max: 100
        column: "id2"
        engine: "hash"
```

This will apply the `RandomInt` transformation to the `id1` and `id2` columns in `tableb` and `tablec` automatically.
