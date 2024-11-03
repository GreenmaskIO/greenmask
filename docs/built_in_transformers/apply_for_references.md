# Apply for References

## Description

Using `apply_for_references`, you can apply transformations to columns involved in a primary key or in tables with a
foreign key that references that column. This simplifies the transformation process by requiring you to define the
transformation only on the primary key column, which will then be applied to all tables referencing that column.

The transformer must support `hash` engine and the `hash` engin must be set in the configuration file.

## End-to-End Identifiers

End-to-end identifiers in databases are unique identifiers that are consistently used across multiple tables in a
relational database schema, allowing for a seamless chain of references from one table to another. These identifiers
typically serve as primary keys in one table and are propagated as foreign keys in other tables, creating a direct,
traceable link from one end of a data relationship to the other.

Greenmask can detect end-to-end identifiers and apply transformations across the entire sequence of tables. These
identifiers are detected when the following condition is met: the foreign key serves as both a primary key and a foreign
key in the referenced table.

## Limitations

- The transformation must be deterministic.
- The transformation condition will not be applied to the referenced column.
- Not all transformers support `apply_for_references`

!!! warning
    
    We do not recommend using `apply_for_references` with transformation conditions, as these conditions are not 
    inherited by transformers on the referenced columns. This may lead to inconsistencies in the data.

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

## Example 1. Simple table references

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

## Example 2. Tables with end-to-end identifiers

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
