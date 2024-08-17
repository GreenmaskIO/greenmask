# Database subset

Greenmask allows you to define a subset condition for filtering data during the dump process. This feature is useful
when you need to dump only a part of the database, such as a specific table or a set of tables. It automatically
ensures data consistency by including all related data from other tables that are required to maintain the integrity of
the subset.

## Detail

The subset is a list of SQL conditions that are applied to table. The conditions are combined with `AND` operator. **You
need** to specify the **schema**, **table** and **column** name when pointing out the column to filter by to avoid
ambiguity. The subset condition must be a valid SQL condition.

```yaml title="Subset condition example"
subset_conds:
  - 'person.businessentity.businessentityid IN (274, 290, 721, 852)'
```

## References with NULL values

For references that **do not have** `NOT NULL` constraints, Greenmask will automatically generate `LEFT JOIN` queries
with the appropriate conditions to ensure integrity checks. You can rely on Greenmask to handle such cases correctly—no
special configuration is needed, as it performs this automatically based on the introspected schema.

## Circular reference

Greenmask **supports circular** references between tables. You can define a subset condition for any table, and
Greenmask will automatically generate the appropriate queries for the table subset using recursive queries. The subset
system ensures data consistency by validating all records found through the recursive queries. If a record does not meet
the subset condition, it will be excluded along with its parent records, preventing constraint violations.

## Example: Dump a subset of the database

!!! info

    All examples based on playground database. Read more about the playground database in the 
    [Playground](playground.md) section.

The following example demonstrates how to dump a subset of the `person` schema. The subset condition is applied to the
`businessentity` and `password` tables. The subset condition filters the data based on the `businessentityid` and
`passwordsalt` columns, respectively.

```yaml title="Subset configuration example"
transformation:
  - schema: "person"
    name: "businessentity"
    subset_conds:
      - 'person.businessentity.businessentityid IN (274, 290, 721, 852)'
    transformers:
      - name: "RandomDate"
        params:
          column: "modifieddate"
          min: "2020-01-01 00:00:00"
          max: "2024-06-26 00:00:00"
          truncate: "day"
          keep_null: false

  - schema: "person"
    name: "password"
    subset_conds:
      - >
        person.password.passwordsalt = '329eacbe-c883-4f48-b8b6-17aa4627efff'
```
