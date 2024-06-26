# Database subset

Greenmask allows you to define a subset condition for filtering data during the dump process. This feature is useful
when you need to dump only a part of the database, such as a specific table or a set of tables. It automatically
ensures data consistency by including all related data from other tables that are required to maintain the integrity of
the subset.

The subset is a list of SQL conditions that are applied to table. The conditions are combined with `AND` operator. You 
need to specify the schema, table and column name when pointing out the column to filter by to avoid ambiguity. 
The subset condition must be a valid SQL condition. Greenmask does not validate the condition, so make sure it 
is correct.

!!! warning

    Greenmask currently does not support cycle dependencies resolution. Going to be fixed in the future versions.

!!! info

    All examples based on playground database. Read more about the playground database in the 
    [Playground](playground.md) section.

# Example: Dump a subset of the database

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
