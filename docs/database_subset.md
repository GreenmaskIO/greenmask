# Database subset

Greenmask allows you to define a subset condition for filtering data during the dump process. This feature is useful
when you need to dump only a part of the database, such as a specific table or a set of tables. It automatically
ensures data consistency by including all related data from other tables that are required to maintain the integrity of
the subset.

!!! info

    Greenmask genrates queries for subset conditions based on the introspected schema using joins and recursive queries.
    It cannot be responsible for query optimization. The subset quries might be slow due to the complexity of
    the queries and/or lack of indexes. Circular dependencies resolution requires recursive queries execution.

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

!!! warning

    Currently, can resolve multi-cylces in one strogly connected component, but only for one group of vertexes. For 
    instance if you have SSC that contains 2 groups of vertexes, Greenmask will not be able to resolve it. For instance
    we have 2 cycles with tables `A, B, C` (first group) and `D, E, F` (second group). Greenmask will not be able to
    resolve it. But if you have only one group of vertexes one and more cycles in the same group of tables (for instance
    `A, B, C`), Greenmask will be able to resolve it. This might be fixed in the future. See second example below.

You can read the Wikipedia article about Circular reference [here](https://en.wikipedia.org/wiki/Circular_reference).

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

## Example: Dump a subset with circular reference

```postgresql title="Create tables with multi cyles"
-- Step 1: Create tables without foreign keys
DROP TABLE IF EXISTS employees CASCADE;
CREATE TABLE employees
(
    employee_id   SERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    department_id INT -- Will reference departments(department_id)
);

DROP TABLE IF EXISTS departments CASCADE;
CREATE TABLE departments
(
    department_id SERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    project_id    INT -- Will reference projects(project_id)
);

DROP TABLE IF EXISTS projects CASCADE;
CREATE TABLE projects
(
    project_id       SERIAL PRIMARY KEY,
    name             VARCHAR(100) NOT NULL,
    lead_employee_id INT, -- Will reference employees(employee_id)
    head_employee_id INT  -- Will reference employees(employee_id)
);

-- Step 2: Alter tables to add foreign key constraints
ALTER TABLE employees
    ADD CONSTRAINT fk_department
        FOREIGN KEY (department_id) REFERENCES departments (department_id);

ALTER TABLE departments
    ADD CONSTRAINT fk_project
        FOREIGN KEY (project_id) REFERENCES projects (project_id);

ALTER TABLE projects
    ADD CONSTRAINT fk_lead_employee
        FOREIGN KEY (lead_employee_id) REFERENCES employees (employee_id);

ALTER TABLE projects
    ADD CONSTRAINT fk_lead_employee2
        FOREIGN KEY (head_employee_id) REFERENCES employees (employee_id);

-- Insert projects
INSERT INTO projects (name, lead_employee_id)
SELECT 'Project ' || i, NULL
FROM generate_series(1, 10) AS s(i);

-- Insert departments
INSERT INTO departments (name, project_id)
SELECT 'Department ' || i, i
FROM generate_series(1, 10) AS s(i);

-- Insert employees and assign 10 of them as project leads
INSERT INTO employees (name, department_id)
SELECT 'Employee ' || i, (i / 10) + 1
FROM generate_series(1, 99) AS s(i);

-- Assign 10 employees as project leads
UPDATE projects
SET lead_employee_id = (SELECT employee_id
                        FROM employees
                        WHERE employees.department_id = projects.project_id
                        LIMIT 1),
    head_employee_id = 3
WHERE project_id <= 10;
```

This schema has two cycles:

* `employees (department_id) -> departments (project_id) -> projects (lead_employee_id) -> employees (employee_id)`
* `employees (department_id) -> departments (project_id) -> projects (head_employee_id) -> employees (employee_id)`

Greenmask can simply resolve it by generating a recursive query with integrity checks for subset and join conditions.

The example below will fetch the data for both 3 employees and related departments and projects.

```yaml title="Subset configuration example "
transformation:
  - schema: "public"
    name: "employees"
    subset_conds:
      - "public.employees.employee_id in (1, 2, 3)"
```

But this will return empty result, because the subset condition is not met for all related tables because project with 
`project_id=1` has reference to employee with `employee_id=3` that is invalid for subset condition.

```yaml title="Subset configuration example"
transformation:
  - schema: "public"
    name: "employees"
    subset_conds:
      - "public.employees.employee_id in (1, 2)"
```

