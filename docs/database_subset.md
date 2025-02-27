# Database subset

Greenmask allows you to define a subset condition for filtering data during the dump process. This feature is useful
when you need to dump only a part of the database, such as a specific table or a set of tables. It automatically
ensures data consistency by including all related data from other tables that are required to maintain the integrity of
the subset. The subset condition can be defined using `subset_conds` attribute that can be defined on the table in the
`transformation` section (see examples).

!!! info

    Greenmask genrates queries for subset conditions based on the introspected schema using joins and recursive queries.
    It cannot be responsible for query optimization. The subset quries might be slow due to the complexity of
    the queries and/or lack of indexes. Circular are resolved using recursive queries.

## Detail

The subset is a list of SQL conditions that are applied to table. The conditions are combined with `AND` operator. **You
need** to specify the **schema**, **table** and **column** name when pointing out the column to filter by to avoid
ambiguity. The subset condition must be a valid SQL condition.

```yaml title="Subset condition example"
subset_conds:
  - 'person.businessentity.businessentityid IN (274, 290, 721, 852)'
```

## Use cases

* Database scale down - create anonymized dump but for the limited and consistent set of tables
* Data migration - migrate only some records from one database to another
* Data anonymization - dump and anonymize only a specific records in the database
* Database catchup - catchup your another instance of database logically by adding a new records. In this case it
  is recommended to [restore tables in topological order](commands/restore.md#restoration-in-topological-order) using
  `--restore-in-order`.

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

    Currently (v0.2b2), Greenmask can resolve multi-cylces in one strogly connected component, but only for one group 
    of vertexes. If you have SSC that contains 2 groups of vertexes, Greenmask will not be able to 
    resolve it. For instance we have 2 cycles with tables `A, B, C` (first group) and `B, C, E` (second group). 
    Greenmask will not be able to resolve it. But if you have only one group of vertexes one and more cycles in the 
    same group of tables (for instance `A, B, C`), Greenmask works with it. This will be fixed in the future.
    See second example below. In practice this is quite rare situation and 99% of people will not face this issue. 

You can read the Wikipedia article about Circular reference [here](https://en.wikipedia.org/wiki/Circular_reference).

## Virtual references

During the development process, there are **situations where foreign keys need to be removed**. The reasons can
vary—from improving performance to simplifying the database structure. Additionally, some foreign keys may exist within
loosely structured data, such as JSON, where PostgreSQL cannot create foreign keys at all. These limitations could
significantly hinder the capabilities of a subset system. Greenmask offers a flexible solution to this problem by
allowing the declaration of virtual references in the configuration, enabling the preservation and management of logical
relationships between tables, even in the absence of explicit foreign keys. Virtual reference can be called virtual
foreign key as well.

The `virtual_references` can be defined in `dump` section. It contains the list of virtual references. First you set
the table where you want to define virtual reference. In the attribute `references` define the list of tables that are
referenced by the table. In the `columns` attribute define the list of columns that are used in the foreign key
reference. The `not_null` attribute is optional and defines if the FK has not null constraint. If `true` Greenmask will
generate `INNER JOIN` instead of `LEFT JOIN` by default it is `false`. The `expression` needs to be used when you
want to use some expression to get the value of the column in the referencing table. For instance, if you have JSONB
column in the `audit_logs` table that contains `order_id` field, you can use this field as FK reference.

!!! info

    You do not need to define primary key of the referenced table. Greenmask will automatically resolve it and use it in
    the join condition.

```yaml title="Virtual references example"
dump:
  virtual_references:
    - schema: "public" # (1)
      name: "orders" # (2)
      references: # (3)
        - schema: "public" # (4) 
          name: "customers" # (5)
          columns: # (6)
            - name: "customer_id"
          not_null: false # (7)

    - schema: "public"
      name: "audit_logs"
      references:
        - schema: "public"
          name: "orders"
          columns:
            - expression: "(public.audit_logs.log_data ->> 'order_id')::INT" # (8)
```

1. The schema name of table that has foreign key reference (table that own FK reference)
2. The table name that has foreign key reference (table that own FK reference)
3. List of virtual references
4. The schema name of the table that has foreign key reference (referencing table)
5. The table name that has foreign key reference (referencing table)
6. List of columns that are used in the foreign key reference. Each column has one of property defined at the same time:

    * `name` - column name in the referencing table
    * `expression` - expression that is used to get the value of the column in the referencing table

7. `not_null` - is FK has not null constraint. If `true` Default it is `false`
8. `expression` - expression that is used to get the value of the column in the referencing table

## Polymorphic references

Greenmask supports polymorphic references. You can define a virtual reference for a table with polymorphic references
using `polymorphic_exprs` attribute. The `polymorphic_exprs` attribute is a list of expressions that are used to make
a polymorphic reference. For instance we might have a table `comments` that has polymorphic reference to `posts` and
`videos`. The table comments might have `commentable_id` and `commentable_type` columns. The `commentable_type` column
contains the type of the table that is referenced by the `commentable_id` column. The example of the config:

```yaml title="Polymorphic references example"
dump:
  virtual_references:
    - schema: "public"
      name: "comments"
      references:
        - schema: "public"
          name: "videos"
          polymorphic_exprs:
            - "public.comments.commentable_type = 'video'"
          columns:
            - name: "commentable_id"
        - schema: "public"
          name: "posts"
          polymorphic_exprs:
            - "public.comments.commentable_type = 'post'"
          columns:
            - name: "commentable_id"
```

!!! warning

    The plimorphic references cannot be non_null because the `commentable_id` column can be `NULL` if the 
    `commentable_type` is not set or different that the values defined in the `polymorphic_exprs` attribute.

## Troubleshooting

### Exclude the records that has NULL values in the referenced column

If you want to exclude records that have NULL values in the referenced column, you can manually add this condition to
the subset condition for the table. Greenmask does not automatically exclude records with NULL values because it applies
a `LEFT OUTER JOIN` on nullable foreign keys.

### Some table is not filtered by the subset condition

Greenmask builds a table dependency graph based on the introspected schema and existing foreign keys. If a table is not
filtered by the subset condition, it means that the table either does not reference another table that is filtered by
the subset condition or the table itself does not have a subset condition applied.

If you have a table with a removed foreign key and want to filter it by the subset condition, you need to define a
virtual reference. For more information on virtual references, refer to the [Virtual References](#virtual-references)
section.

!!! info

     If you find any issues related to the code or greenmask is not working as expected, do not hesitate to contact us 
     [directly](index.md#links) or by creating an [issue in the repository](https://github.com/GreenmaskIO/greenmask/issues).

### ERROR: column reference "id" is ambiguous

If you see the error message `ERROR: column reference "{column name}" is ambiguous`, you have specified the column name
without the table and/or schema name. To avoid ambiguity, always specify the schema and table name when pointing out the
column to filter by. For instance if you want to filter employees by `employee_id` column, you should
use `public.employees.employee_id` instead of `employee_id`.

```postgresql title="Valid subset condition"
public.employees.employee_id IN (1, 2, 3)
```

### The subset condition is not working correctly. How can I verify it?

Run greenmask with `--log-level=debug` to see the generated SQL queries. You will find the generated SQL queries in the
log output. Validate this query in your database client to ensure that the subset condition is working as expected.

For example:

```bash 
$ greenmask dump --config config.yaml --log-level=debug

2024-08-29T19:06:18+03:00 DBG internal/db/postgres/context/context.go:202 > Debug query Schema=person Table=businessentitycontact pid=1638339
2024-08-29T19:06:18+03:00 DBG internal/db/postgres/context/context.go:203 > SELECT "person"."businessentitycontact".* FROM "person"."businessentitycontact"  INNER JOIN "person"."businessentity" ON "person"."businessentitycontact"."businessentityid" = "person"."businessentity"."businessentityid" AND ( person.businessentity.businessentityid between 400 and 800 OR person.businessentity.businessentityid between 800 and 900 ) INNER JOIN "person"."person" ON "person"."businessentitycontact"."personid" = "person"."person"."businessentityid" WHERE TRUE AND (("person"."person"."businessentityid") IN (SELECT "person"."businessentity"."businessentityid" FROM "person"."businessentity"   WHERE ( ( person.businessentity.businessentityid between 400 and 800 OR person.businessentity.businessentityid between 800 and 900 ) )))
 pid=1638339
```

### Dump is too slow

If the dump process is too slow the generated query might be too complex. In this case you can:

* Check if the database has indexes on the columns used in the subset condition. Create them if possible.
* Move database dumping on the replica to avoid the performance impact on the primary.

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

## Example: Dump a subset with virtual references

In this example, we will create a subset of the tables with virtual references. The subset will include the
`orders` table and its related tables `customers` and `audit_logs`. The `orders` table has a virtual reference to the
`customers` table, and the `audit_logs` table has a virtual reference to the `orders` table.

```postgresql title="Create tables with virtual references"
-- Create customers table
CREATE TABLE customers
(
    customer_id   SERIAL PRIMARY KEY,
    customer_name VARCHAR(100)
);

-- Create orders table
CREATE TABLE orders
(
    order_id    SERIAL PRIMARY KEY,
    customer_id INT, -- This should reference customers.customer_id, but no FK constraint is defined
    order_date  DATE
);

-- Create payments table
CREATE TABLE payments
(
    payment_id     SERIAL PRIMARY KEY,
    order_id       INT, -- This should reference orders.order_id, but no FK constraint is defined
    payment_amount DECIMAL(10, 2),
    payment_date   DATE
);

-- Insert test data into customers table
INSERT INTO customers (customer_name)
VALUES ('John Doe'),
       ('Jane Smith'),
       ('Alice Johnson');

-- Insert test data into orders table
INSERT INTO orders (customer_id, order_date)
VALUES (1, '2023-08-01'), -- Related to customer John Doe
       (2, '2023-08-05'), -- Related to customer Jane Smith
       (3, '2023-08-07');
-- Related to customer Alice Johnson

-- Insert test data into payments table
INSERT INTO payments (order_id, payment_amount, payment_date)
VALUES (1, 100.00, '2023-08-02'), -- Related to order 1 (John Doe's order)
       (2, 200.50, '2023-08-06'), -- Related to order 2 (Jane Smith's order)
       (3, 300.75, '2023-08-08');
-- Related to order 3 (Alice Johnson's order)


-- Create a table with a multi-key reference (composite key reference)
CREATE TABLE order_items
(
    order_id     INT,               -- Should logically reference orders.order_id
    item_id      INT,               -- Composite part of the key
    product_name VARCHAR(100),
    quantity     INT,
    PRIMARY KEY (order_id, item_id) -- Composite primary key
);

-- Create a table with a JSONB column that contains a reference value
CREATE TABLE audit_logs
(
    log_id   SERIAL PRIMARY KEY,
    log_data JSONB -- This JSONB field will contain references to other tables
);

-- Insert data into order_items table with multi-key reference
INSERT INTO order_items (order_id, item_id, product_name, quantity)
VALUES (1, 1, 'Product A', 3), -- Related to order_id = 1 from orders table
       (1, 2, 'Product B', 5), -- Related to order_id = 1 from orders table
       (2, 1, 'Product C', 2), -- Related to order_id = 2 from orders table
       (3, 1, 'Product D', 1);
-- Related to order_id = 3 from orders table

-- Insert data into audit_logs table with JSONB reference value
INSERT INTO audit_logs (log_data)
VALUES ('{
  "event": "order_created",
  "order_id": 1,
  "details": {
    "customer_name": "John Doe",
    "total": 100.00
  }
}'),
       ('{
         "event": "payment_received",
         "order_id": 2,
         "details": {
           "payment_amount": 200.50,
           "payment_date": "2023-08-06"
         }
       }'),
       ('{
         "event": "item_added",
         "order_id": 1,
         "item": {
           "item_id": 2,
           "product_name": "Product B",
           "quantity": 5
         }
       }');
``` 

The following example demonstrates how to make a subset for keys that does not have FK constraints but a data
relationship exists.

* The `orders` table has a virtual reference to the `customers` table, and the `audit_logs` table
  has a virtual reference to the `orders` table.
* The `payments` table has a virtual reference to the `orders` table.
* The `order_items` table has two keys that reference the `orders` and `products` tables.
* The `audit_logs` table has a JSONB column that contains two references to the `orders` and `order_items` tables.

```yaml
dump:
  virtual_references:
    - schema: "public"
      name: "orders"
      references:
        - schema: "public"
          name: "customers"
          columns:
            - name: "customer_id"
          not_null: true

    - schema: "public"
      name: "payments"
      references:
        - schema: "public"
          name: "orders"
          columns:
            - name: "order_id"
          not_null: true

    - schema: "public"
      name: "order_items"
      references:
        - schema: "public"
          name: "orders"
          columns:
            - name: "order_id"
          not_null: true
        - schema: "public"
          name: "products"
          columns:
            - name: "product_id"
          not_null: true

    - schema: "public"
      name: "audit_logs"
      references:
        - schema: "public"
          name: "orders"
          columns:
            - expression: "(public.audit_logs.log_data ->> 'order_id')::INT"
          not_null: false
        - schema: "public"
          name: "order_items"
          columns:
            - expression: "(public.audit_logs.log_data -> 'item' ->> 'item_id')::INT"
            - expression: "(public.audit_logs.log_data ->> 'order_id')::INT"
          not_null: false

  transformation:

    - schema: "public"
      name: "customers"
      subset_conds:
        - "public.customers.customer_id in (1)"
```

As a result, the `customers` table will be dumped with the `orders` table and its related tables `payments`,
`order_items`, and `audit_logs`. The subset condition will be applied to the `customers` table, and the data will be
filtered based on the `customer_id` column.

## Example: Dump a subset with polymorphic references

In this example, we will create a subset of the tables with polymorphic references. This example includes the
`comments` table and its related tables `posts` and `videos`.

```postgresql title="Create tables with polymorphic references and insert data"
-- Create the Posts table
CREATE TABLE posts
(
    id      SERIAL PRIMARY KEY,
    title   VARCHAR(255) NOT NULL,
    content TEXT         NOT NULL
);

-- Create the Videos table
CREATE TABLE videos
(
    id    SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    url   VARCHAR(255) NOT NULL
);

-- Create the Comments table with a polymorphic reference
CREATE TABLE comments
(
    id               SERIAL PRIMARY KEY,
    commentable_id   INT         NOT NULL, -- Will refer to either posts.id or videos.id
    commentable_type VARCHAR(50) NOT NULL, -- Will store the type of the associated record
    body             TEXT        NOT NULL,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Insert data into the Posts table
INSERT INTO posts (title, content)
VALUES ('First Post', 'This is the content of the first post.'),
       ('Second Post', 'This is the content of the second post.');

-- Insert data into the Videos table
INSERT INTO videos (title, url)
VALUES ('First Video', 'https://example.com/video1'),
       ('Second Video', 'https://example.com/video2');

-- Insert data into the Comments table, associating some comments with posts and others with videos
-- For posts:
INSERT INTO comments (commentable_id, commentable_type, body)
VALUES (1, 'post', 'This is a comment on the first post.'),
       (2, 'post', 'This is a comment on the second post.');

-- For videos:
INSERT INTO comments (commentable_id, commentable_type, body)
VALUES (1, 'video', 'This is a comment on the first video.'),
       (2, 'video', 'This is a comment on the second video.');
```

The `comments` table has a polymorphic reference to the `posts` and `videos` tables. Depending on the value of the
`commentable_type` column, the `commentable_id` column will reference either the `posts.id` or `videos.id` column.

The following example demonstrates how to make a subset for tables with polymorphic references.

```yaml title="Subset configuration example"
dump:
  virtual_references:
    - schema: "public"
      name: "comments"
      references:
        - schema: "public"
          name: "posts"
          polymorphic_exprs:
            - "public.comments.commentable_type = 'post'"
          columns:
            - name: "commentable_id"
        - schema: "public"
          name: "videos"
          polymorphic_exprs:
            - "public.comments.commentable_type = 'video'"
          columns:
            - name: "commentable_id"

  transformation:
    - schema: "public"
      name: "posts"
      subset_conds:
        - "public.posts.id in (1)"
```

This example selects only the first post from the `posts` table and its related comments from the `comments` table. 
The comments are associated with `videos` are included without filtering because the subset condition is applied only to
the `posts` table and related comments. 

The resulted records will be:

```plaintext
transformed=# select * from comments;
 id | commentable_id | commentable_type |                 body                  |         created_at         
----+----------------+------------------+---------------------------------------+----------------------------
  1 |              1 | post             | This is a comment on the first post.  | 2024-09-18 05:27:54.217405
  2 |              2 | post             | This is a comment on the second post. | 2024-09-18 05:27:54.217405
  3 |              1 | video            | This is a comment on the first video. | 2024-09-18 05:27:54.229794
(3 rows)

```

