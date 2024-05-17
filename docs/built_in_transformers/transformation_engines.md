# Transformation engine

The greenmask provides two engines `random` and `hash`. Most of the transformers has `engine` parameters that
by default is set to `random`. Use `hash` engine when you need to generate deterministic data - the same input
will always produce the same output.

!!! warning

    The hash engine does not guarantee the uniqueness of generated values. Although transformers such as `Hash`, 
    `RandomEmail`, and `RandomUuid` typically have a low probability of producing duplicate values The **feature to 
    ensure uniqueness is currently under development** at Greenmask and is expected to be released in future updates. 
    For the latest status, please visit the [Greenmask roadmap](https://github.com/orgs/GreenmaskIO/projects/6).

## Details

### Example schema

The next examples will be run on the following schema and sample data:

```sql
CREATE TABLE account
(
    id         SERIAL PRIMARY KEY,
    gender     VARCHAR(1) NOT NULL,
    email      TEXT       NOT NULL NOT NULL UNIQUE,
    first_name TEXT       NOT NULL,
    last_name  TEXT       NOT NULL,
    birth_date DATE,
    created_at TIMESTAMP  NOT NULL DEFAULT NOW()
);

INSERT INTO account (first_name, gender, last_name, birth_date, email)
VALUES ('John', 'M', 'Smith', '1980-01-01', 'john.smith@gmail.com');

CREATE TABLE orders
(
    id          SERIAL PRIMARY KEY,
    account_id  INTEGER REFERENCES account (id),
    total_price NUMERIC(10, 2),
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    paid_at     TIMESTAMP
);

INSERT INTO orders (account_id, total_price, created_at, paid_at)
VALUES (1, 100.50, '2024-05-01', '2024-05-02'),
       (1, 200.75, '2024-05-03', NULL);
```

### Random engine

The random engine serves as the default engine for the greenmask. It operates using a pseudo-random number generator,
which is initialized with a random seed sourced from a cryptographically secure random number generator. Employ the
random engine when you need to generate random data and do not require reproducibility of the same transformation
results with the same input.

The following example demonstrates how to configure the `RandomDate` transformer to generate random.

```yaml
- schema: "public"
  name: "account"
  transformers:
    - name: "RandomDate"
      params:
        column: "birth_date"
        engine: "random" # (1)
        min: '1970-01-01'
        max: '2000-01-01'
```

1. `random` engine is explicitly specified, although it is the default value.

Results:

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>birth_date</td><td><span style="color:green">1980-01-01</span></td><td><span style="color:red">1970-02-23</span></td>
</tr>
</table>

Keep in mind that the `random` engine is always generates different values for the same input. For instance in we run
the previous example multiple times we will get different results.

### Hash engine

The hash engine is designed to generate deterministic data. It uses the `SHA-3` algorithm to hash the input value. The
hash engine is particularly useful when you need to generate the same output for the same input. For example, when you
want to transform values that are used as primary or foreign keys in a database.

For secure reason it is suggested set global greenmask salt via `GREENMASK_GLOBAL_SALT` environment variable. The salt
is added to the hash input to prevent the possibility of reverse engineering the original value from the hashed output.
The value is hex encoded with variadic length. For example, `GREENMASK_GLOBAL_SALT=a5eddc84e762e810`.
Generate a strong random salt and keep it secret.

The following example demonstrates how to configure the `RandomInt` transformer to generate deterministic data using the
`hash` engine. The `public.account.id` and `public.orders.account_id` columns will have the same values.

```yaml
- schema: "public"
  name: "account"
  transformers:

    - name: "RandomInt"
      params:
        column: "id"
        engine: hash
        min: 1
        max: 2147483647

- schema: "public"
  name: "orders"
  transformers:

    - name: "RandomInt"
      params:
        column: "account_id"
        engine: hash
        min: 1
        max: 2147483647
```

Result:

* public.account

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>id</td><td><span style="color:green">1</span></td><td><span style="color:red">130162079</span></td>
</tr>
</table>

* public.orders

<table>
<tr>
<th>Column</th><th>OriginalValue</th><th>TransformedValue</th>
</tr>
<tr>
<td>account_id</td><td><span style="color:green">1</span></td><td><span style="color:red">130162079</span></td>
</tr>
</table>

