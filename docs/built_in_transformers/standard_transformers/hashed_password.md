Replace a password hash with a bcrypt hash of one known password, so test accounts can log in.

## Parameters

| Name           | Description                                                                                   | Default | Required | Supported DB types                   |
|----------------|-----------------------------------------------------------------------------------------------|---------|----------|--------------------------------------|
| column         | The name of the column to be affected                                                         |         | Yes      | text, varchar, char, bpchar, citext  |
| password       | The password to hash. Use `resolve_env` and `${VAR}` to keep it out of the config file        |         | Yes      | -                                    |
| bcrypt_variant | The bcrypt version prefix: `2a`, `2b` or `2y`                                                 | `2a`    | No       | -                                    |
| cost           | The bcrypt cost, from 4 to 31                                                                 | `10`    | No       | -                                    |
| per_row_salt   | Hash every row with its own salt. By default all rows get one shared hash                     | `false` | No       | -                                    |
| keep_null      | Indicates whether NULL values should be replaced with transformed values or not               | `true`  | No       | -                                    |

## Description

The `HashedPassword` transformer writes a bcrypt hash of the given password into the column. After the
restore, every user can log in with that password. The original hash is not kept.

`2a`, `2b` and `2y` are the same algorithm with a different prefix. Choose the one your application
accepts:

* PostgreSQL `pgcrypto` (`crypt()`) understands only `2a`. With `2b` or `2y` it gives no error, but the
  password check is always false.
* PHP `password_hash()` writes `2y`.
* Go, Python and Spring Security accept all three.

bcrypt is slow on purpose. With the default `per_row_salt: false` the password is hashed once and the
same hash goes to every row, so the dump stays fast. Set `per_row_salt: true` when rows must not share a
hash; each row then costs one bcrypt run.

!!! warning

    Never put a real password into the config file. Pass it through an environment variable with
    `resolve_env: true`, as in the example below.

## Example: Test logins for the `users` table

``` yaml title="HashedPassword transformer example"
- schema: "public"
  name: "users"
  transformers:
    - name: "HashedPassword"
      resolve_env: true
      params:
        column: "password_hash"
        password: "${TEST_LOGIN_PASSWORD?set TEST_LOGIN_PASSWORD}"
        bcrypt_variant: "2a"
```

```bash
export TEST_LOGIN_PASSWORD="Password123!"
greenmask --config config.yml dump
```

After the restore, this query returns `true` for every row:

```sql
SELECT crypt('Password123!', password_hash) = password_hash FROM users;
```
