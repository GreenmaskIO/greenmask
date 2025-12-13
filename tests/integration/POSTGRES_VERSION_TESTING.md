# PostgreSQL Version Compatibility Testing

## Overview

Greenmask is tested against multiple PostgreSQL versions (currently 11-17) to ensure backward and forward compatibility. The test suite verifies:

- Successful database dumps with transformations
- Compatibility with native `pg_restore` tool
- Full restoration of dumped data

## Supported Versions

Currently tested versions: **PostgreSQL 11, 12, 13, 14, 15, 16, 17**

## Running Tests Locally

### Quick Start

```bash
PG_VERSIONS="17" docker compose -f docker-compose-integration.yml --profile pg17 up

# to run against all versions
PG_VERSIONS="11,12,13,14,15,16,17" docker compose -f docker-compose-integration.yml --profile all up
```

### Available Profiles

- **`pg17`, `pg16`, `pg15`, `pg14`, `pg13`, `pg12`, `pg11`** - Test a specific PostgreSQL version
- **`all`** - Test all supported versions 11-17 (for CI/CD)

This will:

1. Spin up the selected PostgreSQL containers based on profile
2. Fill each database with test data
3. Run the full compatibility test suite against selected versions
4. Exit with status code 2 if any version fails

## Adding Support for a New PostgreSQL Version

### Step 1: Update Docker Compose

Add a new database service in `docker-compose-integration.yml`:

```yaml
db-18: # New version
  profiles: ["pg18", "all"]
  volumes:
    - "/var/lib/postgresql/data"
  image: postgres:18
  ports:
    - "54318:5432"
  restart: always
  environment:
    POSTGRES_PASSWORD: example
  healthcheck:
    test: ["CMD", "psql", "-U", "postgres"]
    interval: 5s
    timeout: 1s
    retries: 3
```

Update the profiles in `test-dbs-filler` and `greenmask` services:

```yaml
test-dbs-filler:
  profiles:
    ["pg11", "pg12", "pg13", "pg14", "pg15", "pg16", "pg17", "pg18", "all"]
  depends_on:
    # ... existing dbs ...
    db-18:
      condition: service_healthy
      required: false

greenmask:
  profiles:
    ["pg11", "pg12", "pg13", "pg14", "pg15", "pg16", "pg17", "pg18", "all"]
```

### Step 3: Run the Tests

```bash
# Test only the new version
PG_VERSIONS="18" docker compose -f docker-compose-integration.yml --profile pg18 up
```

### Step 4: Fix Any Compatibility Issues

If tests fail, check:

- New PostgreSQL features that need support
- Deprecated features that were removed
- Changes in `pg_dump`/`pg_restore` behavior
- SQL syntax changes

The main test is in: `tests/integration/greenmask/backward_compatibility_test.go`

## Key Files

- **Test Suite**: `tests/integration/greenmask/backward_compatibility_test.go`
- **Docker Compose**: `docker-compose-integration.yml`
- **Test Container**: `docker/integration/tests/Dockerfile`
- **DB Filler**: `docker/integration/filldb/filldb.sh`
- **Test Config**: `tests/integration/greenmask/args.go`
