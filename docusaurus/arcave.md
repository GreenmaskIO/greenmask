---
title: Save and reset test environments with arcave
description: Restore a masked Greenmask dump into a Docker Compose project once, save it as an arcave savepoint, and reset the whole environment in one command instead of restoring again.
keywords: ["arcave", "greenmask", "docker compose", "savepoint", "rollback", "environment reset", "test data management", "ephemeral environments", "agentic pipeline", "PostgreSQL anonymization", "Open-Source"]
---

# Save and reset test environments with arcave

[arcave](/docs/arcave/) is an open-source CLI from the Greenmask team for
[savepoints and rollback of Docker Compose projects](/docs/arcave/). It saves the whole live state of a
running Compose project (every volume, every container's filesystem and the compose spec) and puts it
back in one command. It runs on the Docker you already have.

## Why pair it with Greenmask

Greenmask gives you a realistic, safe copy of production data. Restoring it takes time, and every test,
migration or demo that runs against it changes it. With arcave you restore the masked dump **once**,
save that state, and return to it with a single command whenever you need a clean environment again,
instead of restoring the dump again. The reset covers the database together with the caches, object
storage and other services around it.

## Before you start

* **Install arcave** — see [Installation](/docs/arcave/installation/). It needs Docker 25 or newer and
  runs on Linux and macOS; there is no Windows build. See [Requirements](/docs/arcave/reference/requirements/).
* **Run arcave on the host**, from the directory of your Compose project, not inside a container.
* **The project must exist in the Docker daemon.** `docker compose stop` is fine; `docker compose down`
  removes the containers arcave reads the project from.
* **Declare a health check on every stateful service.** arcave waits for health checks after each restart,
  and a service without one counts as up as soon as its container starts.
* **Expect a short downtime.** Every savepoint and every rollback stops the whole project, moves the data
  and starts it again. The window grows with volume size and startup time. arcave is a development and CI
  tool, not for production.

:::danger[Keep unmasked data out of the project you snapshot]

A savepoint captures **every** volume and container filesystem of the Compose project. If the source
database you dump from runs in the same project — or in the same Postgres instance as the restore target —
the "masked" savepoint also holds the original, unmasked data, written to arcave's storage
(`.arcave/storage` by default). Run the source database outside the project you snapshot, and protect the
savepoint storage as you would the data inside it.
:::

:::warning[A rollback also rolls back dump storage in the project]

If Greenmask's dump storage (for example a MinIO or S3-compatible service) runs in the same Compose project,
a rollback returns it to the state it had when the savepoint was taken. Dumps created after the savepoint
disappear. Keep the dump storage outside the project, or take a new savepoint after each dump you want to keep.
:::

## Workflow

1.  **Restore the masked dump** into the database of your local Compose project. Point the restore at that
    database, for example through the port the service publishes on the host:

    ```yaml title="config.yml"
    restore:
      pg_restore_options:
        dbname: "host=localhost port=5432 user=postgres password=example dbname=app"
    ```

    ```shell
    greenmask --config config.yml restore latest
    ```

2.  **Save the state** as an arcave savepoint. Run this on the host, in the Compose project's directory:

    ```shell
    arcave init                              # writes .arcave/config.yaml — add .arcave/ to .gitignore
    export ARCAVE_PROJECT_NAME=myapp         # the project name as `docker compose ls` lists it
    arcave savepoint create masked-baseline --description "masked dump, do not delete"
    ```

3.  **Run** your tests, migrations, demos or AI agent against the environment.

4.  **Reset** to the masked baseline in one command, as often as you need:

    ```shell
    arcave rollback masked-baseline
    ```

    Before overwriting anything, arcave takes a safety snapshot of the current state and puts it back
    automatically if the rollback fails.

### Refreshing the baseline

When a new masked dump is available, restore it into a clean database (drop and recreate the target first),
save it under a new name, and remove the old baseline by id:

```shell
greenmask --config config.yml restore latest
arcave savepoint create masked-baseline-2026-10 --description "masked dump, do not delete"
arcave savepoint list                  # find the id of the old baseline
arcave savepoint remove <old-id>
```

:::warning
Retention rules do not know which savepoint is precious: `arcave savepoint remove --retain-recent 5` deletes
the baseline once five newer savepoints exist. Remove savepoints by id, or preview with `--dry-run`. See
[savepoint remove](/docs/arcave/reference/cli/savepoint_remove/). To keep throwaway savepoints away from the
baseline altogether, work in sessions — see below.
:::

## Working in parallel on one masked baseline

A [session](/docs/arcave/sessions/) is an independent line of savepoints inside the project. `arcave init`
puts you in the `baseline` session. Keep that session for masked baselines only — one savepoint per dump —
and do the actual work in sessions forked from it: one per feature branch, migration attempt or AI agent task.

* **The baseline is safe.** Savepoints and pruning in a work session never touch the `baseline` session.
* **Lines do not mix.** A fork copies the savepoint it starts from, so nothing one session writes reaches another.
* **Cleanup is one command.** Deleting a session deletes its savepoints and the data they hold.

1.  **Save each masked dump in `baseline`**, as in the [workflow](#workflow) above:

    ```shell
    greenmask --config config.yml restore latest
    arcave savepoint create masked-2026-10 --description "masked dump of October"
    ```

2.  **Fork a session** from that savepoint. The fork deploys the saved state into the project:

    ```shell
    arcave session create --from=baseline/masked-2026-10 --name=feat/billing --force
    ```

3.  **Work in the session.** Name it in every command — creating a session does not switch to it, and a
    command that names no session works in `baseline`:

    ```shell
    export ARCAVE_SESSION=feat/billing
    arcave savepoint create step-1         # optional checkpoints along the way
    arcave rollback masked-2026-10         # the fork carries the baseline in under its own name
    ```

4.  **Delete the session** when the work is done:

    ```shell
    arcave session delete feat/billing
    ```

When a new masked dump arrives, restore it and save it as a new savepoint in `baseline` (`masked-2026-11`),
then fork new work from it. Sessions forked from the older dump keep it until you delete them.

:::warning
* `session create` refuses to run while the project is up unless you pass `--force`, and `--force` takes **no**
  safety snapshot: state you have not saved is lost. Save it first if you need it.
* There is still one running environment. Sessions do not run side by side; switching to another session
  deploys its state over the project.
* Each fork costs the size of the savepoint it starts from.

See [arcave session](/docs/arcave/reference/cli/session/).
:::

## Using it with the Greenmask playground

The [playground](playground.md) is a Compose project named `greenmask`, after the repository directory.
The `greenmask` shell you work in is a one-off `docker-compose run` container: arcave leaves it alone, but
it stops `playground-db` and `playground-storage` during every savepoint and rollback, so an open `psql_t`
session is disconnected and has to reconnect.

Open a second terminal on the host, in the repository directory:

```shell
arcave init
export ARCAVE_PROJECT_NAME=greenmask
arcave savepoint create transformed-baseline
# ...experiment in the playground...
arcave rollback transformed-baseline
```

Keep in mind that the playground is built for trying Greenmask, not for keeping data apart:

* The `original` and `transformed` databases share one Postgres instance, so the savepoint contains the
  original data too.
* MinIO stores the dumps inside the project, so a rollback removes dumps you created after the savepoint.

## Beyond a single reset

* **Branch per feature.** Keep each feature's state in its own session forked from the masked baseline, as
  [above](#working-in-parallel-on-one-masked-baseline) — see also
  [Environment branching](/docs/arcave/use_cases/environment_branching/).
* **Let an AI agent work on masked data.** Give each agent task its own session; the agent takes a savepoint
  before it acts and rolls back when validation fails. `arcave mcp` exposes the same operations as MCP tools —
  see [Agent iteration loops](/docs/arcave/use_cases/agent_loops/).
* **Run it in CI.** Take a baseline, run migrations and integration tests, and roll back on failure — see
  [Testing destructive migrations](/docs/arcave/use_cases/migration_testing/).

## Learn more

* [Getting started with arcave](/docs/arcave/getting_started/) — take your first savepoint and roll it back.
* [Resetting fixtures and demos](/docs/arcave/use_cases/fixture_reset/) — seed an environment once and return to it in one command.
* [Testing destructive migrations](/docs/arcave/use_cases/migration_testing/) — run a migration against masked data as many times as it takes.
* [Limitations](/docs/arcave/reference/limitations/) — what arcave does not do, before you build a workflow on it.
