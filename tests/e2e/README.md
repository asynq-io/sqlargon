# End-to-end suite

Runs the whole stack against a real database. Every test is parametrised over
the selected backends, so a failure names the backend it happened on:

| backend    | provided by                      | dialect sqlargon sees |
|------------|----------------------------------|-----------------------|
| `sqlite`   | a file on disk (no container)    | `sqlite`              |
| `postgres` | a `postgres:18-alpine` container | `postgresql`          |
| `mysql`    | a `mysql:8.4` container          | `mysql`               |
| `mariadb`  | a `mariadb:11.4` container       | `mysql`               |

MySQL and MariaDB both run on InnoDB (asserted by `test_capabilities.py`) and
both connect over `mysql+asyncmy://`, so both reach the MySQL query builder and
column types — `mariadb+asyncmy://` reports its dialect as `mariadb`, which the
dispatch in `get_query_builder` does not recognise. Tell the two servers apart
with `Backend.is_mariadb`.

The containers are started once per backend by
[testcontainers](https://testcontainers-python.readthedocs.io) and torn down
at the end of the session; nothing needs to be running beforehand beyond a
Docker daemon.

## Running

```bash
pytest                                        # unit suite only (in-memory SQLite)
pytest --e2e                                  # unit suite + every backend
pytest --e2e ./tests/e2e                      # e2e only
pytest --e2e --e2e-backends=mysql,mariadb     # selected backends
pytest --e2e -m e2e --count=1                 # e2e only, without pytest-repeat
```

Without `--e2e` these tests are collected and skipped, so no container is ever
started by an ordinary `pytest` run.

Images are overridable:

```bash
SQLARGON_E2E_MARIADB_IMAGE=mariadb:10.11 pytest --e2e --e2e-backends=mariadb
```

## Layout

- `backends.py` -- the backends, their container lifecycle and what each
  server supports.
- `models.py` -- models and repositories, plus the tables the suite creates.
- `conftest.py` -- backend parametrisation, schema and per-test database.
- `test_capabilities.py` -- what a server supports next to what the query
  builder claims; the gaps are asserted rather than skipped.

## Backend capabilities

A test needing a capability the backend lacks is skipped through a
`needs_*` fixture, and the gap itself is pinned by `test_capabilities.py`.
Known gaps:

- **`RETURNING`.** The MySQL query builder claims it unconditionally, but
  MySQL has none at all and MariaDB has it for `INSERT` (10.5+) and `DELETE`
  (10.0.5+) and never for `UPDATE`. So `create`, `create_or_update` and
  `get_or_create` work on MariaDB but not MySQL, while `update_one`,
  `update_many` and `SoftDeleteRepository.restore` work on neither.
- **`QueryBuilder.excluded` is unimplemented for the MySQL family**, so an
  upsert whose conflict set references the inserted row is PostgreSQL and
  SQLite only. The cron scheduler stays clear of it, and of `RETURNING`, so
  `test_cron.py` runs everywhere.
- **Upserting a partial row silently nulls the omitted column.** Every column
  of the conflict set is assigned from the value the insert would have given
  it, so one with no default becomes NULL and the stored value is lost —
  on PostgreSQL, SQLite and MariaDB alike. MySQL 8.0.19+ is the only backend
  that refuses instead, because the row alias it uses in place of `excluded`
  exposes only the columns the insert names.
- The SQLite `has_any_key`/`has_all_keys` operators match JSON values, not
  object keys.
- `GenerateUUIDV7` needs PostgreSQL 18 for `uuidv7()`, and falls back to a
  random, v4 shaped value on SQLite.
