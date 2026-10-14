# Contributing

Thanks for helping. Bug reports, fixes, new source types and documentation are all welcome. For a
larger change, open an issue first so the approach can be agreed before you invest in it.

By contributing, you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE). This project follows a [Code of Conduct](CODE_OF_CONDUCT.md). Report
security issues privately, as described in [SECURITY.md](SECURITY.md), not in public issues.

## Development setup

You need [uv](https://docs.astral.sh/uv/) and Python 3.10 or later. Install an Airflow release with
its constraints, then this package in editable mode with the test tools:

```bash
AIRFLOW_VERSION=3.3.2
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"
uv venv -p 3.11
uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "$CONSTRAINTS"
uv pip install --editable . pytest mypy --constraint "$CONSTRAINTS"
```

Install the package rather than adding `src` to the path: the plugin is only loaded through its
`airflow.plugins` entry point, which the API server tests rely on. Editable mode applies changes
without reinstalling; CI installs the built wheel instead.

## Tests

```bash
.venv/bin/pytest
.venv/bin/mypy src
```

The tests need no Airflow setup: `tests/conftest.py` creates a throwaway Airflow home with a SQLite
database. To run them on PostgreSQL or MySQL, as CI does, name a database in
`INBOUND_WEBHOOKS_TEST_DATABASE`. The tests delete Dags, assets and connections, so give them a
database of their own, never one Airflow runs on:

```bash
docker run --rm --detach --name webhooks-test-postgres --publish 127.0.0.1:55432:5432 \
  --env POSTGRES_USER=airflow --env POSTGRES_PASSWORD=airflow --env POSTGRES_DB=airflow postgres:17
uv pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" --constraint "$CONSTRAINTS"
INBOUND_WEBHOOKS_TEST_DATABASE=postgresql+psycopg2://airflow:airflow@127.0.0.1:55432/airflow .venv/bin/pytest
```

For MySQL, run `mysql:8.4` with `--env MYSQL_ROOT_PASSWORD=airflow --env MYSQL_DATABASE=airflow`,
install the `mysql` extra and use `mysql+mysqldb://root:airflow@127.0.0.1:<port>/airflow`. Building
its `mysqlclient` driver needs `pkg-config`, the MySQL client headers and a Python with its headers,
such as one from `uv venv --managed-python`. Start a new database for each Airflow version: an older
Airflow cannot migrate a database that a newer one has upgraded.

The tests are organised as:

| File | Covers |
|---|---|
| `tests/test_sources.py` | Verification and parsing of each source type |
| `tests/test_naming.py` | The event naming rule |
| `tests/test_app.py` | The endpoint, in process: routing, responses, connection handling |
| `tests/test_api_server.py` | The real `airflow api-server` with the installed plugin, over HTTP, including the README quick start |
| `tests/test_plugin.py` | That importing the plugin, which every Airflow process does, leaves the endpoint's Airflow internals unimported |
| `tests/test_examples.py` | That every example Dag file builds against the installed Airflow |
| `tests/test_provider_info.py` | That the provider metadata conforms to the installed Airflow's schema |

Every change in behaviour needs a test that fails without it.

## Static checks

The checks are the [Apache Airflow](https://github.com/apache/airflow) prek hooks that apply outside
its monorepo, pinned to the same versions, plus Airflow's ruff rule set in `pyproject.toml`:

```bash
uv tool install prek
prek install          # run the checks on every commit, including the commit message check
prek run --all-files
```

They cover license headers (added automatically), ruff lint and format, bandit, codespell, flynt,
blacken-docs, yamllint, markdownlint, zizmor for the workflows, file hygiene, and a few regular
expression checks, including inclusive language and not raising `AirflowException`.

## Continuous integration

`.github/workflows/ci.yml` runs on every push and pull request, and weekly:

- the static checks;
- the tests, installed from the built wheel, against Airflow 3.1.1, the newest patch of each
  supported minor version and the newest release, each on the oldest and the newest Python it
  supports, with SQLite;
- the tests on PostgreSQL 17 and MySQL 8.4, against Airflow 3.1.1 and the newest release;
- mypy against the newest release, whose type annotations are the most precise;
- a check that installing next to Airflow 3.1.0 is refused.

When a new Airflow minor version is released, add its newest patch to the matrix, on the oldest and
the newest Python it supports. When the newest release supports a new Python, move the `latest`
entries to it.

## Adding a source type

A source type turns one kind of sender's deliveries into event names. Everything else, naming,
lookup and recording, is shared. Source types live in `src/airflow_inbound_webhooks/sources.py`:

1. Subclass `WebhookSource` and implement:
   - `verify(headers, body, secret)`: raise `WebhookVerificationError` unless the request carries a
     valid signature or token for `secret`. Compare with `hmac.compare_digest`, never `==`.
   - `parse(headers, body)`: return a `WebhookEvent` with the `path` (where it happened, `/`-separated,
     broad to narrow) and `kinds` (what happened, each a tuple from broad to specific), and raise
     `ValueError` for a malformed body. Read payload fields with `get_text()`, which treats a value
     of any other type as absent, and raise `ValueError` only when a field the event needs is
     missing: any other exception answers the delivery with a `500`, which services retry.
   - `from_extra(extra)`, if the type has options: build it from the connection extra and raise
     `ValueError` naming the problem, which the endpoint reports as a misconfigured connection.
2. Register it in `SOURCE_TYPES`, and add it, and any new options, to the connection form in
   `src/airflow_inbound_webhooks/connection_form.py`.
3. Add tests in `tests/test_sources.py` for verification (valid, missing, wrong) and parsing, and an
   end-to-end case in `tests/test_app.py`. Add a delivery carrying every field `parse()` reads to
   `FULL_DELIVERIES`, which checks that a delivery of any shape is named or refused.
4. Document it under "Source types" in the README.

Use the service's own event and action names, so its webhook documentation stays the reference for
Dag authors.

Event names are part of the interface. Once a source type is released, changing a name that an
existing delivery produces is a breaking change; see "Stability" under "Event names" in the README.
Adding a kind or a detail is not, because the shorter names are still offered. The connection
options a source type reads, and the fields a summary keeps, are public interfaces too; see
"Versioning" in the README.

## Commits and pull requests

- Keep each pull request to one change, with tests and documentation updated alongside the code.
- Write the commit subject as plain prose in the imperative ("Add a GitLab tag source"), not
  Conventional Commits (`feat:`, `fix:`); a commit-message hook enforces this.
- Add a line to [CHANGELOG.md](CHANGELOG.md) under "Unreleased" for user-visible changes. A change
  to the names existing deliveries produce goes under "Changed event names", with the old and new
  names.
