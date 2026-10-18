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
| `tests/test_name_stability.py` | That deliveries posted to the endpoint keep the names `tests/candidate_names.json` records for them, and that the names the README and the example Dags use are offered |
| `tests/test_app.py` | The endpoint, in process: routing, responses, connection handling |
| `tests/test_api_server.py` | The real `airflow api-server` with the installed plugin, over HTTP, including the README quick start |
| `tests/test_plugin.py` | That importing the plugin, which every Airflow process does, leaves the endpoint's Airflow internals unimported |
| `tests/test_examples.py` | That every example Dag file builds against the installed Airflow, and that the README quick start writes the quick start Dag unchanged |
| `tests/test_provider_info.py` | That the provider metadata conforms to the installed Airflow's schema |
| `tests/test_db.py` | The delivery table: created by `airflow db migrate`, and the same whether built from the metadata or the migrations |
| `tests/test_deliveries.py` | The deliveries API and page, in process, with the auth manager replaced |
| `tests/test_cli.py` | The `airflow webhooks` commands, and that both ways of running them offer them |

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
- a check that installing next to Airflow 3.1.0 is refused;
- **CI passed**, which succeeds only if every job above did. It is the one check a pull request
  needs before it can be merged into `main`, so the matrix can change without updating that rule.

A pull request from a fork runs CI once a maintainer approves it.

When a new Airflow minor version is released, add its newest patch to the matrix, on the oldest and
the newest Python it supports. When the newest release supports a new Python, move the `latest`
entries to it.

## Adding a source type

A source type turns one kind of sender's deliveries into event names. Everything else, naming,
lookup and recording, is shared. Source types live in `src/airflow_inbound_webhooks/sources.py`:

1. Subclass `WebhookSource` and implement:
   - `verify(headers, body, secret)`: raise `WebhookVerificationError` unless the request carries a
     valid signature or token for `secret`. Compare with `hmac.compare_digest`, never `==`. Any
     other exception is answered like a failed verification, with `401`, and logged with its
     traceback.
   - `parse(headers, body)`: return a `WebhookEvent` with the `path` (where it happened, `/`-separated,
     broad to narrow) and `kinds` (what happened, each a tuple from broad to specific), and raise
     `ValueError` for a malformed body. Read payload fields with `get_text()`, which treats a value
     of any other type as absent, and raise `ValueError` only when a field the event needs is
     missing: any other exception answers the delivery with a `500`, which services retry.
   - `from_extra(extra)`, if the type has options: build it from the connection extra and raise
     `ValueError` naming the problem, which the endpoint reports as a misconfigured connection.
   - `summarize(payload)`, if every delivery carries a full copy of the object it is about: return
     the parts worth storing, and set `has_summary = True`, so `store_payload` can be `summary`. Set
     `default_store_payload = "summary"` too if that should be the default, as `jira` does. `parse()`
     always returns the payload as received.
2. Register it in `SOURCE_TYPES`, and add it, and any new options, to the connection form in
   `src/airflow_inbound_webhooks/connection_form.py`.
3. Add tests in `tests/test_sources.py` for verification (valid, missing, wrong) and parsing, and an
   end-to-end case in `tests/test_app.py`. Add a delivery carrying every field `parse()` reads to
   `FULL_DELIVERIES`, which checks that a delivery of any shape is named or refused. Add deliveries
   of the events it names, as the service sends them, to `DELIVERIES` in
   `tests/test_name_stability.py`, with their webhook's connection in `CONNECTIONS` and the headers
   that authenticate them in `sign()`; then record their names, as below.
4. Document it under "Source types" in the README.

Use the service's own event and action names, so its webhook documentation stays the reference for
Dag authors.

Event names are part of the interface. Once a source type is released, changing a name that an
existing delivery produces is a breaking change; see "Stability" under "Event names" in the README.
Adding a kind or a detail is not, because the shorter names are still offered. The connection
options a source type reads, and the fields a summary keeps, are public interfaces too; see
"Versioning" in the README.

`tests/test_name_stability.py` posts its deliveries to the endpoint, and fails when the names a
delivery's record keeps are not those `tests/candidate_names.json` records for it. Once a change to
them is deliberate, record them again, and review the changes before committing them:

```bash
INBOUND_WEBHOOKS_RECORD_NAMES=1 .venv/bin/pytest tests/test_name_stability.py
git diff tests/candidate_names.json
```

The same module checks the names the README and the example Dags use against those deliveries, so
each webhook the README sets up, with a connection or a URL, needs deliveries there:

- A `text` block of names in the README, with a name at the start of each line and two spaces or
  more before what it means, lists all the names one delivery offers, in the order it offers them.
  A block of one name shows just that name.
- Every other name must be one that a delivery offers: in the example Dags, every asset a file
  defines; in the README, every asset its Python builds, in code blocks and between backticks, and
  every name between backticks, where `gitlab/platform/*/...` stands for the names that start with
  `gitlab/platform/*/`. To build assets in the README some other way, add it to `README_NAMESPACE`.

## Changing the delivery table

The table is defined in `src/airflow_inbound_webhooks/models.py` and created by the migrations in
`src/airflow_inbound_webhooks/migrations/versions/`. Depending on the Airflow version, and on
whether Airflow's own tables exist already, `airflow db migrate` builds the table from either one, so
a change needs both:

1. Change the table in `models.py`.
2. Add a revision whose `down_revision` is the current head, named
   `<number>_<release>_<description>.py` like the first one, with `inbound_webhooks_version` set to
   the release that ships it.
3. Run `tests/test_db.py`, which fails until the model and the migrations give the same schema.

## Releasing

`.github/workflows/release.yml` publishes a release to PyPI when its version tag is pushed. It uses
[trusted publishing](https://docs.pypi.org/trusted-publishers/), so no PyPI token is stored anywhere.

Once, before the first release:

1. On PyPI, under **Your account → Publishing**, add a pending trusted publisher for the project
   `airflow-provider-inbound-webhooks`: owner `dheerajturaga`, repository
   `airflow-provider-inbound-webhooks`, workflow `release.yml` and environment `pypi`. Add the same
   on TestPyPI with the environment `testpypi`. A pending publisher does not reserve the name: until
   the first upload, anyone can register it, and the publisher stops working.
2. In the repository's **Settings → Environments**, create the environment `pypi`. Add yourself as a
   required reviewer, and under deployment branches and tags allow only the tag pattern `v*`. Create
   `testpypi` too, allowing only the branch `main`.

For each release:

1. In a pull request, set `version` in `pyproject.toml`, rename the changelog's "Unreleased"
   section to `## <version> - <date>`, and add an empty "Unreleased" section above it.
2. Once it is merged and CI has passed on `main`, start a dry run: on the **Actions** tab, run the
   **Release** workflow on `main`. It publishes `main` to TestPyPI as `<version>.dev<run number>`.
   Check the project page it links to, and that the package installs from TestPyPI.
3. Tag the merge commit and push the tag:

   ```bash
   git tag -a v0.1.0 -m "Release 0.1.0"
   git push origin v0.1.0
   ```

4. The workflow checks that the tagged commit is on `main`, that the tag matches the version and
   that the changelog has its section, builds the sdist and wheel, and waits for you to approve the
   `pypi` deployment. Approved, it uploads them with attestations that this workflow built them. A
   version can be uploaded only once, so a mistake found later needs a new version.
5. Create a GitHub release from the tag, with the changelog section as its notes.

## Commits and pull requests

- Keep each pull request to one change, with tests and documentation updated alongside the code.
- Write the commit subject as plain prose in the imperative ("Add a GitLab tag source"), not
  Conventional Commits (`feat:`, `fix:`); a commit-message hook enforces this.
- Add a line to [CHANGELOG.md](CHANGELOG.md) under "Unreleased" for user-visible changes. A change
  to the names existing deliveries produce goes under "Changed event names", with the old and new
  names.
