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

Editable mode matters: the plugin is only loaded through its `airflow.plugins` entry point, and the
API server tests rely on it.

## Tests

```bash
.venv/bin/pytest
.venv/bin/mypy src
```

The tests need no Airflow setup: `tests/conftest.py` creates a throwaway Airflow home with a SQLite
database. They are organised as:

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
- the tests against Airflow 3.1.1, the newest patch of each supported minor version and the newest
  release, plus mypy against the newest release, whose type annotations are the most precise;
- a check that installing next to Airflow 3.1.0 is refused.

When a new Airflow minor version is released, add its newest patch to the matrix.

## Adding a source type

A source type turns one kind of sender's deliveries into event names. Everything else, naming,
lookup and recording, is shared. Source types live in `src/airflow_inbound_webhooks/sources.py`:

1. Subclass `WebhookSource` and implement:
   - `verify(headers, body, secret)`: raise `WebhookVerificationError` unless the request carries a
     valid signature or token for `secret`. Compare with `hmac.compare_digest`, never `==`.
   - `parse(headers, body)`: return a `WebhookEvent` with the `path` (where it happened, `/`-separated,
     broad to narrow) and `kinds` (what happened, each a tuple from broad to specific), and raise
     `ValueError` for a malformed body.
   - `from_extra(extra)`, if the type has options: build it from the connection extra and raise
     `ValueError` naming the problem, which the endpoint reports as a misconfigured connection.
2. Register it in `SOURCE_TYPES`, and add it, and any new options, to the connection form in
   `src/airflow_inbound_webhooks/connection_form.py`.
3. Add tests in `tests/test_sources.py` for verification (valid, missing, wrong) and parsing, and an
   end-to-end case in `tests/test_app.py`.
4. Document it under "Source types" in the README.

Use the service's own event and action names, so its webhook documentation stays the reference for
Dag authors.

## Commits and pull requests

- Keep each pull request to one change, with tests and documentation updated alongside the code.
- Write the commit subject as plain prose in the imperative ("Add a GitLab tag source"), not
  Conventional Commits (`feat:`, `fix:`); a commit-message hook enforces this.
- Add a line to [CHANGELOG.md](CHANGELOG.md) under "Unreleased" for user-visible changes.
