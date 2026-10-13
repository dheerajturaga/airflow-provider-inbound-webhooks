# airflow-provider-inbound-webhooks

Inbound webhooks for [Apache Airflow](https://airflow.apache.org/). Not affiliated with or endorsed
by the Apache Software Foundation.

Every event from GitHub, GitLab or any service that signs its body or sends a shared secret is an
asset. A Dag subscribes to an event by scheduling on its name; nobody writes routing code.

```python
@dag(schedule=Asset("github/my-org/web/pull_request/labeled/deploy-preview"))
def web_deploy_preview(): ...
```

## Status

Prototype. Developed and tested only against Airflow `main` (3.4.0.dev); the latest Airflow
release is 3.3.x. Next steps:

1. A standalone test setup. The tests still use fixtures from Airflow's `tests_common`, so they run
   inside Airflow's Breeze (see "Run the tests").
2. Set the minimum Airflow version, likely 3.1.1. Webhook connections are read through the public
   `airflow.sdk.Connection`, which only reads connections stored in the metadata database inside the
   API server from 3.1.1 (3.0.x and 3.1.0 answer every delivery with a 500); connections in
   environment variables or an external secrets backend work from 3.0. Before 3.4, asset events are
   batched by default, so a run can carry several deliveries.
3. CI on GitHub Actions running the static checks and tests, plus mypy and zizmor (see "Development").
4. Provider metadata: a `webhook` connection type with form fields instead of JSON in the extra.

## Setup

**Deployment Manager, once per sender:**

1. `pip install airflow-provider-inbound-webhooks` on the API server. The plugin registers itself
   through the `airflow.plugins` entry point and serves `POST /webhooks/<name>`.
2. Create a connection `webhook_<name>` whose password is the shared secret.
3. Point the sender at `https://<api-server>/webhooks/<name>` with the same secret.

| Sender | Connection | Sender-side setup |
|---|---|---|
| GitHub | `webhook_github`, password only | Org settings → Webhooks, JSON, "Send me everything" |
| GitLab | `webhook_gitlab`, password only | Group → Webhooks (Premium) or per project; secret token |
| Signs the body (CircleCI, PagerDuty, Shopify, ...) | `webhook_<name>`, extra `{"source_type": "hmac_json", "signature_header": "X-Signature", "event_field": "event"}` | Signing secret |
| Sends a secret header (Datadog, Hugging Face, Argo CD notifications, ...) | `webhook_<name>`, extra `{"source_type": "token_header", "token_header": "Authorization", "token_prefix": "Bearer ", "event_field": "event_type"}` | Custom header carrying the secret |

`hmac_json` also takes `signature_prefix` (e.g. `v1=`) and `signature_encoding` (`hex` or `base64`).
Both generic types take the event name from `event_field` (a dotted path into the payload) or
`event_header`, and an optional `path_field`. A bare token proves who sent a delivery but not that
it was unmodified, so prefer `hmac_json` when the service can sign.

The connection is the only configuration: adding a sender needs no code or restart. A second
GitHub (e.g. Enterprise Server) is `webhook_ghes` with extra `{"source_type": "github"}`. The
extra also accepts `team_name` (multi-team attribution) and `max_body_bytes` (default 1 MiB).

**Dag author:** schedule on the event's name. Each delivery gets its own run (Airflow 3.4) and
the full payload is in `triggering_asset_events[asset][0].extra["payload"]`. See
`examples/dags/ci_cd.py`.

## The naming rule

`<webhook name>/<path>/<event>/<action>/<detail>`. Every prefix is subscribable, at the event's own
path and with the last path segment replaced by `*`.

| Source | path | event / action / detail |
|---|---|---|
| GitHub | `org/repo`; `org/*` for org-level events | GitHub's event and action names; detail is the label (`labeled`, `unlabeled`), branch (`push`), or workflow name (`workflow_run`) |
| GitLab | `group/subgroup/project` | `object_kind`, then `object_attributes.action` or `status`; detail is the branch (`push`) or tag (`tag_push`). Labels added to an MR or issue become `<kind>/labeled/<label>` |
| hmac_json, token_header | the `path_field` value, if configured | the `event_field` or `event_header` value |

Examples: `github/my-org/web/push/main`, `github/my-org/*/release/published`,
`gitlab/platform/api/pipeline/success`, `deploybot/billing/deployed`. Non-ASCII characters are
folded (`🚀 deploy` → `deploy`) because MySQL rejects non-ASCII asset names.

Unsure of a name? Trigger the event once: every response lists its `candidates`, visible in
GitHub's or GitLab's delivery log. Deliveries nobody subscribes to get `202 no_subscribers` and
store nothing.

## Adding a service

A source is two methods, `verify()` and `parse()`, in `src/airflow_inbound_webhooks/sources.py`. `parse()`
returns a path and kinds; naming, lookup and recording are shared. GitHub is ~40 lines; Gitea or
Forgejo would be a GitHub subclass with different header names.

## Development

The static checks are the apache/airflow prek hooks that apply outside its monorepo, pinned to the
same versions, plus Airflow's ruff rule set in `pyproject.toml`:

```bash
uv tool install prek
prek install          # run the checks on every commit, including the commit message check
prek run --all-files
```

They cover license headers, ruff lint and format, bandit, codespell, flynt, blacken-docs, yamllint,
markdownlint, file hygiene (merge conflicts, debug statements, private keys, whitespace), and
Airflow's regex checks: inclusive language, `urlparse`, raising `AirflowException`, `start_date` in
`default_args`, and Conventional Commit subjects (write commit subjects as plain prose).

Not yet: mypy, which needs an installable Airflow release this package supports, and zizmor, which
needs CI workflows to check.

## Run the tests

Until the standalone setup exists, clone this repository into the `files/` directory of an Airflow
checkout, then from that checkout:

```bash
breeze run pytest /files/airflow-provider-inbound-webhooks/tests -v
```

## Open questions before proposing for core

1. **Replay protection.** GitHub signs the body but not a timestamp; GitLab sends a bare token.
   Deduplicating on the delivery ID needs a store.
2. **Unauthenticated cost.** Any `/webhooks/<name>` triggers a connection lookup before it can be
   rejected, which may hit a remote secrets backend. Cache lookups, or list enabled names in config?
3. **Plugin import weight.** Every component that loads plugins imports the FastAPI app.
4. **Payload retention.** Full payloads are stored on each recorded asset event.
5. **Audit.** The core REST endpoint writes an action log entry per user; a webhook has no user.
6. **Name stability.** Renaming a repository or project changes its asset names.
7. **Source types from providers.** `SOURCE_TYPES` is a fixed dict; providers could contribute
   through an entry point.
8. **Internal APIs.** `asset_manager` and `AssetModel` are not public API; core support would remove
   that dependency.
