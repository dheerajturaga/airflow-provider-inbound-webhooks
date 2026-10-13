# airflow-provider-inbound-webhooks

[![CI](https://github.com/dheerajturaga/airflow-provider-inbound-webhooks/actions/workflows/ci.yml/badge.svg)](https://github.com/dheerajturaga/airflow-provider-inbound-webhooks/actions/workflows/ci.yml)
![Airflow 3.1.1+](https://img.shields.io/badge/Airflow-3.1.1%2B-017cee)
![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue)

Inbound webhooks for [Apache Airflow](https://airflow.apache.org/): any service that can send a
signed or secret-carrying webhook can trigger your Dags.

Each verified delivery becomes an **asset event**, named after what happened. A Dag subscribes by
scheduling on that name, and Airflow does the rest. There is no routing code to write, and adding a
sender is one Airflow connection.

```python
@dag(schedule=Asset("demo/billing/deployed"))
def smoke_test_billing(): ...
```

## Why

Airflow can be triggered from outside through its REST API, but webhook senders can't use it: they
cannot obtain or attach an Airflow API token. They sign their requests with a shared secret instead,
and each service posts its own payload format. The usual workarounds are a relay service in front of
Airflow, or sensors that poll the other service and hold worker or triggerer capacity while they
wait.

This plugin adds the missing piece: an endpoint on the Airflow API server that verifies each
service's signature and records the delivery as an asset event.

## How it works

```mermaid
flowchart LR
    sender["Webhook sender<br/>GitHub, GitLab or any<br/>service with a secret"]:::sender

    subgraph airflow ["Apache Airflow"]
        subgraph plugin ["API server · inbound webhooks plugin"]
            verify{"Signature<br/>valid?"}:::plugin
            name["Name the event<br/>demo/billing/deployed"]:::plugin
        end
        conn[("Connection<br/>webhook_#lt;name#gt;")]:::store
        event[("Asset event<br/>demo/billing/deployed")]:::store
        sched["Scheduler"]:::airflow
        dag1["Dag run<br/>smoke_test_billing"]:::dag
        dag2["Dag run<br/>notify_on_deploy"]:::dag
    end

    sender -- "POST /webhooks/#lt;name#gt;" --> verify
    verify -. "reads secret" .-> conn
    verify -- "yes" --> name
    name -- "record" --> event
    event --> sched
    sched --> dag1 & dag2
    verify -. "401 if invalid" .-> sender

    classDef sender fill:#ffffff,stroke:#8c959f,color:#1f2328
    classDef plugin fill:#ddf4ff,stroke:#0969da,color:#0a3069
    classDef store fill:#fff8c5,stroke:#9a6700,color:#4d2d00
    classDef airflow fill:#ddf4ff,stroke:#0969da,color:#0a3069
    classDef dag fill:#dafbe1,stroke:#1a7f37,color:#044f1e
    style airflow fill:none,stroke:#017cee,stroke-width:2px
    style plugin fill:none,stroke:#54aeff,stroke-dasharray:4 3
    %% Arrows are numbered from 0 in order, counting "dag1 & dag2" as two: 7 is the 401 arrow.
    linkStyle 7 stroke:#cf222e
```

The plugin never runs Dags itself. It records asset events, Airflow queues every Dag that schedules
on them, and the scheduler creates the runs. So one delivery can start several Dags, paused Dags stay
paused, and asset expressions such as `a | b` work as usual.

## Quick start

This uses a generic sender, so it needs no third-party account. On the machine running the Airflow
API server (Airflow 3.1.1 or later):

1. **Install the plugin** into the API server's environment, with your Airflow version's
   constraints. Until the first PyPI release, install from a clone:

   ```bash
   git clone https://github.com/dheerajturaga/airflow-provider-inbound-webhooks
   AIRFLOW_VERSION=3.3.2  # your Airflow version
   PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
   pip install ./airflow-provider-inbound-webhooks --constraint \
     "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
   ```

   The plugin registers itself through the `airflow.plugins` entry point. Restart the API server so
   it is loaded.

2. **Create the connection** that defines the webhook. In the Airflow UI, under Admin →
   Connections, add a connection of type **Inbound Webhook**:

   | Field | Value |
   |---|---|
   | Connection Id | `webhook_demo`, so the URL path is `/webhooks/demo` |
   | Shared secret | `change-me` |
   | Source type | `hmac_json` |
   | Signature header | `X-Signature` |
   | Event field | `event` |
   | Path field | `service` |

   Or with the CLI:

   ```bash
   airflow connections add webhook_demo --conn-type inbound_webhook --conn-password 'change-me' \
     --conn-extra '{"source_type": "hmac_json", "signature_header": "X-Signature",
                    "event_field": "event", "path_field": "service"}'
   ```

3. **Add a Dag** that schedules on the event: copy
   [`examples/dags/quickstart.py`](examples/dags/quickstart.py) into your Dags folder. It runs on
   `demo/billing/deployed`. Once it appears in the Airflow UI, unpause it, since new Dags start
   paused and a paused Dag is not queued:

   ```bash
   airflow dags unpause smoke_test_billing
   ```

4. **Send a signed delivery** with the bundled script, which uses only the Python standard library:

   ```bash
   python airflow-provider-inbound-webhooks/examples/send_test_event.py \
     --url http://localhost:8080/webhooks/demo --secret 'change-me' \
     --data '{"event": "deployed", "service": "billing"}'
   ```

   The response lists the asset event it recorded, and `smoke_test_billing` starts a run.

Connections are read on every delivery, so adding or changing a webhook later needs no restart.

## Event names

Every delivery is named `<webhook name>/<path>/<event>/<action>/<detail>`, and every prefix of that
name can be subscribed to. Each prefix is also offered with the last path segment replaced by `*`.
For the quick start delivery the names are:

```text
demo/billing/deployed     billing deployments
demo/*/deployed           deployments of any service
```

When a label is added to a pull request, the built-in GitHub source offers six names, from broad to
specific:

```text
github/my-org/web/pull_request                           any pull request activity in my-org/web
github/my-org/*/pull_request                             ... in any repository of my-org
github/my-org/web/pull_request/labeled                   any label added
github/my-org/*/pull_request/labeled
github/my-org/web/pull_request/labeled/deploy-preview    the deploy-preview label added
github/my-org/*/pull_request/labeled/deploy-preview
```

**Finding a name:** every response lists the names it offered as `candidates`, so send the event
once and copy one. A delivery that matches no subscribed name is answered with
`202 no_subscribers` and nothing is stored.

Non-ASCII characters are folded (`🚀 deploy` → `deploy`), because Airflow rejects non-ASCII asset
names on MySQL.

## Source types

The connection's `source_type` decides how deliveries are verified and named. It defaults to the
webhook name, so `webhook_github` and `webhook_gitlab` need nothing but a password.

| `source_type` | Authentication | Path | Event / action / detail |
|---|---|---|---|
| `hmac_json` | HMAC-SHA256 signature of the body | `path_field` value | `event_field` or `event_header` value |
| `token_header` | Shared secret in a header | `path_field` value | `event_field` or `event_header` value |
| `github` | `X-Hub-Signature-256` | `org/repo`, or `org/*` for organization events | see [`github`](#github) |
| `gitlab` | `X-Gitlab-Token` | `group/subgroup/project` | see [`gitlab`](#gitlab) |

### `hmac_json`: services that sign the body

For services that sign the raw body with HMAC-SHA256 and send the digest in a header. Prefer this
over `token_header` whenever a service can sign, because the signature also proves the body was not
changed.

| Extra field | Required | Meaning |
|---|---|---|
| `signature_header` | yes | Header carrying the signature |
| `signature_prefix` | no | Text before the digest, e.g. `sha256=` or `v1=` |
| `signature_encoding` | no | `hex` (default) or `base64` |
| `event_field` or `event_header` | one of them | Dotted path into the JSON body (`data.type`), or a header, naming the event |
| `path_field` | no | Dotted path to a value that becomes the path, such as a service or project name |

### `token_header`: services that send a secret header

For services that can only send the shared secret in a header you configure. A bare token proves
who sent a delivery but not that it was unmodified, so it relies on TLS.

| Extra field | Required | Meaning |
|---|---|---|
| `token_header` | yes | Header carrying the secret, e.g. `Authorization` |
| `token_prefix` | no | Text before the secret, e.g. `"Bearer "` |
| `event_field` or `event_header` | one of them | As for `hmac_json` |
| `path_field` | no | As for `hmac_json` |

### `github`

GitHub.com and GitHub Enterprise Server. An organization webhook covers every repository in the
organization: set the payload URL to `https://<api-server>/webhooks/github`, the content type to
JSON, and the secret to the connection's password.

Names use GitHub's own event and action names. The detail is the label for `labeled` and
`unlabeled`, the branch for `push` (tag pushes have none), and the workflow name for
`workflow_run`. A second GitHub instance is a second connection, e.g. `webhook_ghes` with extra
`{"source_type": "github"}`.

### `gitlab`

GitLab.com and self-managed GitLab. Group webhooks cover every project in a group (a GitLab Premium
feature); otherwise add the webhook per project. GitLab sends the secret token itself rather than
signing the body.

Names use the payload's `object_kind`, then `object_attributes.action` (or its `status`, for
pipelines). The detail is the branch for `push` and the tag for `tag_push`. GitLab has no label
events, so a label added to a merge request or issue is reported as `<object_kind>/labeled/<label>`.

### Other services

A service whose signature covers more than the body (for example a timestamp plus the body), or that
needs a verification handshake when the webhook is registered, needs its own source type. A source
type is a small class with `verify()` and `parse()`; see [CONTRIBUTING.md](CONTRIBUTING.md).

## Writing Dags

Schedule on an event name and read the delivery from `triggering_asset_events`. Each event's
`extra` holds the full payload:

```python
from airflow.sdk import Asset, dag, task

deployed = Asset("demo/*/deployed")


@dag(schedule=deployed)
def on_any_deployment():
    @task
    def handle(*, triggering_asset_events=None):
        for event in triggering_asset_events[deployed]:
            print(event.extra["payload"])

    handle()


on_any_deployment()
```

Iterate over all events rather than taking the first: before Airflow 3.4, and on any Dag with
`batch_asset_events`, deliveries that arrive before the scheduler creates a run share that run.
Events are not ordered; sort by `event.timestamp` if order matters. `extra` also holds `source`,
`path` and `delivery_id` (where the service sends one).

More examples, for GitHub labels and pushes, organization-wide releases and GitLab pipelines, are in
[`examples/dags/ci_cd.py`](examples/dags/ci_cd.py).

## Reference

### Connection

One connection per webhook, named `webhook_<name>`, where `<name>` is lowercase letters, digits,
`-` and `_`. Use the connection type **Inbound Webhook** (`inbound_webhook`) to get a form in the
Airflow UI; its fields are stored in the connection extra under the keys below. The plugin reads
connections of any type, so connections created elsewhere, for example from environment variables,
work too. Fields left empty are treated as unset.

| Form field | Extra key | Meaning |
|---|---|---|
| Shared secret | (password) | The shared secret. Required. |
| Source type | `source_type` | Source type; defaults to `<name>` |
| Team | `team_name` | With multi-team Airflow (3.3 or later), events count as coming from this team, which decides which teams' Dags are queued. Without it, they only reach teamless Dags. Rejected on older Airflow. |
| Maximum body size | `max_body_bytes` | Larger requests are refused before verification. Default 1 MiB. |
| Other fields | | Options of the source type, listed above |

Connections stored in Airflow's metadata database, in environment variables
(`AIRFLOW_CONN_WEBHOOK_<NAME>`) or in a secrets backend all work.

### Responses

| Status | Meaning |
|---|---|
| `202` `accepted` | Recorded; `asset_events` lists the asset event per subscribed name |
| `202` `no_subscribers` | Verified, but no Dag schedules on any of the `candidates`; nothing stored |
| `400` | The body is not a JSON object, or the event name is missing |
| `401` | Signature or token does not match |
| `404` | No such webhook: no `webhook_<name>` connection |
| `413` | Body larger than `max_body_bytes` |
| `503` | The connection is misconfigured; the API server log says why |

## Security

- **The endpoint is open to anyone who can reach it.** It does not use Airflow's login: it has to
  accept services that cannot log in. Every delivery is verified against the webhook's secret before
  anything is recorded, and unknown webhook names are answered with `404`.
- **Expose only `/webhooks/`.** If the API server must be reachable by an external service, put a
  reverse proxy in front that forwards only `/webhooks/*`, rather than exposing the whole Airflow UI
  and API.
- **Use HTTPS.** It matters most for `token_header` and GitLab, where the secret itself travels in a
  header.
- **Use strong secrets, ideally from a secrets backend,** which keeps them out of the metadata
  database.
- **No replay protection yet.** A captured delivery can be sent again. Signatures over a timestamp
  would allow rejecting old deliveries; see [docs/design.md](docs/design.md).
- Bodies larger than `max_body_bytes` are refused before verification.

To report a vulnerability, see [SECURITY.md](SECURITY.md).

## Compatibility

| Airflow | Support |
|---|---|
| 3.3 and later | Supported, including `team_name` |
| 3.1.1 to 3.2 | Supported; `team_name` is rejected because these versions cannot attribute asset events to a team. On 3.1.1 and 3.1.2 the connection form only shows its fields when Flask-AppBuilder is installed (for example with the FAB provider); otherwise set the fields as JSON in the connection's extra |
| 3.0, 3.1.0 | Not supported: their API server cannot read metadata-database connections through `airflow.sdk.Connection`, and installing next to them is refused |
| 2.x | Not supported: there is no FastAPI API server for the plugin to mount on |

CI tests Airflow 3.1.1, the newest patch of each supported minor version and the newest release, on
Python 3.11. Each test run starts the real `airflow api-server` and sends it signed deliveries.

## Limitations

- No replay protection.
- Each recorded asset event stores the full delivery payload.
- Asset events from webhooks are not written to Airflow's audit log, because a delivery has no
  Airflow user.
- Renaming a repository, project or service changes its event names, so Dags must be updated.
- The plugin uses some Airflow internals (`asset_manager`, `AssetModel`, the API server's database
  session), which CI checks against every supported version.

[docs/design.md](docs/design.md) covers the design, these limitations and the path towards Airflow
core.

## FAQ

**Why not call Airflow's REST API from the service?** Most services can only send webhooks; they
cannot obtain an Airflow token or post Airflow's request format. If a service can run arbitrary
code, such as a CI job, the REST API (`POST /api/v2/assets/events`) is the simpler choice.

**How does this compare to event-driven scheduling with `AssetWatcher`?** `AssetWatcher` pulls
events from a queue such as SQS or Kafka. The queue keeps events while Airflow is down, so it suits
high-volume or must-not-lose streams. Webhooks are pushed: simpler to set up, but a delivery made
while the API server is unreachable is only retried if the sending service retries.

**Does it replace sensors?** For "wait until another system finishes something", yes: the other
system's webhook triggers the Dag instead of a sensor polling it.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, tests and adding a source type, and
[CHANGELOG.md](CHANGELOG.md) for changes.

## License

[Apache License 2.0](LICENSE).

Apache Airflow, Airflow, Apache, the Apache feather logo, and the Apache Airflow project logo are
either registered trademarks or trademarks of The Apache Software Foundation in the United States
and other countries. This project is not affiliated with or endorsed by The Apache Software
Foundation.
