# Reference

## Source types

The connection's `source_type` decides how deliveries are verified and named. `hmac_json` and
`token_header` work with any service and are configured through connection options; the others are
built in for one service each. It defaults to the webhook name, so `webhook_github`,
`webhook_gitlab` and `webhook_jira` need nothing but a password.

| `source_type` | Authentication | Path | Event / action / detail | Stored payload |
|---|---|---|---|---|
| `hmac_json` | HMAC-SHA256 signature of the body | `path_field` value | `event_field` or `event_header` value | The body |
| `token_header` | Shared secret in a header | `path_field` value | `event_field` or `event_header` value | The body |
| `github` | `X-Hub-Signature-256` | `org/repo`, or `org/*` for organization events | see [`github`](event-names.md#github) | The body |
| `gitlab` | `X-Gitlab-Token` | `group/subgroup/project` | see [`gitlab`](event-names.md#gitlab) | The body |
| `jira` | `X-Hub-Signature` | project key | see [`jira`](event-names.md#jira) | A summary; see [`jira`](event-names.md#jira) |

Each asset event stores the delivery's body as `extra["payload"]`, as received, unless every
delivery also carries a full copy of the object it is about. Of the built-in services only Jira does,
sending the whole issue each time, so `jira` stores a summary by default. The connection's
`store_payload` chooses for any source type: `full`, `summary` (only `jira` has one) or `none`, and
applies to the delivery record too.

### `hmac_json`: services that sign the body

For services that sign the raw body with HMAC-SHA256 and send the digest in a header. Prefer this
over `token_header` whenever a service can sign, because the signature also proves the body was not
changed.

| Extra field | Required | Meaning |
|---|---|---|
| `signature_header` | yes | Header carrying the signature |
| `signature_prefix` | no | Text before the digest, e.g. `sha256=` or `v1=` |
| `signature_encoding` | no | `hex` (default) or `base64` |
| `event_field` or `event_header` | one of them | Dotted path into the JSON body (`data.type`), or a header, naming the event. Prefer `event_field` when the service names the event in both: the signature covers the body but not the headers |
| `path_field` | no | Dotted path to a value that becomes the path, such as a service or project name |
| `delivery_id_field` or `delivery_id_header` | no | Dotted path into the JSON body, or a header, holding the ID the service gives each delivery and repeats when it retries one, so that [retries are recognised](#delivery-records). An integer ID is read as text. Prefer `delivery_id_field` when the service sends the ID in both: the signature covers it there, so a captured delivery sent again is answered as a retry too |

### `token_header`: services that send a secret header

For services that can only send the shared secret in a header you configure. A bare token proves
who sent a delivery but not that it was unmodified, so it relies on TLS.

| Extra field | Required | Meaning |
|---|---|---|
| `token_header` | yes | Header carrying the secret, e.g. `Authorization` |
| `token_prefix` | no | Text before the secret, e.g. `"Bearer "` |
| `event_field` or `event_header` | one of them | As for `hmac_json` |
| `path_field` | no | As for `hmac_json` |
| `delivery_id_field` or `delivery_id_header` | no | As for `hmac_json` |

### Other services

Built-in source types for more services are on the way. A service needs one when its signature
covers more than the body (for example a timestamp plus the body), when it needs a verification
handshake as the webhook is registered, or to name its events in more detail than one field.

To ask for a service, [open a feature
request](https://github.com/dheerajturaga/airflow-provider-inbound-webhooks/issues/new?template=feature_request.yml)
with a link to its webhook documentation. To add one yourself: a source type is a small class with
`verify()` and `parse()`; see [CONTRIBUTING.md](../CONTRIBUTING.md).

## Connection

One connection per webhook, named `webhook_<name>`, where `<name>` is lowercase letters, digits,
`-` and `_`. Its type is **Inbound Webhook** (`inbound_webhook`), which has a form in the Airflow
UI; the form's fields are stored in the connection extra under the keys below. Connections of other
types still work, but are deprecated: the API server logs a warning, `airflow webhooks check`
reports them, and a later release will refuse them. Fields left empty are treated as unset.

| Form field | Extra key | Meaning |
|---|---|---|
| Shared secret | (password) | The shared secret. Required. |
| Source type | `source_type` | Source type; defaults to `<name>` |
| Team | `team_name` | With multi-team Airflow (3.3 or later), events count as coming from this team, which decides which teams' Dags are queued. Without it, they only reach teamless Dags. Rejected on older Airflow. |
| Maximum body size | `max_body_bytes` | Larger requests are refused before verification. Default 1 MiB. |
| Stored payload | `store_payload` | What asset events and the delivery record keep of each delivery: `full`, the body as received; `summary`, which only `jira` has, and which keeps the whole payload on any other source type, as it did before 0.2; or `none`. Defaults to `summary` for `jira` and `full` otherwise. |
| Other fields | | Options of the source type, listed above |

Connections stored in Airflow's metadata database, in environment variables or in a secrets backend
all work. In an environment variable, give the type in JSON:
`AIRFLOW_CONN_WEBHOOK_GITHUB='{"conn_type": "inbound_webhook", "password": "<secret>"}'`.

## Configuration

Options in the `[inbound_webhooks]` section of `airflow.cfg` on the API server, or the environment
variables `AIRFLOW__INBOUND_WEBHOOKS__<OPTION>`, except `dag_checks`, which is read where Dag files
are parsed:

| Option | Default | Meaning |
|---|---|---|
| `webhook_names` | empty | Comma-separated names of the webhooks to serve, such as `github,jira`. Requests for any other name are answered with `404` without looking up a connection. Empty serves no webhook, and the API server log says so once. `*` serves any name and reads its connection on every delivery, which lets anyone who can reach the endpoint make a call to your secrets backend; it is deprecated, logs a warning, and will be removed in a later release. Changes need an API server restart. |
| `connection_cache_ttl` | `60` | Seconds a listed webhook's connection is reused before it is read again, including a missing or misconfigured one. A changed option takes up to this long to apply. A delivery that fails verification has the connection read again, at most once per this period, so a secret changed in the connection applies to the first delivery signed with it. `0` reads the connection on every delivery. |
| `dag_checks` | `off` | `error` refuses to load a Dag that uses a `WebhookTopic` in a way no delivery can serve: in a task's `outlets` or `inlets`, in a `PartitionedAssetTimetable`, or in a `\|` condition with another topic that one delivery can also match. Its Dag file then shows an import error that says why, and every Dag in that file stops running, not only the refused one, so keep Dags that schedule on topics in files of their own first; see "Checks" under [Writing Dags](writing-dags.md). Set the same value on the Dag processor and the workers, which parse Dag files: a worker that refuses a Dag cannot run its tasks. `off` checks nothing, and so does any other value, with a warning in the log. There is no warning mode: a warning that a cluster policy raises reaches only the Dag processor's log. |
| `delivery_retention` | `7` | Days of delivery records that `airflow webhooks clean` keeps unless told otherwise. Keep it longer than any of your services retries a delivery: a retry that comes after its first delivery's record is deleted is recorded again. |
| `max_event_payload_bytes` | `0` | Bytes of the largest payload copied into a delivery's asset events, measured as compact JSON in UTF-8 after `store_payload`, so for `jira` the summary is measured. A larger payload is left out: the events have `payload` `null` and `payload_truncated` `true`. The delivery record keeps the payload, and the deliveries page and API show it, until `airflow webhooks clean` deletes the record after `delivery_retention` days. `0` copies every payload. AIP-125 proposes `65536` as Airflow's default. |
| `rate_limit_per_minute` | `0` | Verified deliveries each API server worker records per webhook and minute; more are answered with `429` and `Retry-After` before anything is recorded. Requests that fail verification do not count, so they cannot use the limit up. Each worker counts on its own, so N workers record up to N times as many. GitLab disables a webhook after four failed deliveries in a row, `429` included, so set the limit above what a GitLab webhook sends. `0` turns the limit off. |

## Delivery records

Every verified delivery is stored in the table `inbound_webhooks_delivery` of Airflow's metadata
database, in the transaction that records its asset events. The table records the webhook, the
sender's delivery ID, when the delivery arrived, the names it offered and the asset events it
recorded. When it recorded any, the row also keeps its payload, as `store_payload` keeps it, even
one that `max_event_payload_bytes` leaves out of the asset events, until the row is deleted.

A retry repeats a delivery's ID and its body. It is answered with status `200` and `outcome`
`duplicate`, and records nothing. A redelivery of a delivery that recorded nothing is processed
again, so redelivering an event after subscribing to one of its names records it. Only
senders that send a delivery ID can be recognised this way: GitHub (`X-GitHub-Delivery`), GitLab
17.4 and later (`Idempotency-Key`), Jira (`X-Atlassian-Webhook-Identifier`), and `hmac_json` and
`token_header` services whose connection sets `delivery_id_field` or `delivery_id_header`.
Deliveries with an empty ID are never taken for retries.

`airflow db migrate` creates and upgrades the table, and `airflow db reset` drops it. On Airflow 3.1
both need [`external_db_managers`](compatibility.md). `airflow db clean` does not remove its rows:
`airflow webhooks clean` deletes those older than `delivery_retention`, so run it on a
schedule, as you would `airflow db clean`.

## Reading deliveries

Under **Browse → Webhook deliveries**, the Airflow UI lists the webhooks in `webhook_names` whose
deliveries you may read, and has a field for any other webhook's name. A webhook's page lists its
latest deliveries: when each arrived, its outcome, delivery ID and path, and the asset events it
recorded. Its search field finds the deliveries with a delivery ID, the `delivery_id` of their
responses. Each delivery's page shows the names it offered and its stored payload, which makes it
the place to find the name to subscribe to.

The same records are served as JSON, to a bearer token from Airflow's `/auth/token` or the UI's
session:

| Route | Returns |
|---|---|
| `GET /webhooks/_deliveries/<webhook>?limit=50&before=<time>&before_id=<id>&delivery_id=<ID>` | A webhook's deliveries, newest first, without payloads. `delivery_id` keeps those the sender gave that delivery ID, the `delivery_id` of their responses; several can share one, each with another body. `limit` is 1 to 500. To page back, pass the last delivery's `received_at` and `id` as `before` and `before_id` |
| `GET /webhooks/_deliveries/<webhook>/<id>` | One delivery, with its payload, by the `id` of its record, which the list shows: not the sender's delivery ID |

Reading a webhook's deliveries needs permission to read its connection, `webhook_<name>`, checked
for the team its extra's `team_name` names, and shows only the deliveries attributed to that team.
With Airflow's simple auth manager, the Op and Admin roles have it, and Viewer and User do not. A
webhook whose connection cannot be read shows nothing: Airflow reports a secrets backend it could
not read as a missing connection, so there would be no team to check. The page and these routes are
not a public interface yet, and can change in any release.

`GET /webhooks/_health` answers `200` without a login, for load balancers and probes.

## Command line

`airflow webhooks` has four commands. On Airflow 3.1, which does not add a provider's commands to
`airflow`, run them as `python -m airflow_inbound_webhooks.cli <command>`.

| Command | Does |
|---|---|
| `airflow webhooks check <webhook>` | Reports how deliveries to a webhook would be served: whether `webhook_names` lists it, whether its connection is valid, whether each `[inbound_webhooks]` option that takes a number is an integer, and whether the delivery table is ready. Exits with `1` if deliveries would fail |
| `airflow webhooks deliveries <webhook>` | Shows the latest deliveries of every team (`--limit`, 1 to 500, and `--output`), with `--delivery-id` only those the sender gave that delivery ID, the `delivery_id` of their responses, or with `--id` one delivery as JSON, with its payload, by the `id` of its record, which the list shows |
| `airflow webhooks clean` | Deletes delivery records older than `delivery_retention` days, or `--older-than` days, after asking. `--yes` skips the question, which a schedule needs; `--dry-run` only counts |
| `airflow webhooks send-test` | Sends a signed test delivery, as `python -m airflow_inbound_webhooks.send_test_event` does |

## Audit log and metrics

A delivery that records asset events writes an entry to Airflow's audit log, in the same
transaction: event `webhook_delivery`, owner `webhook:<name>`, shown as "Webhook <name>", with the
delivery ID and the recorded asset events in its extra. The entry has no Dag or run, because the
scheduler creates the runs later. Deliveries that record nothing, retries and refused requests
write no entry.

The endpoint sends these metrics through Airflow's metrics configuration, under the names AIP-125
proposes for Airflow, so dashboards keep working when webhooks move into Airflow:

| Metric | Type | Tags | Meaning |
|---|---|---|---|
| `webhooks.deliveries` | Counter | `webhook`, `outcome` | Answered deliveries |
| `webhooks.rejections` | Counter | `webhook`, `status` | Refused deliveries, by status code. Names not in `webhook_names` share the `webhook` value `other` |
| `webhooks.delivery_duration` | Timer | `webhook` | Time spent recording a verified delivery |

The tags need a metrics backend that keeps them: OpenTelemetry (`[metrics] otel_on`), or StatsD with
`statsd_datadog_enabled` or `statsd_influxdb_enabled`. Plain StatsD drops tags, so it counts the
deliveries of every webhook and outcome together.

On Airflow 3.3 and later, Airflow's asset listeners (`on_asset_changed`, `on_asset_event_emitted`)
hear of a delivery's asset events once the delivery is committed, so never of events that are rolled
back. On 3.1 and 3.2, Airflow notifies them while it records each event, inside the delivery's
transaction: a listener can hear of an event that is then rolled back, when the commit fails or when
a copy of the delivery arrives while another is being recorded and is answered as a duplicate. A
listener that fails is logged, and the delivery is answered as it would be without it.

## Responses and troubleshooting

`airflow webhooks check <webhook>` reports what would happen to a webhook's deliveries, and why they
would fail. A `200` or `202` answer has two fields:

| Field | Meaning |
|---|---|
| `delivery_id` | The sender's ID for the delivery, or `null` if it sends none |
| `outcome` | What became of the delivery: `accepted` when it recorded asset events; `no_subscribers`; `dag_paused` when every Dag that schedules on one of its names is paused (or, on Airflow 3.4 and later, draining), so nothing is recorded; or `duplicate` for a retry, answered `200`, which records nothing |

Nothing else is in the answer: services show answers to people who need not be Airflow users, as
GitHub's **Recent Deliveries** and GitLab's **Recent events** do, and the asset events a delivery
recorded would tell them which Dags exist. Under **Browse → Webhook deliveries**, the webhook's page
finds the delivery by its `delivery_id`, and the delivery's page shows the names it offered and the
asset events it recorded; see [Reading deliveries](#reading-deliveries).

| Status | Meaning | What to check |
|---|---|---|
| `200` `duplicate` | A retry of a delivery that recorded asset events: nothing is recorded again, not even for a Dag that has subscribed to one of its names since | Nothing, if the sender sent the delivery again. To try a Dag that subscribed since, trigger a new event rather than redelivering this one |
| `202` `accepted` | Recorded: an asset event on each subscribed name, which the delivery's page lists | If no Dag run starts, check that the scheduler is running. Before Airflow 3.4, a delivery on a name its Dag is already queued for, such as one that arrives while the Dag waits at `max_active_runs`, runs only with a later delivery; see [When Dags run](writing-dags.md#when-dags-run) |
| `202` `no_subscribers` | Verified, but no Dag schedules on any of the names it offered, so no asset event is recorded | The Dag schedules on one of the names the delivery's page lists, and has been parsed: `airflow dags reserialize` parses it now. The API server also logs the names at debug level |
| `202` `dag_paused` | Verified, but every Dag that schedules on one of its names is paused (or, on Airflow 3.4 and later, draining), so no asset event is recorded | Unpause the Dag, then redeliver the delivery: unpausing queues nothing, and a delivery that recorded nothing is processed again |
| `400` | The body is not a JSON object, holds `NaN` or `Infinity`, which JSON does not allow, or a number too large to store, such as `1e400`, or nests objects and arrays more than 64 levels deep; or the event name is missing, or the delivery ID is longer than 255 characters | GitHub: the content type is `application/json`. Jira: *Exclude body* is unchecked |
| `401` | Signature or token does not match | The secret in the service equals the connection's password |
| `404` | No such webhook: the name is not in `webhook_names`, or has no `webhook_<name>` connection | The name is in `webhook_names` and the API server was restarted after it changed; the connection ID is `webhook_` plus the name in the URL. Airflow's connection lookup also reports a database or secrets backend it could not read as a missing connection, which is then reused for `connection_cache_ttl` |
| `405` | The plugin is not loaded, so Airflow's UI answers | Restart the API server after installing; `airflow plugins` lists `webhooks` |
| `413` | Body larger than `max_body_bytes` | Raise `max_body_bytes` in the connection |
| `429` | More verified deliveries than `rate_limit_per_minute` allows; `Retry-After` says when to try again | Raise `rate_limit_per_minute`, or have the sender send fewer events |
| `503` | The connection is misconfigured, the database is unavailable, or the delivery table is missing; database failures carry `Retry-After` | The API server log says why. For the delivery table, run `airflow db migrate` |
