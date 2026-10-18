# Changelog

All notable changes are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/) for the public interfaces that "Versioning" in the README
lists. A change to the event names that existing deliveries produce is a
breaking change, listed under "Changed event names" with the old and new names.

## Unreleased

### Added

- Delivery records: every verified delivery is stored in a new table, `inbound_webhooks_delivery`,
  in the transaction that records its asset events. A retry, which repeats the sender's delivery ID
  and the body, gets the answer the delivery got and records nothing, so a service's retries no
  longer queue Dags again. A redelivery of a delivery that recorded nothing is processed again. A
  delivery ID longer than 255 characters is refused with `400`, and an empty one counts as none.
- `outcome` and `delivery_id` in every answer. `outcome` is `accepted`, `no_subscribers`,
  `dag_paused`, or `duplicate` for a retry, which is answered `200` instead of `202`.
- `from_webhook`, `webhook`, `source_type`, `inbound_delivery_id` and `correlation_key` in each
  asset event's `extra`, the keys AIP-125 proposes for webhook events. `inbound_delivery_id` is the
  ID of the delivery's record, so an event leads back to its delivery while the record is kept, even
  when the service sends no delivery ID. `source` stays, holding the same as `webhook`, and
  `correlation_key` is `null` until a source type sets one.
- `store_payload` for every source type: `full`, `summary` (only `jira` has one) or `none`, applied
  to asset events and delivery records alike. The defaults are unchanged: `jira` stores a summary,
  every other source type the body as received. `summary` on another source type, which 0.1.0's
  form offered, still keeps the body as received, and logs a warning.
- **Browse → Webhook deliveries** in the Airflow UI, and the same records as JSON under
  `GET /webhooks/_deliveries/<name>`. Both need an Airflow login and permission to read the
  webhook's connection.
- `GET /webhooks/_health`, answered without a login.
- `delivery_id_field` and `delivery_id_header` for `hmac_json` and `token_header`: where the service
  puts the ID it gives each delivery, so that its retries are recognised too.
- `airflow webhooks check`, `deliveries`, `clean` and `send-test`, on Airflow 3.2 and later, and as
  `python -m airflow_inbound_webhooks.cli` on 3.1. `clean` deletes delivery records older than the
  new `[inbound_webhooks] delivery_retention`, 7 days by default.
- An audit log entry for each delivery that records asset events, owned by `webhook:<name>`.
- Metrics, under the names AIP-125 proposes for Airflow: `webhooks.deliveries`, tagged with the
  webhook and the outcome; `webhooks.rejections`, tagged with the webhook and the status code; and
  `webhooks.delivery_duration`, tagged with the webhook.
- `[inbound_webhooks] rate_limit_per_minute`, off by default: verified deliveries over it are
  answered with `429` and `Retry-After` before anything is recorded. Requests that fail verification
  do not count, and each API server worker counts on its own.
- `[inbound_webhooks] max_event_payload_bytes`, off by default; AIP-125 proposes 65536 for Airflow.
  A payload larger than it, as compact JSON, is left out of the delivery's asset events, whose
  `extra` then has `payload` `null` and `payload_truncated` `true`; the delivery record keeps it
  until `airflow webhooks clean` deletes the record. Every event's `extra` has `payload_truncated`,
  which is `false` unless the payload was left out.
- `WebhookTopic`, imported from `airflow_inbound_webhooks`, the documented way for a Dag to
  subscribe: an asset in the group `webhook`, named from its segments as the plugin names
  deliveries, as in `WebhookTopic(webhook="github", path="my-org/web", event="pull_request")`, or
  from the whole name, which it refuses if no delivery can offer it. It refuses `extra` and
  `watchers`. `Asset(name)` keeps working. Dag files that use it need the package wherever they are
  parsed, not only on the API server.
- `[inbound_webhooks] dag_checks`, `off` by default. `error` refuses to load a Dag that has a
  `WebhookTopic` in a task's `outlets` or `inlets`, in a `PartitionedAssetTimetable`, or in a `|`
  condition with another topic that one delivery can also match. Its Dag file shows an import
  error that says why, which stops every Dag in that file, so keep Dags that schedule on topics in
  files of their own before turning the checks on. The checks are a cluster policy, which the
  package registers through the `airflow.policy` entry point.

### Changed

- Asset events are only recorded on names that a Dag schedules on, directly or through
  `Asset.ref(name=...)` or `Asset.ref(uri=...)`, which is not stale, paused or, on Airflow 3.4 and
  later, draining. A delivery that only paused or draining Dags subscribe to records
  nothing and is answered with `outcome` `dag_paused`; before, it recorded asset events that Airflow
  queued no Dag for. Stale Dags no longer count as subscribers.
- A delivery that cannot be recorded because the database is unavailable, or the delivery table is
  missing, is answered with `503` and `Retry-After` instead of `500`, so senders that retry try again.
  The failure is logged at most once a minute.
- A source type whose verification raises an unexpected exception answers `401`, like any failed
  verification, instead of `500`.
- A delivery that fails verification has its webhook's connection read again, at most once per
  `connection_cache_ttl`, and is verified again, so a secret changed in the connection applies to
  the first delivery signed with it rather than once the cached connection expires. Unauthenticated
  requests can now cost two connection reads per webhook and `connection_cache_ttl`, not one.
- A JSON body holding `NaN` or `Infinity`, which JSON does not allow and MySQL refuses to store, is
  answered with `400`.

- Upgrading needs `airflow db migrate`, which creates the table. On Airflow 3.1, first add
  `airflow_inbound_webhooks.db.WebhooksDBManager` to `[database] external_db_managers`.
- `gitlab` deliveries take their `delivery_id` from `Idempotency-Key`, which GitLab 17.4 and later
  keep across retries, instead of `X-Gitlab-Event-UUID`, which webhooks triggered by earlier webhooks
  share. Deliveries from older GitLab versions have no `delivery_id`.
- On Airflow 3.3 and later, asset listeners hear of a delivery's asset events once it is committed,
  rather than while the events are recorded, so never of events that are rolled back, and one that
  fails is logged. On 3.1 and 3.2, Airflow still notifies them inside the transaction.
- The `path` in each asset event's `extra` is cut to 1500 characters, as in the delivery record: a
  longer path cannot be part of an asset name.
- A JSON body holding a number too large to store, such as `1e400`, which Python reads as infinity,
  is answered with `400`, as one holding `Infinity` is. Before, it was recorded, and stored as
  `Infinity` where the database accepted it.
- A JSON body that nests objects and arrays more than 64 levels deep is answered with `400`.
  Before, MySQL refused to store one nested about 100 levels deep, which was answered with `503`, so
  a sender that retries kept sending it; one nested about 255 levels deep was recorded in asset
  events that Airflow's REST API cannot return; and one nested a little less deeply than Python can
  parse was answered with `500`.

### Deprecated

- `[inbound_webhooks] webhook_names = *`, which serves any name and so reads a connection on every
  delivery. It logs a warning once per API server worker and will be removed in a later release:
  list the webhook names to serve instead.
- Webhook connections whose type is not `inbound_webhook`, such as `generic` connections from
  environment variables. The API server logs a warning once per worker and connection,
  `airflow webhooks check` reports them, and a later release will refuse them: set the connection's
  type to `inbound_webhook`.

## 0.1.0 - 2026-10-14

### Added

- `POST /webhooks/<name>` on the Airflow API server, registered as a plugin through the
  `airflow.plugins` entry point. Each verified delivery is recorded as asset events on the names Dags
  schedule on.
- Configuration through one `webhook_<name>` connection per webhook: the password is the shared
  secret, and the extra selects and configures the source type.
- `[inbound_webhooks] webhook_names`, the names of the webhooks the endpoint serves. Requests for
  any other name are refused without reading a connection; while it is unset, no webhook is served.
- `[inbound_webhooks] connection_cache_ttl`: each listed webhook's connection is read at most once a
  minute by default, so unauthenticated requests cannot each cost a call to a secrets backend.
- Source types `hmac_json` (HMAC-SHA256 body signatures, hex in either case or base64), `token_header` (shared
  secret in a header), `github` and `gitlab`.
- Source type `jira`, for Jira Cloud and Data Center webhooks with a secret. It names events after
  status transitions, added labels and changed fields, and stores a summary of the issue unless
  `store_payload` is `full`.
- The event naming rule `<webhook name>/<path>/<event>/<action>/<detail>`. Every prefix can be
  subscribed to, also with `*` in place of each trailing part of the path, and each response lists
  the offered names as `candidates`. Names are folded to ASCII, and parts left empty are dropped.
- Multi-team attribution through `team_name` on Airflow 3.3 and later.
- The "Inbound Webhook" connection type, with a form in the Airflow UI for every source type option.
- A quick start, with a test sender, `python -m airflow_inbound_webhooks.send_test_event`, and
  example Dags.
- Support for Airflow 3.1.1 and later, on Python 3.10 to 3.13 (3.14 from Airflow 3.2), with a SQLite,
  PostgreSQL or MySQL metadata database.
