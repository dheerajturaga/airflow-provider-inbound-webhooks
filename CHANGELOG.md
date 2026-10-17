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

### Changed

- Upgrading needs `airflow db migrate`, which creates the table. On Airflow 3.1, first add
  `airflow_inbound_webhooks.db.WebhooksDBManager` to `[database] external_db_managers`.
- `gitlab` deliveries take their `delivery_id` from `Idempotency-Key`, which GitLab 17.4 and later
  keep across retries, instead of `X-Gitlab-Event-UUID`, which webhooks triggered by earlier webhooks
  share. Deliveries from older GitLab versions have no `delivery_id`.

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
