# Changelog

All notable changes are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project will use
[Semantic Versioning](https://semver.org/) from its first release. A change to the event names that
existing deliveries produce is a breaking change, listed under "Changed event names" with the old and
new names.

## Unreleased

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
  the offered names as `candidates`.
- Multi-team attribution through `team_name` on Airflow 3.3 and later.
- The "Inbound Webhook" connection type, with a form in the Airflow UI for every source type option.
- A quick start with `examples/send_test_event.py` and example Dags.
- Support for Airflow 3.1.1 and later.
