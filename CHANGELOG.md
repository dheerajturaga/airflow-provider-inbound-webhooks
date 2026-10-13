# Changelog

All notable changes are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project will use
[Semantic Versioning](https://semver.org/) from its first release.

## Unreleased

### Added

- `POST /webhooks/<name>` on the Airflow API server, registered as a plugin through the
  `airflow.plugins` entry point. Each verified delivery is recorded as asset events on the names Dags
  schedule on.
- Configuration through one `webhook_<name>` connection per webhook: the password is the shared
  secret, and the extra selects and configures the source type.
- Source types `hmac_json` (HMAC-SHA256 body signatures, hex or base64), `token_header` (shared
  secret in a header), `github` and `gitlab`.
- The event naming rule `<webhook name>/<path>/<event>/<action>/<detail>`, with every prefix and a
  `*` path wildcard subscribable, and the offered names returned as `candidates`.
- Multi-team attribution through `team_name` on Airflow 3.3 and later.
- The "Inbound Webhook" connection type, with a form in the Airflow UI for every source type option.
- A quick start with `examples/send_test_event.py` and example Dags.
- Support for Airflow 3.1.1 and later.
