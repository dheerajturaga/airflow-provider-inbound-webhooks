# Compatibility

| Airflow | Support |
|---|---|
| 3.3 and later | Supported, including `team_name` |
| 3.2 | Supported; `team_name` is rejected because this version cannot attribute asset events to a team, and asset listeners are notified before the delivery is committed (see [Audit log and metrics](reference.md#audit-log-and-metrics)) |
| 3.1.1 to 3.1.x | Supported, without `team_name` and with asset listeners notified before the commit, as on 3.2. `airflow db migrate` only runs the DB managers `[database] external_db_managers` names, so add the plugin's before creating or upgrading its table: `external_db_managers = airflow_inbound_webhooks.db.WebhooksDBManager`, or `AIRFLOW__DATABASE__EXTERNAL_DB_MANAGERS` in the environment. Separate several managers with commas and no spaces, which 3.1 does not strip. On 3.1.1 and 3.1.2 the connection form only shows its fields when Flask-AppBuilder is installed (for example with the FAB provider); otherwise set the fields as JSON in the connection's extra. On later 3.1 patches with Python 3.10, fields with a fixed set of values, such as *Source type*, show as text boxes unless Flask-AppBuilder is installed; their descriptions list the values |
| 3.0, 3.1.0 | Not supported: their API server cannot read metadata-database connections through `airflow.sdk.Connection`, and installing next to them is refused |
| 2.x | Not supported: there is no FastAPI API server for the plugin to mount on |

CI tests the built wheel against Airflow 3.1.1, the newest patch of each supported minor version and
the newest release, each on the oldest and the newest Python it supports (3.10 to 3.13, and 3.14 from
Airflow 3.2) with SQLite, and against 3.1.1 and the newest release on PostgreSQL 17 and MySQL 8.4.
Each test run starts the real `airflow api-server` and sends it signed deliveries.

## Versioning

From 0.1.0 the project follows [Semantic Versioning](https://semver.org/) for its public interfaces:

- the endpoint `POST /webhooks/<name>`: its status codes, the `Retry-After` header of `429` and
  `503` answers, and the `delivery_id` and `outcome` fields of its responses;
- the `inbound_webhook` connection type, the connection options of each source type and the
  `[inbound_webhooks]` configuration options;
- [event names](event-names.md);
- each asset event's `extra`: the keys `from_webhook`, `webhook`, `source_type`, `source`, `path`,
  `delivery_id`, `inbound_delivery_id`, `correlation_key`, `payload` and `payload_truncated`, and
  for `jira` the issue fields its summary keeps;
- `WebhookTopic`, imported from `airflow_inbound_webhooks`: its arguments, the names it builds and
  its group `webhook`.

Below 1.0, a change that breaks any of them ships only in a new minor version, and the
[changelog](../CHANGELOG.md) describes it. Apart from `WebhookTopic`, which Dag files import, the
Python modules are not a public interface yet: code that imports anything else from
`airflow_inbound_webhooks` can break in any release. Neither are the messages in a response's
`detail`, in an error or in the API server log, nor the deliveries page and API.

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
