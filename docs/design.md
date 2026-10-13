# Design notes

Background for contributors, and for proposing this feature to Apache Airflow itself. User
documentation is in the [README](../README.md).

## Request flow

1. `POST /webhooks/<name>` reaches the plugin's FastAPI app, mounted by the API server under
   `/webhooks` without Airflow authentication.
2. The connection `webhook_<name>` is read through `airflow.sdk.Connection`. Its password is the
   shared secret; its extra selects the source type and its options. A missing connection is a
   `404`, an invalid one a `503`.
3. The body is read up to `max_body_bytes`.
4. The source type verifies the signature or token, then parses the delivery into a path and kinds.
5. `naming.build_candidate_names` turns those into every subscribable name.
6. One query finds which candidates are active assets, that is, referenced by a Dag.
7. `asset_manager.register_asset_change` records an asset event on each, which queues the Dags that
   schedule on it. The endpoint commits its own database session before responding, so `accepted`
   means the events are stored.

The plugin never creates Dag runs; Airflow's own asset scheduling does.

## The connection form

The package is also an Airflow provider: an `apache_airflow_provider` entry point returns its
metadata, which registers the `inbound_webhook` connection type with `InboundWebhookHook`. The hook
only anchors the form; the endpoint reads connections itself.

Airflow builds connection forms differently across the supported versions, so
`connection_form.py` defines the fields once and renders both formats:

| Airflow | Form source |
|---|---|
| 3.2 and later | `conn-fields` and `ui-field-behaviour` in the provider metadata. The hook methods are then skipped, so their deprecation warning does not appear. |
| 3.1.3 to 3.1.x | The hook's `get_connection_form_widgets()` and `get_ui_field_behaviour()`, called with WTForms, Flask-Babel and Flask-AppBuilder replaced by mocks |
| 3.1.1 and 3.1.2 | The same hook methods, but these versions only mock those packages when they are installed, so without Flask-AppBuilder no provider's form has fields |

The form saves every field, with `null` for those left empty, so the endpoint drops `null` and `""`
values from the extra before reading it.

## Airflow internals the plugin uses

| Dependency | Why | Risk |
|---|---|---|
| `airflow.assets.manager.asset_manager.register_asset_change` | Records the asset event and queues Dags exactly as Airflow does | Its signature changed in 3.2 and 3.3, and it accepts and drops unknown keyword arguments, so a renamed argument would be lost silently. The endpoint inspects `AssetManager.register_asset_change`: deliveries fail with a `500` if `asset`, `extra` or `session` is no longer a named parameter, and `source_is_api` and `api_user_teams` are passed only where they are (3.3 and later). A deployment can replace the manager with `[core] asset_manager_class`; the check reads Airflow's own class, so a replacement that forwards `**kwargs` still works. |
| `airflow.models.asset.AssetModel` | Finds which candidate names are active assets | Database models are internal; a relationship was renamed in 3.1 |
| `airflow.utils.session.create_session` | A database session the endpoint commits before it responds | Same signature in every supported version. It is what the API server's own `SessionDep` wraps; `SessionDep` is marked private and commits in dependency cleanup, whose timing depends on FastAPI (Airflow pins it with `scope="function"` from 3.2). |
| Plugin apps are mounted without authentication | The endpoint must accept services that cannot log in | If Airflow ever required login for plugin apps, every delivery would be rejected |

Public interfaces used: `AirflowPlugin` with `fastapi_apps` and the `airflow.plugins` entry point,
`airflow.sdk.Connection`, `airflow.exceptions` and, in the examples, `airflow.sdk`.

`airflow.sdk.Connection` reads metadata-database connections only in a server context. The API
server declares itself one from Airflow 3.1.1, which sets the minimum version: on 3.0.x and 3.1.0 the
lookup fails and every delivery gets a `500`.

An alternative to the three internal APIs is calling Airflow's public REST API
(`POST /api/v2/assets/events`) from the plugin with a service account. It would add access control
per asset and audit logging, at the cost of managing a token, an extra HTTP round trip and a lookup
from names to asset IDs. It would not remove the dependency on unauthenticated plugin apps.

## Open questions

1. **Replay protection.** GitHub signs the body but not a timestamp; GitLab sends a bare token.
   Deduplicating on the delivery ID needs a store; source types whose signature covers a timestamp
   could reject old deliveries.
2. **Cost of unauthenticated requests.** Any `/webhooks/<name>` request triggers a connection lookup
   before it can be rejected, which may hit a remote secrets backend. Cache lookups, or list the
   enabled names in configuration?
3. **Plugin import weight.** Every Airflow component that loads plugins, including each task run,
   imports the plugin. The endpoint's Airflow internals are imported on the first delivery, so
   other components only pay for FastAPI and building the app, about 70 ms on Airflow 3.3. Avoiding
   that too would mean giving Airflow a lazily built app rather than the FastAPI app `fastapi_apps`
   is documented to take.
4. **Payload retention.** Each recorded asset event stores the full payload. Should source types
   store a summary?
5. **Audit.** The REST API writes an action log entry per user; a delivery has no user.
6. **Name stability.** Renaming a repository or project changes its event names.
7. **Source types from other packages.** `SOURCE_TYPES` is a fixed dictionary; other packages could
   contribute source types through an entry point.
8. **Verification handshakes.** Some services verify a URL when the webhook is registered and expect
   a custom response, which the `WebhookSource` interface cannot produce yet.

## Towards Airflow core

As a core feature, the endpoint would use these internals legitimately and remove the dependency on
unauthenticated plugin apps. A possible split: core provides the endpoint, connection resolution,
the naming rule and a public `WebhookSource` interface, while providers contribute source types,
much as the `common.messaging` provider defines a queue interface that other providers implement.

Proposing that needs a discussion on the Airflow dev list, and probably an Airflow Improvement
Proposal, because it adds a class of routes that skip Airflow authentication.
