# Design notes

Background for contributors, and for proposing this feature to Apache Airflow itself. User
documentation is in the [README](../README.md).

## Request flow

1. `POST /webhooks/<name>` reaches the plugin's FastAPI app, mounted by the API server under
   `/webhooks` without Airflow authentication.
2. A name that `[inbound_webhooks] webhook_names` does not list is a `404` before anything is read.
   Otherwise the connection `webhook_<name>` is read through `airflow.sdk.Connection`, and the
   result, a missing or invalid connection included, is reused for `connection_cache_ttl` seconds.
   Its password is the shared secret; its extra selects the source type and its options. A missing
   connection is a `404`, an invalid one a `503`.
3. The body is read up to `max_body_bytes`.
4. The source type verifies the signature or token, then parses the delivery into a path and kinds.
5. `naming.build_candidate_names` turns those into every subscribable name.
6. If the delivery table already holds the delivery's ID and body digest for this webhook, and that
   delivery recorded events, it is a retry: it is answered from that row, and nothing is written. A
   delivery that recorded nothing is processed again, so a redelivery after subscribing records it.
7. One query finds which candidates are active assets, that is, referenced by a Dag.
8. `asset_manager.register_asset_change` records an asset event on each, which queues the Dags that
   schedule on it, and the delivery's row is inserted. The endpoint commits its own database session
   before responding, so `accepted` means the events and the row are stored. A copy of the delivery
   that arrives while the first is being recorded fails on the unique constraint, or for a
   redelivery on an update that only matches a row with no events, which rolls back its events, and
   it is answered from the first copy's row.

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
| 3.1.3 to 3.1.x | The hook's `get_connection_form_widgets()` and `get_ui_field_behaviour()`, called with WTForms, Flask-Babel and Flask-AppBuilder replaced by mocks. On Python 3.10, `mock.patch` resolves `wtforms.validators.any_of` through the `wtforms` mock instead of the separate `wtforms.validators` mock the hook imports, so choice fields lose their values and show as text, for every provider; the descriptions list the values |
| 3.1.1 and 3.1.2 | The same hook methods, but these versions only mock those packages when they are installed, so without Flask-AppBuilder no provider's form has fields |

The form saves every field, with `null` for those left empty, so the endpoint drops `null` and `""`
values from the extra before reading it.

## Configuration options

`configuration.py` defines the `[inbound_webhooks]` options once. The provider metadata declares
them under `config`, so `airflow config list` and `airflow config get-value` know their defaults, and
the endpoint reads them with the same defaults as fallbacks, so it does not depend on the process
having loaded provider configuration.

## The delivery table

`models.py` defines `inbound_webhooks_delivery` in a metadata of its own, with Airflow's naming
convention and schema setting, because Airflow refuses an external DB manager whose tables are in
its own metadata. `db.WebhooksDBManager` puts the table under `airflow db migrate`. Airflow 3.2 and
later find it through `db-managers` in the provider metadata; 3.1 reads only
`[database] external_db_managers`.

On Airflow 3.1, `airflow db migrate` builds the table from the metadata when Airflow's own tables
are new too, and by running the migrations in `migrations/versions` when they exist already.
Later versions build it from the metadata either way, and stamp the newest revision, unless
`--use-migration-files` is given. `tests/test_db.py` checks that both give the same schema, so a
change to the table needs a model change and a migration that agree.

## Airflow internals the plugin uses

| Dependency | Why | Risk |
|---|---|---|
| `airflow.assets.manager.asset_manager.register_asset_change` | Records the asset event and queues Dags exactly as Airflow does | Its signature changed in 3.2 and 3.3, and it accepts and drops unknown keyword arguments, so a renamed argument would be lost silently. The endpoint inspects `AssetManager.register_asset_change`: deliveries fail with a `500` if `asset`, `extra` or `session` is no longer a named parameter, and `source_is_api` and `api_user_teams` are passed only where they are (3.3 and later). A deployment can replace the manager with `[core] asset_manager_class`; the check reads Airflow's own class, so a replacement that forwards `**kwargs` still works. |
| `airflow.models.asset.AssetModel` | Finds which candidate names are active assets | Database models are internal; a relationship was renamed in 3.1 |
| `airflow.utils.session.create_session` | A database session the endpoint commits before it responds | Same signature in every supported version. It is what the API server's own `SessionDep` wraps; `SessionDep` is marked private and commits in dependency cleanup, whose timing depends on FastAPI (Airflow pins it with `scope="function"` from 3.2). |
| `airflow.utils.db_manager.BaseDBManager` | Creates and migrates the delivery table as part of `airflow db migrate` | Internal, and its behaviour differs by version: on an existing Airflow installation, 3.1 runs the migrations, and otherwise the table is created from the metadata. The manager overrides none of its methods, and `tests/test_db.py` checks that both ways give the same schema |
| `airflow.models.base` (`_get_schema`, `naming_convention`) and `airflow.utils.sqlalchemy.UtcDateTime` | Give the table Airflow's schema setting, constraint names and timestamp type | Internal; the same in every supported version |
| `airflow.configuration.conf` | Reads the `[inbound_webhooks]` options | How every Airflow component reads its configuration, with the same `get` and `getint` in every supported version. `airflow.sdk.conf` is the Task SDK's parser and only exists from 3.2. |
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
   The delivery table recognises a retry by the sender's delivery ID and the body's digest, which only
   catches a service's own retries: GitHub's `X-GitHub-Delivery` and GitLab's `Idempotency-Key` are headers no signature
   covers, so a deliberate replay can change them, and the event headers along with them. Source types
   whose signature covers a timestamp could reject old deliveries. Jira's does, through the `timestamp` in the signed body, but Jira
   Cloud retries a failed delivery for more than an hour, so the window would have to be as long.
2. **Probing webhook names.** `webhook_names` and the connection cache bound what unauthenticated
   requests cost: an unlisted name is refused before any lookup, and each API server worker reads a
   listed webhook's connection at most once per `connection_cache_ttl`, even when the lookup or the
   verification fails. With `*` nothing is cached, because the cache would keep an entry for
   every name anyone sends. A listed name still answers a bad signature with `401` rather than
   `404`, so which names are served can be probed. That is kept because the `404` is the main
   troubleshooting signal and names such as `github` are easy to guess; the secret is what protects
   a webhook. Should unlisted and unverified requests get the same answer?
3. **Plugin import weight.** Every Airflow component that loads plugins, including each task run,
   imports the plugin. The endpoint's Airflow internals are imported on the first delivery, so
   other components only pay for FastAPI and building the app, about 70 ms on Airflow 3.3. Avoiding
   that too would mean giving Airflow a lazily built app rather than the FastAPI app `fastapi_apps`
   is documented to take.
4. **Payload retention.** Each source type stores the body as received, unless every delivery also
   carries a full copy of the object it is about; only Jira's do, so `jira` alone stores a summary by
   default. Anyone who can read asset events can read the payloads. A `store_payload` option for the
   other source types, offering a summary or nothing, could be added later without breaking anyone,
   as long as their default stays the full body.
5. **Audit.** The REST API writes an action log entry per user; a delivery has no user.
6. **Name stability.** From 0.1.0, changing a name that an existing delivery produces is a breaking
   change, released in a new minor version below 1.0 (README, "Event names"). Renaming a repository
   or project still changes its names, which the plugin cannot prevent.
7. **Source types from other packages.** `SOURCE_TYPES` is a fixed dictionary; other packages could
   contribute source types through an entry point. That would make `WebhookSource` and `WebhookEvent`
   a public interface; until then the Python modules are internal (README, "Versioning").
8. **Verification handshakes.** Some services verify a URL when the webhook is registered and expect
   a custom response, which the `WebhookSource` interface cannot produce yet.

## Towards Airflow core

As a core feature, the endpoint would use these internals legitimately and remove the dependency on
unauthenticated plugin apps. A possible split: core provides the endpoint, connection resolution,
the naming rule and a public `WebhookSource` interface, while providers contribute source types,
much as the `common.messaging` provider defines a queue interface that other providers implement.

Proposing that needs a discussion on the Airflow dev list, and probably an Airflow Improvement
Proposal, because it adds a class of routes that skip Airflow authentication.
