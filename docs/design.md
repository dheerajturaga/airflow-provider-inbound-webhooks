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
4. The source type verifies the signature or token. A delivery that fails verification has the
   connection read again, at most once per `connection_cache_ttl`, and is verified against what that
   finds, so a secret changed in the connection applies at once. The cache entry keeps its expiry,
   so a read before the change does not keep the old secret cached longer. A verified delivery over
   `rate_limit_per_minute` is a `429`, counted per process; requests that fail verification do not
   count, so unsigned traffic cannot have the sender's own deliveries refused. The source type then
   parses the delivery into a path and kinds.
5. `naming.build_candidate_names` turns those into every subscribable name.
6. If the delivery table already holds the delivery's ID and body digest for this webhook, and that
   delivery recorded events, it is a retry: it is answered `200` from that row, and nothing is written. A
   delivery that recorded nothing is processed again, so a redelivery after subscribing records it.
7. One query finds which candidates are active assets that a Dag schedules on which is not stale,
   paused or draining, directly or through `Asset.ref(name=...)` or `Asset.ref(uri=...)`, which
   Airflow queues too. That is the test Airflow applies before it runs an asset's watchers, plus
   draining (Airflow 3.4 and later), which Airflow's queueing skips too. If only paused or draining
   Dags schedule on a candidate, the outcome is `dag_paused` and nothing is recorded, so unpausing
   never replays the delivery.
8. `asset_manager.register_asset_change` records an asset event on each, which queues the Dags that
   schedule on it, and the delivery's row is inserted, or for a redelivery updated. Every event's
   `extra` holds the row's `id` as `inbound_delivery_id`, which the endpoint chooses before it
   records the events, so an event leads back to its delivery even when the sender sends no delivery
   ID; a redelivery's events hold the `id` of the row it updates, which takes the team they are
   attributed to now. The endpoint commits its own database session before responding, so
   `accepted` means the events and the row are stored. A copy of the delivery that arrives while the
   first is being recorded fails on the unique constraint, or for a redelivery on an update that
   only matches a row with no events, which rolls back its events, and it is answered from the first
   copy's row. A database that cannot be used, or lacks the delivery table, is a `503` with
   `Retry-After`, logged at most once a minute.
9. Once the transaction has committed, the endpoint sends the asset listener notifications that
   `register_asset_change` handed back through `callback_sink`, so listeners hear only of events
   that are stored. One that fails is logged, and the delivery is still answered as recorded.
   Airflow 3.1 and 3.2 have no `callback_sink` and notify listeners inside the transaction, so there
   a listener can hear of events that are rolled back, such as those of a copy that lost the race.

The plugin never creates Dag runs; Airflow's own asset scheduling does.

## The connection form

The package is also an Airflow provider: an `apache_airflow_provider` entry point returns its
metadata, which registers the `inbound_webhook` connection type with `InboundWebhookHook`. The hook
only anchors the form; the endpoint reads connections itself. It still serves connections of other
types, as 0.1 did, but logs a deprecation warning: AIP-125 serves only `inbound_webhook` connections.

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

## Reading deliveries

`deliveries.py` serves the delivery records under `/webhooks/_deliveries` as JSON and under
`/webhooks/_ui` as a page, which the plugin adds to the UI's Browse menu as an external view. The UI
frames the page from that view's `href`, sandboxed without scripts, so the page is plain HTML with
links and runs no script. Its `href` carries the path of `[api] base_url`, as Airflow derives its own
root path.

Plugin apps are mounted without Airflow's authorization, so these routes check the login themselves:
Airflow's `get_user` takes a bearer token or the UI's session cookie, and the auth manager decides
whether the user may read the webhook's connection, for the team in its extra, which is the team the
endpoint attributes the webhook's events to. Only deliveries recorded for that team are shown. When
the connection cannot be read, which `airflow.sdk`'s `Connection` also reports for a secrets backend
it could not reach, nothing is shown rather than checking the reader against no team. Everything a
sender wrote is escaped, and a Content-Security-Policy lets the page load nothing and be framed
only by Airflow.

Pages go back by the last delivery's `(received_at, id)`, both descending, so deliveries received
at the same time are neither skipped nor shown twice, and the database can read the
`(webhook, received_at)` index backwards. The page lists only the webhooks in `webhook_names`, since
listing those with stored deliveries would mean a `DISTINCT` over the whole table on every view.

## Webhook topics

`WebhookTopic`, in `topics.py`, is an `Asset` whose class sets `asset_type` to `webhook`. `Asset`'s
`group` defaults to its class's `asset_type`, as for Airflow's own `Dataset` and `Model`, so topics
get the group `webhook`. Serialization keeps the group but not the class: the Dag processor stores a
topic as a plain asset, and the scheduler and the endpoint, which match by name, never see the
subclass.

A topic must have a name that some delivery can offer, or its Dag would never run. From segments,
the name is built with the endpoint's own normalization: `naming.normalize_path`,
`normalize_segment` and `build_name`, which `build_candidate_names` uses too. A whole name is
checked with `naming.can_be_offered` and refused rather than normalized, because a name that
normalization would change is a mistake, and changing it silently could subscribe the Dag to other
events. The URI is the name. `attrs.evolve` calls `__init__` with every field, so an empty `extra`
and `watchers` and the name as `uri` must pass; `copy.copy` and `copy.deepcopy` restore the fields
without calling it.

Dag files import `WebhookTopic` from the package. A module `__getattr__` imports `topics.py` only
when a Dag file asks for it, because `topics.py` imports the Task SDK, and so Airflow, which
initializes as it is imported. Importing the package alone, as
`python -m airflow_inbound_webhooks.send_test_event` does, imports nothing from Airflow. Airflow's
own processes gain nothing by it: `import airflow` imports the Task SDK too, through
`conf.mask_secrets()`, in every supported version.

A topic does not equal a plain `Asset` of its name, because their groups differ. Looking events up
with the topic, as in `triggering_asset_events[topic]`, works, because the accessor keys events by
name and URI; the keys it yields when iterated over are plain assets.

## Parse-time checks

`policy.py` is a cluster policy, registered under the `airflow.policy` entry point. Airflow loads
policy entry points as it initializes, when first imported, in every process (`settings.initialize`
calls `load_policy_plugins` in 3.1 to 3.3), so the module imports only what Airflow has loaded by
then. `dag_policy` returns at once while the process has not imported `topics.py`, since no Dag can
then hold a topic, and while `[inbound_webhooks] dag_checks` is `off`. Otherwise it collects every
problem of the Dag and raises one `AirflowClusterPolicyViolation`, as Airflow's documentation
recommends.

`DagBag.bag_dag` calls the policy for each Dag of a file once the file is imported: in the Dag
processor, and when a worker parses the file to run a task. An `AirflowClusterPolicyViolation`
becomes the file's import error, and only that Dag is left out of the file's Dags. A warning would
not be seen: Airflow captures a file's warnings only while importing it, before policies run, and
shows none in the UI. The Dag processor reports no warnings on 3.1, and only those of its Dag
stability check from 3.2. So there is no warning mode.

The checks find topics in each task's `outlets` and `inlets`, and in the timetable's
`asset_condition`. The timetables the Task SDK builds from `schedule`, and from 3.2 those
`airflow.sdk` exports, keep the Dag file's `WebhookTopic` objects. From 3.2 a timetable imported from
`airflow.timetables` instead turns them into serialized assets, so a Dag that uses one is not
checked. Two topics in one `|` condition, or in `|` conditions directly inside it, are refused when
`naming.can_be_offered_together` says one delivery can offer both names through one kind; its
docstring states the rule and its two limits. Topics on either side of an `&` are not compared: one
delivery that matches both satisfies the `&` once.

## Airflow internals the plugin uses

| Dependency | Why | Risk |
|---|---|---|
| `airflow.assets.manager.asset_manager.register_asset_change` | Records the asset event and queues Dags exactly as Airflow does | Its signature changed in 3.2 and 3.3, and it accepts and drops unknown keyword arguments, so a renamed argument would be lost silently. The endpoint inspects `AssetManager.register_asset_change`: deliveries fail with a `500` if `asset`, `extra` or `session` is no longer a named parameter, and `source_is_api`, `api_user_teams` and `callback_sink` are passed only where they are (3.3 and later). A deployment can replace the manager with `[core] asset_manager_class`; the check reads Airflow's own class, so a replacement that forwards `**kwargs` still works. |
| `airflow.models.asset.AssetModel` | Finds which candidate names are active assets | Database models are internal; a relationship was renamed in 3.1 |
| `DagModel`, `DagScheduleAssetReference`, `DagScheduleAssetNameReference` and `DagScheduleAssetUriReference` | Find the Dags that schedule on a candidate, and whether they are stale, paused or draining | Internal. `DagModel.is_draining` only exists from 3.4, so the query uses it only where it exists |
| `airflow.utils.session.create_session` | A database session the endpoint commits before it responds | Same signature in every supported version. It is what the API server's own `SessionDep` wraps; `SessionDep` is marked private and commits in dependency cleanup, whose timing depends on FastAPI (Airflow pins it with `scope="function"` from 3.2). |
| `airflow.utils.db_manager.BaseDBManager` | Creates and migrates the delivery table as part of `airflow db migrate` | Internal, and its behaviour differs by version: on an existing Airflow installation, 3.1 runs the migrations, and otherwise the table is created from the metadata. The manager overrides none of its methods, and `tests/test_db.py` checks that both ways give the same schema |
| `airflow.models.base` (`_get_schema`, `naming_convention`) and `airflow.utils.sqlalchemy.UtcDateTime` | Give the table Airflow's schema setting, constraint names and timestamp type | Internal; the same in every supported version |
| `airflow.models.log.Log` | Writes the audit log entry for a delivery that records events | Internal; the same constructor in every supported version, and a `team_name` column only from 3.4, which the entry sets where it exists |
| `Stats` from `airflow.sdk.observability.stats`, or `airflow.stats` on 3.1 | Sends the delivery metrics | It moved to the Task SDK in 3.2; `airflow.stats` remains as a deprecated alias |
| `airflow.api_fastapi.core_api.security.get_user`, `airflow.api_fastapi.app.get_auth_manager` and `ConnectionDetails` | Check the login and permissions of the deliveries page and API | Internal. `get_user`'s second parameter has a different name in 3.1 and 3.2 than in 3.3, so it is called positionally. The real API server tests check a bearer token and the UI's session cookie on every supported version |
| `airflow.cli.cli_config` (`ActionCommand`, `GroupCommand`, `Arg`, `lazy_load_command`, `ARG_OUTPUT`, `ARG_YES`) and `airflow.cli.simple_table.AirflowConsole` | Define `airflow webhooks` for the provider's `cli`, and build the parser of `python -m airflow_inbound_webhooks.cli` from the same commands | Internal, but what Airflow's own providers use for their commands; the same in every supported version |
| `airflow.configuration.conf` | Reads the `[inbound_webhooks]` options | How every Airflow component reads its configuration, with the same `get` and `getint` in every supported version. `airflow.sdk.conf` is the Task SDK's parser and only exists from 3.2. |
| `Asset.asset_type` | Gives `WebhookTopic` its default group, `webhook` | A class attribute Airflow's own `Dataset` and `Model` set, not documented as a way to set the group. `Asset`'s `group` defaults to it in every supported version, and `access_control`, which `WebhookTopic` passes on when given, only exists from 3.3 |
| A timetable's `asset_condition`, and the `objects` of `AssetAny` and `AssetAll` | Find the topics a Dag schedules on, for the checks | Attributes of public classes, not documented. The same in every supported version; from 3.2, timetables imported from `airflow.timetables` hold serialized assets instead of the Dag file's objects |
| Plugin apps are mounted without authentication | The endpoint must accept services that cannot log in | If Airflow ever required login for plugin apps, every delivery would be rejected |

Public interfaces used: `AirflowPlugin` with `fastapi_apps` and the `airflow.plugins` entry point,
cluster policies through the `airflow.policy` entry point with `airflow.policies.hookimpl`,
`airflow.sdk.Connection`, `airflow.sdk.Asset`, which `WebhookTopic` subclasses, `airflow.exceptions`
and, in the examples, `airflow.sdk`.

`airflow.sdk.Connection` reads metadata-database connections only in a server context. The API
server declares itself one from Airflow 3.1.1, which sets the minimum version: on 3.0.x and 3.1.0 the
lookup fails and every delivery gets a `500`.

An alternative to the three internal APIs is calling Airflow's public REST API
(`POST /api/v2/assets/events`) from the plugin with a service account. It would add access control
per asset and audit logging, at the cost of managing a token, an extra HTTP round trip and a lookup
from names to asset IDs. It would not remove the dependency on unauthenticated plugin apps.

## Open questions

1. **Replay protection.** GitHub signs the body but not a timestamp; GitLab sends a bare token. The
   delivery table recognises a retry by the sender's delivery ID and the body's digest, which only
   catches a service's own retries: GitHub's `X-GitHub-Delivery` and GitLab's `Idempotency-Key` are
   headers no signature covers, so a deliberate replay can change them, and the event headers along
   with them. An `hmac_json` connection that reads the delivery ID from the body
   (`delivery_id_field`) has it signed, so a replay of one of its deliveries is answered as a retry
   while its row is kept. Source types whose signature covers a timestamp could reject old
   deliveries. Jira's does, through the `timestamp` in the signed body, but Jira Cloud retries a
   failed delivery for more than an hour, so the window would have to be as long.
2. **Probing webhook names.** `webhook_names` and the connection cache bound what unauthenticated
   requests cost: an unlisted name is refused before any lookup, and each API server worker reads a
   listed webhook's connection at most twice per `connection_cache_ttl`: when its cache entry
   expires, and once more for a delivery that fails verification, so a changed secret applies at
   once. A failed lookup is not retried before its entry expires. With `*`, which is deprecated,
   nothing is cached, because the cache would keep an entry for every name anyone sends. A listed
   name still answers a bad signature with `401` rather than `404`, so which names are served can be
   probed. That is kept because the `404` is the main troubleshooting signal and names such as
   `github` are easy to guess; the secret is what protects a webhook. Should unlisted and unverified
   requests get the same answer?
3. **Plugin import weight.** Every Airflow component that loads plugins, including each task run,
   imports the plugin. The endpoint's Airflow internals are imported on the first delivery, so
   other components only pay for FastAPI and building the app, about 70 ms on Airflow 3.3. Avoiding
   that too would mean giving Airflow a lazily built app rather than the FastAPI app `fastapi_apps`
   is documented to take.
4. **Payload retention.** Each source type stores the body as received, unless every delivery also
   carries a full copy of the object it is about; only Jira's do, so `jira` alone stores a summary by
   default. Every connection can set `store_payload` to `full`, `summary` where the source type has
   one, or `none`. Anyone who can read asset events can read the payloads that are kept. A delivery
   that matches several names copies its payload into each of their events, and every task it
   starts receives the copies, so `max_event_payload_bytes` leaves a payload larger than it out of
   the events, with `payload_truncated` set. The delivery record keeps it until it is deleted after
   `delivery_retention`, and tasks cannot read it there, so a Dag that needs the payload fetches the
   object from the service. The size is that of compact JSON in UTF-8, measured once per delivery
   that records events, and the extra's `path` is shortened as in the record, so the cap bounds the
   extra. The option is off by default, so Dags that read payloads keep them; AIP-125 proposes 65536
   for Airflow, which has no such Dags yet.
5. **Audit.** The REST API writes an audit log entry per user; a delivery has no user, so a
   delivery that records events writes one owned by `webhook:<name>`, with a display name, which
   Airflow's audit log page needs to show the owner. Rejected requests write none: anyone can send
   them, and they are counted in a metric instead.
6. **Name stability.** From 0.1.0, changing a name that an existing delivery produces is a breaking
   change, released in a new minor version below 1.0 (README, "Event names"). Renaming a repository
   or project still changes its names, which the plugin cannot prevent.
7. **Source types from other packages.** `SOURCE_TYPES` is a fixed dictionary; other packages could
   contribute source types through an entry point. That would make `WebhookSource` and `WebhookEvent`
   a public interface; until then the Python modules other than `WebhookTopic` are internal
   (README, "Versioning").
8. **Verification handshakes.** Some services verify a URL when the webhook is registered and expect
   a custom response, which the `WebhookSource` interface cannot produce yet.

## Towards Airflow core

As a core feature, the endpoint would use these internals legitimately and remove the dependency on
unauthenticated plugin apps. A possible split: core provides the endpoint, connection resolution,
the naming rule and a public `WebhookSource` interface, while providers contribute source types,
much as the `common.messaging` provider defines a queue interface that other providers implement.

Proposing that needs a discussion on the Airflow dev list, and probably an Airflow Improvement
Proposal, because it adds a class of routes that skip Airflow authentication.
