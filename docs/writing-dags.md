# Writing Dags

Schedule on a webhook topic, an event name as a `WebhookTopic`, and read the delivery from
`triggering_asset_events`. Each event's `extra` holds the payload, or for Jira a summary of it,
unless the connection's `store_payload` says otherwise or the payload is larger than
[`max_event_payload_bytes`](reference.md#configuration):

```python
from airflow.sdk import dag, task

from airflow_inbound_webhooks import WebhookTopic

deployed = WebhookTopic(webhook="demo", path="*", event="deployed")


@dag(schedule=deployed)
def on_any_deployment():
    @task
    def handle(*, triggering_asset_events=None):
        for event in triggering_asset_events[deployed]:
            print(event.extra["payload"])

    handle()


on_any_deployment()
```

`WebhookTopic` takes the parts of an [event name](event-names.md) as keywords, `webhook`, `path`,
`event`, `action` and `detail`, and builds the name as the plugin names deliveries: `deployed` above
is `demo/*/deployed`. Only `webhook` and `event` are required. A `path` can hold several segments,
with `*` in place of trailing ones, as in `my-org/*`, but nowhere else: `event`, `action` and
`detail` take no `*`. To match any action or detail, leave it out, as `event="pull_request"` matches
every pull request event. An event without an action, such as GitHub's `push`, takes its detail
right after it: `event="push", detail="main"`. Values are folded to ASCII as delivery names are, so
`detail="🚀 Café"` subscribes to the label `🚀 Café`. `WebhookTopic` also takes the whole name, as a
delivery's page lists the names it offered:
`WebhookTopic("github/my-org/web/pull_request/labeled/deploy-preview")`, which can also spell out a
path that holds a literal `*`. A name that no delivery offers, such as one that is not folded to
ASCII, is refused.

Look a topic's events up with the topic, as in `triggering_asset_events[deployed]` above. A topic
with no events in the run, as in a run started by hand, gives an empty list, so test for events with
`if triggering_asset_events[deployed]:`; `deployed in triggering_asset_events` is true for any
asset, with or without events. The keys you get by iterating over `triggering_asset_events` are
plain assets, which never equal a topic, since their group differs.

A topic is an asset in the group `webhook`. It refuses `extra` and `watchers`, because only
deliveries record its events, and takes `access_control` on Airflow 3.3 and later; assets have none
on 3.1 and 3.2. The plugin and the scheduler match by name, so `Asset("demo/*/deployed")` keeps
working and matches the same deliveries; `WebhookTopic` is the form AIP-125 documents. It costs two
things:

- Dag files that use it import this package, so it must be installed wherever Dag files are parsed:
  on the Dag processor and on the workers, which parse a Dag file to run its tasks, not only on the
  API server.
- The group `webhook` is only a label. Airflow keeps one group per asset name, set by whichever Dag
  file it parsed last, so a name declared as `Asset` in one file and as `WebhookTopic` in another
  switches between `asset` and `webhook`.

Iterate over all events rather than taking the first: a run can carry several deliveries before
Airflow 3.4, and from 3.4 when the Dag batches them; see [When Dags run](#when-dags-run). Events are
not ordered; sort by `event.timestamp` if order matters.

Besides `payload`, `extra` holds:

| Key | Value |
|---|---|
| `from_webhook` | `true` |
| `webhook` | The webhook's name, as in `/webhooks/<name>`; `source` holds it too |
| `source_type` | The connection's source type, such as `github` |
| `path` | Where the event happened, as in its names, up to 1500 characters |
| `delivery_id` | The service's ID for the delivery, where it sends one |
| `inbound_delivery_id` | The ID of the delivery's [record](reference.md#delivery-records), set even when the service sends no delivery ID. While the record is kept (`delivery_retention`), the deliveries page and API find it by this ID, for Airflow users allowed to read the webhook's connection, which a task is not; see [Reading deliveries](reference.md#reading-deliveries) |
| `correlation_key` | `null`: no source type sets one yet |
| `payload_truncated` | `true` if the payload was larger than `max_event_payload_bytes` and left out, so `payload` is `null`; otherwise `false` |

When `payload_truncated` is `true`, fetch what the Dag needs from the service instead, such as the
pull request from GitHub's API. The delivery record keeps the payload only until
`airflow webhooks clean` deletes it after `delivery_retention` days, and shows it only to Airflow
users allowed to read the webhook's connection, which a task is not.

Anyone allowed to create asset events, through the REST API or the UI, can create one with the same
keys, so `from_webhook` says where an event claims to come from, not that a service sent it. Read the
payload defensively. Anyone who can read asset events can read the payload too; see
[Security](security.md).

More examples, for GitHub labels and pushes, organization-wide releases and GitLab pipelines, are in
[`examples/dags/ci_cd.py`](../examples/dags/ci_cd.py), for Jira transitions and new issues in
[`examples/dags/jira_issues.py`](../examples/dags/jira_issues.py), and for both forms of
`WebhookTopic` in [`examples/dags/webhook_topics.py`](../examples/dags/webhook_topics.py).

**Checks:** with [`dag_checks`](reference.md#configuration) set to `error` where Dag files are
parsed, Airflow refuses to load a Dag that uses a `WebhookTopic` in a way no delivery can serve:

- a topic in a task's `outlets` or `inlets`: only deliveries record a topic's events, and a topic
  is not data a task reads;
- a `PartitionedAssetTimetable` over a topic, on Airflow 3.2 and later: topic events carry no
  partition key, so the Dag would never run;
- two topics that one delivery can both match, such as a name and its prefix (`.../pull_request`
  and `.../pull_request/labeled`) or a name and its wildcard form (`github/my-org/web/push` and
  `github/*/push`), in a condition joined by `|` alone. Each matched name records its own event,
  and from Airflow 3.4 each event starts its own run, so the Dag runs twice for that delivery. A
  condition with an `&` of several assets, a list included, batches the events into one run, as
  does a timetable with `batch_asset_events`, so neither is checked; before 3.4 every run batches,
  but such a Dag is refused there too, ahead of the upgrade. Schedule one topic per Dag.

A refused Dag makes its file an import error, which says why, and that stops every Dag in the file,
not only the refused one: Airflow marks all of them as stale, so they no longer run and deliveries
no longer reach them. Keep Dags that schedule on topics in files of their own before you turn the
checks on. When several Dags of one file are refused, the import error gives one of their reasons.

The checks have limits:

- They look only at `WebhookTopic`s: a Dag that schedules on a plain `Asset` is never refused, and
  a plain `Asset` on a topic's name can still be an outlet. They guard against mistakes, not misuse.
- They see one Dag at a time, so they cannot see the same name declared as an `Asset` elsewhere.
- From Airflow 3.2 they do not see topics in a timetable imported from `airflow.timetables` rather
  than `airflow.sdk`, which turns them into plain assets.
- They compare names, not deliveries, and do not know the webhook's source type. Two names that one
  delivery offers through two of its kinds, such as a Jira update that changes the status and adds
  a label, are not caught. And two names can look related that a GitHub delivery never offers
  together: the labels `kind` and `kind/bug`, whose `/` reads as a segment boundary, or
  `github/my-org/*/release` and `github/my-org/web/push/release`, which a path of three segments,
  such as a GitLab project's, offers together, but a GitHub path never has three. For such a pair,
  declare one side as a plain `Asset` of the same name, which the checks ignore, or schedule the two
  in two Dags.

Installing the package registers the checks as an Airflow cluster policy in every Airflow process;
while `dag_checks` is `off`, the default, they return at once. Any value but `off` and `error`
checks nothing either, and is logged as a warning once per process. There is no warning mode,
because a warning that a cluster policy raises reaches only the Dag processor's log, never the UI.

## When Dags run

Airflow's scheduler creates the runs from the asset events a delivery records, one on each name a
Dag schedules on. Which deliveries a run carries depends on the Airflow version.

**Airflow 3.4 and later:** on a schedule of one name, or of names joined with `|`, each event starts
its own run, also when several arrive before the scheduler's next pass, and the runs are created in
the order their deliveries were recorded. To have the deliveries waiting for a Dag share its next
run instead, set `batch_asset_events=True` on its timetable:
`schedule=AssetTriggeredTimetable(assets=..., batch_asset_events=True)`, with
`AssetTriggeredTimetable` from `airflow.sdk` and, as `assets`, one name or names joined with `|`,
not a list. Or set `[scheduler] batch_asset_events = True` in the Dag processor's configuration: it
reads the option when it parses a Dag whose timetable does not set `batch_asset_events`, so a change
applies to a Dag once it is parsed again. `batch_asset_events=False` on a timetable keeps one run
per event whatever the option says.

**Before Airflow 3.4:** a Dag is queued at most once per name, and its run carries the events
recorded on its names since its previous run, up to the last delivery that queued it. A delivery on
a name the Dag is already queued for does not queue it again, so only a run that a later delivery
queues carries it: of two deliveries on one name that arrive before the scheduler's next pass, or
while the Dag is at `max_active_runs`, the first starts a run, and the second waits for the run a
third delivery starts, however much later that comes.

**Several names:** `schedule=[a, b]` means `a & b`: the Dag runs once each name has an event since
its last run, and the run carries all of them. From 3.4 such a schedule always batches, and
`batch_asset_events=False` on it fails when the Dag is parsed. `a | b` runs for an event on either
name. One delivery offers several names, its shorter names and their `*` forms among them (see
[Event names](event-names.md)), and records an event on each that a Dag schedules on. So `a | b`
over two names of one delivery, such as a name and its prefix or its `*` form, gives that delivery
two runs from 3.4, and one run with both events before. Schedule a Dag on one name instead, the
narrowest that covers every delivery it handles, and skip the ones it does not want in the task.

**Missed deliveries and `catchup`:** a delivery that only paused Dags schedule on records nothing
(`outcome` `dag_paused`), so no run carries it unless the sender delivers it again. From 3.4, a
draining Dag counts as paused, here and below. When a Dag that is not paused also schedules on the
name, the event is recorded. Whether a Dag that was paused at the time, or subscribed only later,
runs for it depends on the version and on the Dag's `catchup`, which is off unless
`[scheduler] catchup_by_default` turns it on:

- From 3.4, a Dag with `catchup=False` runs only for events recorded while it was subscribed and
  not paused. With `catchup=True`, the next delivery that queues it also starts runs for every
  earlier event on its names that no run of the Dag carried, including those from before it
  subscribed or while it was paused.
- Before 3.4, a run carries the events recorded since the Dag's previous run whatever `catchup`
  says, those recorded while it was paused included. A Dag's first run carries every earlier event
  on its names, except from 3.3.1 with `catchup=False`, where it leaves out those from before the
  Dag subscribed.

`catchup=True` needs a `start_date`, but does not stop at it: events from before the `start_date`
are caught up too.

**Order:** a Dag whose queued and running runs reach `max_active_runs` gets no new run until one
finishes. From 3.4, a Dag that does not batch keeps its deliveries waiting in order, so
`max_active_runs=1` handles one delivery at a time, in the order they were recorded. A run of a Dag
that batches, as every `[a, b]` or `a & b` schedule does, or of any Dag before 3.4, can still carry
several deliveries. With `max_active_runs` above 1, runs overlap and can finish in any order.

**Manual runs:** a run started by hand, such as with `airflow dags trigger`, carries no events:
`triggering_asset_events[asset]` is an empty list there, so a task that needs an event checks that
the list is not empty, as `examples/dags/ci_cd.py` does; `asset in triggering_asset_events` is true
even then. Runs that deliveries start have an empty `dag_run.conf`; the delivery is only in the
events.
