# Event names

Every delivery is named `<webhook name>/<path>/<event>/<action>/<detail>`, and also offers that name
without its detail, and without its action and detail. Each of those names is also offered with `*`
in place of each trailing part of the path, so `*` stands for anything below that point. The event,
the action and the detail stay one part each, even when they contain `/`: a push to the branch
`release/2.0` offers `push/release/2.0`, and no name ends in `push/release`. For the quick start
delivery the names are:

```text
demo/billing/deployed     billing deployments
demo/*/deployed           deployments of any service
```

When a label is added to a pull request, the built-in GitHub source offers nine names, from broad to
specific:

```text
github/my-org/web/pull_request                           any pull request activity in my-org/web
github/my-org/*/pull_request                             ... in any repository of my-org
github/*/pull_request                                    ... in any repository
github/my-org/web/pull_request/labeled                   any label added
github/my-org/*/pull_request/labeled
github/*/pull_request/labeled
github/my-org/web/pull_request/labeled/deploy-preview    the deploy-preview label added
github/my-org/*/pull_request/labeled/deploy-preview
github/*/pull_request/labeled/deploy-preview
```

Deeper paths work the same way. For a GitLab project `platform/backend/api`,
`gitlab/platform/backend/*/...` covers the `backend` subgroup, `gitlab/platform/*/...` the whole
`platform` group, subgroups included, and `gitlab/*/...` every project.

**Finding a name:** send the event once, then copy one of the names it offered from its delivery
under **Browse → Webhook deliveries**. A webhook's page there finds the delivery by the
`delivery_id` of its response, the ID the sender gives each delivery, such as the one GitHub shows
under **Recent Deliveries**; a delivery without one is found by when it arrived.
`airflow webhooks deliveries <webhook> --delivery-id <ID> --output json` and
`GET /webhooks/_deliveries/<webhook>?delivery_id=<ID>` find it too, with the names as `candidates`;
see [Reading deliveries](reference.md#reading-deliveries).

Reading deliveries needs permission to read the webhook's connection, which Airflow's simple auth
manager gives the Op and Admin roles but not Viewer or User. Anyone can also build a name by the
rule above, which needs no permission. The response to the sender does not list the names, because
services show responses to people who need not be Airflow users. A delivery that matches no
subscribed name is answered with `202` and `outcome` `no_subscribers`, and records no asset event;
its [delivery record](reference.md#delivery-records) keeps its names, without the payload.

Non-ASCII characters are folded (`🚀 deploy` → `deploy`) on every database, because MySQL stores
asset names as latin1 and fails any lookup that includes a non-ASCII name. Parts that end up empty
are dropped, so no name contains `//`: a path `ops/🚀/api` becomes `ops/api`, and a label `🚀` adds
no detail.

**Stability:** event names are a public interface (see [Versioning](compatibility.md#versioning)),
and the one most easily broken, because a Dag whose name stops matching does not fail; it just stops
running. Changing a name that an existing delivery produces, for example how it is normalized or
what its detail is, is a breaking change, and the [changelog](../CHANGELOG.md) lists the old and new
names under "Changed event names". Adding names, such as a detail for an event that had none, is
not, because the shorter names are still offered.

## Names by source type

### `github`

GitHub.com and GitHub Enterprise Server. An organization webhook covers every repository in the
organization. A second GitHub instance is a second webhook, such as `ghes`: add it to
`webhook_names` and create its connection `webhook_ghes` with extra `{"source_type": "github"}`.

Names use GitHub's own event and action names. They start with the webhook's name and a path:
`github/<path>/` for the webhook `github`, `ghes/<path>/` for `ghes`. The path is the repository,
such as `my-org/web`, also offered as `my-org/*` and `*`. A delivery about an organization rather
than one of its repositories has the path `my-org/*`, also offered as `*`; one about neither has no
path, so its names start with `github/<event>`. After the path, one delivery offers:

| Event | Names after `github/<path>/` | Detail |
|---|---|---|
| `pull_request` `labeled` | `pull_request`, `pull_request/labeled`, `pull_request/labeled/deploy-preview` | The label, for `labeled` and `unlabeled` on any event, such as `issues` |
| `push` to a branch | `push`, `push/main` | The branch, as one part: `release/2.0` offers `push/release/2.0`, not `push/release` |
| `push` of a tag | `push` | None |
| `workflow_run` `completed` | `workflow_run`, `workflow_run/completed`, `workflow_run/completed/CI` | The workflow's name |
| Any other event, such as `release` `published` | `release`, `release/published` | None |
| An event without an action, such as `ping` | `ping` | None |
| An event about an organization, such as `organization` `member_added` | `organization`, `organization/member_added` | None |

Every detail is one part, whatever `/` it contains: the label `area/api` offers
`pull_request/labeled/area/api` but no `pull_request/labeled/area`, and a workflow named
`deploy/prod` offers `workflow_run/completed/deploy/prod` but no `workflow_run/completed/deploy`.
An organization's `member_added` delivery offers exactly four names: `github/my-org/*/organization`,
`github/*/organization`, `github/my-org/*/organization/member_added` and
`github/*/organization/member_added`.

### `gitlab`

GitLab.com and self-managed GitLab. Group webhooks cover every project in a group (a GitLab Premium
feature); otherwise add the webhook per project. To subscribe to every project in a group,
subgroups included, use `gitlab/<group>/*/...`. GitLab sends the secret token itself rather than
signing the body.

Names use the payload's `object_kind`, then `object_attributes.action` (or its `status`, for
pipelines). The detail is the branch for `push` and the tag for `tag_push`. GitLab has no label
events, so a label added to a merge request or issue is reported as `<object_kind>/labeled/<label>`.

### `jira`

Jira Cloud and Jira Data Center. Events are named from the body, so the webhook must not exclude
it. Webhooks that Connect or OAuth 2.0 apps register are authenticated differently and not
supported.

Names use the payload's `webhookEvent` without its `jira:` prefix, under the issue's project key.
The detail is the issue type for `issue_created`. `issue_updated` offers a name for each field in
the changelog, with the new status for status changes and, as for GitLab,
`issue_updated/labeled/<label>` for each label added. Moving an issue of project `OPS` to *Ready for
Deploy* and adding the label `hotfix` offers:

```text
jira/OPS/issue_updated                            any change to an OPS issue
jira/*/issue_updated                              ... in any project
jira/OPS/issue_updated/status                     any transition
jira/*/issue_updated/status
jira/OPS/issue_updated/status/Ready for Deploy    moved to Ready for Deploy
jira/*/issue_updated/status/Ready for Deploy
jira/OPS/issue_updated/labels                     any change to the labels
jira/*/issue_updated/labels
jira/OPS/issue_updated/labeled                    any label added
jira/*/issue_updated/labeled
jira/OPS/issue_updated/labeled/hotfix             the hotfix label added
jira/*/issue_updated/labeled/hotfix
```

Field names are the changelog's, such as `Story Points` for a custom field. Events about neither an
issue nor a project, such as `sprint_started`, have no path: `jira/sprint_started`.

Jira sends the whole issue, every custom field included, with each delivery. So each asset event
stores a summary: the payload with the issue's fields reduced to `project`, `issuetype`, `status`,
`summary`, `labels`, `priority`, `assignee`, `resolution`, `fixVersions` and `parent`. The
changelog and the rest of the payload are kept. A Dag that needs other fields can fetch the issue
from Jira, which also gives its current state, or the connection can store the whole payload:

| Extra field | Required | Meaning |
|---|---|---|
| `store_payload` | no | `summary` (default), `full` or `none` |

Jira sends `issue_updated` for every edit, so select only the events your Dags need and narrow the
webhook with a JQL filter such as `project = OPS`. Jira Cloud retries a delivery that fails or times
out. Its `delivery_id`, Jira Cloud's `X-Atlassian-Webhook-Identifier`, stays the same across
retries, so a retry is recognised and records nothing.

On a Jira without webhook secrets, an Automation for Jira rule with a "Send web request" action can
send the secret in a header for `token_header` instead.
