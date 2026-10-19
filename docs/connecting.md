# Connecting services

Step-by-step guides for GitLab, Jira and any other service. GitHub's is in the README, under
[Connect GitHub](../README.md#connect-github).

Before you start:

- The plugin is installed on the API server: [Quick start](quick-start.md), step 1.
- The service can reach the API server. A cloud service, such as GitHub.com, GitLab.com or Jira
  Cloud, needs a public HTTPS address: put a reverse proxy in front of the API server that forwards
  only the paths of your webhooks, such as `/webhooks/github` (see [Security](security.md)).
  `<your-airflow>` below stands for that address.

## Connect GitLab

**In Airflow**

1. Add `gitlab` to [`webhook_names`](reference.md#configuration), the comma-separated list in
   `airflow.cfg`, and restart the API server.

2. Create the connection `webhook_gitlab` with a new secret, and copy the secret it prints:

   ```bash
   SECRET="$(openssl rand -hex 32)" && echo "$SECRET"
   airflow connections add webhook_gitlab --conn-type inbound_webhook --conn-password "$SECRET"
   ```

**In GitLab**

3. In the project, open **Settings → Webhooks → Add new webhook**. To cover every project of a group,
   use the group's **Settings → Webhooks** instead, as a group owner (GitLab Premium).

4. Fill in the form, then select **Add webhook**:

   | Field | Value |
   |---|---|
   | URL | `https://<your-airflow>/webhooks/gitlab` |
   | Secret token | The secret from step 2. GitLab also offers a *Signing token*, which this plugin does not check yet; you can set both. |
   | Trigger | The events your Dags need, such as *Push events*, *Merge request events* or *Pipeline events* |
   | Enable SSL verification | Checked |

5. Check it. In the list of webhooks, select **Test → Push events**. GitLab should report `HTTP 202`;
   **Edit → Recent events → View details** shows the response.

**In Airflow**

6. Add a Dag that schedules on an event, such as `Asset("gitlab/platform/api/pipeline/success")`, and
   unpause it. To find a name, trigger the event once and copy one of the names it offered from its
   delivery under **Browse → Webhook deliveries**, which you can search by the `delivery_id` of its
   response. That needs permission to read the connection `webhook_gitlab`; anyone can build a name
   as [Event names](event-names.md) describes.

## Connect Jira

**In Airflow**

1. Add `jira` to [`webhook_names`](reference.md#configuration), the comma-separated list in
   `airflow.cfg`, and restart the API server.

2. Create the connection `webhook_jira` with a new secret, and copy the secret it prints:

   ```bash
   SECRET="$(openssl rand -hex 32)" && echo "$SECRET"
   airflow connections add webhook_jira --conn-type inbound_webhook --conn-password "$SECRET"
   ```

**In Jira**, as a Jira administrator

3. Open **Settings (⚙) → System → WebHooks → Create a WebHook**. On Jira Data Center, the menu is
   **Administration (⚙) → System → WebHooks**.

4. Fill in the form, then create the webhook:

   | Field | Value |
   |---|---|
   | Name | Any, such as `Airflow` |
   | Status | Enabled |
   | URL | `https://<your-airflow>/webhooks/jira` |
   | Secret | The secret from step 2. Jira cannot show it again once saved. |
   | Issue related events | A JQL filter such as `project = OPS`, and the events your Dags need, such as issue *created* and *updated* |
   | Exclude body | Unchecked |

**In Airflow**

5. Add a Dag that schedules on an event, such as
   `Asset("jira/OPS/issue_updated/status/Ready for Deploy")`, and unpause it. See
   [`jira`](event-names.md#jira) for the names Jira events get.

6. Check it by making that change in Jira, here moving an `OPS` issue to *Ready for Deploy*: the Dag
   starts a run. Jira does not show webhook responses, so if nothing happens, look for the delivery
   under **Browse → Webhook deliveries**. If it is there, its outcome and the names it offered say
   why no Dag ran; see [Responses and troubleshooting](reference.md#responses-and-troubleshooting).
   If it is not, Airflow refused it or Jira did not send it: the API server log reports a delivery
   whose secret does not match, or a misconfigured connection. Otherwise check that `jira` is in
   `webhook_names`, that *Exclude body* is unchecked, and that the webhook's JQL filter and events
   cover the change.

## Connect any other service

For a service without a built-in source type, such as a deploy tool named `deploybot` here, you
describe its deliveries in the connection.

**In the service's webhook documentation**, find:

| What | For example | Connection option |
|---|---|---|
| How it authenticates deliveries | An HMAC-SHA256 signature of the body, or the secret itself, in a header | `source_type`: `hmac_json` or `token_header` |
| The header, and any text before the signature or secret | `X-Signature: sha256=<hex>` | `signature_header` and `signature_prefix`, or `token_header` and `token_prefix` |
| Where the event is named | `{"type": "deployed"}` in the body, or a header | `event_field` or `event_header` |
| Optionally, where a project or service is named | `{"service": "billing"}` | `path_field` |
| Optionally, the ID it gives each delivery and repeats when it retries one | `{"id": "evt_123"}`, or a header | `delivery_id_field` or `delivery_id_header` |

**In Airflow**

1. Add `deploybot` to [`webhook_names`](reference.md#configuration), the comma-separated list in
   `airflow.cfg`, and restart the API server.

2. Create the connection `webhook_deploybot` with a new secret and those options, and copy the secret
   it prints:

   ```bash
   SECRET="$(openssl rand -hex 32)" && echo "$SECRET"
   airflow connections add webhook_deploybot --conn-type inbound_webhook --conn-password "$SECRET" \
     --conn-extra '{"source_type": "hmac_json", "signature_header": "X-Signature",
                    "signature_prefix": "sha256=", "event_field": "type", "path_field": "service"}'
   ```

   [`hmac_json`](reference.md#hmac_json-services-that-sign-the-body) and
   [`token_header`](reference.md#token_header-services-that-send-a-secret-header) list every option,
   and the **Inbound Webhook** connection form in the Airflow UI has a field for each.

**In the service**

3. Add a webhook with the URL `https://<your-airflow>/webhooks/deploybot` and the secret from step 2,
   sending JSON.

**In Airflow**

4. Add a Dag that schedules on an event, such as `Asset("deploybot/billing/deployed")`, and unpause
   it. To find a name, send the event once and copy one of the names it offered from its delivery
   under **Browse → Webhook deliveries**. That needs permission to read the connection
   `webhook_deploybot`; anyone can build a name as [Event names](event-names.md) describes.

A service whose signature covers more than the body, such as a timestamp, or that needs a handshake
when the webhook is registered, needs a built-in source type: see [Other
services](reference.md#other-services).
