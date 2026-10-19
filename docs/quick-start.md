# Quick start

Try it with the bundled test sender, so no third-party account is needed. Run these commands on the
machine running the Airflow API server (Airflow 3.1.1 or later).

1. **Install the plugin** and create its table in Airflow's metadata database:

   ```bash
   pip install airflow-provider-inbound-webhooks "apache-airflow==$(airflow version)"
   airflow db migrate
   ```

   Pinning `apache-airflow` to the version you run keeps pip from changing it. On Airflow 3.1,
   `airflow db migrate` only creates the table once `external_db_managers` names the plugin; see
   [Compatibility](compatibility.md).

2. **Enable the webhook name** `demo`, then restart the API server. The plugin serves only the names
   listed in [`webhook_names`](reference.md#configuration), so add this to `airflow.cfg` in your
   Airflow home:

   ```ini
   [inbound_webhooks]
   webhook_names = demo
   ```

   Or set `AIRFLOW__INBOUND_WEBHOOKS__WEBHOOK_NAMES=demo` in the API server's environment. After the
   restart, `airflow plugins` lists `webhooks`.

3. **Create the webhook.** It is a connection, named `webhook_demo` for the URL `/webhooks/demo`:

   ```bash
   airflow connections add webhook_demo --conn-type inbound_webhook --conn-password 'change-me' \
     --conn-extra '{"source_type": "hmac_json", "signature_header": "X-Signature",
                    "event_field": "event", "path_field": "service"}'
   ```

   <details>
   <summary>Or in the Airflow UI</summary>

   Under Admin → Connections, add a connection of type **Inbound Webhook**:

   | Field | Value |
   |---|---|
   | Connection Id | `webhook_demo` |
   | Shared secret | `change-me` |
   | Source type | `hmac_json` |
   | Signature header | `X-Signature` |
   | Event field | `event` |
   | Path field | `service` |

   </details>

4. **Add a Dag** that runs on the event, and unpause it, since new Dags start paused:

   ```bash
   dags_folder="$(airflow config get-value core dags_folder)"
   mkdir -p "$dags_folder"
   cat > "$dags_folder/quickstart.py" <<'EOF'
   """Quick start Dag: runs when the ``demo`` webhook receives a ``deployed`` event for the ``billing`` service."""

   from __future__ import annotations

   from airflow.sdk import Asset, dag, task

   billing_deployed = Asset("demo/billing/deployed")


   @dag(schedule=billing_deployed)
   def smoke_test_billing():
       @task
       def smoke_test(*, triggering_asset_events=None):
           # One run can carry several deliveries (always possible before Airflow 3.4).
           for event in triggering_asset_events[billing_deployed]:
               print(f"Smoke-testing billing after deployment: {event.extra['payload']}")

       smoke_test()


   smoke_test_billing()
   EOF
   airflow dags reserialize  # parse it now rather than at the next scan of the Dags folder
   airflow dags unpause smoke_test_billing
   ```

5. **Send a signed test delivery** with the test sender the package includes:

   ```bash
   python -m airflow_inbound_webhooks.send_test_event \
     --url http://localhost:8080/webhooks/demo --secret 'change-me' \
     --data '{"event": "deployed", "service": "billing"}'
   ```

   It prints the response, and `smoke_test_billing` starts a run:

   ```text
   HTTP 202
   {"delivery_id":null,"outcome":"accepted"}
   ```

   The delivery's page under **Browse → Webhook deliveries** shows the names it offered and the
   asset event it recorded. Reading it needs permission to read the connection `webhook_demo`, which
   the Admin and Op roles of Airflow's simple auth manager have. Anything else? See
   [Responses and troubleshooting](reference.md#responses-and-troubleshooting).

A webhook's connection is read again within a minute of a change, so changing its secret or options
needs no restart. Adding a webhook does, because its name must be added to `webhook_names`.
