# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Schedule on webhook topics: event names as assets in the group ``webhook``.

``WebhookTopic`` builds a name from its segments as the plugin names deliveries, or takes the whole
name, as a delivery's page under Browse → Webhook deliveries lists the names it offered. Dag files
that use it need the package installed wherever they are parsed: on the Dag processor and the
workers, not only on the API server.
"""

from __future__ import annotations

from airflow.sdk import dag, task

from airflow_inbound_webhooks import WebhookTopic

# github/my-org/web/pull_request/labeled/deploy-preview
preview_requested = WebhookTopic(
    webhook="github", path="my-org/web", event="pull_request", action="labeled", detail="deploy-preview"
)
# A push to main in any repository of my-org; push has no action, so the branch follows the event.
any_repo_main_push = WebhookTopic("github/my-org/*/push/main")


def get_payloads(triggering_asset_events, topic) -> list[dict]:
    # Look events up with the topic itself. One run can carry several deliveries (always possible
    # before Airflow 3.4), which are not ordered, so sort them oldest first.
    events = sorted(triggering_asset_events[topic], key=lambda event: event.timestamp)
    # A payload over [inbound_webhooks] max_event_payload_bytes is left out of its event, which says so
    # in payload_truncated. These Dags skip it; one that needs it fetches the object from GitHub.
    return [event.extra["payload"] for event in events if not event.extra.get("payload_truncated")]


@dag(schedule=preview_requested)
def deploy_preview():
    @task
    def deploy(*, triggering_asset_events=None):
        for payload in get_payloads(triggering_asset_events, preview_requested):
            pr = payload["pull_request"]
            print(f"Deploying a preview of PR #{pr['number']} at {pr['head']['sha']}")

    deploy()


@dag(schedule=any_repo_main_push)
def audit_main_pushes():
    @task
    def audit(*, triggering_asset_events=None):
        for push in get_payloads(triggering_asset_events, any_repo_main_push):
            print(f"Auditing {push['repository']['full_name']} at {push['after']}")

    audit()


deploy_preview()
audit_main_pushes()
