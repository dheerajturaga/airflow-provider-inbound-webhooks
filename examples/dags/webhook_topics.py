# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Schedule on webhook topics: event names as assets in the group ``webhook``.

``WebhookTopic`` builds a name from its segments as the plugin names deliveries, or takes the whole
name as a delivery's ``candidates`` list it. Dag files that use it need the package installed
wherever they are parsed: on the Dag processor and the workers, not only on the API server.
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


@dag(schedule=preview_requested)
def deploy_preview():
    @task
    def deploy(*, triggering_asset_events=None):
        # One run can carry several deliveries (always possible before Airflow 3.4).
        for event in triggering_asset_events[preview_requested]:
            pr = event.extra["payload"]["pull_request"]
            print(f"Deploying a preview of PR #{pr['number']} at {pr['head']['sha']}")

    deploy()


@dag(schedule=any_repo_main_push)
def audit_main_pushes():
    @task
    def audit(*, triggering_asset_events=None):
        for event in triggering_asset_events[any_repo_main_push]:
            push = event.extra["payload"]
            print(f"Auditing {push['repository']['full_name']} at {push['after']}")

    audit()


deploy_preview()
audit_main_pushes()
