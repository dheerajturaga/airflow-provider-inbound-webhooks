# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Jira: schedule on a project's status transitions and on new issues of a type, in any project.

The ``jira`` source stores a summary of the issue, with its key and main fields; fetch the issue from
Jira in the task if a Dag needs other fields.
"""

from __future__ import annotations

from airflow.sdk import Asset, dag, task

ops_ready_for_deploy = Asset("jira/OPS/issue_updated/status/Ready for Deploy")
any_project_new_bug = Asset("jira/*/issue_created/Bug")


def get_issues(triggering_asset_events, asset) -> list[dict]:
    # Before Airflow 3.4, or with batch_asset_events, one run can carry several deliveries; events
    # are not ordered, so sort them oldest first.
    events = sorted(triggering_asset_events[asset], key=lambda event: event.timestamp)
    # A summary over [inbound_webhooks] max_event_payload_bytes is left out of its event, which says so
    # in payload_truncated. These Dags skip it; fetch such an issue from Jira to handle it.
    return [event.extra["payload"]["issue"] for event in events if not event.extra.get("payload_truncated")]


@dag(schedule=ops_ready_for_deploy)
def ops_deploy_ready_issues():
    @task
    def deploy(*, triggering_asset_events=None):
        for issue in get_issues(triggering_asset_events, ops_ready_for_deploy):
            print(f"Deploying the fix for {issue['key']}: {issue['fields']['summary']}")

    deploy()


@dag(schedule=any_project_new_bug)
def triage_new_bugs():
    @task
    def triage(*, triggering_asset_events=None):
        for issue in get_issues(triggering_asset_events, any_project_new_bug):
            priority = (issue["fields"].get("priority") or {}).get("name", "none")
            print(f"Triaging {issue['key']} in {issue['fields']['project']['key']}, priority {priority}")

    triage()


ops_deploy_ready_issues()
triage_new_bugs()
