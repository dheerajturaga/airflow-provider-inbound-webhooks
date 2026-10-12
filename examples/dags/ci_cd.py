# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Dag author's side: schedule on an event by its name, ``<webhook>/<path>/<event>/<action>/<detail>``.

Not sure of a name? Trigger the event once and copy it from ``candidates`` in the delivery's response.
"""

from __future__ import annotations

from airflow.sdk import Asset, dag, task

web_deploy_preview_label = Asset("github/my-org/web/pull_request/labeled/deploy-preview")
web_main_push = Asset("github/my-org/web/push/main")
any_repo_release = Asset("github/my-org/*/release/published")
api_pipeline_success = Asset("gitlab/platform/api/pipeline/success")


def get_payload(triggering_asset_events, asset):
    # Airflow 3.4 gives each delivery its own run unless the Dag sets batch_asset_events.
    return triggering_asset_events[asset][0].extra["payload"]


@dag(schedule=web_deploy_preview_label)
def web_deploy_preview():
    @task
    def deploy(*, triggering_asset_events=None):
        pr = get_payload(triggering_asset_events, web_deploy_preview_label)
        print(f"Deploying preview of PR #{pr['number']} at {pr['pull_request']['head']['sha']}")

    deploy()


@dag(schedule=web_main_push)
def web_deploy_staging():
    @task
    def deploy(*, triggering_asset_events=None):
        print(f"Deploying {get_payload(triggering_asset_events, web_main_push)['after']} to staging")

    deploy()


@dag(schedule=any_repo_release)
def publish_release_notes():
    @task
    def publish(*, triggering_asset_events=None):
        release = get_payload(triggering_asset_events, any_repo_release)
        print(f"Publishing notes for {release['repository']['full_name']} {release['release']['tag_name']}")

    publish()


@dag(schedule=api_pipeline_success)
def api_deploy():
    @task
    def deploy(*, triggering_asset_events=None):
        pipeline = get_payload(triggering_asset_events, api_pipeline_success)
        print(f"Deploying {pipeline['object_attributes']['sha']}")

    deploy()


web_deploy_preview()
web_deploy_staging()
publish_release_notes()
api_deploy()
