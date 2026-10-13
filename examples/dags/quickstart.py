# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
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
