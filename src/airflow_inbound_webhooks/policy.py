# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Parse-time checks of Dags that use webhook topics, as an Airflow cluster policy.

The ``airflow.policy`` entry point has Airflow import this module in every process while it
initializes, and an error raised importing it would stop every process from starting, checks on or
off. So importing it only defines the policy, with Airflow's hookimpl marker; what the checks need is
imported when a Dag is checked, and ``dag_policy`` returns at once unless
``[inbound_webhooks] dag_checks`` is ``error``.
"""

from __future__ import annotations

import functools
import itertools
import logging
import sys
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

try:
    from airflow.policies import hookimpl
except ImportError:  # Should Airflow move its marker, pluggy's of the same name is the same marker.
    from pluggy import HookimplMarker

    hookimpl = HookimplMarker("airflow.policy")

if TYPE_CHECKING:
    from airflow.sdk import DAG

log = logging.getLogger(__name__)

TOPICS_MODULE = "airflow_inbound_webhooks.topics"


@hookimpl
def dag_policy(dag: DAG) -> None:
    """Refuse a Dag that uses webhook topics in a way no delivery can serve, if checks are on."""
    # A Dag can only hold a topic once a Dag file has imported its class, so while none has, as in
    # every process that parses no such file, a Dag costs one lookup.
    if TOPICS_MODULE not in sys.modules or not are_checks_on():
        return
    if problems := find_problems(dag):
        from airflow.exceptions import AirflowClusterPolicyViolation

        from airflow_inbound_webhooks import configuration

        raise AirflowClusterPolicyViolation(
            f"Dag {dag.dag_id!r} uses webhook topics in ways no delivery can serve, which "
            f"[{configuration.SECTION}] {configuration.DAG_CHECKS} = {configuration.DAG_CHECKS_ERROR} "
            "refuses:\n- " + "\n- ".join(problems)
        )


def are_checks_on() -> bool:
    from airflow.configuration import conf

    from airflow_inbound_webhooks import configuration

    value = conf.get(configuration.SECTION, configuration.DAG_CHECKS, fallback=configuration.DAG_CHECKS_OFF)
    if (value := value.strip().lower()) not in (configuration.DAG_CHECKS_OFF, configuration.DAG_CHECKS_ERROR):
        warn_unknown_dag_checks(value)
    return value == configuration.DAG_CHECKS_ERROR


@functools.cache
def warn_unknown_dag_checks(value: str) -> None:
    from airflow_inbound_webhooks import configuration

    log.warning(
        "[%s] %s is %r rather than %s or %s, so Dags are not checked",
        configuration.SECTION,
        configuration.DAG_CHECKS,
        value,
        configuration.DAG_CHECKS_OFF,
        configuration.DAG_CHECKS_ERROR,
    )


def find_problems(dag: DAG) -> list[str]:
    """Describe each way the Dag uses a topic that no delivery can serve."""
    from airflow_inbound_webhooks import naming
    from airflow_inbound_webhooks.topics import WebhookTopic

    problems = []
    for task in dag.tasks:
        for topic in task.outlets:
            if isinstance(topic, WebhookTopic):
                problems.append(
                    f"task {task.task_id!r} has the webhook topic {topic.name!r} in its outlets, but only "
                    "deliveries may record a topic's events"
                )
        for topic in task.inlets:
            if isinstance(topic, WebhookTopic):
                problems.append(
                    f"task {task.task_id!r} has the webhook topic {topic.name!r} in its inlets, but a "
                    "topic is not data to read: the delivery is in triggering_asset_events"
                )
    if (condition := getattr(dag.timetable, "asset_condition", None)) is None:
        return problems
    names = dict.fromkeys(obj.name for obj in iter_assets(condition) if isinstance(obj, WebhookTopic))
    if isinstance(dag.timetable, get_partitioned_timetables()):
        for name in names:
            problems.append(
                f"it schedules on the webhook topic {name!r} with a PartitionedAssetTimetable, which "
                "never runs for it: topic events have no partition key"
            )
    # A timetable that batches consumes a delivery's events in one run, whichever names it matched.
    if not batches_events(dag.timetable, condition):
        for name, other in itertools.combinations(names, 2):
            if naming.can_be_offered_together(name, other):
                problems.append(
                    f"it schedules on {name!r} | {other!r}, which one delivery can both match, and from "
                    "Airflow 3.4 each of its events starts a run: schedule each topic in a Dag of its own"
                )
    return problems


@functools.cache
def get_partitioned_timetables() -> tuple[type, ...]:
    try:
        from airflow.sdk import PartitionedAssetTimetable
    except ImportError:  # Airflow 3.1 has no partitions.
        return ()
    return (PartitionedAssetTimetable,)


def batches_events(timetable: Any, condition: Any) -> bool:
    """
    Return whether the timetable consumes the events of one delivery in one run.

    From Airflow 3.4 each event starts its own run, unless the timetable's ``batch_asset_events``
    says otherwise, which it does for a condition with an ``&`` of several assets, a list included.
    Before 3.4 every run batches, so the condition alone decides, as it will once Airflow is upgraded.
    """
    from airflow.sdk import AssetAll

    if (batch := getattr(timetable, "batch_asset_events", None)) is not None:
        return bool(batch)
    return any(isinstance(node, AssetAll) and len(node.objects) > 1 for node in iter_nodes(condition))


def get_operands(condition: Any) -> list[Any] | None:
    """Return the operands of an ``&`` or ``|`` condition, or None for an asset, reference or alias."""
    from airflow.sdk import AssetAll, AssetAny

    return list(condition.objects) if isinstance(condition, (AssetAll, AssetAny)) else None


def iter_nodes(condition: Any) -> Iterator[Any]:
    """Yield an asset condition and every condition and asset in it."""
    yield condition
    for operand in get_operands(condition) or []:
        yield from iter_nodes(operand)


def iter_assets(condition: Any) -> Iterator[Any]:
    """Yield the assets, references and aliases anywhere in an asset condition."""
    return (node for node in iter_nodes(condition) if get_operands(node) is None)
