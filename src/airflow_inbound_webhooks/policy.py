# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
Parse-time checks of Dags that use webhook topics, as an Airflow cluster policy.

The ``airflow.policy`` entry point has Airflow import this module in every process while it
initializes, so it imports only Airflow modules loaded by then and none of its own that need the Task
SDK, and ``dag_policy`` returns at once unless ``[inbound_webhooks] dag_checks`` is ``error``.
"""

from __future__ import annotations

import functools
import itertools
import logging
import sys
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowClusterPolicyViolation
from airflow.policies import hookimpl

from airflow_inbound_webhooks import configuration, naming

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
        raise AirflowClusterPolicyViolation(
            f"Dag {dag.dag_id!r} uses webhook topics in ways no delivery can serve, which "
            f"[{configuration.SECTION}] {configuration.DAG_CHECKS} = {configuration.DAG_CHECKS_ERROR} "
            "refuses:\n- " + "\n- ".join(problems)
        )


def are_checks_on() -> bool:
    value = conf.get(configuration.SECTION, configuration.DAG_CHECKS, fallback=configuration.DAG_CHECKS_OFF)
    if (value := value.strip().lower()) not in (configuration.DAG_CHECKS_OFF, configuration.DAG_CHECKS_ERROR):
        warn_unknown_dag_checks(value)
    return value == configuration.DAG_CHECKS_ERROR


@functools.cache
def warn_unknown_dag_checks(value: str) -> None:
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
    if isinstance(dag.timetable, get_partitioned_timetables()):
        topics = (obj for obj in iter_assets(condition) if isinstance(obj, WebhookTopic))
        for name in dict.fromkeys(topic.name for topic in topics):
            problems.append(
                f"it schedules on the webhook topic {name!r} with a PartitionedAssetTimetable, which "
                "never runs for it: topic events have no partition key"
            )
    for group in iter_or_groups(condition):
        names = dict.fromkeys(obj.name for obj in group if isinstance(obj, WebhookTopic))
        for name, other in itertools.combinations(names, 2):
            if naming.can_be_offered_together(name, other):
                problems.append(
                    f"it schedules on {name!r} | {other!r}, which one delivery can both match, so the "
                    "Dag would handle that delivery twice: schedule each topic in a Dag of its own"
                )
    return problems


@functools.cache
def get_partitioned_timetables() -> tuple[type, ...]:
    try:
        from airflow.sdk import PartitionedAssetTimetable
    except ImportError:  # Airflow 3.1 has no partitions.
        return ()
    return (PartitionedAssetTimetable,)


def get_operands(condition: Any) -> list[Any] | None:
    """Return the operands of an ``&`` or ``|`` condition, or None for an asset, reference or alias."""
    from airflow.sdk import AssetAll, AssetAny

    return list(condition.objects) if isinstance(condition, (AssetAll, AssetAny)) else None


def iter_assets(condition: Any) -> Iterator[Any]:
    """Yield the assets, references and aliases anywhere in an asset condition."""
    if (operands := get_operands(condition)) is None:
        yield condition
        return
    for operand in operands:
        yield from iter_assets(operand)


def iter_or_groups(condition: Any) -> Iterator[list[Any]]:
    """
    Yield the operands of each ``|`` condition, with those of the ``|`` conditions directly in it.

    An ``&`` condition is no operand of a group but starts groups of its own: a delivery that matches
    names on both sides of an ``&`` satisfies it once.
    """
    from airflow.sdk import AssetAll, AssetAny

    if not isinstance(condition, AssetAny):
        for operand in get_operands(condition) or []:
            yield from iter_or_groups(operand)
        return
    group = []
    pending = list(condition.objects)
    while pending:
        operand = pending.pop(0)
        if isinstance(operand, AssetAny):
            pending[:0] = operand.objects
        elif isinstance(operand, AssetAll):
            yield from iter_or_groups(operand)
        else:
            group.append(operand)
    yield group
