# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""
The "Inbound Webhook" connection form, defined once.

Airflow 3.2 and later read the form from the provider metadata (``conn-fields`` and
``ui-field-behaviour``); Airflow 3.1 only from the hook's ``get_connection_form_widgets()`` and
``get_ui_field_behaviour()``. Both are built from ``FIELDS`` so they cannot drift apart.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

CONN_TYPE = "inbound_webhook"
HOOK_NAME = "Inbound Webhook"


@dataclass(frozen=True)
class FormField:
    """An extra field of the form, stored under ``name`` in the connection extra."""

    name: str
    label: str
    description: str
    kind: str = "string"
    choices: tuple[str, ...] | None = None


FIELDS = (
    FormField(
        "source_type",
        "Source type",
        "How deliveries are verified and named. Leave empty to use the webhook name, so that "
        "webhook_github is the github source type.",
        choices=("hmac_json", "token_header", "github", "gitlab"),
    ),
    FormField(
        "signature_header",
        "Signature header",
        "hmac_json: header carrying the HMAC-SHA256 signature, e.g. X-Signature. Required for hmac_json.",
    ),
    FormField(
        "signature_prefix",
        "Signature prefix",
        "hmac_json: text before the digest in the signature header, e.g. sha256= or v1=.",
    ),
    FormField(
        "signature_encoding",
        "Signature encoding",
        "hmac_json: encoding of the digest. Defaults to hex.",
        choices=("hex", "base64"),
    ),
    FormField(
        "token_header",
        "Token header",
        "token_header: header carrying the shared secret, e.g. Authorization. Required for token_header.",
    ),
    FormField(
        "token_prefix",
        "Token prefix",
        'token_header: text before the secret in the token header, e.g. "Bearer ".',
    ),
    FormField(
        "event_field",
        "Event field",
        "hmac_json and token_header: dotted path into the JSON body naming the event, e.g. event or "
        "data.type. Set this or Event header.",
    ),
    FormField(
        "event_header",
        "Event header",
        "hmac_json and token_header: header naming the event. Set this or Event field.",
    ),
    FormField(
        "path_field",
        "Path field",
        "hmac_json and token_header, optional: dotted path to a value that becomes the event's path, "
        "such as a service or project name.",
    ),
    FormField(
        "team_name",
        "Team",
        "Optional, Airflow 3.3 or later with multi-team enabled: attribute events to this team.",
    ),
    FormField(
        "max_body_bytes",
        "Maximum body size",
        "Optional: larger deliveries are refused, in bytes. Defaults to 1048576 (1 MiB).",
        kind="integer",
    ),
)

HIDDEN_FIELDS = ["host", "schema", "login", "port"]
RELABELING = {"password": "Shared secret"}


def build_conn_fields() -> dict[str, dict[str, Any]]:
    """Return the form's extra fields in the provider metadata's ``conn-fields`` format."""
    conn_fields = {}
    for field in FIELDS:
        schema: dict[str, Any] = {"type": [field.kind, "null"]}
        if field.choices:
            schema["enum"] = list(field.choices)
        if field.kind == "integer":
            schema["minimum"] = 1
        conn_fields[field.name] = {"label": field.label, "description": field.description, "schema": schema}
    return conn_fields


def build_ui_field_behaviour() -> dict[str, Any]:
    """Return the standard-field behaviour in the hook method format (Airflow 3.1)."""
    return {"hidden_fields": HIDDEN_FIELDS, "relabeling": RELABELING}


def build_form_widgets() -> dict[str, Any]:
    """
    Return the form's extra fields as WTForms fields, for Airflow 3.1.

    Airflow 3.1 calls this with WTForms, Flask-Babel and Flask-AppBuilder replaced by mocks, so the
    imports must stay inside this function and use only names Airflow mocks.
    """
    from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
    from flask_babel import lazy_gettext
    from wtforms import IntegerField, StringField
    from wtforms.validators import Optional, any_of

    widgets = {}
    for field in FIELDS:
        validators = [Optional()]
        if field.choices:
            validators.append(any_of(list(field.choices)))
        field_class = IntegerField if field.kind == "integer" else StringField
        widgets[field.name] = field_class(
            lazy_gettext(field.label),
            widget=BS3TextFieldWidget(),
            description=field.description,
            validators=validators,
        )
    return widgets
