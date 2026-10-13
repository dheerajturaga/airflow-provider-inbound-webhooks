# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
r"""
Send a signed test delivery to an inbound webhook, as a service would.

Signs the body with HMAC-SHA256, prints the response, and exits non-zero unless it is a 2xx.
Standard library only, so it runs anywhere.

    python examples/send_test_event.py --url http://localhost:8080/webhooks/demo \
        --secret change-me --data '{"event": "deployed", "service": "billing"}'

GitHub-style delivery:

    python examples/send_test_event.py --url http://localhost:8080/webhooks/github \
        --secret change-me --signature-header X-Hub-Signature-256 --signature-prefix sha256= \
        --header "X-GitHub-Event: push" \
        --data '{"ref": "refs/heads/main", "repository": {"full_name": "my-org/web"}}'
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import sys
import urllib.error
import urllib.request


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", required=True, help="Webhook URL, e.g. http://localhost:8080/webhooks/demo")
    parser.add_argument("--secret", required=True, help="Shared secret, the webhook connection's password")
    parser.add_argument("--data", required=True, help="JSON body to send")
    parser.add_argument("--signature-header", default="X-Signature", help="Header carrying the signature")
    parser.add_argument("--signature-prefix", default="", help="Prefix before the hex digest, e.g. sha256=")
    parser.add_argument(
        "--header", action="append", default=[], metavar="'NAME: VALUE'", help="Extra header; repeatable"
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    body = args.data.encode()
    signature = hmac.new(args.secret.encode(), body, hashlib.sha256).hexdigest()
    headers = {
        "Content-Type": "application/json",
        args.signature_header: f"{args.signature_prefix}{signature}",
    }
    for header in args.header:
        name, _, value = header.partition(":")
        headers[name.strip()] = value.strip()
    request = urllib.request.Request(args.url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status, text = response.status, response.read().decode()
    except urllib.error.HTTPError as e:
        status, text = e.code, e.read().decode()
    print(f"HTTP {status}\n{text}")
    return 0 if 200 <= status < 300 else 1


if __name__ == "__main__":
    sys.exit(main())
