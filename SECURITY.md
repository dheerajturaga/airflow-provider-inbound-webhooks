# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub: on the repository's **Security** tab, choose
**Report a vulnerability**. Do not open a public issue.

Include the affected version or commit, the Airflow version, how to reproduce the problem and its
impact. You will get an acknowledgement, and a fix will be coordinated with you before the details
are made public.

## Supported versions

Security fixes are made in a new release of the newest version only, so upgrade to it to receive
them.

## What counts as a vulnerability

The endpoint `POST /webhooks/<name>` is reachable without an Airflow login by design: the services
that send webhooks cannot log in to Airflow. Its protection is the per-webhook shared secret.

Reports we want to hear about include:

- a delivery accepted without a valid signature or token for its webhook's secret;
- a delivery that records asset events outside its own webhook's namespace, the names starting with
  `<name>/`;
- reading a webhook secret, or another connection, through the endpoint;
- a way to make the endpoint execute code, or to crash or exhaust the API server with requests that
  are cheap for the sender.

These are known and documented limitations, not vulnerabilities:

- replaying a captured, validly signed delivery, including with headers its signature does not
  cover changed, such as GitHub's `X-GitHub-Event` or an `event_header`, which name the event
  (there is no replay protection yet);
- the `token_header` and `gitlab` source types sending the secret itself, which relies on TLS;
- the endpoint being reachable without an Airflow login;
- a holder of a webhook's secret choosing, through the payload, which event names under that
  webhook's `<name>/` namespace a delivery produces;
- anyone able to edit Airflow connections changing a webhook's secret or source type, since editing
  connections already requires trusted access to Airflow.
