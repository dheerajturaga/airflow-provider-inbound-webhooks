# Security

- **The endpoint is open to anyone who can reach it.** It does not use Airflow's login: it has to
  accept services that cannot log in. Every delivery is verified against the webhook's secret before
  anything is recorded, and unknown webhook names are answered with `404`.
- **Only listed webhooks reach your secrets backend.** A request for a name that is not in `webhook_names` is
  refused before any connection is read, and each listed webhook's connection is read at most twice
  per `connection_cache_ttl` by each API server worker, whatever arrives: when the cached connection
  expires, and for a delivery that fails verification, in case the secret has just changed. So a
  flood of unauthenticated requests costs your secrets backend about two calls per webhook a minute.
  Setting `webhook_names` to `*` gives this up, which is why it is deprecated.
- **Expose only your webhooks.** If the API server must be reachable by an external service, put a
  reverse proxy in front that forwards only their paths, such as `/webhooks/github`, rather than
  exposing the whole Airflow UI and API, and have it rate-limit them. `rate_limit_per_minute` also
  limits each webhook's verified deliveries in each API server worker, which bounds what a chatty
  sender writes; requests that fail verification never count against it. Forward each webhook's
  path rather than all of `/webhooks`, which also holds the deliveries page and API.
- **Reading deliveries needs a login.** The deliveries page and API under `/webhooks` need an
  Airflow login, and permission to read the webhook's connection for the team its extra names.
  Deliveries attributed to another team are not shown. The answer to a sender holds only the
  delivery ID and outcome, because services show answers to people who need not be Airflow users.
- **Use HTTPS.** It matters most for `token_header` and GitLab, where the secret itself travels in a
  header.
- **Use strong secrets, ideally from a secrets backend,** which keeps them out of the metadata
  database. To rotate one, change the connection first, then the service: the first delivery signed
  with the new secret has the connection read again, and is accepted. Deliveries the service signs
  with the old secret in between are refused once the cached connection expires. The other way
  round, deliveries are refused until the connection changes, and then for up to
  `connection_cache_ttl`. Redeliver refused deliveries from the service's delivery log.
- **Payloads are visible to anyone who can read asset events.** Each asset event stores its
  delivery's payload, and the Airflow UI and REST API show it to every user allowed to read assets,
  not only to the Dags that subscribe. GitHub push payloads, for example, include each commit
  author's name and email address. `jira` stores only a summary of the issue by default, and
  `store_payload` set to `none` keeps no payload at all.
- **Retries are recognised, replays are not.** A delivery sent again with the same delivery ID is
  answered as a retry and records nothing, which covers a service's own retries. A captured delivery
  sent again with another ID is recorded as new: GitHub's delivery ID is a header its signature does
  not cover, and GitLab sends a token rather than a signature. Only an `hmac_json` service whose
  delivery ID is in the body (`delivery_id_field`) has it signed, so a captured delivery sent again
  is answered as a retry while its record is kept. Because a retry must repeat the body too, a
  captured body sent under another delivery's ID cannot take that delivery's place. Other headers
  outside the signature can be changed too. Where the event name comes from such a header, as
  GitHub's `X-GitHub-Event` or an `event_header` does, a replayed body can be given another event's
  name. Signatures over a timestamp would allow rejecting old deliveries; see
  [docs/design.md](design.md).
- Bodies larger than `max_body_bytes` are refused before verification.

To report a vulnerability, see [SECURITY.md](../SECURITY.md).

## Limitations

- No replay protection: retries are recognised by their delivery ID, deliberate replays are not,
  except for `hmac_json` services whose delivery ID is in the body.
- Each recorded asset event stores the delivery's payload, unless `store_payload` or
  `max_event_payload_bytes` says otherwise, and anyone who can read asset events can see it.
- Delivery records are only deleted by `airflow webhooks clean`, which needs to be scheduled.
- Renaming a repository, project or service changes its event names, so Dags must be updated.
- The plugin uses some Airflow internals (`asset_manager`, `AssetModel`, `create_session`,
  `BaseDBManager`), which CI checks against every supported version.

[docs/design.md](design.md) covers the design, these limitations and the path towards Airflow
core.
