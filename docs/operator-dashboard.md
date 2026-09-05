# Operator dashboard

Open a node's HTTP port in a browser and CamusDB serves a read-only dashboard. It answers four
questions without a shell: is this node healthy, what load does it carry, what data does it hold,
and what has the engine done since it started.

The dashboard is read-only. It runs no DML and no DDL, opens no coordinated transaction, and offers
no operator action. Nothing on the page changes the node.

## What it shows

The page is a dark identity band followed by a grid of cards. Each card refreshes on its own timer,
and each fails on its own: a card that cannot load shows one line of explanation while every other
card keeps working.

| Card | Content | Refresh |
| --- | --- | --- |
| Node band | Endpoint, role, readiness, cluster mode, authentication state, version, uptime, data directory | 2 s |
| Load strip | In-flight foreground requests, open explicit transactions, prepared statements and the bytes they retain, hosted partitions | 2 s |
| Engine | A curated slice of the node's instruments: request and statement rates, commit duration, cache hit rates, Raft and storage counters | 5 s |
| Cluster | The committed membership roster, each member's endpoint, node id and role | 10 s |
| Databases | The registry, with each database's id, branch parent, and whether this node currently holds it in memory | 30 s |
| Relations | The tables and views of one database | on selection |
| Backups | The most recent backups with kind and size | 60 s |
| Slow queries | The newest statements over the slow-query threshold, with duration, rows read against rows returned, and the full-scan and spill flags | 15 s |
| Configuration | Every setting this node resolved, with its value, whether a change needs a restart, and whether the fleet must agree | once |
| Overlay | The live cluster-settings entries | once |

### Reading the Memory column

**Loaded** means this node currently holds the database open. **Not loaded** means it does not, and
that is an ordinary state rather than a fault. It covers two histories the page cannot tell apart:
a database nobody has opened since the node started was never in memory, and one that idle eviction
reclaimed is the eviction policy doing its job. Neither is coloured as a warning.

Selecting a row reads that database's relations, and that is the one action on the page with an
effect: reading relations opens a descriptor, so a not-loaded database becomes loaded. It is driven
by a click rather than a timer for exactly that reason — on a schedule it would keep every database
an operator can see in memory for as long as the browser tab stayed open, and idle eviction would
never reclaim one.

## Signing in

**With authentication enabled**, a browser at `/` is redirected to `/SignIn`. The form exchanges the
password for the same short-lived token `/login` issues, and stores it in a cookie named
`__Host-camus_session`. The cookie is `HttpOnly`, so page scripts cannot read it; `SameSite=Strict`,
so another site cannot cause it to be sent; and always `Secure`. It expires when the token does.
Sign out revokes the token and clears the cookie.

The `__Host-` prefix makes the browser enforce three properties rather than trusting the server to
assert them: the cookie must be `Secure`, must have `Path=/`, and must name no `Domain`. The last one
also stops another host on the same registrable domain from writing a cookie this one would read.

**The dashboard therefore needs HTTPS, or loopback.** `Secure` is set unconditionally, not from
whether this particular connection was TLS. Behind a proxy that terminates TLS, the inbound hop is
plaintext even though the browser connected over HTTPS, and reading the flag off that hop dropped it
in exactly the deployment that needed it. Setting it always is also what removes any need to trust an
`X-Forwarded-Proto` header, which the server cannot verify. Browsers treat loopback as a secure
context, so local development is unaffected; a plaintext connection to a remote address cannot hold
the cookie, and sign-in there will not work.

### Browser security headers

The dashboard's pages and the error page carry a `Content-Security-Policy`, `X-Frame-Options: DENY`
and `X-Content-Type-Options: nosniff`. The JSON endpoints deliberately do not: a policy means nothing
on a response no browser treats as a document, and sending one there would only invite a future
policy written for the dashboard to be inherited by an endpoint nobody re-read.

The policy allows inline script only through a per-request nonce, because two pages need one inline
script each — the theme stamp that has to run before the first paint, and the sign-in handler. If you
add a script or a stylesheet to a dashboard page, serve it from `wwwroot` rather than inlining it, or
the browser will refuse it.

The cookie authenticates the dashboard's pages and its `/v1/dashboard/` endpoints, and nothing else.
Every other route — `/execute-sql-non-query`, `/insert`, `/start-transaction` and the rest — still
requires the bearer token in an `Authorization` header. That is what keeps a write route out of reach
of a cross-site request: a browser attaches a cookie by itself, but never a header.

**With authentication disabled** there is no principal to check any panel against, so the dashboard
is served to loopback connections only. A browser on another machine gets 403 and a message naming
the setting to turn on. To reach the dashboard from another machine, enable authentication.

### Reading the Slow queries card

The card is empty on a node that has had nothing slow **and** on a node that is not recording at all,
so it says which. With [the slow query log](slow-query-log.md) switched off it shows one line telling
you to turn it on, rather than an empty table you would read as a healthy node.

Two flags carry most of the value. **Full scan** means the plan read a whole relation instead of
seeking an index. **Spilled** means a sort, grouping, distinct or join outgrew its memory budget and
wrote to disk. Together with rows read against rows returned they usually answer "why was this slow"
without a second run.

Statement text is clipped to fit the cell; the full text is the tooltip, and all of it is in
`SHOW SLOW QUERIES`. The card also says when older entries have been overwritten, so a short list is
never mistaken for a complete history.

Polling this card does not disturb what it shows: `SHOW SLOW QUERIES` is never itself recorded.

## What a non-superuser sees

Three cards need a superuser, because the statements behind them do: **Engine** runs
`SHOW ENGINE STATS`, **Configuration** runs `SHOW VARIABLES` and `SHOW CLUSTER SETTINGS`, and
**Slow queries** runs `SHOW SLOW QUERIES`. Those outputs describe the node's whole security posture,
limits and workload volume, which no per-database grant scopes down — and the slow-query rows carry
the literal SQL text of statements other users ran, which can hold values from tables the reader has
no grant on.

Any other authenticated user sees a complete page with those three cards replaced by one line of
explanation. The database list is filtered to the databases that user may already reach.

## Settings

| Setting | Default | Change takes effect | Agreement |
| --- | --- | --- | --- |
| `dashboard_enabled` | `true` | restart | per node |
| `dashboard_refresh_seconds` | `2` | live | per node |

```yml
dashboard_enabled: true
dashboard_refresh_seconds: 2
```

`dashboard_enabled: false` removes the surface entirely. The pages and the endpoints all answer 404,
as if they were never built.

`dashboard_refresh_seconds` is read per request and handed to the page, so lowering or raising it
reaches an already-open tab on its next poll. The node clamps it to between 1 and 300 seconds.

Both settings appear in `SHOW VARIABLES` and on the Configuration card, like every other setting. See
[runtime and cluster settings](runtime-cluster-settings.md) for the wider settings surface.

## Limits worth knowing

**Every number is node-local.** The load counters describe this process. The metrics come from this
process's instruments. Readiness and the stalled-partition warning describe this node's own view.
Even the cluster roster reports only the partitions this node leads. Open the dashboard on each node
for a picture of the fleet.

**Counters accumulate from process start and never reset.** The page turns them into rates by
subtracting two readings, so a rate appears one refresh after the page loads, and a restart starts
every rate again from nothing.

**The Engine card is empty when `engine_metrics_enabled` is false.** The card says so rather than
showing an empty table. Turning it back on needs a restart.

**The Slow queries card is empty when `slow_query_log_enabled` is false**, and it says so for the
same reason. Turning it on needs a restart. The log lives in memory, holds a bounded sample, and does
not survive a restart — see [the slow query log](slow-query-log.md) for sizing it.

**CamusDB's own instruments record only when diagnostics are on.** With `diagnostics.enabled` false
the engine's request, execute, commit and cache instruments exist but never record, so they do not
appear on the card. The Raft and storage counters appear either way.

**This is not a monitoring system.** It shows the present, plus one interval of change. It stores no
history, raises no alert, and aggregates nothing across nodes. For alerting, history and fleet-wide
views, use the OpenTelemetry or Prometheus export instead.

## Appearance

The dashboard follows the palette, typography and component treatments of the documentation site, so
the two read as one product. It respects the browser's light or dark preference, and the control in
the navigation bar overrides it for that browser.

The page carries no decorative animation. On a surface that refreshes every two seconds, movement
would hide the one change that matters.
