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
`camus_session`. The cookie is `HttpOnly`, so page scripts cannot read it; `SameSite=Strict`, so
another site cannot cause it to be sent; and `Secure` whenever the connection is TLS. It expires when
the token does. Sign out revokes the token and clears the cookie.

The cookie authenticates the dashboard's pages and its `/v1/dashboard/` endpoints, and nothing else.
Every other route — `/execute-sql-non-query`, `/insert`, `/start-transaction` and the rest — still
requires the bearer token in an `Authorization` header. That is what keeps a write route out of reach
of a cross-site request: a browser attaches a cookie by itself, but never a header.

**With authentication disabled** there is no principal to check any panel against, so the dashboard
is served to loopback connections only. A browser on another machine gets 403 and a message naming
the setting to turn on. To reach the dashboard from another machine, enable authentication.

## What a non-superuser sees

Two cards need a superuser, because the statements behind them do: **Engine** runs
`SHOW ENGINE STATS`, and **Configuration** runs `SHOW VARIABLES` and `SHOW CLUSTER SETTINGS`. Those
outputs describe the node's whole security posture, limits and workload volume, which no per-database
grant scopes down.

Any other authenticated user sees a complete page with those two cards replaced by one line of
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
