# Operator dashboard

Open a node's HTTP port in a browser and CamusDB serves a read-only dashboard. It shows, without a
shell, whether this node is healthy and how much load it carries. It also shows the data on
the node and what the engine has done since it started.

The dashboard is read-only. It executes no DML or DDL and opens no coordinated transaction. It also
offers no operator action. The page does not change the node.

## What it shows

The page is a dark identity band followed by a grid of cards. Each card has a separate refresh timer,
and the cards fail independently. A card that cannot load shows one line of explanation, and the
other cards continue to work.

| Card | Content | Refresh |
| --- | --- | --- |
| Node band | Endpoint, role, readiness, cluster mode, authentication state, version, uptime, data directory | 2 s |
| Load strip | In-flight foreground requests, open explicit transactions, prepared statements and the bytes they retain, hosted partitions | 2 s |
| Engine | A curated slice of the node's instruments: request and statement rates, commit duration, cache hit rates, Raft and storage counters | 5 s |
| Cluster | The committed membership roster, each member's endpoint, node id and role | 10 s |
| Databases | The registry, with each database's id and branch parent. It also shows whether the database is loaded in memory on this node | 30 s |
| Relations | The tables and views of one database | on selection |
| Backups | The most recent backups with kind and size | 60 s |
| Slow queries | The newest statements over the slow-query threshold, with duration, rows read against rows returned, and the full-scan and spill flags | 15 s |
| Configuration | The settings this node resolved, with their values, whether a change needs a restart, and whether the fleet must agree | once |
| Overlay | The live cluster-settings entries | once |

### Reading the Memory column

**Loaded** means the database is currently open on this node. **Not loaded** means it is not open.
That is an ordinary state. It covers two cases that the page cannot tell apart. In the first case,
nobody has opened the database since the node started. In the second case, idle eviction reclaimed
it, which is the eviction policy doing its job. Neither case is coloured as a warning.

Selecting a row reads that database's relations. This is the one action on the page with an effect.
Reading relations opens a descriptor, and a not-loaded database becomes loaded. For that reason, the
read happens on a click and not on a timer. On a schedule, it would load all databases that an
operator can see and leave them in memory while the browser tab is open. Idle eviction would then not
reclaim any of them.

## Signing in

**With authentication enabled**, a browser at `/` is redirected to `/SignIn`. The form exchanges the
password for the same short-lived token `/login` issues, and stores it in a cookie named
`__Host-camus_session`. The cookie has three attributes that limit how a browser handles it.

- `HttpOnly`, which means that page scripts cannot read it.
- `SameSite=Strict`, which means that another site cannot cause it to be sent.
- `Secure`, which is always set.

The cookie expires when the token does.
Sign out revokes the token and clears the cookie.

The `__Host-` prefix makes the browser enforce three properties that the server would otherwise only
assert.

- The cookie must be `Secure`.
- The cookie must have `Path=/`.
- The cookie must name no `Domain`. This also stops another host on the same registrable domain from
  writing a cookie that this one would read.

**The dashboard therefore needs HTTPS, or loopback.** `Secure` is always set, whether or not
this particular connection was TLS. Behind a proxy that terminates TLS, the inbound hop is
plaintext even though the browser connected over HTTPS, and reading the flag off that hop dropped it
in exactly the deployment that needed it. Setting it always also removes any need to trust an
`X-Forwarded-Proto` header, which the server cannot verify. Browsers treat loopback as a secure
context. This means that local development is unaffected. A plaintext connection to a remote address
cannot store the cookie, and sign-in there will not work.

### Browser security headers

The dashboard's pages and the error page carry a `Content-Security-Policy`, `X-Frame-Options: DENY`
and `X-Content-Type-Options: nosniff`. The JSON endpoints deliberately do not. A policy has no effect
on a response that no browser treats as a document. Sending one there would also invite a future
policy written for the dashboard to be inherited by an endpoint nobody re-read.

The policy allows inline script only through a per-request nonce. Two pages need one inline script
each: the theme stamp that has to execute before the first paint, and the sign-in handler. If you
add a script or a stylesheet to a dashboard page, serve it from `wwwroot`. The browser will block an
inlined one.

The cookie authenticates the dashboard's pages and its `/v1/dashboard/` endpoints, and no other route.
The other routes, such as `/execute-sql-non-query` and `/start-transaction`, require the bearer token
in an `Authorization` header. This puts a write route out of reach of a cross-site request. A browser
attaches a cookie by itself, but it does not attach a header by itself.

**With authentication disabled** there is no principal to check any panel against. This means that the
dashboard is served to loopback connections only. A browser on another machine gets 403 and a message naming
the setting to turn on. To reach the dashboard from another machine, enable authentication.

### Reading the Slow queries card

The card is empty on a node that has had no slow statements **and** on a node that is not recording
at all. The card says which case applies. With [the slow query log](slow-query-log.md) switched off,
it shows one line telling you to turn it on. An empty table would otherwise read as a healthy node.

Two flags carry most of the value. **Full scan** means the plan read the complete relation and did not
seek an index. **Spilled** means a sort, grouping, distinct or join outgrew its memory budget and
wrote to disk. Together with rows read against rows returned, they usually explain a slow statement
from one run.

Statement text is clipped to fit the cell. The full text is the tooltip, and all of it is in
`SHOW SLOW QUERIES`. The card also says when older entries have been overwritten. This means that a
short list is not mistaken for a complete history.

Polling this card does not disturb what it shows. `SHOW SLOW QUERIES` is not itself recorded.

## What a non-superuser sees

Four cards need a superuser. Three of them need one because the statements behind them do.

| Card | Statement |
| --- | --- |
| **Engine** | `SHOW ENGINE STATS` |
| **Configuration** | `SHOW VARIABLES` and `SHOW CLUSTER SETTINGS` |
| **Slow queries** | `SHOW SLOW QUERIES` |

Those outputs describe the node's security posture, limits and workload volume. A per-database grant
cannot scope this down. Also, the slow-query rows carry the literal SQL text of statements other
users ran. This text can contain values from tables the reader is not granted access to. The fourth
card, **Cluster**, names each peer endpoint. This is the same kind of topology that `SHOW ENGINE STATS`
is gated for. Its data comes from `/v1/dashboard/cluster`, which applies the same access rule as
`/v1/cluster/membership`.

Any other authenticated user sees a complete page with those four cards replaced by one line of
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
as if they did not exist.

`dashboard_refresh_seconds` is read per request and handed to the page. This means that a new value
reaches an already-open tab on its next poll. The node clamps it to between 1 and 300 seconds.

Both settings appear in `SHOW VARIABLES` and on the Configuration card, like the other settings. See
[runtime and cluster settings](runtime-cluster-settings.md) for the wider settings surface.

## Limits worth knowing

**All numbers are node-local.** The load counters describe this process. The metrics come from this
process's instruments. Readiness and the stalled-partition warning describe this node's view.
Even the cluster roster shows only the partitions this node leads. Open the dashboard on each node
for a picture of the fleet.

**Counters accumulate from process start and never reset.** The page turns them into rates by
subtracting two readings. This means that a rate appears one refresh after the page loads. After a
restart, the rates start again from zero.

**The Engine card is empty when `engine_metrics_enabled` is false.** The card says so in place of
an empty table. Turning it back on needs a restart.

**The Slow queries card is empty when `slow_query_log_enabled` is false**, and it says so for the
same reason. Turning it on needs a restart. The log is a bounded sample in memory, and it does
not survive a restart. For information about sizing it, see [the slow query log](slow-query-log.md).

**CamusDB's own instruments record only when diagnostics are on.** With `diagnostics.enabled` false,
the engine's request, execute, commit and cache instruments exist but do not record. They do not
appear on the card. The Raft and storage counters appear either way.

**This is not a monitoring system.** It shows the present, plus one interval of change. It does not store
history or raise alerts. It also does not aggregate data across nodes. For alerting, history and fleet-wide
views, use the OpenTelemetry or Prometheus export instead.

## Appearance

The dashboard follows the palette, typography and component treatments of the documentation site.
The two read as one product. It respects the browser's light or dark preference, and the control in
the navigation bar overrides it for that browser.

The page carries no decorative animation. On a surface that refreshes at a two-second interval,
movement would hide the one change that matters.
