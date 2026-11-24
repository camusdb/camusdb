# Running queries and connections — `SHOW QUERIES`, `SHOW CONNECTIONS`, `CANCEL QUERY`

CamusDB does not use session objects. A client sends each statement over HTTP or gRPC, and a
transaction lives across requests as a handle. The following statements show what the clients are
doing now.

```sql
SHOW QUERIES;                          -- statements that run on this node now
SHOW QUERIES LIKE '%FROM orders%';     -- the same, filtered by the statement text
SHOW CLUSTER QUERIES;                  -- the same, from every node of the cluster

SHOW CONNECTIONS;                      -- client connections this node holds open now
SHOW CLUSTER CONNECTIONS;              -- the same, from every node of the cluster

CANCEL QUERY 'b71a90e314-45';          -- stop one running query, on any node
```

They are the CamusDB form of CockroachDB `SHOW CLUSTER QUERIES` / `CANCEL QUERY` and MySQL
`SHOW FULL PROCESSLIST` / `KILL QUERY`. None of the words is reserved: `queries`, `connections`,
`cancel` and `query` can be used as table and column names.

All five statements are server-level. They need no database and open no transaction. You can send
them with an empty database name.

## `SHOW QUERIES`

One row per running statement, oldest first. The statement that asks is in the list too.

| Column | Meaning |
|---|---|
| `query_id` | The id `CANCEL QUERY` takes. Unique across the cluster. |
| `node` | The Raft endpoint of the node where the statement is running. |
| `connection_id` | The `SHOW CONNECTIONS` row the statement arrived on. NULL when no host connection carried it. |
| `client_address` | The client's IP address. |
| `transport` | `http`, `grpc`, `grpc-stream` (an op on a `BatchExecute` stream), or `embedded`. |
| `user_name` | The authenticated user. NULL when authentication is off. |
| `database_name` | The context database of the statement. |
| `transaction_id` | The transaction handle as `{pt}.{counter}`, the pair a client sends to resume it. |
| `isolation` | The isolation level of that transaction. |
| `kind` | The statement kind, for example `select`, `update`, `create_table`. `unknown` until the parse of the statement ends. |
| `phase` | `planning` before the first row is read, `executing` after. A write is `executing` from the start. |
| `started_at` | When the statement started, UTC. |
| `elapsed_ms` | How long it has run. |
| `rows_returned` | Rows the client has read so far. |
| `cancellable` | True when `CANCEL QUERY` can stop it now — see [What a cancel stops](#what-a-cancel-stops). |
| `cancel_requested` | True after a `CANCEL QUERY` named it. |
| `sql` | The statement text, with credential literals masked, cut to `query_activity_max_sql_length`. |
| `error` | NULL on a statement row. See [Cluster forms](#cluster-forms). |

A row-returning statement is listed until its cursor ends, even if the server has already produced
all of its rows. A query whose client reads slowly is therefore listed for as long as the client
reads, with `rows_returned` growing.

## `SHOW CONNECTIONS`

One row per TCP or QUIC connection the node accepted on a client-facing listener (HTTP, HTTPS and the
gRPC client port), oldest first. QUIC connections require `http3_enabled`. The Raft listener is not tracked.

| Column | Meaning |
|---|---|
| `connection_id` | The id the `connection_id` column of `SHOW QUERIES` refers to. |
| `node` | The node that accepted the connection. |
| `client_address` | The client's IP address. |
| `protocol` | `HTTP/1.1`, `HTTP/2` or `HTTP/3`. A gRPC client is `HTTP/2`. `HTTP/3` appears only on the HTTPS port with `http3_enabled`. |
| `kind` | `client`, or `peer` for a connection another node opened to call an `/internal/` endpoint. |
| `user_name` | The user of the last statement on the connection. NULL before the first one. |
| `opened_at` | When the connection was accepted, UTC. |
| `age_ms` | How long it has been open. |
| `idle_ms` | Time since the last statement or request on it; 0 while a statement is running on it. |
| `requests` | HTTP requests started on it. A `BatchExecute` stream is one request. |
| `active_queries` | Statements that run on it now. |
| `open_streams` | `BatchExecute` streams open on it now. The .NET client uses a small pool of them. |
| `error` | NULL on a connection row. See [Cluster forms](#cluster-forms). |

HTTP/2 carries many calls on one connection. This means that one gRPC client with one channel is one
row, however many of its statements are in flight at once.

## `CANCEL QUERY`

`CANCEL QUERY '<query_id>'` stops a running query and returns no rows. The client of the cancelled
statement receives `CADB0554` (gRPC status `CANCELLED`). Rows it read before the cancel were real.

The id names its node, which means that you can send the cancel to any node. A node that does not own the id
forwards it to the node that does.

| Code | Meaning |
|---|---|
| `CADB0551` | No running query has this id. The query may have ended or may belong to another user. The id may also be wrong. |
| `CADB0552` | The statement is a write or schema change, which a cancel cannot stop, or its parse has not ended yet. |
| `CADB0553` | The node that may own the statement did not answer. The query may not have stopped; send the cancel again. |

### What a cancel stops

**A cancel stops reads only.** A `SELECT`, a `SHOW`, an `EXPLAIN ANALYZE` stop at their next row.
An `INSERT`, `UPDATE`, `DELETE` or a schema change does not stop. Once its first mutation lands, it
continues to its commit or its rollback. If it stopped part way, its locks would be left to expire,
while a commit or rollback releases them. These statements show `cancellable = false`, and a cancel
of one returns `CADB0552`. The cancel does not report a success that did not happen.

An `INSERT`, `UPDATE` or `DELETE` with a `RETURNING` list returns rows the way a read does. Only its
parse shows that it is a write.
For that reason, a cancel of a row-returning statement is allowed only after its parse shows a read.
Before that, the statement shows `cancellable = false`, and a cancel returns `CADB0552`. A parse
usually ends in microseconds. If this occurs for a read, send the cancel again.

**A cancel takes effect when the statement asks for its next row.** The server writes rows into the
network buffer ahead of a slow client. A client that reads very slowly first receives the rows that
are already in that buffer, then the error. Until then, the statement is listed with
`cancel_requested = true`.

## Who sees what

- With authentication off, all callers see all rows and can cancel any query.
- A superuser sees all rows and can cancel any query.
- Any other user sees only their own statements and connections, and can cancel only their own
  queries. A cancel of another user's query returns `CADB0551`, the same as an unknown id. This means
  that the answer does not tell which ids other users own.

The rows carry the literal SQL text of statements, which is why the filter is per user. The filter
is applied on the node that owns the rows, and another user's text never leaves that node.

## Cluster forms

`SHOW CLUSTER QUERIES` and `SHOW CLUSTER CONNECTIONS` ask the other cluster members for their rows,
in parallel, over the node-to-node HTTP channel that the node secret authenticates.

A member that does not answer within `cluster_activity_peer_timeout_ms` does not fail the
statement. It gives one row with its `node` and the `error` column set, and all other columns NULL.
A list that failed because one node is down would fail exactly when you need it.

One cluster statement makes one request to each peer. To stop a flood of them from multiplying
internal traffic by the cluster size, a node allows at most `cluster_activity_max_concurrent_fan_outs`
cluster statements at one time. A statement past that cap returns **CADB0558**
(`ConcurrencyLimitExceeded`, HTTP 503). It is safe to retry. The local forms are not subject to this
cap.

On a standalone node, the cluster forms return the same rows as the local forms.

## Settings

```yml
query_activity_enabled: true
query_activity_max_sql_length: 1024
cluster_activity_peer_timeout_ms: 2000
cluster_activity_max_concurrent_fan_outs: 8
```

| Setting | Default | What it does |
|---|---|---|
| `query_activity_enabled` | `true` | Lists running statements. Off: no statement is listed, and none can be cancelled with `CANCEL QUERY`. |
| `query_activity_max_sql_length` | `1024` | Characters of statement text shown per row. |
| `cluster_activity_peer_timeout_ms` | `2000` | How long the cluster forms and a forwarded cancel wait for one peer. |
| `cluster_activity_max_concurrent_fan_outs` | `8` | Maximum number of cluster forms in progress on one node at one time. `0` disables the cap. |

All four are runtime settings. A change made with `SET CLUSTER SETTING` affects the next statement,
and no restart is needed. Turning `query_activity_enabled` off does not remove statements that are
already listed; they leave when they end. With the setting off, `SHOW CONNECTIONS` works, and a
connection records the user of each statement on it.

The cost per statement is one small entry and one dictionary insert and remove. A read also gets one
linked cancellation token. The statement text is not copied; it is masked and cut at the time someone
reads the list. The `kind` column comes from the parse that the statement does anyway. A read of the
list parses no SQL.

## Limits

- The typed row APIs (`/query`, `/insert`, `/update`, `/delete` and the gRPC `CamusRows` service)
  send no SQL and are not listed.
- The span scans that a node performs for another node's distributed query are not listed on that
  node. The coordinator's statement is listed.
- An explicit transaction that waits between statements is not running a statement. It does not
  appear in `SHOW QUERIES`, and its connection shows a growing `idle_ms`.
- There is no `KILL CONNECTION` yet.
