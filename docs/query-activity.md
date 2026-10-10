# Running queries and connections — `SHOW QUERIES`, `SHOW CONNECTIONS`, `CANCEL QUERY`

CamusDB has no session object of its own. A client sends each statement over HTTP or gRPC, and a
transaction lives across requests as a handle. These statements show what the clients do now anyway:

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
`cancel` and `query` stay usable as table and column names.

All five statements are server-level. They need no database and open no transaction, so send them
with an empty database name if you like.

## `SHOW QUERIES`

One row per statement that runs now, oldest first. The statement that asks is in the list too.

| Column | Meaning |
|---|---|
| `query_id` | The id `CANCEL QUERY` takes. Unique across the cluster. |
| `node` | The Raft endpoint of the node that runs the statement. |
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

A row-returning statement stays in the list until its cursor ends, not until the server has
produced its rows. A query whose client reads slowly is therefore listed for as long as the client
reads, with `rows_returned` growing.

## `SHOW CONNECTIONS`

One row per TCP connection the node accepted on a client-facing listener (HTTP, HTTPS and the gRPC
client port), oldest first. The Raft listener is not tracked.

| Column | Meaning |
|---|---|
| `connection_id` | The id the `connection_id` column of `SHOW QUERIES` refers to. |
| `node` | The node that holds the connection. |
| `client_address` | The client's IP address. |
| `protocol` | `HTTP/1.1` or `HTTP/2`. A gRPC client is `HTTP/2`. |
| `kind` | `client`, or `peer` for a connection another node opened to call an `/internal/` endpoint. |
| `user_name` | The user of the last statement on the connection. NULL before the first one. |
| `opened_at` | When the connection was accepted, UTC. |
| `age_ms` | How long it has been open. |
| `idle_ms` | Time since the last statement or request on it; 0 while a statement runs on it. |
| `requests` | HTTP requests started on it. A `BatchExecute` stream is one request. |
| `active_queries` | Statements that run on it now. |
| `open_streams` | `BatchExecute` streams open on it now. The .NET client keeps a small pool of them. |
| `error` | NULL on a connection row. See [Cluster forms](#cluster-forms). |

HTTP/2 carries many calls on one connection, so one gRPC client with one channel is one row, however
many statements it runs at once.

## `CANCEL QUERY`

`CANCEL QUERY '<query_id>'` stops a running query and returns no rows. The client of the cancelled
statement receives `CADB0554` (gRPC status `CANCELLED`). Rows it read before the cancel were real.

The id names its node, so you can send the cancel to any node. A node that does not own the id
forwards it to the node that does.

| Code | Meaning |
|---|---|
| `CADB0551` | No running query has this id. It ended, the id is wrong, or it belongs to another user. |
| `CADB0552` | The statement is a write or schema change, which a cancel cannot stop, or its parse has not ended yet. |
| `CADB0553` | The node that may own the statement did not answer. The query may still run; send the cancel again. |

### What a cancel stops

**A cancel stops reads only.** A `SELECT`, a `SHOW`, an `EXPLAIN ANALYZE` stop at their next row.
An `INSERT`, `UPDATE`, `DELETE` or a schema change does not stop: once its first mutation lands it
runs to its commit or its rollback, because stopping it part way would leave its locks to expire
instead of releasing them. These statements show `cancellable = false`, and a cancel of one fails
with `CADB0552` rather than report a success that did not happen.

An `INSERT`, `UPDATE` or `DELETE` with a `RETURNING` list returns rows the way a read does, so only
its parse shows that it is a write.
For that reason, a row-returning statement accepts a cancel only after its parse shows a read.
Before that it shows `cancellable = false`, and a cancel fails with `CADB0552`. A parse usually ends
in microseconds, so send the cancel again if this occurs for a read.

**A cancel takes effect when the statement asks for its next row.** The server writes rows into the
network buffer ahead of a slow client. A client that reads very slowly first receives the rows that
are already in that buffer, then the error. The statement stays listed, with
`cancel_requested = true`, until then.

## Who sees what

- With authentication off, every caller sees every row and can cancel any query.
- A superuser sees every row and can cancel any query.
- Any other user sees only their own statements and connections, and can cancel only their own
  queries. A cancel of another user's query fails with `CADB0551`, the same as an unknown id, so the
  answer does not tell which ids other users hold.

The rows carry the literal SQL text of statements, which is why the filter is per user. The filter
runs on the node that holds the rows, so another user's text never leaves that node.

## Cluster forms

`SHOW CLUSTER QUERIES` and `SHOW CLUSTER CONNECTIONS` ask every other cluster member for its rows,
in parallel, over the node-to-node HTTP channel that the node secret authenticates.

A member that does not answer within `cluster_activity_peer_timeout_ms` does not fail the
statement. It gives one row with its `node` and the `error` column set, and every other column NULL.
A list that failed because one node is down would fail exactly when you need it.

On a standalone node, the cluster forms return the same rows as the local forms.

## Settings

```yml
query_activity_enabled: true
query_activity_max_sql_length: 1024
cluster_activity_peer_timeout_ms: 2000
```

| Setting | Default | What it does |
|---|---|---|
| `query_activity_enabled` | `true` | Lists running statements. Off: no statement is listed, and none can be cancelled with `CANCEL QUERY`. |
| `query_activity_max_sql_length` | `1024` | Characters of statement text shown per row. |
| `cluster_activity_peer_timeout_ms` | `2000` | How long the cluster forms and a forwarded cancel wait for one peer. |

All three are runtime settings: `SET CLUSTER SETTING` changes them without a restart, and a change
affects the next statement. Turning `query_activity_enabled` off does not remove statements that
are already listed; they leave when they end. `SHOW CONNECTIONS` works with the setting off, and a
connection still records the user of each statement on it.

The cost per statement is one small entry and one dictionary insert and remove. A read also gets one
linked cancellation token. The statement text is not copied; it is masked and cut only when someone
reads the list. The `kind` column comes from the parse that the statement does anyway, so a read of
the list parses no SQL.

## Limits

- The typed row APIs (`/query`, `/insert`, `/update`, `/delete` and the gRPC `CamusRows` service)
  send no SQL and are not listed.
- The span scans a node runs for another node's distributed query are not listed on the node that
  runs them. The coordinator's statement is listed.
- An explicit transaction that waits between statements holds no statement, so it does not appear in
  `SHOW QUERIES`. Its connection shows a growing `idle_ms`.
- There is no `KILL CONNECTION` yet.
