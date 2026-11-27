# Running queries, connections and transactions — `SHOW QUERIES`, `SHOW CONNECTIONS`, `SHOW TRANSACTIONS`, `CANCEL QUERY`, `KILL TRANSACTION`

CamusDB does not use session objects. A client sends each statement over HTTP or gRPC, and a
transaction lives across requests as a handle. The following statements show what the clients are
doing now.

```sql
SHOW QUERIES;                          -- statements that run on this node now
SHOW QUERIES LIKE '%FROM orders%';     -- the same, filtered by the statement text
SHOW CLUSTER QUERIES;                  -- the same, from every node of the cluster

SHOW CONNECTIONS;                      -- client connections this node holds open now
SHOW CLUSTER CONNECTIONS;              -- the same, from every node of the cluster

SHOW TRANSACTIONS;                     -- open explicit transactions on this node, idle ones included
SHOW CLUSTER TRANSACTIONS;             -- the same, from every node of the cluster

CANCEL QUERY 'b71a90e314-45';                   -- stop one running query, on any node
KILL TRANSACTION 'b71a90e314-t1764000000000.3'; -- roll back one open transaction, on any node
```

They are the CamusDB form of CockroachDB `SHOW CLUSTER QUERIES` / `CANCEL QUERY` and MySQL
`SHOW FULL PROCESSLIST` / `KILL QUERY`. None of the words is reserved: `queries`, `connections`,
`transactions`, `cancel`, `query` and `kill` can be used as table and column names.

All eight statements are server-level. They need no database and open no transaction. You can send
them with an empty database name.

To find and end a transaction that blocks other work, see
[Find and end a blocking transaction](#find-and-end-a-blocking-transaction).

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

## `SHOW TRANSACTIONS`

One row per open explicit transaction that this node tracks, oldest first: a transaction a client
began with `BEGIN` (`/start-transaction`, gRPC `StartTransaction`, or a `START` op on a `BatchExecute`
stream) and has not committed or rolled back yet. A transaction that waits for its client between
statements is listed too; it is the kind that `SHOW QUERIES` cannot show. The server-side transaction
of an autocommit statement is not listed: `SHOW QUERIES` lists that statement with its `transaction_id`.

| Column | Meaning |
|---|---|
| `transaction_id` | The id `KILL TRANSACTION` takes, `{node and process tag}-t{pt}.{counter}`. `{pt}.{counter}` is the handle that `SHOW QUERIES` reports as `transaction_id`. |
| `node` | The node that holds the transaction: the node its client began it on. |
| `database_name` | The database the transaction was begun in. |
| `user_name` | The user who began it. NULL when authentication is off, and, for a gRPC `StartTransaction`, until its first statement. |
| `client_address` | The client's IP address at the begin. |
| `transport` | `http`, `grpc`, `grpc-stream` or `embedded`, for the request that began it. |
| `isolation` | `ReadCommitted` or `Serializable`. |
| `locking` | `Pessimistic` or `Optimistic`. |
| `mode` | `ReadWrite` or `ReadOnly`. |
| `started_at` | When the transaction began, UTC. |
| `age_ms` | How long it has been open. |
| `idle_ms` | How long it has waited for its client since its last statement ended. 0 unless `state` is `idle`. |
| `state` | `idle`: no statement runs. `running`: a statement runs. `finalizing`: a commit, a rollback or a kill's rollback runs. `killing`: `KILL TRANSACTION` ended it, and its rollback waits for a running statement. |
| `query_id` | The `SHOW QUERIES` id of the oldest statement that runs on it now. NULL when none runs. |
| `last_sql` | The last statement it ran, with credential literals masked, cut to `query_activity_max_sql_length`. NULL before the first statement. The typed row APIs send no SQL and leave it unchanged. |
| `modified_keys` | The keys it has written so far: rows, index entries and out-of-line values. A transaction that wrote nothing holds no row lock. |
| `holds_ddl_lock` | True when a DDL statement of it holds its database's DDL lock (`transactional_ddl_enabled`). Every other DDL of that database waits until it ends. |
| `error` | NULL on a transaction row. See [Cluster forms](#cluster-forms). |

## `KILL TRANSACTION`

`KILL TRANSACTION '<transaction_id>'` rolls back one open explicit transaction and returns no rows.
Its locks are released, and a writer that waited for them proceeds. A transaction whose DDL held the
DDL lock gives up its schema changes and the lock.

The id names its node and its process, which means that you can send the kill to any node, and an id
copied before the node restarted cannot end a transaction of the restarted process.

The client of the killed transaction receives `CADB0565` (gRPC `FAILED_PRECONDITION`) on its next
statement and on its `COMMIT`, with the user and time of the kill. Its `ROLLBACK` succeeds. The client
must begin a new transaction. The node writes one Warning line to its log for each kill, naming the
transaction, its owner and who killed it.

| Code | Meaning |
|---|---|
| `CADB0566` | No open transaction has this id. The transaction may have ended or may belong to another user. The id may also be wrong. |
| `CADB0568` | The kill is in effect, and a statement of the transaction still runs. See [What a kill does to a running statement](#what-a-kill-does-to-a-running-statement). |
| `CADB0501` | A commit or a rollback of the transaction won: it committed before the kill, or its outcome is not known yet. The message says which. |
| `CADB0567` | The node that may own the transaction did not answer. The transaction may still be open; send the kill again. |

A kill of a transaction that the kill already ended succeeds again. That is how you confirm a pending
kill.

### What a kill does to a running statement

The kill ends the transaction at once: from that moment it runs no new statement and cannot commit.
The rollback itself waits until no statement of the transaction runs, because a rollback beside a
running write would race the write's own changes.

- A read stops at its next row with `CADB0565`.
- A write stops before its next write reaches storage, with `CADB0565`.
- When the statement ends, the rollback runs and the locks are released.

The kill waits for that up to half of `cluster_activity_peer_timeout_ms`. If the statement has not
ended by then, the kill returns `CADB0568` (gRPC `DEADLINE_EXCEEDED`), `SHOW TRANSACTIONS` shows the
transaction as `killing`, and the rollback still runs when the statement ends. Send the same kill
again to learn whether it ran.

A statement ends when it asks for its next row or makes its next write. A client that stops reading a
large result in the middle holds its statement, and with it the rollback, until it reads again or its
connection closes. The server notices a closed connection at once, and a client host that died
without closing it within about two minutes (TCP keepalive).

### A kill and a commit at once

The kill and the transaction's own `COMMIT` or `ROLLBACK` compete for the same claim, and exactly one
of them wins. When the commit wins, the kill reports `CADB0501` and says that the transaction
committed. When the kill wins, the commit reports `CADB0565`. A transaction whose last `COMMIT` got no
answer cannot be killed: its outcome is decided by the storage layer, and the kill reports
`CADB0501` so the client can send its `COMMIT` again to learn it.

## Find and end a blocking transaction

A table refuses writes with a lock or conflict error (`CADB0502`, `CADB0504`, `CADB0555`), or a schema
change waits, and no running statement in `SHOW QUERIES` explains it. The usual cause is an explicit
transaction that wrote to the table, or ran DDL, and then stopped sending statements.

1. List the open transactions of the whole cluster:

   ```sql
   SHOW CLUSTER TRANSACTIONS;
   ```

2. Look for a row of the table's database with `state = 'idle'`, a large `idle_ms`, and
   `modified_keys` above 0 or `holds_ddl_lock = true`. `last_sql` shows what it ran last, and
   `user_name` and `client_address` show whose it is.

3. End it:

   ```sql
   KILL TRANSACTION 'b71a90e314-t1764000000000.3';
   ```

   The blocked writer can run again at once. If the kill returns `CADB0568`, a statement of the
   transaction still runs; the rollback runs when it ends.

4. Tell the owner of the client. Their next statement on that transaction fails with `CADB0565`.

The idle reaper ends such a transaction too, but only after `transaction_idle_timeout_ms` (5 minutes
by default), and every statement of the transaction restarts that time.

## Who sees what

- With authentication off, all callers see all rows and can cancel any query and kill any
  transaction.
- A superuser sees all rows and can cancel any query and kill any transaction.
- Any other user sees only their own statements, connections and transactions, and can cancel and kill
  only their own. A cancel or kill of another user's id returns `CADB0551` or `CADB0566`, the same as an
  unknown id. This means that the answer does not tell which ids other users own.

The rows carry the literal SQL text of statements, which is why the filter is per user. The filter
is applied on the node that owns the rows, and another user's text never leaves that node.

## Cluster forms

`SHOW CLUSTER QUERIES`, `SHOW CLUSTER CONNECTIONS` and `SHOW CLUSTER TRANSACTIONS` ask the other
cluster members for their rows, in parallel, over the node-to-node HTTP channel that the node secret
authenticates. A `CANCEL QUERY` or `KILL TRANSACTION` for an id of another node goes to that node
over the same channel.

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
| `cluster_activity_peer_timeout_ms` | `2000` | How long the cluster forms, a forwarded cancel and a forwarded kill wait for one peer. A kill waits half of it for its rollback. |
| `cluster_activity_max_concurrent_fan_outs` | `8` | Maximum number of cluster forms in progress on one node at one time. `0` disables the cap. |

All four are runtime settings. A change made with `SET CLUSTER SETTING` affects the next statement,
and no restart is needed. Turning `query_activity_enabled` off does not remove statements that are
already listed; they leave when they end. With the setting off, `SHOW CONNECTIONS` works, and a
connection records the user of each statement on it.

The cost per statement is one small entry and one dictionary insert and remove. A read also gets one
linked cancellation token. The statement text is not copied; it is masked and cut at the time someone
reads the list. The `kind` column comes from the parse that the statement does anyway. A read of the
list parses no SQL.

An explicit transaction costs one small object at its begin, for `SHOW TRANSACTIONS`, and one
reference write per statement for `last_sql`. An autocommit statement pays nothing for it. A write
checks for a kill once per batch it writes. These costs do not depend on `query_activity_enabled`.

## Limits

- The typed row APIs (`/query`, `/insert`, `/update`, `/delete` and the gRPC `CamusRows` service)
  send no SQL and are not listed.
- The span scans that a node performs for another node's distributed query are not listed on that
  node. The coordinator's statement is listed.
- An explicit transaction that waits between statements is not running a statement. It does not
  appear in `SHOW QUERIES`; it appears in `SHOW TRANSACTIONS` with `state = 'idle'`.
- A conflict error names the table and the keys, but not the transaction that holds them. Find it
  with [Find and end a blocking transaction](#find-and-end-a-blocking-transaction).
- `SHOW TRANSACTIONS` does not list which tables a transaction locks or which statements wait for it.
- A kill record, which lets the killed client read `CADB0565`, is kept for one hour. After that, the
  client's next request on the old handle gets "Unknown transaction".
- There is no `KILL CONNECTION` yet.
