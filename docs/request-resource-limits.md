# Request Resource Limits

A client that can reach the API ports can send inputs that cost the client little and the node a
lot. These limits bound that cost per node. Authentication is off by default, so any network client
can reach them; with authentication on, they still bound a client that has an account.

For every limit, a value of `0` or less disables it. All of them are runtime settings:
`SET CLUSTER SETTING` changes them without a restart, and the change applies to the next request.

| Setting | Default | What it bounds | Refusal |
|---|---|---|---|
| `max_buffered_result_rows` | `1000000` | Rows in one buffered response | CADB0556, HTTP 400 |
| `max_buffered_result_bytes` | `268435456` (256 MiB) | Estimated memory of one buffered response | CADB0556, HTTP 400 |
| `max_statement_bytes` | `8388608` (8 MiB) | Inline SQL text that one statement may send to the parser | CADB0557, HTTP 413 |
| `max_open_explicit_transactions` | `10000` | Explicit transactions open at one time on this node | CADB0558, HTTP 503 |
| `max_client_connections` | `10000` | Client connections open at one time on this node | The connection is closed |
| `max_databases` | `10000` | Databases in the registry | CADB0408, HTTP 500 |
| `cluster_activity_max_concurrent_fan_outs` | `8` | `SHOW CLUSTER QUERIES` / `CONNECTIONS` running at one time | CADB0558, HTTP 503 |

## Buffered results

`/execute-sql-query` and `/query` keep the whole result in memory, and send it only after the last
row. Without a cap, one short statement such as
`SELECT * FROM generate_series(1, 1000000000)` grows the heap until the process dies. A dead process
returns no error and leaves a cluster with one node fewer.

When a result passes either cap, the node stops the read and returns **CADB0556**
(`ResultSetTooLarge`). The check runs before the commit, so the statement's transaction is rolled
back. This includes a write with a `RETURNING` list.

The streaming transports are not bounded by these caps, because they hold one row at a time:

- `/execute-sql-query-stream` (newline-delimited JSON)
- the gRPC query stream

To read a large result, use a streaming transport, or page the result with `LIMIT` and `OFFSET`.

The byte estimate is the one the query result cache uses. It counts each string at two bytes per
character plus fixed costs per value and per row, so it is higher than the JSON size.

## Statement size

The node measures inline SQL text in UTF-8 bytes before the parse. Text over `max_statement_bytes`
gets **CADB0557** (`StatementTooLarge`) and costs no parser time. New text always misses the parse
cache, so before this cap the only bound was the request body limit of about 28 MB.

Bound parameter values do not count, because the node does not parse them. A large bulk `INSERT`
can bind its values instead of putting them in the text. A prepared statement also has the smaller
limit `max_prepared_statement_bytes`, because the node keeps its text; see
[prepared-statements.md](prepared-statements.md).

## Explicit transactions

An explicit transaction is one that the client begins and ends itself: `/start-transaction`, gRPC
`BeginTransaction`, or `BEGIN` on a batch stream. It holds state on the node until a commit, a
rollback, or the idle reaper ends it. A begin past `max_open_explicit_transactions` gets
**CADB0558** (`ConcurrencyLimitExceeded`). Retry it after a back-off.

Autocommit statements are not counted, and this cap never refuses them.

This cap bounds memory. It does not stop one client from holding locks: a Serializable transaction
that wrote keeps its range locks until it ends, up to the lifetime cap. With authentication on,
grant write access only to accounts you trust with that.

## Client connections

Each connection on the HTTP, HTTPS or gRPC listener takes one slot of `max_client_connections`.
A connection past the cap is closed when the node accepts it, before it reads a request. The Raft
listener is not counted, so the cap never refuses peer traffic. Connections that are open when you
lower the value stay open.

## Databases

`CREATE DATABASE`, a branch create, and a relink are refused with **CADB0408**
(`SchemaLimitExceeded`) when the registry holds `max_databases` databases. The node counts its own
view of the registry. When two nodes create a database at the same moment, each can see the last
free slot, so the cluster can go past the cap by the number of nodes that create at once.

## IN lists

This is not a setting. An `IN` list of more than eight constant items (literals, parameters, and
casts or negations of those) is evaluated once per statement into a hash set. This applies in every
position, not only at the top of a `WHERE` clause. Before, a list inside a `CASE` or a function
argument was scanned again for each row, so a list of N items over M rows cost N × M comparisons.
The result is the same as the scan, including the rules for `NULL` items and mixed numeric types.

## What these limits do not cover

- There is no per-client rate limit. Put a rate-limiting proxy in front of a node that faces an
  untrusted network.
- There is no statement timeout. A statement that reads a large table and returns few rows, such as
  `SELECT count(*) FROM generate_series(1, 1000000000000)`, uses CPU until it ends or until the
  client disconnects.
- Backup and restore write outside the disk-space admission gate. With authentication off, the
  node accepts these requests from loopback only.
