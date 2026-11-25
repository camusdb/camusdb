# Request Resource Limits

A client that can reach the API ports can send inputs that cost the client little and the node a
lot. These limits bound that cost per node. Authentication is off by default. This means that any
network client can reach the API ports. With authentication on, the limits also bound a client that
has an account.

For each limit, a value of `0` or less disables it. All of them are runtime settings. You can change
them with `SET CLUSTER SETTING`, and the change applies to the next request. A restart is not needed.

| Setting | Default | What it bounds | Refusal |
|---|---|---|---|
| `max_buffered_result_rows` | `1000000` | Rows in one buffered response | CADB0556, HTTP 400 |
| `max_buffered_result_bytes` | `268435456` (256 MiB) | Estimated memory of one buffered response | CADB0556, HTTP 400 |
| `max_statement_bytes` | `8388608` (8 MiB) | Inline SQL text that one statement may send to the parser | CADB0557, HTTP 413 |
| `max_tables_per_statement` | `64` | Relations that one statement reads, view bodies included | CADB0564, HTTP 400 |
| `max_recursive_cte_iterations` | `10000` | Steps of one `WITH RECURSIVE` query that make rows | CADB0560, HTTP 400 |
| `max_recursive_cte_rows` | `10000000` | Rows that one run of a `WITH RECURSIVE` query makes | CADB0560, HTTP 400 |
| `max_table_function_rows` | `5000000` | Rows that one read of a function in `FROM` makes | CADB0561, HTTP 400 |
| `statement_timeout_ms` | `0` (no limit) | Time that one SQL statement may take | CADB0562, HTTP 400 |
| `max_inline_definitions_per_user_per_minute` | `60` | Statements that compile inline C#, per user per minute | CADB0848, HTTP 429 |
| `max_view_definition_bytes` | `1048576` (1 MiB) | Bytes of one normalized view body, at `CREATE` | CADB0563, HTTP 400 |
| `max_view_definition_nodes` | `200000` | Syntax-tree nodes of one view body, at `CREATE` | CADB0563, HTTP 400 |
| `max_view_output_columns` | `1600` | Output columns of one view, at `CREATE` | CADB0563, HTTP 400 |
| `max_view_direct_dependencies` | `1000` | Relations one view body reads directly, at `CREATE` | CADB0563, HTTP 400 |
| `max_view_expanded_nodes` | `2000000` | Syntax-tree nodes that expanding the views of one statement makes | CADB0563, HTTP 400 |
| `max_view_expanded_relations` | `10000` | View references that expanding one statement substitutes | CADB0563, HTTP 400 |
| `max_open_explicit_transactions` | `10000` | Explicit transactions open at one time on this node | CADB0558, HTTP 503 |
| `max_client_connections` | `10000` | Client connections open at one time on this node | The connection is closed |
| `max_databases` | `10000` | Databases in the registry | CADB0408, HTTP 500 |
| `cluster_activity_max_concurrent_fan_outs` | `8` | `SHOW CLUSTER QUERIES` / `CONNECTIONS` running at one time | CADB0558, HTTP 503 |

## Recursive queries

By default a statement does not have a time limit. A `WITH RECURSIVE` query whose recursion does not
end will continue until the disk is full. Its work table spills to disk, and for this reason memory
does not stop it. The two limits stop it with **CADB0560** (`RecursiveQueryLimitExceeded`), and the
message names the setting. These two settings are cluster settings. All nodes must give the same
answer to the same statement.
See [recursive queries](common-table-expressions.md#recursive-queries).

## Views

A view names other views, and each reference is expanded into the view's body before the
statement is planned. A view graph can be shallow and very wide at the same time. For example, when each of ten views
reads the one below it twice, a read of the top view expands 1024 copies of the bottom one.
`max_view_expansion_depth` bounds nesting only and does not stop that. Without the limits in this
section, any user who may create views could make later queries of them cost unbounded CPU and
memory.

`max_view_expanded_relations` and `max_view_expanded_nodes` bound the expansion of one statement.
Each view is charged before its body is copied. This means that the statement stops with
**CADB0563** (`ViewComplexityLimitExceeded`) before the memory is spent. `CREATE VIEW` expands the
views its body reads under the same budget. A definition that exceeds it returns the error when it
is created. The four
`max_view_definition_*`, `max_view_output_columns` and `max_view_direct_dependencies` settings bound
one definition, and are checked at `CREATE` and `CREATE OR REPLACE`. The message names the setting.
See [views](views.md#configuration).

## Functions in FROM

A function in `FROM`, such as `generate_series`, makes its rows from its arguments. A table is not
read. This means that one short statement can ask for a trillion rows. Memory does not stop it. The
rows stream, and the blocking operators spill to disk. By default a statement does not have a time
limit either. `SELECT count(*) FROM generate_series(1, 1000000000000)` would take hours.

`max_table_function_rows` stops it. When the function makes the row past the limit, the statement
returns **CADB0561** (`TableFunctionRowLimitExceeded`). The message names the function and the
setting.

- The count applies to one read. When a join reads the function again, the count starts again.
- The count includes the rows that a `WHERE` filter then drops. Each of those rows costs the work
  to make it.
- If a `LIMIT` stops the read early, the read does not reach the limit. `LIMIT 10` over a series of
  a billion rows returns its 10 rows. However, a `LIMIT` above an `ORDER BY`, a `GROUP BY` or an
  aggregate does not stop the read. These operators read all rows first.
- The check happens as the rows are made. The value of a parameter argument is not known before
  then.

This setting is a cluster setting. All nodes must give the same answer to the same statement. See
[set-returning functions](set-returning-functions.md#how-it-runs).

## Statement timeout

`statement_timeout_ms` is the longest time, in milliseconds, that one SQL statement may take. A
statement that has not finished at the limit returns **CADB0562** (`StatementTimeout`; HTTP 400, gRPC
`RESOURCE_EXHAUSTED`). The message names the setting.

```sql
SET CLUSTER SETTING statement_timeout_ms = 30000;
```

The default is `0`, no limit, as in PostgreSQL: a limit fails long statements that work today, such
as a large `INSERT … SELECT` or a report. Choose a value above your longest legitimate statement. It
is a cluster setting, and a change applies to the next statement. Unlike PostgreSQL's
`statement_timeout`, it is one value for the cluster. A session or a user cannot set a different
value. This means that a client cannot raise it for itself.

The other limits bound one kind of input each. This one bounds what they do not: a statement that
reads many rows and returns few, such as `SELECT count(*) FROM a, b` over two tables of 100,000 rows,
and a statement that calls an inline C# function once for each row of a large table. The budgets of
an inline function apply to one call. Only a limit per statement can bound the whole statement (see
[C# extensions](csharp-extensions.md#budgets)).

### How a statement stops

- **A read** (`SELECT`, `SHOW`, `EXPLAIN`) stops where it is, for example in a scan or in a
  fragment that a peer node executes for it. It is the mechanism `CANCEL QUERY` uses. Rows the
  client read before the limit were real.
- **A write** (`INSERT`, `UPDATE` and `DELETE` with or without `RETURNING`, `INSERT … SELECT`,
  `INSERT … ON CONFLICT`, `REFRESH MATERIALIZED VIEW`) does not stop for a cancel once it has
  written. For this reason it checks the time itself at the following points.
  - At each row that its scans read.
  - At each row that an `UPDATE` or a `DELETE` writes.
  - At each row that the source of an `INSERT … SELECT` returns.
  - In each inline function call and trigger that it calls.

  Work between two of these points finishes first. Examples are a sort or a join that the source
  query of the write has in memory, and the batch write of the rows of an `INSERT`.
- **Inline code**: an inline function call or an inline trigger fire that is active when the time is
  up stops at the deadline. It does not continue to the end of its budget.

In all three cases the statement fails like any other failed statement. An autocommit statement is
rolled back. An explicit transaction is marked rollback-only. Later statements in it return an error,
and it can only be rolled back. The changes that the statement wrote are discarded.

### What the limit covers

- The SQL that a trigger or a procedure executes is part of the statement that fired it. It has the
  same deadline, and a statement that it starts after the deadline fails at once.
- Each automatic retry of a Serializable autocommit statement starts with a new clock.
- The commit of an autocommit statement is not part of the time. A commit that has started is never
  stopped.
- In cluster mode, the node that coordinates the statement measures the time. At the deadline it
  cancels the fragments that peers execute for it. Each fragment also carries the time the statement had left,
  and the peer stops the fragment after that time when the cancel does not reach it.
- Schema DDL, such as `CREATE INDEX` and `ALTER TABLE`, is not bounded. Its backfills and replicated
  changes are executed as jobs that a statement error cannot stop honestly.
  `CREATE TABLE … AS` is the exception. It loads its rows in the statement and drops the new table
  when the load fails. This means that it is bounded like `INSERT … SELECT`.
- Requests that are not SQL statements, such as the typed row endpoints and backups, are not bounded.

## Inline C# compile rate

`CREATE [OR REPLACE] FUNCTION ... LANGUAGE csharp` and `CREATE TRIGGER ... LANGUAGE csharp` compile
C# on the node that executes them. Each other node compiles the new code again at its next call. The
compile limits bound one compile. `max_inline_definitions_per_user_per_minute` bounds how many such
statements one user can execute per minute on a node. The statement past it returns **CADB0848**
(`InlineDefinitionRateExceeded`, HTTP 429, gRPC `RESOURCE_EXHAUSTED`) before it compiles. The
message says when the next one is allowed. Superusers are not limited, and with authentication off
nobody is. See [Compile rate](csharp-extensions.md#compile-rate).

## Buffered results

`/execute-sql-query` and `/query` build the complete result in memory, and send it only after the
last row. If there were no cap, one short statement such as `SELECT * FROM a, b` over two tables of
100,000 rows would grow the heap until the process dies. A dead process returns no error and leaves a cluster with
one node fewer.

When a result passes either cap, the node stops the read and returns **CADB0556**
(`ResultSetTooLarge`). The check is made before the commit. This means that the statement's
transaction is rolled back. This includes a write with a `RETURNING` list.

The streaming transports send one row at a time. They are not bounded by these caps.

- `/execute-sql-query-stream` (newline-delimited JSON)
- the gRPC query stream

To read a large result, use a streaming transport. You can also page the result with `LIMIT` and
`OFFSET`.

The byte estimate is the one the query result cache uses. It counts each string at two bytes per
character plus fixed costs per value and per row. For this reason it is higher than the JSON size.

## Statement size

The node measures inline SQL text in UTF-8 bytes before the parse. Text over `max_statement_bytes`
gets **CADB0557** (`StatementTooLarge`) and costs no parser time. New text always misses the parse
cache. Before this cap, the only bound was the request body limit of about 28 MB.

Bound parameter values do not count. The node does not parse them. A large bulk `INSERT` can bind
its values and leave them out of the text. A prepared statement also has the smaller limit
`max_prepared_statement_bytes`. That limit is smaller because the node stores the text of a prepared
statement. For more information, see
[prepared-statements.md](prepared-statements.md).

## Tables per statement

Planning time and memory grow with the square of the relations in one join. On one node, a join of
64 tables plans in about 5 ms, and a join of 300 tables takes about 100 ms and 30 MB. The
[nesting limit](statement-nesting-limit.md) stops one join at about 300 relations. However, a
statement can contain many joins of that size, for example in derived tables and subqueries. One
statement of a few megabytes could make a node plan for minutes.

A statement that reads more than `max_tables_per_statement` relations returns **CADB0564**
(`TooManyTablesInStatement`; HTTP 400, gRPC `INVALID_ARGUMENT`) before it is planned. The message
names the setting. No part of the statement is executed. The default, 64, is also the most relations that the planner's join
graph handles in one join.

```sql
SET CLUSTER SETTING max_tables_per_statement = 128;
```

### What counts

- Each table, materialized view and function in `FROM` counts as one each time the statement names
  it. This applies in `FROM` lists, `JOIN` clauses, `UPDATE … FROM`, `DELETE … USING` and
  subqueries. A self join counts two.
- The table that an `UPDATE` or a `DELETE` changes counts as one. The table that an `INSERT` writes
  does not count.
- A view counts as the relations of its body, nested views included. `SELECT * FROM v` over a view of
  three tables counts three.
- A derived table and a CTE reference count as the relations of their query. A `VALUES` list, or a
  derived table that does not have a `FROM`, counts as one.
- A CTE that is inlined counts at each reference. A CTE that the statement computes once counts its
  query once, and each further reference counts as one. See
  [WITH queries](common-table-expressions.md).
- Each branch of a `UNION`, `INTERSECT` or `EXCEPT` counts the relations it reads.

### When it is checked

- Each SQL statement is checked when it is executed, with its views expanded. A change of the
  setting applies to the next statement.
- `CREATE VIEW` and `CREATE MATERIALIZED VIEW` check their body with its views expanded. This means
  that you can't create a view that no statement could read. A view created before the limit was
  lowered returns the error when a statement reads it.
- `CREATE TABLE … AS`, `INSERT … SELECT` and `REFRESH MATERIALIZED VIEW` check their source query.
- A write through a view is checked again after it is rewritten onto the view's table. Each mention
  of a computed view column becomes a copy of that column's expression. A column with a subquery
  therefore counts that subquery's relations at each mention.

For a statement that reads views, this limit is usually reached long before the
[view budgets](#views): `max_view_expanded_relations` counts the view references one statement
expands, and its default of 10000 bounds expansion work. `max_tables_per_statement` counts the
relations the expanded statement reads.

It is a cluster setting. All nodes must give the same answer to the same statement.

## Explicit transactions

An explicit transaction is one that the client begins and ends itself, with `/start-transaction`,
gRPC `BeginTransaction` or `BEGIN` on a batch stream. Its state is stored on the node until it is
committed or rolled back, or until the idle reaper ends it. A begin past `max_open_explicit_transactions` gets
**CADB0558** (`ConcurrencyLimitExceeded`). Retry it after a back-off.

Autocommit statements are not counted. This cap does not apply to them.

This cap bounds memory. It does not stop one client from owning locks. A Serializable transaction
that wrote owns its range locks until it ends, up to the lifetime cap. With authentication on,
grant write access only to accounts you trust with that.

### Idle transactions

The idle reaper rolls back an explicit transaction that waits for its client for longer than
`transaction_idle_timeout_ms` (5 minutes by default). This frees the locks of a client that
disconnected, crashed or forgot to commit. A later statement in the transaction returns `Unknown
transaction`. A later rollback succeeds and does nothing.

The reaper uses the following rules.

- The idle time counts from the end of the last statement, or from the begin when there was none.
- A transaction with a running statement is not idle, however long the statement takes. A statement
  that returns rows ends when its last row is written, so a client that reads slowly keeps its
  transaction. To limit a statement, set [`statement_timeout_ms`](#statement-timeout).
- A batch operation that reached the node and waits for an execution slot is not idle.
- The request of an autocommit statement owns its transaction until it ends. The reaper takes that
  transaction only when the request failed to commit it and to roll it back.
- A client that disconnects in a row stream ends its statement, and the transaction is rolled back at
  once. A client that disconnects in a write does not stop the write. The write ends on its own, and
  the idle time starts then.
- The commit and the rollback of a statement do not stop when its client disconnects.
- A write does not start when its client left before its transaction began. A Serializable write that
  loses a conflict is not retried for a client that left.
- A client host that dies or becomes unreachable without a TCP close is found by TCP keepalive, after
  about 2 minutes without an answer. Its statements then end as for any other disconnect.
- A client that stays connected and does not read its rows keeps its transaction, as in PostgreSQL.
  Close its connection to end it. The storage layer reclaims its locks when the transaction is older
  than `max_serializable_transaction_lifetime_ms`, one hour by default.

`transaction_reaper_interval_ms` (30 seconds by default) is how often the node looks for idle
transactions. A transaction can therefore stay idle for the timeout plus one interval.

## Client connections

Each connection on the HTTP, HTTPS or gRPC listener takes one slot of `max_client_connections`.
A connection past the cap is closed as soon as it is accepted, before a request is read. The Raft
listener is not counted. This means that the cap does not apply to peer traffic. Connections that
are open when you lower the value are not closed.

## Databases

When the registry contains `max_databases` databases, `CREATE DATABASE` returns **CADB0408**
(`SchemaLimitExceeded`). A branch create and a relink return the same error. The node counts the
databases in its local view of the registry. When two nodes create a database at the same moment,
each can see the last free slot. This means that the cluster can go past the cap by the number of
nodes that create at once.

## IN lists

This is not a setting. An `IN` list of more than eight constant items is evaluated once per
statement into a hash set. A constant item is a literal or a parameter, or a cast or negation of
one. This applies in any position, at the top of a `WHERE` clause and anywhere else. Before this
change, a list in a `CASE` or a function argument was scanned again for each row. A list of N items
over M rows cost N × M comparisons.
The result is the same as the scan, including the rules for `NULL` items and mixed numeric types.

## What these limits do not cover

- There is no per-client rate limit, except for the statements that compile inline C#
  ([Compile rate](csharp-extensions.md#compile-rate)). Put a rate-limiting proxy in front of a node
  that faces an untrusted network.
- The statement timeout is off by default. Until you set `statement_timeout_ms`, a statement that
  reads many rows and returns few, such as `SELECT count(*) FROM a, b` over two tables of 100,000
  rows, uses CPU until it ends or until the client disconnects. See [Statement timeout](#statement-timeout).
- Backup and restore write outside the disk-space admission gate. With authentication off, these
  requests are allowed from loopback only.
