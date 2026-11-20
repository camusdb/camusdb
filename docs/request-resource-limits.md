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
| `max_tables_per_statement` | `64` | Relations that one statement reads, view bodies included | CADB0564, HTTP 400 |
| `max_recursive_cte_iterations` | `10000` | Steps of one `WITH RECURSIVE` query that make rows | CADB0560, HTTP 400 |
| `max_recursive_cte_rows` | `10000000` | Rows that one run of a `WITH RECURSIVE` query makes | CADB0560, HTTP 400 |
| `max_table_function_rows` | `5000000` | Rows that one read of a function in `FROM` makes | CADB0561, HTTP 400 |
| `statement_timeout_ms` | `0` (no limit) | Time that one SQL statement runs | CADB0562, HTTP 400 |
| `max_inline_definitions_per_user_per_minute` | `60` | Statements that compile inline C# that one user runs per minute | CADB0848, HTTP 429 |
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

A `WITH RECURSIVE` query whose recursion does not end runs until the disk is full, because a
statement has no time limit by default. Its work table spills to disk, so memory does not stop it. The two
limits stop it with **CADB0560** (`RecursiveQueryLimitExceeded`), and the message names the setting.
These two settings are cluster settings: every node must give the same answer to the same statement.
See [recursive queries](common-table-expressions.md#recursive-queries).

## Views

A view names other views, and each reference is expanded into the view's body before the
statement is planned. A view graph can be shallow and still very wide: when each of ten views
reads the one below it twice, a read of the top view expands 1024 copies of the bottom one.
`max_view_expansion_depth` bounds nesting only, so it does not stop that. Any user who may create
views could otherwise make every later query of them cost unbounded CPU and memory.

`max_view_expanded_relations` and `max_view_expanded_nodes` bound the expansion of one statement.
Each view is charged before its body is copied, so the statement stops with **CADB0563**
(`ViewComplexityLimitExceeded`) before the memory is spent. `CREATE VIEW` expands the views its body
reads under the same budget, so such a definition is refused when it is created. The four
`max_view_definition_*`, `max_view_output_columns` and `max_view_direct_dependencies` settings bound
one definition, and are checked at `CREATE` and `CREATE OR REPLACE`. The message names the setting.
See [views](views.md#configuration).

## Functions in FROM

A function in `FROM`, such as `generate_series`, makes its rows without a table, so one short
statement can ask for a trillion rows. Memory does not stop it, because the rows stream and the
blocking operators spill to disk. A statement has no time limit by default, so
`SELECT count(*) FROM generate_series(1, 1000000000000)` would run for hours.

`max_table_function_rows` stops it. The row past the limit fails the statement with **CADB0561**
(`TableFunctionRowLimitExceeded`), and the message names the function and the setting.

- The count applies to one read. When a join reads the function again, the count starts again.
- The count includes the rows that a `WHERE` filter then drops, because each row costs the work to
  make it.
- A `LIMIT` that stops the read early keeps the read under the limit. `LIMIT 10` over a series of a
  billion rows returns its 10 rows. A `LIMIT` above an `ORDER BY`, a `GROUP BY` or an aggregate
  does not stop the read, because these operators read every row first.
- The check runs while the rows are made, because the value of a parameter argument is known only
  then.

This setting is a cluster setting: every node must give the same answer to the same statement. See
[set-returning functions](set-returning-functions.md#how-it-runs).

## Statement timeout

`statement_timeout_ms` is the longest time, in milliseconds, that one SQL statement may run. A
statement still running at the limit fails with **CADB0562** (`StatementTimeout`; HTTP 400, gRPC
`RESOURCE_EXHAUSTED`), and the message names the setting.

```sql
SET CLUSTER SETTING statement_timeout_ms = 30000;
```

The default is `0`, no limit, as in PostgreSQL: a limit fails long statements that work today, such
as a large `INSERT … SELECT` or a report. Choose a value above your longest legitimate statement. It
is a cluster setting, and a change applies to the next statement. Unlike PostgreSQL's
`statement_timeout`, it is one value for the whole cluster: no session or user can set its own, so a
client cannot raise it for itself.

The other limits bound one kind of input each. This one bounds what they do not: a statement that
reads many rows and returns few, such as `SELECT count(*) FROM a, b` over two tables of 100,000 rows,
and a statement that calls an inline C# function once for each row of a large table. The budgets of
an inline function apply to one call, so only a limit per statement bounds that statement (see
[C# extensions](csharp-extensions.md#budgets)).

### How a statement stops

- **A read** (`SELECT`, `SHOW`, `EXPLAIN`) stops where it is: in a scan, a join, a sort, a lock wait,
  or a fragment that a peer node runs for it. It is the mechanism `CANCEL QUERY` uses. Rows the
  client read before the limit were real.
- **A write** (`INSERT`, `UPDATE` and `DELETE` with or without `RETURNING`, `INSERT … SELECT`,
  `INSERT … ON CONFLICT`, `REFRESH MATERIALIZED VIEW`) does not stop for a cancel once it has
  written, so it checks the time itself: at each row its scans read, at each row an `UPDATE` or a
  `DELETE` writes, at each row the source of an `INSERT … SELECT` returns, and in each inline
  function call and trigger it runs. Work between two of these points finishes first, such as a sort
  or a join that the source query of the write holds in memory, or the batch write of the rows of an
  `INSERT`.
- **Inline code**: an inline function call or an inline trigger fire that runs when the time is up
  stops at the deadline, not at the end of its own budget.

Either way the statement fails like any other failed statement. An autocommit statement is rolled
back. An explicit transaction is marked rollback-only: later statements in it are refused, and it
can only be rolled back. Nothing the statement wrote is kept.

### What the limit covers

- The SQL that a trigger or a procedure runs is part of the statement that fired it. It has the same
  deadline, and a statement that it starts after the deadline fails at once.
- Each automatic retry of a Serializable autocommit statement starts its own clock.
- The commit of an autocommit statement is not part of the time. A commit that has started is never
  stopped.
- In cluster mode, the node that coordinates the statement keeps the time. At the deadline it cancels
  the fragments that peers run for it. Each fragment also carries the time the statement had left,
  and the peer stops the fragment after that time when the cancel does not reach it.
- Schema DDL (`CREATE INDEX`, `ALTER TABLE` and the other schema changes) is not bounded: its
  backfills and replicated changes run as jobs that a statement error cannot stop honestly.
  `CREATE TABLE … AS` is the exception. It loads its rows in the statement and drops the new table
  when the load fails, so it is bounded like `INSERT … SELECT`.
- Requests that are not SQL statements, such as the typed row endpoints and backups, are not bounded.

## Inline C# compile rate

`CREATE [OR REPLACE] FUNCTION ... LANGUAGE csharp` and `CREATE TRIGGER ... LANGUAGE csharp` compile
C# on the node that runs them, and every node compiles the new code again at its next call. The
compile limits bound one compile; `max_inline_definitions_per_user_per_minute` bounds how many such
statements one user runs per minute on a node. The statement past it fails with **CADB0848**
(`InlineDefinitionRateExceeded`, HTTP 429, gRPC `RESOURCE_EXHAUSTED`) before it compiles, and the
message says when the next one can run. Superusers are not limited, and with authentication off
nobody is. See [Compile rate](csharp-extensions.md#compile-rate).

## Buffered results

`/execute-sql-query` and `/query` keep the whole result in memory, and send it only after the last
row. Without a cap, one short statement such as `SELECT * FROM a, b` over two tables of 100,000
rows grows the heap until the process dies. A dead process returns no error and leaves a cluster with
one node fewer.

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

## Tables per statement

Planning time and memory grow with the square of the relations in one join. On one node, a join of
64 tables plans in about 5 ms, and a join of 300 tables takes about 100 ms and 30 MB. The
[nesting limit](statement-nesting-limit.md) stops one join at about 300 relations, but a statement can
hold many joins of that size in derived tables, `UNION` branches and subqueries, so one statement of a
few megabytes could keep a node planning for minutes.

A statement that reads more than `max_tables_per_statement` relations fails with **CADB0564**
(`TooManyTablesInStatement`; HTTP 400, gRPC `INVALID_ARGUMENT`) before it is planned, and the message
names the setting. Nothing runs. The default, 64, is also the most relations that the planner's join
graph handles in one join.

```sql
SET CLUSTER SETTING max_tables_per_statement = 128;
```

### What counts

- Each table, materialized view and function in `FROM` counts as one each time the statement names
  it: in every `FROM` list and `JOIN`, in `UPDATE … FROM` and `DELETE … USING`, and in subqueries. A
  self join counts two.
- The table that an `UPDATE` or a `DELETE` changes counts as one. The table that an `INSERT` writes
  does not count.
- A view counts as the relations of its body, nested views included. `SELECT * FROM v` over a view of
  three tables counts three.
- A derived table and a CTE reference count as the relations of their query. A `VALUES` list, or a
  derived table with no `FROM`, counts as one.
- A CTE that is inlined counts at each reference. A CTE that the statement computes once counts its
  query once, and each further reference counts as one. See
  [WITH queries](common-table-expressions.md).
- Each branch of a `UNION`, `INTERSECT` or `EXCEPT` counts its own relations.

### When it is checked

- Every SQL statement is checked when it runs, with its views expanded. A change of the setting
  applies to the next statement.
- `CREATE VIEW` and `CREATE MATERIALIZED VIEW` check their body with its views expanded, so a view
  that no statement could read is refused when it is created. A view created before the limit was
  lowered is refused when a statement reads it.
- `CREATE TABLE … AS`, `INSERT … SELECT` and `REFRESH MATERIALIZED VIEW` check their source query.
- A write through a view is checked again after it is rewritten onto the view's table. Each mention
  of a computed view column becomes a copy of that column's expression, so a column with a subquery
  counts that subquery's relations at each mention.

For a statement that reads views, this limit is usually reached long before the
[view budgets](#views): `max_view_expanded_relations` counts the view references one statement
expands, and its default of 10000 bounds expansion work. `max_tables_per_statement` counts the
relations the expanded statement reads.

It is a cluster setting: every node must give the same answer to the same statement.

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
position: at the top of a `WHERE` clause and anywhere else. Before, a list inside a `CASE` or a function
argument was scanned again for each row, so a list of N items over M rows cost N × M comparisons.
The result is the same as the scan, including the rules for `NULL` items and mixed numeric types.

## What these limits do not cover

- There is no per-client rate limit, except for the statements that compile inline C#
  ([Compile rate](csharp-extensions.md#compile-rate)). Put a rate-limiting proxy in front of a node
  that faces an untrusted network.
- The statement timeout is off by default. Until you set `statement_timeout_ms`, a statement that
  reads many rows and returns few, such as `SELECT count(*) FROM a, b` over two tables of 100,000
  rows, uses CPU until it ends or until the client disconnects. See [Statement timeout](#statement-timeout).
- Backup and restore write outside the disk-space admission gate. With authentication off, the
  node accepts these requests from loopback only.
