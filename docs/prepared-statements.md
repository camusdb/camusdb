# Prepared statements

A prepared statement is registered once and executed many times with different values. The SQL text and
the parameter names do not travel again per execution. The larger saving is that the server no longer
re-parses the statement on each request to decide how to route it.

Both transports support it: gRPC over the duplex `BatchExecute` stream, and REST over
`/prepare-sql-statement` plus the normal SQL endpoints.

## What it saves

An *inline* request pays two costs before any engine work starts.

1. **Re-sending and re-deserializing the statement.** Over gRPC this is the `sql` and `database`
   strings plus one .NET string per parameter *key*, allocated fresh by the protobuf runtime for each
   request. Over REST it is the same text in the JSON body.
2. **A transport-layer SQL parse.** Both transports parse the statement to decide routing
   (whether it is a `SHOW DATABASES`, or a database-scoped statement that must not open a transaction).
   That parse does **not** go through the executor's parser cache. Its full cost is paid each time.

A prepared statement records the parsed root node and the exact string instances once. This means
that an execution pays neither cost. The following table shows the costs measured on a 5-column
parameterized INSERT (see `CamusDB.MicroBenchmarks/BENCH-RESULTS.md`).

| | Inline | Prepared |
|---|---:|---:|
| Wire bytes per execution | 198 B | 43 B |
| Transport prologue (message parse + routing parse) | 9,304 ns / 6152 B | 136 ns / 856 B |

Execution past that point is identical. A prepared execution builds the same ticket and takes the
same engine path, and isolation and retries behave the same way as for an inline statement. Cache
hints also behave the same way.

## Two lifetimes, and why they differ

|  | gRPC | REST |
|---|---|---|
| A handle belongs to | one `BatchExecute` stream | this node, and the principal that prepared it |
| Handle type | `int32` | opaque string |
| Freed by | stream teardown of any kind (normal end, cancel, fault) | explicit close, idle timeout, or process exit |
| Unknown handle means | the stream was rebuilt — prepare again | expired, another node, or another principal — prepare again |

The difference follows from the transports. A gRPC stream already answers who owns a handle and when
it ends. HTTP does not give the server a session it can trust. Connections can be pooled,
multiplexed over HTTP/2 or load-balanced. For this reason, REST supplies those answers explicitly.

- The handle can't be guessed.
- The owner is recorded at registration.
- An idle timeout is the backstop for clients that never close what they prepared.

**Handles are node-local on both transports.** They are not replicated and are valid only on the
node that minted them.

## Parameter binding is positional

`PREPARE` replies with the statement's distinct placeholder names **in binding order**, first
occurrence first, verbatim including the leading `@`. An execution sends values by ordinal: the value
at index *i* binds to the name at index *i*.

```sql
UPDATE robots SET name = @name, year = @year WHERE id = @id
-- parameterNames: ["@name", "@year", "@id"]
```

A name used more than once occupies exactly one slot. All of its occurrences resolve to that one value.
Placeholders in subqueries are included. The count you send must equal the declared count
exactly.

Names do not travel on the wire; removing them is a large part of the saving. A client that
prefers to bind by name maps its arguments onto ordinals locally, using the published names. The
.NET gRPC client does that for you: `ExecuteQueryAsync(new { id, name })` binds by property
name and sends ordinals. The match is case-insensitive, and the leading `@` is optional. A property
that matches no parameter is an error. A parameter that matches no property is also an error. This
means that a misspelling cannot become a silent NULL and a wrong answer.

## What can be prepared

You can prepare `SELECT`, `INSERT`, `UPDATE` and `DELETE` statements. You can also prepare all `SHOW …`
statements that the grammar has. This includes the node-scoped ones, such as `SHOW ENGINE STATS`,
that do not use a database. It also includes the ones that name a database, such as
`SHOW TABLES FROM db`. They behave the same way whatever database the handle was prepared in.

Schema and database/user administration cannot be prepared. Those statements are one-shot, and
several return no database descriptor. A handle gives them no benefit. `/execute-sql-ddl` and the
unary gRPC RPCs return an error for a request that carries a handle.

## gRPC

Prepared statements are available on the duplex `BatchExecute` stream only. A unary call does not
have a stream that a handle could be scoped to.

```
PREPARE (database, sql)  -> PrepareReply { statement_id, parameter_names[] }
QUERY | NON_QUERY (statement_id, positional_parameters[])
CLOSE (statement_id)     -> CloseReply
```

**Await the `PrepareReply` before sending anything that references the id** — the same contract
`START` has. Ops on a stream are executed concurrently. This means that an execution sent before its
registration is acknowledged can arrive first.

An execution that names a handle must not also carry `sql`, `database` or the named `parameters` map. A
request that sends both returns an error. There is no precedence rule.

`CLOSE` is idempotent — closing an unknown or already-closed id succeeds. Skipping it entirely is
safe too, since the stream frees everything when it ends, but a long-lived stream that prepares many
distinct statements should close what it no longer needs to stay under
`GrpcMaxPreparedStatementsPerStream`.

### .NET client

The client hides handles and streams entirely. The following example prepares and executes two
statements.

```csharp
await using CamusConnection connection = CamusConnection.Connect("https://localhost:5001");

await using CamusPreparedStatement insert = await connection.PrepareAsync(
    "productiondb", "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)");

await insert.ExecuteNonQueryAsync(["optimus", 1984L]);          // by ordinal
await insert.ExecuteNonQueryAsync(new { name = "wall-e", year = 2008L });   // or by name

await using CamusPreparedStatement select = await connection.PrepareAsync(
    "productiondb", "SELECT name FROM robots WHERE year = @year");
QueryResult result = await select.ExecuteQueryAsync([2008L]);

// Inside a transaction — the statement registers itself on the session's pinned stream.
CamusTransactionSession txn = await connection.BeginTransactionAsync("productiondb");
await txn.ExecuteNonQueryAsync(insert, ["bumblebee", 1985L]);
await txn.CommitAsync();
```

A `CamusPreparedStatement` stands for the *statement*, whichever stream executes it. The client
multiplexes autocommit work across a pool of streams. For this reason, the statement registers itself
lazily on whichever stream an execution lands on. When a stream faults and is rebuilt, its handles
are lost with it. The client detects this, because it compares the registration's transport identity
immediately before writing. It then registers again transparently. Callers do not see an
unknown-statement error.

## REST

### Prepare

```http
POST /prepare-sql-statement
{ "databaseName": "productiondb",
  "sql": "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)" }

200 { "status": "ok", "statementId": "a1b2c3.9f8e…", "parameterNames": ["@name", "@year"] }
```

Preparing parses and registers only. It performs no privilege check. Authorization is checked on
execution, against the executing request's principal, as for an inline statement. This means that
preparing reveals only whether the SQL parses. A statement that does not parse fails here, at
prepare time.

### Execute

The `/execute-sql-query`, `/execute-sql-query-stream` and `/execute-sql-non-query` endpoints take
`statementId` and `positionalParameters` in place of the SQL text.

```http
POST /execute-sql-non-query
{ "statementId": "a1b2c3.9f8e…",
  "positionalParameters": [
    { "type": 3, "strValue": "optimus" },
    { "type": 2, "longValue": 1984 }
  ] }
```

Values use the same encoding as a value of the inline `parameters` map. An existing client can reuse
its serialization unchanged. Everything else on the request — `txnIdPT`/`txnIdCounter`,
`isolationLevel`, `transactionMode`, `locking`, `causalToken` — behaves as it does inline.
The request must not contain `sql`, `databaseName` or `parameters`.

### Close

```http
POST /close-sql-statement
{ "statementId": "a1b2c3.9f8e…" }        ->  200 { "status": "ok" }
```

Idempotent. Skipping it is safe — the idle reaper collects abandoned handles — but a client that
prepares unbounded distinct SQL will eventually hit its cap.

### Handling `CADB0520`

If the node does not recognize the handle of an execution, the execution returns `CADB0520`
(`UnknownPreparedStatement`, **HTTP 404**). Treat it as routine and **prepare again, then replay the
execution once**. The error is returned in the following cases.

- the handle sat unused past `prepared_statement_idle_timeout_ms`;
- the node restarted;
- a load balancer routed the request to a different node;
- the handle belongs to a different principal. This case returns the same error on purpose. An
  ownership-specific error would confirm to a caller that a handle it does not own exists).

Behind a load balancer, either use sticky sessions or accept one re-prepare per node per statement.
After warm-up, the steady state is one registration per node. On the streaming endpoint the
handle is resolved before any bytes are written. This means that a 404 arrives as a normal JSON
error, before the stream starts.

## Limits

All are `config.yml` keys (see [configuration.md](configuration.md)) and are validated at startup. A
negative value is a startup error. It is not treated as an unbounded limit.

| Key | Default | Meaning |
|---|---:|---|
| `grpc_max_prepared_statements_per_stream` | 512 | Live statements one `BatchExecute` stream may hold. `0` = unbounded. |
| `rest_max_prepared_statements_per_principal` | 512 | Live REST statements one principal may hold on a node. |
| `rest_max_prepared_statements` | 8192 | Live REST statements a node may have across all principals. |
| `max_prepared_statement_bytes` | 65536 | Largest single statement (database + SQL + parameter names, UTF-16), either transport. |
| `grpc_max_prepared_statement_bytes_per_stream` | 8 MiB | Retained statement text per `BatchExecute` stream. |
| `rest_max_prepared_statement_bytes_per_principal` | 8 MiB | Retained statement text per REST principal. |
| `rest_max_prepared_statement_bytes` | 64 MiB | Retained statement text per node. |
| `prepared_statement_idle_timeout_ms` | 600000 | How long an unused REST statement survives. `0` disables reaping. |
| `prepared_statement_sweep_interval_ms` | 60000 | Reaper sweep interval; must be > 0. |

**Counts and bytes are both limited.** A count cap alone does not bound memory: 512 statements permits 512 ×
whatever the largest accepted request happens to be. The per-statement limit bounds the individual
term and the budgets bound the total. Together, the two bound memory. A statement over
`max_prepared_statement_bytes` returns an *invalid input* error. It is not a quota failure, because
closing other statements would not make it fit.

If a registration exceeds a cap or budget, it returns `CADB0521` (`PreparedStatementLimitExceeded`,
HTTP 429). A live handle is not evicted. Silently dropping the least recently used one would make a
correct client fail at an unpredictable later moment. The server returns the error for the new
statement and asks the caller to close what it no longer needs. Expired REST entries are reclaimed
first. This means that a caller meets a limit only if all its statements are in use. Admission is
taken atomically. Concurrent registrations cannot all slip through the same last free slot.

A `BatchExecute` stream also has a finite id space (2³¹ registrations). A stream that exhausts it is
told to open a new one. Wrapped ids would be unusable and would eventually collide.

## Related

- `docs/grpc-client-protocol.md` — the wire protocol, including `BatchExecute` framing and the retry
  taxonomy.
- `docs/grpc-dotnet-client.md` — the .NET client surface.
