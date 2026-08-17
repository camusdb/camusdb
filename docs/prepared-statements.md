# Prepared statements

A prepared statement is registered once and executed many times with different values. Neither the
SQL text nor the parameter names travel again per execution, and — the larger win — the server stops
re-parsing the statement on every request just to decide how to route it.

Both transports support it: gRPC over the duplex `BatchExecute` stream, and REST over
`/prepare-sql-statement` plus the normal SQL endpoints.

## What it saves

Every *inline* request pays two costs before any engine work starts:

1. **Re-sending and re-deserializing the statement.** Over gRPC this is the `sql` and `database`
   strings plus one .NET string per parameter *key*, allocated fresh by the protobuf runtime on every
   request. Over REST it is the same text inside the JSON body.
2. **A transport-layer SQL parse.** Both transports parse the statement to answer a routing question
   (is this a `SHOW DATABASES`? is it a database-scoped statement that must not open a transaction?),
   and that parse does **not** go through the executor's parser cache — it is paid in full, every
   time.

A prepared statement records the parsed root node and the exact string instances once, so an
execution does neither. Measured on a 5-column parameterized INSERT (see
`CamusDB.MicroBenchmarks/BENCH-RESULTS.md`):

| | Inline | Prepared |
|---|---:|---:|
| Wire bytes per execution | 198 B | 43 B |
| Transport prologue (message parse + routing parse) | 9,304 ns / 6152 B | 136 ns / 856 B |

Execution past that point is identical — a prepared execution builds the same ticket and takes the
same engine path, with the same isolation, retry, and cache-hint behavior.

## Two lifetimes, and why they differ

|  | gRPC | REST |
|---|---|---|
| A handle belongs to | one `BatchExecute` stream | this node, and the principal that prepared it |
| Handle type | `int32` | opaque string |
| Freed by | stream teardown (normal, cancel, or fault) | explicit close, idle timeout, or process exit |
| Unknown handle means | the stream was rebuilt — prepare again | expired, another node, or another principal — prepare again |

The difference is not a design preference. A gRPC stream already answers who owns a handle and when
it ends, so nothing else is needed. HTTP has no session the server can trust — connections are
pooled, HTTP/2-multiplexed, and load-balanced — so REST supplies those answers explicitly: an
unguessable handle, an owner recorded at registration, and an idle timeout as the backstop for
clients that never close what they prepared.

**Handles are node-local on both transports.** They are never replicated and never valid anywhere but
where they were minted.

## Parameter binding is positional

`PREPARE` replies with the statement's distinct placeholder names **in binding order**, first
occurrence first, verbatim including the leading `@`. An execution sends values by ordinal: the value
at index *i* binds to the name at index *i*.

```sql
UPDATE robots SET name = @name, year = @year WHERE id = @id
-- parameterNames: ["@name", "@year", "@id"]
```

A name used more than once occupies exactly one slot; every occurrence resolves to that one value.
Placeholders inside subqueries are included. The count you send must equal the declared count
exactly.

Names deliberately do not travel on the wire — removing them is much of the point. A client that
prefers to bind by name maps its own arguments onto ordinals locally, using the published names. The
.NET gRPC client does exactly that for you: `ExecuteQueryAsync(new { id, name })` binds by property
name (case-insensitively, with or without the leading `@`) and sends ordinals. A property that
matches no parameter, or a parameter with no property, is an error rather than a silent NULL — a
misspelling should not quietly become a wrong answer.

## What can be prepared

`SELECT`, `INSERT`, `UPDATE`, `DELETE`, and the `SHOW …` statements — every `SHOW` the grammar has,
including the node-scoped ones (`SHOW ENGINE STATS`, `SHOW VARIABLES`, `SHOW CLUSTER SETTINGS`) that
run against no database at all.

Schema and database/user administration cannot be prepared: those statements are one-shot, several
return no database descriptor, and nothing about them benefits from a handle. `/execute-sql-ddl` and
the unary gRPC RPCs reject a handle outright rather than ignoring it.

## gRPC

Prepared statements live on the duplex `BatchExecute` stream only — a unary call has no stream to
scope a handle to.

```
PREPARE (database, sql)  -> PrepareReply { statement_id, parameter_names[] }
QUERY | NON_QUERY (statement_id, positional_parameters[])
CLOSE (statement_id)     -> CloseReply
```

**Await the `PrepareReply` before sending anything that references the id** — the same contract
`START` has. Ops on a stream run concurrently, so an execution sent before its registration is
acknowledged may legitimately arrive first.

An execution that names a handle must not also carry `sql`, `database`, or the named `parameters`
map; sending both is refused rather than resolved by a precedence rule.

`CLOSE` is idempotent — closing an unknown or already-closed id succeeds. Skipping it entirely is
safe too, since the stream frees everything when it ends, but a long-lived stream that prepares many
distinct statements should close what it no longer needs to stay under
`GrpcMaxPreparedStatementsPerStream`.

### .NET client

The client hides handles and streams entirely:

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

A `CamusPreparedStatement` stands for the *statement*, not for one server-side handle: the client
multiplexes autocommit work across a pool of streams, so the statement registers itself lazily on
whichever stream an execution lands on. When a stream faults and is rebuilt, its handles die with it;
the client notices (it compares the registration's transport identity immediately before writing) and
registers again transparently. Callers never see an unknown-statement error.

## REST

### Prepare

```http
POST /prepare-sql-statement
{ "databaseName": "productiondb",
  "sql": "INSERT INTO robots (id, name, year) VALUES (gen_id(), @name, @year)" }

200 { "status": "ok", "statementId": "a1b2c3.9f8e…", "parameterNames": ["@name", "@year"] }
```

Preparing parses and registers only. It performs no privilege check — authorization runs on
execution, against the executing request's principal, exactly as for an inline statement — so
preparing reveals nothing beyond whether the SQL parses. A statement that does not parse fails here
rather than at some later execution.

### Execute

`statementId` + `positionalParameters` are accepted by `/execute-sql-query`,
`/execute-sql-query-stream`, and `/execute-sql-non-query`:

```http
POST /execute-sql-non-query
{ "statementId": "a1b2c3.9f8e…",
  "positionalParameters": [
    { "type": 3, "strValue": "optimus" },
    { "type": 2, "longValue": 1984 }
  ] }
```

Values use the same encoding as a value of the inline `parameters` map, so an existing client reuses
its serialization unchanged. Everything else on the request — `txnIdPT`/`txnIdCounter`,
`isolationLevel`, `transactionMode`, `locking`, `causalToken` — behaves exactly as it does inline.
`sql`, `databaseName`, and `parameters` must be absent.

### Close

```http
POST /close-sql-statement
{ "statementId": "a1b2c3.9f8e…" }        ->  200 { "status": "ok" }
```

Idempotent. Skipping it is safe — the idle reaper collects abandoned handles — but a client that
prepares unbounded distinct SQL will eventually hit its cap.

### Handling `CADB0520` — this is normal, not a bug

An execution whose handle the node does not recognize fails with `CADB0520`
(`UnknownPreparedStatement`, **HTTP 404**). Treat it as routine and **prepare again, then replay the
execution once**. It happens whenever:

- the handle sat unused past `prepared_statement_idle_timeout_ms`;
- the node restarted;
- a load balancer routed the request to a different node;
- the handle belongs to a different principal (reported identically on purpose — an
  ownership-specific error would confirm to a caller that a handle it does not own exists).

Behind a load balancer, either use sticky sessions or accept one re-prepare per node per statement;
the steady state after warm-up is still one registration per node. On the streaming endpoint the
handle is resolved before any bytes are written, so a 404 arrives as a normal JSON error rather than
mid-stream.

## Limits

All are `config.yml` keys (see [configuration.md](configuration.md)) and are validated at startup — a
negative value is a startup error, never a silently-unbounded limit.

| Key | Default | Meaning |
|---|---:|---|
| `grpc_max_prepared_statements_per_stream` | 512 | Live statements one `BatchExecute` stream may hold. `0` = unbounded. |
| `rest_max_prepared_statements_per_principal` | 512 | Live REST statements one principal may hold on a node. |
| `rest_max_prepared_statements` | 8192 | Live REST statements a node holds across all principals. |
| `max_prepared_statement_bytes` | 65536 | Largest single statement (database + SQL + parameter names, UTF-16), either transport. |
| `grpc_max_prepared_statement_bytes_per_stream` | 8 MiB | Retained statement text per `BatchExecute` stream. |
| `rest_max_prepared_statement_bytes_per_principal` | 8 MiB | Retained statement text per REST principal. |
| `rest_max_prepared_statement_bytes` | 64 MiB | Retained statement text per node. |
| `prepared_statement_idle_timeout_ms` | 600000 | How long an unused REST statement survives. `0` disables reaping. |
| `prepared_statement_sweep_interval_ms` | 60000 | Reaper sweep interval; must be > 0. |

**Why both counts and bytes.** A count cap alone does not bound memory: 512 statements permits 512 ×
whatever the largest accepted request happens to be. The per-statement limit bounds the individual
term and the budgets bound the total, so the two together mean something. A statement over
`max_prepared_statement_bytes` is rejected as *invalid input* rather than as a quota failure — no
amount of closing other statements would make it fit.

Exceeding a cap or budget fails the *registration* with `CADB0521` (`PreparedStatementLimitExceeded`,
HTTP 429). It never evicts a live handle: silently dropping the least recently used one would make a
correct client fail at an unpredictable later moment, so the server refuses the new statement and
asks the caller to close what it no longer needs. Expired REST entries are reclaimed first, so a
caller only meets a limit when its statements are genuinely all in use. Admission is taken
atomically, so concurrent registrations cannot all slip through the same last free slot.

A `BatchExecute` stream also has a finite id space (2³¹ registrations). A stream that exhausts it is
told to open a new one rather than being handed ids that would wrap into unusable — and eventually
colliding — values.

## Related

- `docs/grpc-client-protocol.md` — the wire protocol, including `BatchExecute` framing and the retry
  taxonomy.
- `docs/grpc-dotnet-client.md` — the .NET client surface.
