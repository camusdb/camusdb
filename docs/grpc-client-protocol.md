# CamusDB gRPC Client Protocol

This document specifies the CamusDB gRPC wire protocol for **client implementers**. It is
programming-language-agnostic: it describes the contract a client must honor, not any particular
language binding. If you are writing a driver, an ORM/EF-Core-style provider, or a thin RPC wrapper
in any language, this is the reference.

The single source of truth for message shapes is the Protobuf definition at
[`CamusDB.Grpc.Contracts/Protos/camus_sql.proto`](../CamusDB.Grpc.Contracts/Protos/camus_sql.proto). This document explains the
**semantics** the `.proto` cannot express: encoding rules, ordering guarantees, transaction and
causal-token threading, the error/retry model, and the duplex batching handshake. Where the two ever
disagree, the `.proto` wins — but the behavioral rules here are mandatory and are enforced
server-side.

---

## 1. Transport and services

- **Protocol:** gRPC over HTTP/2. The server exposes the gRPC endpoint on a dedicated HTTP/2 port,
  configured by `grpc_enabled` / `grpc_port` (see [configuration.md](configuration.md)). It is
  separate from the REST/JSON API; the two are functionally equivalent surfaces over the same engine.
- **Package / namespace:** the proto declares `csharp_namespace = "CamusDB.Grpc"`. Generate bindings
  from the `.proto` with your language's standard gRPC toolchain.
- **Two services**, both defined in the same proto:

  | Service     | Purpose                                                      | Mirrors REST                          |
  |-------------|-------------------------------------------------------------|---------------------------------------|
  | `CamusSql`  | Execute SQL: queries, non-queries, DDL, transaction lifecycle, and duplex batching. | `/execute-sql-query`, `/execute-sql-non-query`, `/execute-sql-ddl`, tx endpoints |
  | `CamusRows` | Typed row CRUD without composing SQL text (hot path).        | Insert / Query / Update / Delete controllers |

Pick `CamusSql` when you have SQL text (the common ORM case). Pick `CamusRows` when you want to
insert/query/update/delete by column values and filters without building a SQL string.

---

## 2. The value model — encoding every column type

All row/parameter data crosses the wire as the `Value` message, a `oneof` over twelve cases. The
enum tag numbers of `ColumnType` are **frozen** and mirror the engine's internal `ColumnType`
integers exactly — never renumber or reuse them:

| ColumnType   | Int | `Value` oneof field | Wire representation |
|--------------|-----|---------------------|---------------------|
| `NULL`       | 0   | `null_value`        | Typed-NULL sentinel enum (`NULL_VALUE_UNSET`). Presence means "NULL was explicitly sent", distinct from an absent field. |
| `ID`         | 1   | `id_value`          | 24 lowercase hex chars (ObjectId). **Distinct field from `string_value`** so `Id` and `String` never collide. |
| `INTEGER64`  | 2   | `int64_value`       | signed 64-bit int |
| `STRING`     | 3   | `string_value`      | UTF-8 string |
| `BOOL`       | 4   | `bool_value`        | bool |
| `FLOAT64`    | 5   | `float64_value`     | IEEE-754 double |
| `FLOAT32`    | 6   | `float32_value`     | IEEE-754 float |
| `BYTES`      | 7   | `bytes_value`       | raw bytes |
| `DATE`       | 8   | `date_value`        | **raw tick count**, `int64`, UTC, midnight-truncated (see below) |
| `DATETIME`   | 9   | `datetime_value`    | **raw tick count**, `int64`, UTC |
| `ARRAY`      | 10  | `array_value`       | `ArrayValue { element_type, repeated Value items }` |
| `UUID`       | 11  | `uuid_value`        | **exactly 16 bytes, big-endian** (high 8 bytes ‖ low 8 bytes) |

### Encoding rules you MUST implement precisely

1. **NULL is typed and explicit.** To send a NULL, set `null_value`. If you leave the `oneof` unset
   entirely, the server treats it as NULL as well, but always prefer sending `null_value` so
   intent is unambiguous. On decode, both "oneof unset" and `null_value` map to NULL.

2. **`Id` vs `String` are different fields.** An ObjectId goes in `id_value`, never `string_value`.
   A client that stuffs an id into `string_value` will produce a `String`-typed value and break
   primary-key matching. The id string is 24 lowercase hex characters.

3. **Date / DateTime are raw ticks, not formatted strings.** The `int64` is a .NET-style tick count
   (100-nanosecond intervals since 0001-01-01) in **UTC**. `DATE` is the same tick scale but
   truncated to midnight. This is the "compact-raw" encoding; it matches the REST positional codec.
   A client must convert its native date/time to/from this tick scale. It is **not** an ISO string
   and **not** Unix epoch seconds/millis.
   - Ticks-since-Unix-epoch conversion: `ticks = 621355968000000000 + unix_millis * 10000`
     (`621355968000000000` is the tick value of 1970-01-01 UTC). Reverse to decode.

4. **UUID is 16 big-endian bytes.** The value is two 64-bit halves — `high` then `low` — each
   written big-endian, concatenated into 16 bytes. Big-endian order equals the canonical RFC-4122
   byte order (bytes 0..15). On decode, reject any `uuid_value` whose length is not exactly 16.
   `UUID` (type 11) is distinct from `BYTES` (type 7) and from `ID` (type 1).

5. **Array carries its element type.** `ArrayValue.element_type` must be set even when `items` is
   empty, so an empty array round-trips its element type. Items are nested `Value`s of that element
   type. Arrays may nest recursively via the same rule.

6. **Float32 vs Float64 are separate cases.** Do not widen a `float32_value` into
   `float64_value` on the wire; keep the type the column declares.

> **Single mapping point.** Server-side, all of this lives in one converter
> (`GrpcValueCodec`) so REST and gRPC can never drift. Your client should likewise centralize the
> `Value` ⇄ native conversion in exactly one place and cover all twelve cases.

---

## 3. Schema-first streaming (query result contract)

Both `CamusSql.ExecuteQuery` and `CamusRows.Query` / `CamusRows.QueryById` are **server-streaming**
and follow one strict shape:

```
QueryStreamMessage(schema)     // ALWAYS first, exactly one, even for an empty result
QueryStreamMessage(row)        // zero or more, in cursor order
QueryStreamMessage(row)
...
```

Rules:

- **The schema always comes first**, as exactly one `ResultSchema { repeated ColumnSchema }`. It is
  emitted **even when the result set is empty** (zero rows). A client can therefore always read the
  column layout before deciding how to materialize rows, and an empty result is "schema, then no
  rows", never "nothing at all".
- **Rows are positional.** `ResultRow.values[i]` aligns to `ResultSchema.columns[i]`. There are no
  per-row column names; the schema defines the ordering and types once. This mirrors the REST
  positional row format.
- **Column type comes from the schema definition, not from row values.** A `NULL` in the first row
  does not weaken the column's declared type — the type in `ColumnSchema` is authoritative. Do not
  infer types from cell values.
- The stream ends by normal gRPC stream completion. (For the batched variant, a `QueryComplete`
  terminator is used instead — see §7.)

Client obligation: read and retain the first `schema` message before processing any `row`. Treat a
stream with no `schema` message as a protocol violation.

---

## 4. Transactions and the causal token

CamusDB supports two execution modes on every SQL/row call:

- **Autocommit** — the request carries **no** `txn_handle`. The server begins a short transaction,
  runs the statement, and commits it. This is the default.
- **Explicit transaction** — the request carries a `txn_handle` obtained from
  `StartTransaction`. The statement joins that transaction; it is not committed until you call
  `CommitTransaction`.

### 4.1 Explicit transaction lifecycle (`CamusSql`)

```
TxnHandle h = StartTransaction(StartTxnRequest{database, isolation_level, transaction_mode, locking})
   ... ExecuteQuery/ExecuteNonQuery/ExecuteDdl with sql_request.txn_handle = h ...
CommitTransaction(h)      -> CommitReply
   // or
RollbackTransaction(h)    -> RollbackReply
```

- `TxnHandle` is the pair `(txn_id_pt, txn_id_counter)` plus a causal token (below). It mirrors the
  HTTP session model's transaction identity.
- All statements in one logical transaction must carry the **same** handle.
- Commit and rollback are addressed by handle.

### 4.2 The causal token — HLC `(N, L, C)`, all three components are load-bearing

Every reply that advances transaction state carries a **causal token**: three fields
`causal_token_n` (int32), `causal_token_l` (int64), `causal_token_c` (int64). These are the three
components of a Hybrid Logical Clock timestamp `HLCTimestamp(N, L, C)`:

- `L` — the logical/physical time component.
- `C` — the counter component.
- `N` — the **node-id dimension**. It participates in HLC equality and is the tie-breaker in HLC
  ordering (`CompareTo`).

**All three must travel together.** A client that forwards only `L` and `C` and drops `N` produces
a *lossy* token — the server cannot correctly order operations relative to other nodes, and
read-your-writes / causal consistency can silently break. Whenever you carry a token forward, carry
`N`, `L`, and `C`.

**Threading rule (mandatory for read-your-writes across a session):**

1. Start with no token (all zero) on the first operation of a session.
2. Every reply (`NonQueryReply`, `DdlReply`, `CommitReply`, `QueryComplete`, and the `TxnHandle`
   returned by `StartTransaction`) carries a token in its `causal_token_n/_l/_c`.
3. **Feed the most recent token back** into the next request's `causal_token_n/_l/_c` (and into the
   `TxnHandle.causal_token_*` when resuming an explicit transaction).

This is how a client that talks to a multi-node cluster guarantees it observes its own prior writes.
Even in single-node use, threading the token is correct and cheap; do it unconditionally.

Where the token lives per message:
- `SqlRequest`: top-level `causal_token_n/_l/_c` (autocommit path) **and** inside `txn_handle` when
  resuming a transaction.
- `TxnHandle`: `causal_token_n/_l/_c` — set them when you resume/commit/rollback a transaction so the
  server sees the session's latest observed time.
- All `CamusRows` requests: top-level `causal_token_n/_l/_c`.

### 4.3 Isolation, mode, and locking

`SqlRequest` and `StartTxnRequest` accept three optional knobs (each has an `UNSPECIFIED = 0` value
meaning "use the server default"):

- `isolation_level` — `READ_COMMITTED` or `SERIALIZABLE`. Default: server default (currently
  Serializable-as-default; see [transactions-locking-and-isolation.md](transactions-locking-and-isolation.md)).
- `transaction_mode` — `READ_WRITE` or `READ_ONLY`.
- `locking` — `PESSIMISTIC` or `OPTIMISTIC`. Absent means the server default (Pessimistic). This
  mirrors `SET TRANSACTION LOCKING` / the REST `locking` field.

For **autocommit** `SqlRequest`s, these override the settings for the one transaction the server
begins for that statement. When a `txn_handle` resumes an existing transaction, the isolation/mode/
locking fields on the request are **ignored** — the transaction's properties were fixed at
`StartTransaction` time.

---

## 5. The `CamusSql` service

| RPC | Shape | Notes |
|-----|-------|-------|
| `ExecuteQuery(SqlRequest) → stream QueryStreamMessage` | server-stream | Schema-first (§3). Use for `SELECT`. |
| `ExecuteNonQuery(SqlRequest) → NonQueryReply` | unary | `INSERT`/`UPDATE`/`DELETE`. Reply carries `affected_rows` + causal token. |
| `ExecuteDdl(SqlRequest) → DdlReply` | unary | `CREATE`/`ALTER`/`DROP`, `CREATE DATABASE`, etc. Reply carries only a causal token. |
| `BatchExecute(stream … → stream …)` | duplex | Pipelined batching (§7). |
| `StartTransaction(StartTxnRequest) → TxnHandle` | unary | Begin explicit transaction. |
| `CommitTransaction(TxnHandle) → CommitReply` | unary | Commit by handle. |
| `RollbackTransaction(TxnHandle) → RollbackReply` | unary | Rollback by handle. |
| `Ping(PingRequest) → PingReply` | unary | Liveness / round-trip check. |

`SqlRequest.parameters` is a `map<string, Value>` for bound parameters — prefer it over string
interpolation to avoid injection and to carry typed values (dates, uuids, bytes) losslessly.

---

## 6. The `CamusRows` service (typed CRUD)

For code paths that operate on rows directly without composing SQL:

| RPC | Request | Reply | Notes |
|-----|---------|-------|-------|
| `InsertRow` | `InsertRowRequest{database, table, map values}` | `NonQueryReply` | `values` maps column name → `Value`. |
| `Query` | `RowQueryRequest{database, table, index_name, filters, order_by}` | `stream QueryStreamMessage` | Schema-first (§3). `index_name` empty = primary scan. |
| `QueryById` | `RowByIdRequest{database, table, id}` | `stream QueryStreamMessage` | Fetch by primary key. Schema-first even when the row is absent (schema, zero rows). |
| `UpdateRows` | `UpdateRowsRequest{…, values, filters}` | `NonQueryReply` | Update matching rows. |
| `UpdateById` | `UpdateByIdRequest{…, id, values}` | `NonQueryReply` | Update the row with that primary key. |
| `DeleteRows` | `DeleteRowsRequest{…, filters}` | `NonQueryReply` | Delete matching rows. |
| `DeleteById` | `RowByIdRequest{…, id}` | `NonQueryReply` | Delete by primary key. |

### Filters and ordering

- `QueryFilter { column_name, op, value }`. **`op` is a string** — `"="`, `">"`, `">="`, `"<"`,
  `"<="`, `"LIKE"`, etc. — mirroring the engine's filter operators exactly, so the surface never
  drifts from what the executor accepts. Multiple filters combine as the engine defines (AND).
- `OrderBy { column_name, direction }` where `direction` is `ORDER_ASCENDING` (0) or
  `ORDER_DESCENDING` (1).

### Primary-key resolution (important)

`QueryById` / `UpdateById` / `DeleteById` take a bare `id` string. The server resolves the **real**
primary-key column from the table's primary-key index — it does **not** assume the PK column is named
`"id"`. It also coerces the `id` string into the PK column's actual type: if the PK column is of type
`Id`, the value becomes an `Id`; otherwise it is treated as a `String`. As a client you just pass the
key's string form; the server handles column-name and type resolution.

---

## 7. Duplex batching — `BatchExecute`

`BatchExecute` pipelines many statements over **one** bidirectional stream, so an ORM/EF-Core-style
provider can avoid a unary round-trip per statement. This mirrors Kahuna's request-batching design.
Both **queries/non-queries** and the **transaction lifecycle** (`START` / `COMMIT` / `ROLLBACK`) are
batchable, so a whole unit of work — begin, statements, commit — rides one stream and many concurrent
transactions keep it busy. **DDL is not batchable** — `CREATE`/`ALTER`/`DROP` stay on the unary
`ExecuteDdl` RPC.

### 7.1 Request / response messages

```
BatchExecuteRequest  { int32 request_id, BatchStatementKind kind, SqlRequest request }
BatchExecuteResponse { int32 request_id,
                       oneof { schema | row | query_complete | non_query | error
                             | start_reply | commit_reply | rollback_reply } }
```

- `kind` selects the op:
  - `QUERY` → `ExecuteQuery`, streams schema + rows.
  - `NON_QUERY` → `ExecuteNonQuery`, single reply.
  - `START` → begin a transaction; the reply is a `TxnHandle` (`start_reply`). Reuses
    `request.database` + `isolation_level` / `transaction_mode` / `locking`; `sql` is ignored.
  - `COMMIT` / `ROLLBACK` → finalize `request.txn_handle`; the reply is `commit_reply` (causal token)
    or `rollback_reply`.
- `request` is the same `SqlRequest` used on the unary path (so it carries `txn_handle`, parameters,
  isolation/mode/locking, and causal token).

### 7.2 Correlation by `request_id`

- The client assigns a **monotonic** `request_id` to each `BatchExecuteRequest`.
- The server **echoes** that `request_id` on every response belonging to that op.
- **Responses for different ids interleave and may arrive out of order.** The client must
  demultiplex by `request_id`, routing each response to the pending operation that owns that id. Do
  not assume responses arrive in request order.

### 7.3 Per-op response sequences

Because one `BatchExecute` call carries many ops, per-op metadata that the unary path puts in gRPC
**trailers** is instead carried **in-band** (trailers are per-call, not per-op):

- **A `QUERY` op** produces, for its `request_id`:
  ```
  schema           (exactly one, first — even for an empty result)
  row              (zero or more)
  query_complete   (terminator: total row count + causal token N/L/C)
  ```
  The `QueryComplete` terminator replaces normal stream completion and carries the op's trailing
  causal token. Thread that token forward like any other (§4.2).

- **A `NON_QUERY` op** produces exactly one `non_query` (`NonQueryReply` with `affected_rows` +
  causal token) for its `request_id`.

- **A `START` op** produces one `start_reply` (a `TxnHandle`) for its `request_id`. The client awaits
  it to learn the server-minted handle, then references that handle on the transaction's later ops.

- **A `COMMIT` op** produces one `commit_reply` (causal token); a **`ROLLBACK` op** produces one
  `rollback_reply`. Thread the commit's token forward like any other (§4.2).

- **Any op that fails** produces exactly one terminal `error` (`BatchError { code, message }`) for
  its `request_id`, where `code` is the `CADBxxxx` domain code (§8). This is the in-band equivalent
  of the unary path's error trailers.

Every op therefore reaches exactly one **terminal** response: `query_complete`, `non_query`,
`start_reply`, `commit_reply`, `rollback_reply`, or `error`. The client marks the op complete on its
terminal message.

### 7.4 Ordering, concurrency, and stream routing

- **Ops sharing the same `txn_handle` execute in arrival order** — the server chains them serially
  per handle so read-your-writes within a transaction holds in the order you sent them. **This chain
  is per stream:** it only orders same-handle ops that arrive on the *same* `BatchExecute` call.
- **`START` and autocommit ops (no `txn_handle`) run concurrently**, bounded server-side by
  `grpc_batch_max_in_flight` (default 64). The server applies backpressure at that bound.
- Concurrency is why responses interleave: while one op's rows stream, another op may already be
  replying.

**Client routing contract (two regimes).** Because the chain is per stream, a client that pools
several `BatchExecute` streams must route by whether the op belongs to a transaction:
  - **Autocommit ops (no handle):** free — send on any stream in the pool (maximize concurrency).
  - **Transactional ops (a handle):** **pin every op for one handle to a single stream** (e.g. hash
    the handle to a pool slot) so they land in the same server-side chain and stay ordered. A
    transaction's `START`/statements/`COMMIT` must all use that one stream.
Combined with issuing a transaction's statements sequentially (await each before sending the next),
ordering is guaranteed. Splitting a transaction's ops across streams breaks it — the server cannot
order across streams by design (a global chain would serialize independent streams).

**Teardown = rollback.** A transaction opened by a batched `START` that the stream never `COMMIT`s or
`ROLLBACK`s is rolled back by the server when the stream ends (half-close, drop, or cancel), so a
client crash cannot orphan an open transaction. Always send an explicit `COMMIT`/`ROLLBACK`; don't
rely on teardown for a *successful* unit of work.

### 7.5 Retry within a batch — the client owns replay

The server does **not** retry a batched op internally. If an op fails with a retryable code (§8),
the server reports it as a `BatchError` for that `request_id`, and **the client decides** whether to
replay it (by issuing a fresh `BatchExecuteRequest` with a new id). Rationale: a streaming query may
have already emitted rows before conflicting, so silent server replay could corrupt the stream. This
matches the "retry only pre-first-write" contract on the unary streaming path — once output has been
written, replay is the client's call.

For a **transactional** op, a retryable failure kills the whole transaction: replay the entire unit
of work — a fresh `START` (new handle) followed by all its statements and a new `COMMIT` — not just
the one failed op. A `commit_reply` that fails with `CADB0509` (finalize unresolved) is the
exception: re-send the *same* `COMMIT` for the *same* handle (the finalize gate makes that safe).

### 7.6 Server write-serialization note (informational)

gRPC forbids concurrent writes to one stream, so the server serializes all its `WriteAsync` calls
onto the batch response stream behind a single lock while still dispatching op *execution*
concurrently. Clients don't need to do anything special for this — just be prepared for interleaved,
out-of-order responses (§7.2) and correlate by id.

---

## 8. Error model

Domain errors are surfaced two ways depending on the call shape:

- **Unary and server-streaming RPCs:** as a gRPC status error (`RpcException`/status) with the
  mapped `StatusCode`, plus **two trailing-metadata entries**:
  - `camus-error-code` — the `CADBxxxx` domain code.
  - `camus-error-message` — the human-readable message.
  No stack traces ever cross the wire. Unexpected (non-domain) failures map to `INTERNAL` with code
  `CADB0000` and a generic message.
- **Batched ops (`BatchExecute`):** as an in-band `BatchError { code, message }` on the op's
  `request_id` (§7.3), since trailers can't be per-op.

### 8.1 Status-code mapping

| gRPC `StatusCode` | Meaning | Example CamusDB codes |
|-------------------|---------|-----------------------|
| `INVALID_ARGUMENT` | Bad client input | invalid input, SQL syntax error, invalid AST, unknown column, unknown type, value too long, schema limit exceeded, NOT NULL violation, CHECK constraint violation |
| `NOT_FOUND` | Missing object | database/table/index doesn't exist, unknown key |
| `ALREADY_EXISTS` | Duplicate | duplicate unique key, duplicate primary key, database/table already exists |
| `FAILED_PRECONDITION` | Non-retryable transaction/state precondition | transaction already completed, database has live descendants |
| `ABORTED` | **Retryable** transaction family | transaction conflict (`CADB0502`), must-retry (`CADB0504`), lifetime exceeded (`CADB0505`), finalize unresolved (`CADB0509`) |
| `RESOURCE_EXHAUSTED` | Permanent for this op | mutation limit exceeded, spill storage unavailable |
| `INTERNAL` | Unexpected server error | `CADB0000` |

### 8.2 The retryable family — how to retry (read `camus-error-code`)

`ABORTED` is intentionally broad; the **`camus-error-code` trailer disambiguates** how to retry.
Never branch on the message text — branch on the code:

- **`CADB0502` / `CADB0504` / `CADB0505` → replay from a fresh `BEGIN`.** The transaction is dead;
  start a new transaction (`StartTransaction`) and re-run the statements from the beginning. On the
  autocommit path this means re-issuing the request. Use exponential backoff between attempts.
- **`CADB0509` (finalize unresolved) → retry the SAME commit/rollback on the SAME handle.** Do not
  start a new transaction; re-issue the exact commit (or rollback) against the same `TxnHandle` until
  it resolves.

For streaming queries, honor the **retry-only-pre-first-write** rule: it is safe to replay a query
only if you have not yet surfaced any of its rows to the caller. Once you have emitted rows, surface
the error rather than silently replaying (the server follows the same rule and will not replay for
you once output has begun — §7.5).

A robust client centralizes this: inspect `camus-error-code`, classify (replay-from-begin vs
retry-same-finalize vs fatal), and apply a bounded backoff loop. See
[serializable-retry-contract.md](serializable-retry-contract.md) for the engine-side contract this
mirrors.

---

## 9. Implementation checklist

Use this as an acceptance list when building a client:

**Values**
- [ ] Centralize `Value` ⇄ native conversion in one place; cover all 12 `ColumnType` cases.
- [ ] `Id` → `id_value`, never `string_value`; 24 lowercase hex.
- [ ] Date/DateTime → raw UTC ticks (100 ns since 0001-01-01); Date truncated to midnight.
- [ ] UUID → 16 big-endian bytes (high‖low); reject non-16-byte on decode.
- [ ] Array always sets `element_type`, even when empty; recurse for nested arrays.
- [ ] NULL sent as `null_value`; decode both `null_value` and unset-oneof to NULL.
- [ ] Keep `Float32`/`Float64` distinct.

**Queries**
- [ ] Always read the leading `schema` message before rows; retain it.
- [ ] Treat rows as positional against the schema; take types from the schema, not cell values.
- [ ] Handle empty results as "schema, zero rows" — never as an empty stream.

**Transactions & causal token**
- [ ] Thread `(N, L, C)` from every reply into the next request — all three components, always.
- [ ] Use the same `TxnHandle` for every statement in one transaction.
- [ ] Send isolation/mode/locking only where they take effect (start / autocommit); know they're
      ignored when resuming an existing handle.

**Batching**
- [ ] Assign monotonic `request_id`s; demultiplex responses by id; tolerate out-of-order interleave.
- [ ] Recognize the three terminals: `query_complete`, `non_query`, `error`.
- [ ] Serialize ops that share a `txn_handle` in send order; let autocommit ops pipeline.
- [ ] Own replay yourself on `BatchError` retryable codes; don't expect server retry.

**Errors**
- [ ] Read `camus-error-code` / `camus-error-message` trailers (unary/stream) and `BatchError`
      (batch); branch on the code, never the message.
- [ ] Implement the retry split: `0502/0504/0505` → new BEGIN; `0509` → retry same finalize;
      others → fatal. Bounded backoff. Respect retry-only-pre-first-write for streams.

---

## 10. Related documents

- The ready-made **.NET client** for this protocol: [grpc-dotnet-client.md](grpc-dotnet-client.md)
  (`CamusDB.Grpc.Client`). Use it if you're on .NET rather than implementing the wire contract yourself.
- Message shapes (source of truth): [`CamusDB.Grpc.Contracts/Protos/camus_sql.proto`](../CamusDB.Grpc.Contracts/Protos/camus_sql.proto)
- Data types & the compact-raw encoding: [data-types.md](data-types.md)
- Isolation, locking, and transaction modes: [transactions-locking-and-isolation.md](transactions-locking-and-isolation.md)
- The retry contract the error model mirrors: [serializable-retry-contract.md](serializable-retry-contract.md)
- Transaction coordinator internals: [kahuna-transaction-coordinator.md](kahuna-transaction-coordinator.md)
- Server configuration (`grpc_*` keys): [configuration.md](configuration.md)
