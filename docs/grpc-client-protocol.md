# CamusDB gRPC Client Protocol

This document specifies the CamusDB gRPC wire protocol for **client implementers**. It describes the
contract a client must honor and does not depend on a particular language binding. If you are writing
a driver, an ORM/EF-Core-style provider, or a thin RPC wrapper in any language, this is the reference.

The single source of truth for message shapes is the Protobuf definition at
[`CamusDB.Grpc.Contracts/Protos/camus_sql.proto`](../CamusDB.Grpc.Contracts/Protos/camus_sql.proto). This document explains the
**semantics** the `.proto` cannot express: encoding rules, ordering guarantees, transaction and
causal-token threading, the error/retry model, and the duplex batching handshake. If the two
disagree, the `.proto` wins. The behavioral rules here are mandatory, and the server enforces
them.

---

## 1. Transport and services

- **Protocol:** gRPC over HTTP/2. The server exposes the gRPC endpoint on a dedicated HTTP/2 port,
  configured by `grpc_enabled` / `grpc_port` (see [configuration.md](configuration.md)). It is
  separate from the REST/JSON API; the two are functionally equivalent surfaces over the same engine.
- **Package / namespace:** the proto declares `csharp_namespace = "CamusDB.Grpc"`. Generate bindings
  from the `.proto` with your language's standard gRPC toolchain.
- **Two services** are defined in the same proto, `CamusSql` and `CamusRows`.

  | Service     | Purpose                                                      | Mirrors REST                          |
  |-------------|-------------------------------------------------------------|---------------------------------------|
  | `CamusSql`  | Execute SQL (queries, non-queries, DDL). Also covers the transaction lifecycle and duplex batching. | `/execute-sql-query`, `/execute-sql-non-query`, `/execute-sql-ddl`, tx endpoints |
  | `CamusRows` | Typed row CRUD on column values (hot path). The client sends no SQL text. | Insert / Query / Update / Delete controllers |

Pick `CamusSql` when you have SQL text (the common ORM case). Pick `CamusRows` when you want to
insert/query/update/delete by column values and filters, and you don't want to build a SQL string.

---

## 2. The value model — encoding every column type

All row/parameter data crosses the wire as the `Value` message, a `oneof` over fourteen cases. The
enum tag numbers of `ColumnType` are **frozen** and mirror the engine's internal `ColumnType`
integers exactly. Do not renumber or reuse them.

| ColumnType   | Int | `Value` oneof field | Wire representation |
|--------------|-----|---------------------|---------------------|
| `NULL`       | 0   | `null_value`        | Typed-NULL sentinel enum (`NULL_VALUE_UNSET`). Presence means "NULL was explicitly sent", distinct from an absent field. |
| `ID`         | 1   | `id_value`          | 24 lowercase hex chars (ObjectId). **Distinct field from `string_value`**. An `Id` and a `String` cannot collide. |
| `INTEGER64`  | 2   | `int64_value`       | signed 64-bit int |
| `STRING`     | 3   | `string_value`      | UTF-8 string |
| `BOOL`       | 4   | `bool_value`        | bool |
| `FLOAT64`    | 5   | `float64_value`     | IEEE-754 double; ±infinity, NaN and -0 travel as they are |
| `FLOAT32`    | 6   | `float32_value`     | IEEE-754 float; ±infinity, NaN and -0 travel as they are |
| `BYTES`      | 7   | `bytes_value`       | raw bytes |
| `DATE`       | 8   | `date_value`        | **raw tick count**, `int64`, UTC, midnight-truncated (see below) |
| `DATETIME`   | 9   | `datetime_value`    | **raw tick count**, `int64`, UTC |
| `ARRAY`      | 10  | `array_value`       | `ArrayValue { element_type, repeated Value items }` |
| `UUID`       | 11  | `uuid_value`        | **exactly 16 bytes, big-endian** (high 8 bytes ‖ low 8 bytes) |
| `NUMERIC`    | 12  | `numeric_value`     | decimal text, for example `"-12.5"` (see below) |
| `JSONB`      | 13  | `json_value`        | the document's JSON text, for example `{"a": [1, 2.50]}` (see below) |
| (interval)   | —   | `string_value`      | The wire defines no type code for an interval. A query result sends its ISO-8601 text, for example `"P1DT2H"`, in a column of type `STRING`. A client sends an interval as text: `CAST(@p AS interval)` |

### Encoding rules you MUST implement precisely

1. **NULL is typed and explicit.** To send a NULL, set `null_value`. If you leave the `oneof` unset
   entirely, the server treats it as NULL as well, but always prefer sending `null_value` so
   intent is unambiguous. On decode, both "oneof unset" and `null_value` map to NULL.

2. **`Id` vs `String` are different fields.** An ObjectId goes in `id_value` and must not go in
   `string_value`. A client that stuffs an id into `string_value` will produce a `String`-typed value
   and break primary-key matching. The id string is 24 lowercase hex characters.
   An `Id` parameter whose text is not 24 hex characters returns `CADB0400` (invalid input), and the
   message names the parameter. Upper-case hex is converted to lower case.
   A GUID is not an ObjectId. Send a GUID as `uuid_value`; do not send its text in `id_value`.
   - A `string_value` is UTF-8 on the wire. A client library changes half of a surrogate pair to
     U+FFFD before it sends the request. This means that the server stores U+FFFD and returns no
     error. Check your text before you send it. An in-process caller that binds half of a pair gets
     `CADB0400` (`INVALID_ARGUMENT`).

3. **Date / DateTime are raw ticks.** The `int64` is a .NET-style tick count
   (100-nanosecond intervals since 0001-01-01) in **UTC**. `DATE` is the same tick scale but
   truncated to midnight. This is the "compact-raw" encoding; it matches the REST positional codec.
   A client must convert its native date/time to/from this tick scale. It is **not** a formatted ISO
   string and **not** Unix epoch seconds/millis.
   - Ticks-since-Unix-epoch conversion: `ticks = 621355968000000000 + unix_millis * 10000`
     (`621355968000000000` is the tick value of 1970-01-01 UTC). Reverse to decode.

4. **UUID is 16 big-endian bytes.** The value is two 64-bit halves — `high` then `low` — each
   written big-endian, concatenated into 16 bytes. Big-endian order equals the canonical RFC-4122
   byte order (bytes 0..15). On decode, reject any `uuid_value` whose length is not exactly 16.
   `UUID` (type 11) is distinct from `BYTES` (type 7) and from `ID` (type 1).

5. **Array carries its element type.** `ArrayValue.element_type` must be set even when `items` is
   empty. This means that an empty array round-trips its element type. Items are nested `Value`s of
   that element type. Arrays may nest recursively via the same rule.

6. **NUMERIC is decimal text.** A NUMERIC has 38 digits, 9 of them after the point, and ranges from
   `-99999999999999999999999999999.999999999` to `99999999999999999999999999999.999999999`. The
   server sends the canonical form (`"1.5"`, `"-0.000000001"`, `"1200"`, `"0"`). The canonical form
   omits the exponent and trailing zeros, and it omits the decimal point for an integer value. A
   client in any language can read all 38 digits from the text. A double has about 17 digits of
   precision. Do not decode the value through a float type.
   - Input can use any decimal spelling, with a sign, trailing zeros or an exponent
     (`"+1.500"`, `"1.2e3"`). More than 9 fraction digits round half away from zero.
   - Text that is not a number returns `CADB0400` (invalid input). A value past the range returns
     `CADB0417`, which maps to the gRPC status `OUT_OF_RANGE`.
   - `NUMERIC` (type 12) is distinct from `STRING`. A number sent in `string_value` is a STRING: it
     converts when it is written into a NUMERIC column, but it does not compare with a NUMERIC column.
   - A client built before this field existed reads a NUMERIC cell as an unset `oneof`, that is, as
     NULL. Upgrade the client before a table gets a NUMERIC column.

7. **JSONB is JSON text.** A `jsonb` cell arrives in `json_value` as the PostgreSQL text form of the
   document, for example `{"a": 1, "b": [1, 2]}`. In this form, each `:` and `,` is followed by a
   space, keys are in the stored order (shorter keys first), duplicate keys are removed and numbers
   are exact: `12345678901234567890.50` is sent with all its digits and its scale. Parse it with any
   JSON reader; read numbers as decimals or text when exact values matter.
   - On input, `json_value` can carry any valid JSON text. Text that is not valid JSON, a `\u0000`
     escape, a lone surrogate or a number past the PostgreSQL limits returns `CADB0418`
     (`INVALID_ARGUMENT`). A document nested deeper than 512 levels returns `CADB0419`
     (`INVALID_ARGUMENT`).
   - A string sent in `string_value` is a STRING. It converts when it is written into a `jsonb`
     column, and it compares with a `jsonb` value as a document.
   - A client built before this field existed reads a `jsonb` cell as an unset `oneof`, that is, as
     NULL, and gets no error. Upgrade the client before a table gets a `jsonb` column.

8. **Float32 vs Float64 are separate cases.** Do not widen a `float32_value` into
   `float64_value` on the wire; keep the type the column declares.
   - Infinity, -Infinity and NaN are valid column values. A cell carries them unchanged in the IEEE
     field, and a parameter can send them. `NaN` equals `NaN` in SQL and sorts above every number. A
     client must not map NaN to NULL or 0. The HTTP JSON API writes these three values as strings.
     JSON cannot represent them as numbers (see [data-types.md](data-types.md#over-the-wire)).

> **Single mapping point.** Server-side, all of this lives in one converter (`GrpcValueCodec`) that
> REST and gRPC share. This means that the two encodings cannot drift apart. Your client should
> likewise centralize the `Value` ⇄ native conversion in exactly one place and cover all fourteen
> cases.

---

## 3. Schema-first streaming (query result contract)

Both `CamusSql.ExecuteQuery` and `CamusRows.Query` / `CamusRows.QueryById` are **server-streaming**,
and each stream sends its messages in one strict order.

```
QueryStreamMessage(schema)          // ALWAYS first, exactly one, even for an empty result
QueryStreamMessage(row)             // zero or more, in cursor order
QueryStreamMessage(row)
...
QueryStreamMessage(cache_metadata)  // at most one — only for a {cache=…}-hinted SELECT
QueryStreamMessage(routing_advice)  // at most one, strictly last — only when the request negotiated
                                    // routing metadata (routing_accept_version = 1)
```

- **The schema always comes first**, as exactly one `ResultSchema { repeated ColumnSchema }`. It is
  emitted **even when the result set is empty** (zero rows). This means that a client can read the
  column layout before it decides how to materialize rows. An empty result arrives as "schema, then
  no rows", and the stream itself is not empty.
- **Rows are positional.** `ResultRow.values[i]` aligns to `ResultSchema.columns[i]`. There are no
  per-row column names; the schema defines the ordering and types once. This mirrors the REST
  positional row format.
- **Column type comes from the schema definition.** A `NULL` in the first row does not weaken the
  column's declared type — the type in `ColumnSchema` is authoritative. Do not infer types from cell
  values.
- **A cache verdict, when present, is last.** A `SELECT` carrying a `{cache=name}` hint ends with one
  `CacheMetadata` message (`status`, `bypass_reason`, `name`, `cached_at_hlc`, `age_ms` — the same
  values as in the REST envelope). It cannot precede the rows: the server only resolves the verdict
  once the cursor has drained. An **absent** message means the statement carried no hint, which is
  distinct from a hinted statement whose verdict was `bypass`. The message is backward compatible: a
  client that ignores it reads a well-formed result.
- **Routing advice, when present, is last of all.** A request that set
  `SqlRequest.routing_accept_version = 1` may receive one trailing `RoutingAdvice` message after the
  rows and after any cache verdict. It is advisory metadata for learned client routing (see
  [sql-routing-advice.md](sql-routing-advice.md)). The query has executed normally, and a client that
  ignores the message reads a well-formed result. The message is not sent on a request that did not
  negotiate (`routing_accept_version` absent or 0, which includes any client built before routing
  advice).
- The stream ends by normal gRPC stream completion. (For the batched variant, a `QueryComplete`
  terminator is used instead — see §7.)

Client obligation: read and retain the first `schema` message before processing any `row`. If a
stream does not start with a `schema` message, treat it as a protocol violation.

---

## 4. Transactions and the causal token

CamusDB supports two execution modes for SQL and row calls.

- **Autocommit** — the request carries **no** `txn_handle`. The server executes the statement in a
  short transaction that it begins and commits. This is the default.
- **Explicit transaction** — the request carries a `txn_handle` obtained from
  `StartTransaction`. The statement joins that transaction; it is not committed until you call
  `CommitTransaction`.

### 4.1 Explicit transaction lifecycle (`CamusSql`)

```
TxnHandle h = StartTransaction(StartTxnRequest{database, isolation_level, transaction_mode, locking,
                                               causal_token_n/_l/_c})
   ... ExecuteQuery/ExecuteNonQuery/ExecuteDdl with sql_request.txn_handle = h ...
CommitTransaction(h)      -> CommitReply
   // or
RollbackTransaction(h)    -> RollbackReply
```

- `TxnHandle` is the pair `(txn_id_pt, txn_id_counter)`. It mirrors the HTTP session model's
  transaction identity. It also has `causal_token_*` fields, which the server does not read (§4.2).
- All statements in one logical transaction must carry the **same** handle.
- Commit and rollback are addressed by handle.

### 4.2 The causal token — HLC `(N, L, C)`, all three components are load-bearing

A reply that advances transaction state carries a **causal token** in three fields:
`causal_token_n` (int32), `causal_token_l` (int64), `causal_token_c` (int64). These are the three
components of a Hybrid Logical Clock timestamp, `HLCTimestamp(N, L, C)`.

- `L` — the logical/physical time component.
- `C` — the counter component.
- `N` — the **node-id dimension**. It participates in HLC equality and is the tie-breaker in HLC
  ordering (`CompareTo`).

**All three must travel together.** A client that forwards only `L` and `C` and drops `N` produces
a *lossy* token — the server cannot correctly order operations relative to other nodes, and
read-your-writes / causal consistency can silently break. When you carry a token forward, carry all
three components.

**Threading rule.** This rule is mandatory for read-your-writes across a session.

1. Start with an all-zero token on the first operation of a session.
2. The replies that end a write or a read (`NonQueryReply`, `DdlReply`, `CommitReply`,
   `QueryComplete`) carry a token in their `causal_token_n/_l/_c`. The `TxnHandle` that
   `StartTransaction` returns carries none.
3. **Feed the most recent token back** into the next request that takes one, such as an autocommit
   query or the start of a transaction (see the list below).

This is how a client of a multi-node cluster is guaranteed to observe its prior writes. Even in
single-node use, threading the token is correct and cheap; do it unconditionally.

Each request type takes the token in a specific place.

- `SqlRequest`: top-level `causal_token_n/_l/_c`. An autocommit query reads its snapshot at a
  timestamp that is at least this token. An autocommit write, a DDL statement and the statements of
  an explicit transaction ignore it. A write locks each row and reads its latest value, and a
  transaction takes its floor once, when it starts.
- `TxnHandle`: `causal_token_n/_l/_c` exist on the message, but the server does not read them and
  `StartTransaction` does not set them. To give a transaction a floor, send the token when you
  start it.
- All `CamusRows` requests: top-level `causal_token_n/_l/_c`.
- `StartTxnRequest`: top-level `causal_token_n/_l/_c`, and the `SqlRequest` of a batch `START`. Send
  the token of your last commit here. Each `SELECT` of a Read Committed transaction reads at one
  statement timestamp, taken from the clock of the node the statement is executed on, and that
  timestamp is at least this token. If you leave the token out, a transaction on a node whose clock
  is behind can miss your last commit (see `transactions-locking-and-isolation.md` §9.2).

### 4.3 Isolation, mode, and locking

`SqlRequest` and `StartTxnRequest` take three optional knobs. Each has an `UNSPECIFIED = 0` value
that means "use the server default".

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
| `ExecuteNonQuery(SqlRequest) → NonQueryReply` | unary | `INSERT`/`UPDATE`/`DELETE`. Reply carries `affected_rows` + causal token, plus `returning_schema` / `returning_rows` for a statement with a `RETURNING` list (see below). |
| `ExecuteDdl(SqlRequest) → DdlReply` | unary | `CREATE`/`ALTER`/`DROP`, `CREATE DATABASE`, etc. Reply carries only a causal token. |
| `BatchExecute(stream … → stream …)` | duplex | Pipelined batching (§7). |
| `StartTransaction(StartTxnRequest) → TxnHandle` | unary | Begin explicit transaction. |
| `CommitTransaction(TxnHandle) → CommitReply` | unary | Commit by handle. |
| `RollbackTransaction(TxnHandle) → RollbackReply` | unary | Rollback by handle. |
| `Ping(PingRequest) → PingReply` | unary | Liveness / round-trip check. |
| `GetNodeStats(NodeStatsRequest) → NodeStatsReply` | unary | Disk, CPU and memory of the node that receives the call. Needs a superuser or `MONITOR` when authentication is on. See [node-stats.md](node-stats.md). |

`SqlRequest.parameters` is a `map<string, Value>` for bound parameters — prefer it over string
interpolation to avoid injection and to carry typed values (dates, uuids, bytes) losslessly.

**`RETURNING`.** Both `ExecuteQuery` and `ExecuteNonQuery` accept an `INSERT`, `UPDATE` or `DELETE`
with a `RETURNING` list (see [returning.md](returning.md)). An `UPDATE` returns the new row. The two
RPCs deliver the returned rows in different ways.

- On `ExecuteQuery` (and a `QUERY` batch op) the rows arrive as an ordinary query stream. An autocommit
  statement uses a writable transaction, and the schema is sent only after the commit. This is also
  true for a prepared statement.
- On `ExecuteNonQuery` (and a `NON_QUERY` batch op) the reply carries `returning_schema` (field 7) and
  `returning_rows` (field 8) beside `affected_rows`. `returning_schema` is a message field: unset means
  "no RETURNING list, or the count only was asked for"; if it is set and `returning_rows` is empty,
  the statement wrote no rows. A client that ignores the two fields sees the same reply as before.
- `SqlRequest.discard_returning_rows` (field 15) asks a no-rows call for the count only. The server
  checks the list and the SELECT privilege in this case too. On `ExecuteQuery`, a request that sets
  it returns `CADB0400`.
- All rows of a no-rows reply travel in one message. A reply larger than a client receives by default
  (4 MiB) returns `CADB0550` (`RESOURCE_EXHAUSTED`) **before** the commit. This means that the
  statement changes no data. Use `ExecuteQuery` or `discard_returning_rows` for such a statement.

---

## 5b. The `CamusAuth` service (credential exchange)

This service applies only if the server has authentication enabled (`CAMUSDB_AUTH_ENABLED=true`). Its
RPCs are the **only** ones a client can call before it has a bearer token, and a client gets its token
from `Login`. For this reason they form a separate service, outside `CamusSql`.

| RPC | Request | Reply | Notes |
|-----|---------|-------|-------|
| `Login` | `LoginRequest{user, password}` | `LoginReply{token, expires_at_unix_ms, expires_in_seconds}` | Exchanges a password for a short-lived bearer token. |
| `Logout` | `LogoutRequest{}` | `LogoutReply{}` | Revokes the token in the `authorization` metadata. Idempotent. |

- **Present the token** on the other calls as `authorization: Bearer <token>` call metadata.
- **Both expiry fields describe the same instant.** `expires_at_unix_ms` is the absolute deadline (UTC
  epoch milliseconds); `expires_in_seconds` is the same deadline as a duration the *server* measured
  when replying. Prefer the duration if your clock may disagree with the server's, and renew before it
  elapses. The server's TTL is configurable, and a client that assumes a fixed lifetime will
  eventually be wrong.
- **`Login` is refused over plaintext** when the server requires TLS (`CADB0519` →
  `FAILED_PRECONDITION`), checked before the credentials are read. A loopback peer is exempt.
- **Credential failures are identical.** An unknown user, a missing password and a wrong password all
  return `UNAUTHENTICATED` with `CADB0516` and the same detail. This means that replies cannot be used
  to enumerate accounts.
- **`Logout` always succeeds**, including when the call carries no token or an already-revoked one.
  The requested end state is reached in both cases, and distinguishing them would leak whether a token
  was valid.

A token minted here is valid on any node, because sessions live in the replicated user catalog. It is
also the same token the REST `/login` route issues, and the two surfaces are interchangeable.

---

## 6. The `CamusRows` service (typed CRUD)

`CamusRows` serves code paths that operate on rows directly and do not compose SQL.

| RPC | Request | Reply | Notes |
|-----|---------|-------|-------|
| `InsertRow` | `InsertRowRequest{database, table, map values}` | `NonQueryReply` | `values` maps column name → `Value`. A column left out of `values` takes its default, as in SQL `INSERT`; see [sequences](sequences.md#identity-columns) for identity columns. |
| `Query` | `RowQueryRequest{database, table, index_name, filters, order_by}` | `stream QueryStreamMessage` | Schema-first (§3). `index_name` empty = primary scan. |
| `QueryById` | `RowByIdRequest{database, table, id}` | `stream QueryStreamMessage` | Fetch by primary key. Schema-first even when the row is absent (schema, zero rows). |
| `UpdateRows` | `UpdateRowsRequest{…, values, filters}` | `NonQueryReply` | Update matching rows. At least one filter is required (see below). |
| `UpdateById` | `UpdateByIdRequest{…, id, values}` | `NonQueryReply` | Update the row with that primary key. |
| `DeleteRows` | `DeleteRowsRequest{…, filters}` | `NonQueryReply` | Delete matching rows. At least one filter is required (see below). |
| `DeleteById` | `RowByIdRequest{…, id}` | `NonQueryReply` | Delete by primary key. |

### Filters and ordering

- `QueryFilter { column_name, op, value }`. **`op` is a string** — `"="`, `">"`, `">="`, `"<"`,
  `"<="`, `"LIKE"`, etc. — and it mirrors the engine's filter operators exactly. This means that the
  operators match what the executor supports. Multiple filters combine as the engine defines (AND).
- `OrderBy { column_name, direction }` where `direction` is `ORDER_ASCENDING` (0) or
  `ORDER_DESCENDING` (1).
- **`UpdateRows` and `DeleteRows` need at least one filter.** A request that has an empty filter list
  returns `INVALID_ARGUMENT` (`camus-error-code` = `CADB0400`, InvalidInput), and no row is changed.
  SQL has the same safety rule: an `UPDATE` or `DELETE` must have a `WHERE` clause. The rule prevents
  one request from changing or deleting every row by accident. To change all rows on purpose, use SQL
  with `WHERE TRUE`. The HTTP `/update` and `/delete` endpoints follow the same rule. In an explicit
  transaction, the refusal ends the transaction, as any failed statement does.

### Primary-key resolution (important)

`QueryById` / `UpdateById` / `DeleteById` take a bare `id` string. The server resolves the **real**
primary-key column from the table's primary-key index — it does **not** assume the PK column is named
`"id"`. It also coerces the `id` string into the PK column's actual type: if the PK column is of type
`Id`, the value becomes an `Id`; otherwise it is treated as a `String`. A client passes the
key's string form; the server handles column-name and type resolution.

---

## 7. Duplex batching — `BatchExecute`

`BatchExecute` pipelines many statements over **one** bidirectional stream. This lets an
ORM/EF-Core-style provider avoid a unary round-trip per statement. This mirrors Kahuna's
request-batching design. Both **queries/non-queries** and the **transaction lifecycle** (`START` /
`COMMIT` / `ROLLBACK`) are batchable. A complete unit of work (begin, statements, commit) can travel
on one stream, and many concurrent transactions can share it. **DDL is not batchable** —
`CREATE`/`ALTER`/`DROP` use the unary `ExecuteDdl` RPC.

### 7.1 Request / response messages

```
BatchExecuteRequest  { int32 request_id, BatchStatementKind kind, SqlRequest request }
BatchExecuteResponse { int32 request_id,
                       oneof { schema | row | query_complete | non_query | error
                             | start_reply | commit_reply | rollback_reply } }
```

(Both messages also have a **frame** form that carries several of them in one stream message — §7.8.)

- `kind` selects which operation the server performs.
  - `QUERY` → `ExecuteQuery`, streams schema + rows.
  - `NON_QUERY` → `ExecuteNonQuery`, single reply.
  - `START` → begin a transaction; the reply is a `TxnHandle` (`start_reply`). Reuses
    `request.database` + `isolation_level` / `transaction_mode` / `locking` + the causal token (the
    floor of the transaction's statement timestamps); `sql` is ignored.
  - `COMMIT` / `ROLLBACK` → finalize `request.txn_handle`; the reply is `commit_reply` (causal token)
    or `rollback_reply`.
- `request` is the same `SqlRequest` used on the unary path, with the same fields (for example
  `txn_handle` and the causal token).

### 7.2 Correlation by `request_id`

- The client assigns a **monotonic** `request_id` to each `BatchExecuteRequest`.
- The server **echoes** that `request_id` on the responses that belong to that op.
- **Responses for different ids interleave and may arrive out of order.** The client must
  demultiplex by `request_id`, routing each response to the pending operation that owns that id. Do
  not assume responses arrive in request order.

### 7.3 Per-op response sequences

One `BatchExecute` call carries many ops, and gRPC trailers are per call. For this reason, per-op
metadata that the unary path puts in gRPC **trailers** is carried **in-band** in a batch.

- **A `QUERY` op** produces three kinds of message for its `request_id`, in this order.
  ```
  schema           (exactly one, first — even for an empty result)
  row              (zero or more)
  query_complete   (terminator: total row count + causal token N/L/C, plus the cache verdict when hinted)
  ```
  The `QueryComplete` terminator replaces normal stream completion and carries the op's trailing
  causal token. Thread that token forward like any other (§4.2). For a `{cache=…}`-hinted statement it
  also carries `cache_metadata` — the batched equivalent of the trailing `CacheMetadata` message on
  the unary stream (§3), absent when the statement carried no hint. When the op negotiated routing
  metadata (`routing_accept_version = 1`) it may also carry `routing` — per-op advisory routing
  advice, correlated by `request_id` like everything else on the shared stream (see
  [sql-routing-advice.md](sql-routing-advice.md)).

- **A `NON_QUERY` op** produces exactly one `non_query` (`NonQueryReply` with `affected_rows` +
  causal token) for its `request_id`; when the op negotiated routing metadata it may also carry
  `routing`, same contract as on `QueryComplete`.

- **A `START` op** produces one `start_reply` (a `TxnHandle`) for its `request_id`. The client awaits
  it to learn the server-minted handle, then references that handle on the transaction's later ops.

- **A `COMMIT` op** produces one `commit_reply` (causal token); a **`ROLLBACK` op** produces one
  `rollback_reply`. Thread the commit's token forward like any other (§4.2).

- **Any op that fails** produces exactly one terminal `error` (`BatchError { code, message }`) for
  its `request_id`, where `code` is the `CADBxxxx` domain code (§8). This is the in-band equivalent
  of the unary path's error trailers.

An op therefore reaches exactly one **terminal** response, and the client marks the op complete on
that message. The terminal kinds are `query_complete`, `non_query`, `start_reply`, `commit_reply`,
`rollback_reply`, `prepare_reply`, `close_reply` and `error`.

### 7.4 Ordering, concurrency, and stream routing

- **Ops sharing the same `txn_handle` execute in arrival order** — the server chains them serially
  per handle. This means that read-your-writes within a transaction is guaranteed in the order you
  sent the ops. **This chain is per stream:** it only orders same-handle ops that arrive on the
  *same* `BatchExecute` call.
- **`START` and autocommit ops (no `txn_handle`) run concurrently**, bounded server-side by
  `grpc_batch_max_in_flight` (default 64). The server applies backpressure at that bound.
- Concurrency is why responses interleave: while one op's rows stream, another op may already be
  replying.

**Client routing contract (two regimes).** The chain is per stream. A client that pools several
`BatchExecute` streams must therefore route each op by whether it belongs to a transaction.
  - **Autocommit ops (no handle):** free — send on any stream in the pool (maximize concurrency).
  - **Transactional ops (a handle):** **send the ops of one handle on a single stream** (e.g. hash
    the handle to a pool slot) so they land in the same server-side chain and stay ordered. A
    transaction's `START`/statements/`COMMIT` must all use that one stream.
Combined with issuing a transaction's statements sequentially (await each before sending the next),
ordering is guaranteed. Splitting a transaction's ops across streams breaks it — the server cannot
order across streams by design (a global chain would serialize independent streams).

**Teardown = rollback.** When a stream ends (half-close, drop or cancel), the server rolls back each
transaction that a batched `START` opened on it and that the stream did not commit or roll back. This
means that a client crash cannot orphan an open transaction. Always send an explicit
`COMMIT`/`ROLLBACK`; don't rely on teardown for a *successful* unit of work.

### 7.5 Retry within a batch — the client owns replay

The server does **not** retry a batched op internally. If an op returns a retryable code (§8), the
error arrives as a `BatchError` for that `request_id`, and **the client decides** whether to replay it
(by issuing a fresh `BatchExecuteRequest` with a new id). A streaming query may have emitted rows
before the conflict, and a silent server replay could corrupt the stream. This matches the "retry
only pre-first-write" contract on the unary streaming path — once output has been written, replay is
the client's call.

For a **transactional** op, any failure ends the transaction, retryable or not. The server rolls the
transaction back when the op fails, as the unary path does. The reason is that the statement may have
staged some of its changes before it failed. A later `COMMIT` on that handle fails, and a `ROLLBACK`
succeeds as a no-op. To retry, replay the entire unit of work: a fresh `START` (new handle), then all
its statements and a new `COMMIT`. Replaying the one failed op is not enough. A `COMMIT` that returns
`CADB0509` (finalize unresolved) is the exception: re-send the *same* `COMMIT` for the *same* handle
(the finalize gate makes that safe).

### 7.6 Prepared statements — `PREPARE` / `CLOSE`

`BATCH_STATEMENT_KIND_PREPARE` registers `(SqlRequest.database, SqlRequest.sql)` on **this stream**
and replies with a `PrepareReply { statement_id, parameter_names[] }`. A later `QUERY` / `NON_QUERY`
op sets `SqlRequest.statement_id` and sends `repeated Value positional_parameters`. It leaves out
`sql`, `database` and the `parameters` map. Value *i* binds to `parameter_names[i]` (names are
verbatim, `@` included, distinct, in first-occurrence order). `BATCH_STATEMENT_KIND_CLOSE` frees one
id and is idempotent.

- **Await the `PrepareReply` before sending anything that references the id** — same contract as
  `START`. Ops run concurrently. This means that an execution sent earlier may arrive first.
- Sending `statement_id` *and* any of `sql` / `database` / `parameters` is refused
  (`CADB0400`). No precedence rule resolves it.
- `positional_parameters` is refused unless `statement_id` is set; the count must equal the declared
  parameter count exactly.
- A handle is valid **only on the stream that minted it**. After a stream is rebuilt, executions
  return `CADB0520` — prepare again and replay. Handles are not migrated to another stream.
- Only `SELECT` / `INSERT` / `UPDATE` / `DELETE` / `SHOW …` may be prepared. A DDL statement or a
  unary RPC that sets `statement_id` returns an error.

For the full semantics, the limits and the REST equivalent, see
[prepared-statements.md](prepared-statements.md).

### 7.7 The server's single response writer (informational)

gRPC forbids concurrent writes to one stream. The server dispatches op *execution* concurrently. The
ops hand their response messages to **one writer per stream** through a bounded first-in first-out
queue (`BatchResponseWriter`). A client can rely on four properties of this writer.

- The messages of one `request_id` arrive in order: `schema`, rows, terminator. Messages of different
  ids interleave (§7.2).
- The writer buffers a write while another message is waiting, and it lets the last write of a burst
  flush. This means that a burst of answers costs one socket flush. A lone answer is written and
  flushed at once; the writer does not delay a message to wait for others.
- The queue is bounded (256 entries, of which at most 4 may be 64 KiB or larger). When a client reads
  slowly, the cursor of a large result stalls, and server memory does not grow.
- A query hands its messages over in **groups**. The `schema` and the first row wait for the next
  event of their op (the second row, the terminator or a failure). Later rows go in groups that double
  from 2 rows up to 64 rows or 32 KiB, and the terminator joins the last group. A group is one queue
  entry. This means that no message of another op comes between its items. Groups let a small result
  be one stream message when frames are on. When frames are off, the group is written message by
  message, in the same order as before. The cost is one cursor step of latency on the first row. A
  query that fails after it produced rows drops the rows it has not handed over yet and answers only
  its `error`.

### 7.8 Stream frames — many ops, and many answers, in one stream message

A stream message has a fixed cost on both ends (a gRPC message frame, an HTTP/2 DATA frame, the
protobuf envelope, thread-pool hops) that does not depend on the statement. A **frame** is one stream
message that carries several complete messages. This means that the cost is paid once for the frame,
which can carry many ops or rows. A frame is a transport optimization only: **it gives no atomicity
and no ordering that the stream does not already give.**

```
BatchExecuteRequest  { kind = BATCH_STATEMENT_KIND_FRAME, repeated BatchExecuteRequest items = 4 }
BatchExecuteResponse { oneof { … | BatchResponseFrame frame = 12 } }
BatchResponseFrame   { repeated BatchExecuteResponse items = 1 }
```

Each item is a complete single message that has a `request_id`, and the server handles it exactly as
if it had arrived alone, in item order. The `request_id` of the frame itself is unused (0). `kind`
decides which field the server reads: on a `FRAME` only `items`, and on the other kinds only
`request`.

**Negotiation.** A server built before frames does not reject an unknown `kind`. It treats the op as a
`NON_QUERY`, and a frame sent to such a server would be executed as SQL. For this reason, frame
support is announced in headers, and a client does not probe for it.

- The server writes the response header `camusdb-batch-frames: 2` (the contract version) when the
  stream opens, before it reads the first request. Version 1 is frames; version 2 adds the two items
  of §7.9, which a client sends only to a server that announced 2 or higher.
- A client sends a request frame on a stream **only after it saw that header on that same stream**.
  It must not wait for the header on the operation path; until the header arrives, and for ever
  against an older server, ops travel one per message.
- The server sends a response frame **only to a peer that proved it reads them**. Two proofs exist:
  the request header `camusdb-batch-frames-accept: 1` (the contract version the client reads), sent
  by the client when it opens the stream — an older server ignores it — or a request frame on that
  stream. A single caller does not send request frames. If it leaves out the header, a lone 100-row
  read takes 102 stream messages; with the header, a lone small result is one message. After the
  proof, *any* answer may arrive in a frame, a `start_reply` included.
- Both decisions are per stream. A rotated or rebuilt stream negotiates again by itself, and a
  rolling upgrade is safe in either order.

**Limits — the sender's duty.** At most **256 items** and **1 MiB** of serialized items per frame
(item cost = 1 tag byte + length prefix + message size). The constants and both header names are in
`CamusDB.Grpc.BatchFrames`. The byte budget is far below the gRPC message limit on purpose. An
oversized message is refused by the transport *before it is parsed*, and the transport resets the
stream that the other ops share. This means that a receiver cannot refuse it item by item. A message
that exceeds the byte budget by itself travels as a single message outside a frame. Only messages
that are already waiting are packed, and a frame of one item is sent as an ordinary single message. This means
that a quiet stream is byte-identical to a stream that does not use frames.

**What the server does with a request frame.**

- Items are admitted in item order, and frames in stream order. This means that the per-handle chain
  (§7.4) sees the same sequence as it would for separate messages. `PREPARE` has to be answered before
  its id can be used (§7.6) — a frame does not change that. `START` has to be answered before its
  *handle* can be used. However, an item of the same frame can reference the `START` by its
  `request_id` (§7.9).
- Flow control and authorization stay per item: one read-buffer slot and one principal resolution
  for each item, taken one at a time. A frame larger than the free slots makes progress; it does not
  reserve its slots up front.
- An item that fails, is refused or is cancelled gets an `error` on its `request_id`, and its
  neighbours are not affected.
- Items past the 256-item limit are **not run** and each is answered with the retryable `CADB0504`
  (no data was written; send it again). Note that a refused `COMMIT` or `ROLLBACK` leaves its
  transaction open, like any op that did not run. A conforming client does not exceed the limit and
  does not meet this case.
- An empty frame starts no operation. A nested frame (a frame within a frame) is not run. The ops
  directly in it are refused with `CADB0400`, each on its `request_id`, which means that no caller
  waits. The nested frame gets no answer. A client drops a nested frame that it finds in a response
  frame.
- A frame is not resent. A frame whose write failed may or may not have reached the server, and the
  same is true of the items in it. Each item faults with the transport error, and the per-op retry
  contract (§7.5) decides.

**Invariants a maintainer must keep.**

1. Do not send a frame on a stream whose server did not announce support, and do not block on the
   header.
2. Do not send a response frame to a peer that gave no proof (accept header or request frame).
3. Admit items in order, one flow-control slot at a time, one authority check each.
4. Keep the messages of one `request_id` in order.
5. Do not resend a frame.
6. Do not delay an operation or a response to fill a frame.
7. On the sending side, a frame must respect both limits.
8. Keep the two copies of `camus_sql.proto` identical: `CamusDB.Grpc.Contracts/Protos/` in this
   repository and `CamusDB.Client/Protos/` in the `camusdb-dotnet` repository. The frame constants
   have the same two homes (`BatchFrames` in both).
9. Do not write `txn_start_ref` or `COMMIT_IF_OK` to a stream whose server announced a version below
   2 (§7.9).

The in-repo client `CamusDB.Grpc.Client` does not send frames. It therefore does not receive them,
and it works unchanged against a server that supports them.

No CamusDB measurement of frames exists yet. The same change in Kahuna's key/value stream was
measured on one machine at roughly 2–4× throughput for cheap requests at concurrency 64, and
throughput did not change at concurrency 1. A SQL statement does far more work per request. Expect a
smaller gain, largest on point reads and small multi-row reads.

### 7.9 A transaction in one frame — `txn_start_ref` and `COMMIT_IF_OK` (contract version 2)

A short transaction costs one exchange per op when the client awaits each answer: six for a bank
transfer (`START`, two reads, two writes, `COMMIT`). At high concurrency the exchanges cost more
latency than the statements. Two additions let a client send a complete transaction, or each
dependent half of one, as a single frame. Neither changes what a frame is. Items get no atomicity
from the frame, ops answer on their `request_id`, and a failed statement ends its transaction (§7.5).

```
BatchExecuteRequest { …, int32 txn_start_ref = 5 }      // items of a frame only
BatchStatementKind.BATCH_STATEMENT_KIND_COMMIT_IF_OK = 9
```

**Naming the `START` of the same frame.** An item may set `txn_start_ref` to the `request_id` of a
`START` *earlier in the same frame*. The item uses this field because it cannot know the
`txn_handle` yet. The server admits the `START` as the head of a chain, and the items that name it
follow in item order. This means that the statements run after the `START`, on the handle it minted.
Ops that use the handle later join the same chain. The `START` answers its `start_reply` as always,
and the client uses that handle for the later frames of the transaction. A reference that breaks one
of these rules returns `CADB0400`, and the op is not run.

- the reference must point backwards, at a `START` of the same frame. It may not point forwards, at
  another op kind or at a `START` of an earlier frame;
- it may not be combined with `request.txn_handle`;
- a `START`, `PREPARE` or `CLOSE` may not carry one;
- outside a frame the field must be 0.

An op with a refused reference is not run. Running it as an autocommit statement would be a silent
write outside the transaction the client meant. When the `START` fails, the items that name it are
not run, and each one is answered with the code and message of the `START`. The client sees the same
failure on `START` and on the statements, as it would on a deferred `BEGIN`.

**Conditional commit.** `COMMIT_IF_OK` carries a handle or a `txn_start_ref` like `COMMIT`, and is
chained after the transaction's earlier ops like `COMMIT`. If no earlier op of that transaction on
this stream failed, it commits and answers `commit_reply`. If one did, it does not try to commit: it
makes sure the transaction is rolled back (the failed statement already did that; the rollback here
is the idempotent backstop) and answers an `error` that carries the **first** failure's code and
message. "Earlier" means earlier on the transaction, including ops in other frames. A `COMMIT_IF_OK`
sent alone after a failed statement answers that failure too. The echo is deliberate. A client that
awaits only its commit learns the cause, and its retry taxonomy sees a conflict code as a conflict.
An op that was *refused* (past the item limit, §7.8) did not run and does not count as a failure of
the transaction. The client decides what to do with a transaction it left open that way.

**A transfer in two frames, or one.** A client that needs the values it reads sends
`[START, SELECT a (ref), SELECT b (ref)]`, then `[UPDATE a, UPDATE b, COMMIT_IF_OK]` by handle: two
exchanges. A client whose writes do not depend on a read (`SET balance = balance + @d`) sends
`[START, UPDATE a (ref), UPDATE b (ref), COMMIT_IF_OK (ref)]`: one exchange. On either shape each
statement's answer arrives as before, and the commit's answer says whether the transaction stored
them.

**Negotiation.** Both items are contract version 2. A version 1 server ignores `txn_start_ref`, which
turns the op into an autocommit statement. It treats `COMMIT_IF_OK` as a `NON_QUERY`, which leaves the
transaction open. For this reason, a client writes either one only on a stream whose header announced
`2` or higher. On other streams it uses one exchange per step (`START` awaited, statements, then
`COMMIT`), which is exactly the pre-version-2 behavior. The in-repo client `CamusDB.Grpc.Client`
sends neither.

**Metrics.** `camus_request_count_total` counts each op in a frame as one request. This means that the
counter cannot show the gain. `camus_batch_request_frames_total` counts the request frames a stream
read and `camus_batch_request_frame_items` is the histogram of ops per frame; a pipelined transfer is
two frames of three ops.

---

## 8. Error model

Domain errors are surfaced in one of two ways, depending on the call shape.

- **Unary and server-streaming RPCs:** as a gRPC status error (`RpcException`/status) with the
  mapped `StatusCode`, plus **two trailing-metadata entries**, `camus-error-code` and
  `camus-error-message`.
  - `camus-error-code` — the `CADBxxxx` domain code.
  - `camus-error-message` — the human-readable message.
  No stack traces ever cross the wire. Unexpected (non-domain) failures map to `INTERNAL` with code
  `CADB0000` and a generic message.
- **Batched ops (`BatchExecute`):** as an in-band `BatchError { code, message }` on the op's
  `request_id` (§7.3), since trailers can't be per-op.

### 8.1 Status-code mapping

| gRPC `StatusCode` | Meaning | Example CamusDB codes |
|-------------------|---------|-----------------------|
| `INVALID_ARGUMENT` | Bad client input | invalid input, SQL syntax error, invalid AST, unknown column, unknown type, value too long, schema limit exceeded, statement too deeply nested (`CADB0413`), invalid JSON text or a document nested too deep (`CADB0418`, `CADB0419`), a NOT NULL violation, a CHECK constraint violation, a C# extension function, trigger or procedure that threw (`CADB0809`, `CADB0818`, `CADB0822`), trigger DDL refusals (`CADB0811`-`CADB0813`, `CADB0816`), extension nesting too deep (`CADB0819`), a statement that an extension's SQL session does not allow (`CADB0820`), an extension that caught an error from the SQL it issued (`CADB0823`), an extension or version this node does not load (`CADB0828`), a required extension that is not installed (`CADB0831`) |
| `OUT_OF_RANGE` | A value outside its type's range | NUMERIC out of range (`CADB0417`), INT64 out of range (`CADB0420`), float overflow or underflow (`CADB0421`), a NUMERIC that does not fit an extension's `decimal` (`CADB0810`) |
| `NOT_FOUND` | Missing object | database/table/index doesn't exist, unknown key, no running query with the id given to `CANCEL QUERY` (`CADB0551`), no open transaction with the id given to `KILL TRANSACTION` (`CADB0566`), unknown trigger on `DROP TRIGGER` (`CADB0815`), unknown procedure on `CALL` (`CADB0821`), extension not installed (`CADB0830`) |
| `ALREADY_EXISTS` | Duplicate | duplicate unique key, duplicate primary key, database/table already exists, trigger already exists (`CADB0814`), extension already installed (`CADB0829`) |
| `FAILED_PRECONDITION` | Non-retryable transaction/state precondition | transaction already completed, database has live descendants, `CANCEL QUERY` of a write (`CADB0552`), a statement or commit in a transaction that `KILL TRANSACTION` ended (`CADB0565`; a `ROLLBACK` of it succeeds), a commit of a rollback-only transaction (`CADB0824`), a write in a read-only transaction (`CADB0849`), a trigger's SQL changing a row its statement still holds (`CADB0825`), an extension name conflict, dependents or a node that does not have the build (`CADB0832`, `CADB0833`, `CADB0835`) |
| `CANCELLED` | `CANCEL QUERY` stopped the statement | query cancelled (`CADB0554`) |
| `DEADLINE_EXCEEDED` | `KILL TRANSACTION` is in effect, and its rollback has not ended yet | kill pending (`CADB0568`); the transaction runs no further statement and cannot commit. The rollback waits for a running statement, or is in progress, and releases the locks when it ends. Send the same kill again to learn how it ended |
| `UNAVAILABLE` | The node that may own a statement or a transaction did not answer a cancel or a kill, or this node cannot run the statement | query owner unreachable (`CADB0553`); the cancel can be sent again. Transaction owner unreachable (`CADB0567`); the transaction may still be open, and the kill can be sent again. A statement uses an extension the database installed and this node does not load in that build (`CADB0834`), or a trigger's type that the installed version does not define (`CADB0817`); a node that deploys the right file can run it |
| `ABORTED` | **Retryable** transaction family | transaction conflict (`CADB0502`), must-retry (`CADB0504`), lifetime exceeded (`CADB0505`), finalize unresolved (`CADB0509`); also `SELECT … FOR UPDATE NOWAIT` on a row another transaction has locked (`CADB0555`), which ends the transaction but is **not** retried for you (§8.2) |
| `RESOURCE_EXHAUSTED` | Permanent for this op | mutation limit exceeded, spill storage unavailable, AFTER-trigger rows over the byte budget (`CADB0826`), statement timeout (`CADB0562`), inline C# compile rate (`CADB0848`) |
| `INTERNAL` | Unexpected server error | `CADB0000`; the rollback of a transaction that `KILL TRANSACTION` ended failed (`CADB0569`), and the storage node releases its locks when its session expires |

### 8.2 The retryable family — how to retry (read `camus-error-code`)

`ABORTED` is intentionally broad; the **`camus-error-code` trailer disambiguates** how to retry.
Make the retry decision from the code alone, and do not parse the message text.

- **`CADB0502` / `CADB0504` / `CADB0505` → replay from a fresh `BEGIN`.** The transaction is dead;
  start a new transaction (`StartTransaction`) and re-run the statements from the beginning. On the
  autocommit path this means re-issuing the request. Use exponential backoff between attempts.
- **`CADB0555` (lock not available) → do not replay automatically.** Only `SELECT … FOR UPDATE
  NOWAIT` raises it, when another transaction has locked a row the statement wants. The transaction is
  dead, as for `CADB0502`. However, the caller asked not to wait, and a retry loop must not hide the
  error. Let the application decide (for example, pick other work). See
  [select-for-update.md](select-for-update.md).
- **`CADB0509` (finalize unresolved) → retry the SAME commit/rollback on the SAME handle.** Do not
  start a new transaction; re-issue the exact commit (or rollback) against the same `TxnHandle` until
  it resolves.

For streaming queries, honor the **retry-only-pre-first-write** rule: it is safe to replay a query
only if you have not yet surfaced any of its rows to the caller. Once you have emitted rows, surface
the error and do not replay (the server follows the same rule and will not replay for
you once output has begun — §7.5).

A robust client centralizes this: inspect `camus-error-code`, classify (replay-from-begin vs
retry-same-finalize vs fatal), and apply a bounded backoff loop. See
[serializable-retry-contract.md](serializable-retry-contract.md) for the engine-side contract this
mirrors.

---

## 9. Implementation checklist

Use this as an acceptance list when building a client.

**Values**
- [ ] Centralize `Value` ⇄ native conversion in one place; cover all fourteen `Value` cases.
- [ ] `Id` → `id_value` (not `string_value`); 24 lowercase hex.
- [ ] Date/DateTime → raw UTC ticks (100 ns since 0001-01-01); Date truncated to midnight.
- [ ] UUID → 16 big-endian bytes (high‖low); reject non-16-byte on decode.
- [ ] Array always sets `element_type`, even when empty; recurse for nested arrays.
- [ ] NULL sent as `null_value`; decode both `null_value` and unset-oneof to NULL.
- [ ] Keep `Float32`/`Float64` distinct.

**Queries**
- [ ] Always read the leading `schema` message before rows; retain it.
- [ ] Treat rows as positional against the schema; take types from the schema and do not infer them
      from cell values.
- [ ] Handle an empty result as "schema, zero rows". An empty stream is a protocol violation.

**Transactions & causal token**
- [ ] Thread `(N, L, C)` from the latest reply into the next request that takes one (autocommit
      query, `CamusRows`, transaction start), with all three components.
- [ ] Send the same `TxnHandle` on the statements of one transaction.
- [ ] Send isolation/mode/locking only where they take effect (start / autocommit); know they're
      ignored when resuming an existing handle.

**Batching**
- [ ] Assign monotonic `request_id`s; demultiplex responses by id; tolerate out-of-order interleave.
- [ ] Recognize the terminals: `query_complete`, `non_query`, `start_reply`, `commit_reply`,
      `rollback_reply`, `prepare_reply`, `close_reply`, `error`.
- [ ] Serialize ops that share a `txn_handle` in send order; let autocommit ops pipeline.
- [ ] Own replay yourself on `BatchError` retryable codes; don't expect server retry.

**Errors**
- [ ] Read `camus-error-code` / `camus-error-message` trailers (unary/stream) and `BatchError`
      (batch); branch on the code and ignore the message text.
- [ ] Implement the retry split: `0502/0504/0505` → new BEGIN; `0509` → retry same finalize;
      `0555` → no automatic retry; others → fatal. Bounded backoff. Respect retry-only-pre-first-write for streams.

---

## 10. Related documents

- **Learned client routing** (advisory `RoutingAdvice` metadata, negotiation, eligibility, the
  endpoint trust map): [sql-routing-advice.md](sql-routing-advice.md)
- The ready-made **.NET client** for this protocol: [grpc-dotnet-client.md](grpc-dotnet-client.md)
  (`CamusDB.Grpc.Client`). On .NET, use it in place of your own implementation of the wire contract.
- Message shapes (source of truth): [`CamusDB.Grpc.Contracts/Protos/camus_sql.proto`](../CamusDB.Grpc.Contracts/Protos/camus_sql.proto)
- Prepared statements on both transports (§7.6 in full): [prepared-statements.md](prepared-statements.md)
- Data types & the compact-raw encoding: [data-types.md](data-types.md)
- Isolation, locking and transaction modes: [transactions-locking-and-isolation.md](transactions-locking-and-isolation.md)
- The retry contract the error model mirrors: [serializable-retry-contract.md](serializable-retry-contract.md)
- Transaction coordinator internals: [kahuna-transaction-coordinator.md](kahuna-transaction-coordinator.md)
- Server configuration (`grpc_*` keys): [configuration.md](configuration.md)
- Node resource state (`GetNodeStats`, the `MONITOR` privilege): [node-stats.md](node-stats.md)
