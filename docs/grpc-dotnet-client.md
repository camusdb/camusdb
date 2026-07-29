# CamusDB .NET gRPC Client (`CamusDB.Grpc.Client`)

`CamusDB.Grpc.Client` is the .NET client library for CamusDB's gRPC API. It is a **multiplexing**
client: many concurrent operations — including many concurrent transactions — share a small pool of
long-lived `BatchExecute` streams, so the network stays busy without a stream per call. You work with
`CamusConnection` and `CamusTransactionSession`; the batching, request/response correlation, stream
routing, and reconnect are handled for you.

This guide covers using the library. For the language-agnostic wire contract it speaks (value encoding,
schema-first streaming, the causal token, the batching handshake, the error taxonomy) see
[grpc-client-protocol.md](grpc-client-protocol.md).

> Status: alpha, alongside the REST/JSON API. The gRPC endpoint is served on a dedicated HTTP/2 port
> (`grpc_enabled` / `grpc_port`; see [configuration.md](configuration.md)).

## Namespaces

```csharp
using CamusDB.Grpc.Client;           // CamusConnection, CamusTransactionSession, CamusGrpcOptions
using CamusDB.Grpc.Client.Batching;  // QueryResult, NonQueryResult, CausalToken, CamusGrpcException
using CamusDB.Grpc;                  // IsolationLevel, TransactionMode, LockingMode, Value, ColumnType
```

## Connecting

```csharp
await using CamusConnection conn = CamusConnection.Connect("https://localhost:5096");
```

`CamusConnection` is thread-safe and intended to be **shared** across your app — concurrent calls from
many threads/tasks multiplex over the pool. Dispose it (via `await using` or `DisposeAsync`) at
shutdown; that tears down the streams and fails any still-pending calls.

Use `http://…` for plaintext HTTP/2 (`h2c`) in local dev, or `https://…` with the TLS options below.

## Autocommit statements

Each call runs in its own short transaction and commits:

```csharp
NonQueryResult insert = await conn.ExecuteNonQueryAsync(
    "mydb", "INSERT INTO items (id, name) VALUES (gen_id(), 'alpha')");
Console.WriteLine(insert.AffectedRows);   // 1

QueryResult result = await conn.ExecuteQueryAsync("mydb", "SELECT id, name FROM items");
foreach (ResultRow row in result.Rows)
{
    // Values are positional — row.Values[i] aligns to result.Schema.Columns[i].
    string name = row.Values[1].StringValue;
}
```

- `QueryResult` carries the ordered `Schema` (one `ColumnSchema { Name, Type }` per output column) and
  the positional `Rows`. A `Value` is a `oneof` over the 12 column types — read the field matching the
  schema's `ColumnType` (`Int64Value`, `StringValue`, `IdValue`, `UuidValue`, `DateValue`, …). See the
  protocol doc §2 for the exact encoding (e.g. dates are raw UTC ticks, UUIDs are 16 big-endian bytes).
- Autocommit calls run **concurrently** — fire many and `await Task.WhenAll` them; they pipeline over
  the pool.

## Prepared statements

Register a parameterized statement once and execute it with values only — the SQL and the parameter
names stop travelling per execution, and the server stops re-parsing the statement to route it:

```csharp
await using CamusPreparedStatement insert = await conn.PrepareAsync(
    "mydb", "INSERT INTO items (id, name) VALUES (gen_id(), @name)");

await insert.ExecuteNonQueryAsync(["widget"]);
await insert.ExecuteNonQueryAsync(["gadget"]);
```

Values bind by ordinal against `insert.ParameterNames`. The statement re-registers itself
transparently when a pooled stream is rebuilt, so handles and streams never surface to the caller.
See [prepared-statements.md](prepared-statements.md).

## DDL

DDL is **not** batched — it goes over the unary RPC — but the API is the same shape:

```csharp
await conn.ExecuteDdlAsync("", "CREATE DATABASE mydb");
await conn.ExecuteDdlAsync("mydb", "CREATE TABLE items (id oid PRIMARY KEY, name string(100))");
```

## Transactions

```csharp
CamusTransactionSession tx = await conn.BeginTransactionAsync(
    "mydb",
    isolation: IsolationLevel.Serializable,     // optional; omit for server default
    mode:      TransactionMode.ReadWrite,       // optional
    locking:   LockingMode.Pessimistic);        // optional

try
{
    await tx.ExecuteNonQueryAsync("INSERT INTO items (id, name) VALUES (gen_id(), 'a')");
    QueryResult rows = await tx.ExecuteQueryAsync("SELECT name FROM items");  // sees the insert
    await tx.CommitAsync();
}
catch (CamusGrpcException ex)
{
    await tx.RollbackAsync();
    throw;
}
```

Rules:

- **Issue a transaction's statements sequentially** — `await` each before the next. The whole unit of
  work (begin → statements → commit) pipelines over one stream, but within a transaction the calls are
  ordered, which is what gives read-your-writes.
- The library pins a session's ops to a single stream automatically (so the server orders them) and
  threads the causal token forward for you. You don't manage handles or tokens.
- After `CommitAsync`/`RollbackAsync` the session is finalized; using it again throws.
- If the connection/stream drops after a `BeginTransactionAsync` without a commit, the server rolls the
  transaction back — no orphaned transactions.

## Options (`CamusGrpcOptions`)

```csharp
CamusGrpcOptions options = new()
{
    ChannelPoolSize      = 2,                       // long-lived streams multiplexed per endpoint
    CoalescingThreshold  = 10,                      // batch small bursts before writing
    CoalescingDelayMs    = 2,
    OperationTimeout     = TimeSpan.FromSeconds(30),// deadline for a call with no CancellationToken

    // TLS / keep-alive
    ConnectTimeout                 = TimeSpan.FromSeconds(10),
    KeepAlivePingDelay             = TimeSpan.FromSeconds(30),
    KeepAlivePingTimeout           = TimeSpan.FromSeconds(10),
    EnableMultipleHttp2Connections = true,
};
options.TrustedServerCertificateThumbprints.Add("A1B2…SHA256HEX");  // pin the server cert

await using CamusConnection conn = CamusConnection.Connect("https://host:5096", options);
```

- The **pool size bounds the number of streams, not the number of in-flight transactions** — many
  transactions hash onto the same streams and interleave. A small pool (2) is normal.
- TLS trust: by default the OS certificate chain is validated. Set
  `TrustedServerCertificateThumbprints` to **pin** by SHA-256 thumbprint instead, or (dev only)
  `AllowInsecureCertificateValidation = true` to accept any certificate. Do not use insecure mode
  against a real deployment.
- Pass a `CancellationToken` to any call to bound it; if you don't, `OperationTimeout` applies (zero =
  no deadline).

## Errors and retries

A failed call throws `CamusGrpcException` whose **`Code`** is the `CADBxxxx` domain code — branch on the
code, never the message. The retryable family and how to retry are defined in the protocol doc §8. In
short:

- Autocommit call, retryable code (`CADB0502` / `CADB0504` / `CADB0505`): re-issue the call.
- **Transactional** retryable failure kills the transaction — replay the **whole unit of work** under a
  fresh `BeginTransactionAsync`, not just the failed statement.
- `CADB0509` on commit: retry the *same* `CommitAsync` on the same session.

The library does not auto-retry (a streaming query may have already surfaced rows); replay is yours to
decide, matching the "retry only pre-first-write" contract.

## Causal token (advanced)

Every `QueryResult` / `NonQueryResult` exposes a `CausalToken (N, L, C)`. Within a session it is threaded
automatically for read-your-writes. You only need to touch it for causality **across** sessions/
connections — carry the latest token forward yourself. All three components matter; don't drop `N`
(see protocol doc §4.2).

## Related

- [grpc-client-protocol.md](grpc-client-protocol.md) — the wire contract (implement a client in any
  language, or understand exactly what this library sends).
- [transactions-locking-and-isolation.md](transactions-locking-and-isolation.md) — isolation/locking
  semantics behind the `IsolationLevel` / `LockingMode` options.
- [configuration.md](configuration.md) — server-side `grpc_*` settings.
