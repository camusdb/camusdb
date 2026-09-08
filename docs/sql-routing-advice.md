# Learned client routing for SQL statements

CamusDB servers can attach **advisory routing metadata** to successful SQL responses. A
multi-endpoint client learns from that metadata which node leads the data behind a repeated
statement, and sends the statement's future executions there directly — removing the
gateway-to-data-leader forwarding hop. This document describes the wire contract, the eligibility
rules, the server configuration, and the in-repo .NET client's routing support.

What routing advice is **not**:

- It is **never an instruction.** The operation that carried it already executed normally on the
  node that answered. A client that ignores it (or predates it) loses nothing but a network hop.
- It does **not** change execution. Isolation, locking, authorization, causal tokens, commit
  handling and results are byte-for-byte identical with routing on or off.
- It does **not** remove replication, transaction-coordinator traffic, or multi-partition work,
  and it cannot spread the write load of one hot table — a table still has one data leader.

## 1. Negotiation

Routing metadata is strictly opt-in per request:

- **gRPC:** set `SqlRequest.routing_accept_version = 1`.
- **REST:** set `"routingAcceptVersion": 1` on `/execute-sql-query` / `/execute-sql-non-query`.

A request without the field (every pre-routing client sends 0) gets its exact historical response
shape. A server that predates the feature ignores the field and simply attaches nothing; existing
advice on the client then expires on its own. Unknown metadata versions must be ignored by clients.

Metadata is emitted on **autocommit** statement responses only. Statements inside an explicit
transaction are pinned to the transaction's node and stream; advice for them is deferred until
there is measured demand.

## 2. The advice message

gRPC carries a `RoutingAdvice` message on the `QueryComplete` / `NonQueryReply` terminators (batch)
and as a trailing `QueryStreamMessage` (unary query, negotiated only). REST carries the same fields
as an optional `routing` object on the success envelope:

```json
{
  "routing": {
    "version": 1,
    "disposition": "prefer",
    "preferredNodeId": "camus-b:7070",
    "reuseScope": "statementParametersIndependent",
    "dependencyToken": "9A2F51C07B3E44D1A6E09C22",
    "maxAgeMs": 5000,
    "provenance": "placementHint",
    "reason": "singleTableHash"
  }
}
```

Contract:

1. `disposition: "prefer"` asks the client to remember `preferredNodeId` for future executions of
   this exact statement context; `"clear"` asks it to forget a previously learned destination.
2. `preferredNodeId` is an **opaque identity**, not an address. The client resolves it through its
   own operator-configured map (§5) and never dials it directly.
3. `reuseScope: "statementParametersIndependent"` explicitly permits reuse across parameter
   values. It is the only scope emitted today; a client must not treat an unknown scope as
   parameter-independent.
4. `dependencyToken` is an opaque change detector over the statement's resolved physical
   dependencies. It changes on schema change, `TRUNCATE`, materialized-view refresh, and database
   recreation. It is not sortable, not a routing authority, and not authentication.
5. `maxAgeMs` bounds reuse, measured by the client with a **monotonic clock from receipt** and
   clamped further by client policy. Expiry — not an invalidation protocol — is what heals a stale
   belief after a leader transfer.
6. `provenance: "placementHint"` states honestly that this is the answering node's local placement
   belief, not proof of which node executed the underlying storage operations.
7. Advice is built only after the statement's normal authorization checks, and it never contains
   SQL literals, row values, encoded keys, credentials, or a cluster map.
8. Advice construction is best-effort and side-effect-free: once a statement committed, no routing
   failure may convert that success into an error.

`reason` values: `singleTableHash` (prefer), `ineligible`, `placementUnknown`, `cacheAffinity`
(clear).

## 3. Eligibility — which statements get a "prefer"

The server advertises a destination only when it can prove the statement's data footprint is **one
ordinary hash-routed table**, in which case every row and index key space of that table shares one
placement group and the destination is independent of parameter values.

| Statement | Advice |
| --- | --- |
| Simple `SELECT` / `INSERT` / `UPDATE` / `DELETE` over one ordinary table | `prefer`, reusable across parameter values |
| Empty point lookup, zero affected rows | Still eligible — placement needs no returned row |
| Join, subquery (anywhere in the statement), derived table, view | `clear` (`ineligible`) |
| `AS OF SYSTEM TIME`, branch database, materialized view | `clear` (`ineligible`) |
| `{cache=…}`-hinted query (hit, miss or bypass) | `clear` (`cacheAffinity`) — cache locality and data locality may prefer different nodes |
| Key-range sharding enabled (deployment-wide gate) | `clear` (`ineligible`) — a ranged access path's destination depends on the parameter |
| DDL, server-level statements, `SHOW`/information queries | No metadata at all |
| Placement not yet initialized on this node, or leader unknown | `clear` (`placementUnknown`) |
| Standalone node | No metadata at all — there is only one destination |

Eligibility is decided from the **bound** statement (resolved tables, effective storage identity),
never from a regex over the SQL text. Anything the server cannot prove is answered conservatively.

## 4. Server configuration

Two restart-class, node-scope settings (see `SHOW VARIABLES`):

```yml
sql_routing_advice_enabled: true   # kill switch; emission still requires client negotiation
sql_routing_advice_ttl_ms: 5000    # advertised maxAgeMs (1 … 600000)
```

Because emission is negotiated per request, leaving the server default on is safe: clients see
nothing until they ask.

## 5. Client support (`CamusDB.Grpc.Client`)

```csharp
CamusGrpcOptions options = new();
options.Routing.Mode = CamusRoutingMode.Learned;   // Off (default) | Learned | Auto
options.Routing.NodeAddresses["camus-a:7070"] = "https://db-a.internal:9090";
options.Routing.NodeAddresses["camus-b:7070"] = "https://db-b.internal:9090";
options.Routing.NodeAddresses["camus-c:7070"] = "https://db-c.internal:9090";

await using CamusConnection connection = CamusConnection.Connect(
    ["https://db-a.internal:9090"], options);
```

- **The trust map is the routing authority.** `NodeAddresses` maps a server-advertised identity to
  an operator-configured address. Advice naming an unmapped identity is ignored; the client never
  dials a response-provided address and never derives one from a server identity. All mapped
  addresses join the endpoint pool with the connection's full TLS/credential/timeout policy.
- **Modes.** `Off` rotates over configured endpoints and never negotiates. `Learned` negotiates
  and prefers learned destinations for unpinned work. `Auto` — the default — behaves as `Learned`
  only when the map names at least two distinct addresses (a single load-balancer URL is not a
  node set), and as `Off` otherwise. The trust map is therefore the opt-in: a configuration
  without `NodeAddresses` sends requests byte-identical to a pre-routing client, and `Off` remains
  the explicit kill switch.
- **What is learned.** Advice is cached per `(database, exact SQL text, operation kind)` — no
  lowercasing, comment stripping, or literal normalization — with TTL
  `min(maxAgeMs, Routing.MaxHintAge)`, bounded by `RouteCacheMaxEntries` (default 4096) and
  `RouteCacheMaxBytes` (default 4 MiB). Late responses and late clears cannot overwrite a newer
  route (revision-guarded). Parameterized and prepared workloads are where reuse pays; the cache
  never generalizes a hint from one SQL string to another.
- **Prepared statements** select their endpoint before each autocommit execution and register
  themselves there on demand; a handle never moves between endpoints or stream incarnations.
  Disposal sweeps every endpoint the statement ever registered on.
- **Transactions** pin to the endpoint and stream chosen at `BeginTransactionAsync` for their
  whole life. The optional `affinity:` parameter reads a warmed prepared statement's learned route
  to choose where the transaction *starts* — it never relocates a live transaction, and cold
  advice just uses rotation with no extra round trip.
- **Failure handling.** A transport failure puts the endpoint on a short cooldown
  (`Routing.EndpointCooldown`, default 1 s) for future **unpinned** work only. A domain SQL error
  is a healthy server answering and triggers nothing. Routing adds no retry of any kind: the
  existing retry taxonomy (§8 of the protocol doc) remains authoritative, and a timeout after
  dispatch may hide a committed write, so nothing is ever replayed to a fallback endpoint.

## 6. Operational notes

- Stale advice is ordinary: after a leader transfer, a routed request lands on a follower, is
  forwarded exactly as an unrouted request would be, and the fresh response's advice (or expiry)
  converges the client. No correctness property ever depends on advice freshness.
- A node id may be advertised without the client having a mapping for it; that makes the hint
  unusable on that client, not an error.
- Advice does not cross databases, credentials, or connections; the cache lives on the client
  connection and dies with it.
- Performance claims should come from the qualification benchmark (routing off vs learned vs a
  manually selected leader on identical concurrency and pool budgets), not from intuition. The
  win is bounded by the eligible fraction of the workload and by how often statements repeat.
