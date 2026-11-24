# Learned client routing for SQL statements

CamusDB servers can attach **advisory routing metadata** to successful SQL responses. A
multi-endpoint client learns from that metadata which node leads the data behind a repeated
statement, and sends the statement's future executions there directly — removing the
gateway-to-data-leader forwarding hop. This document describes the wire contract and the eligibility
rules. It also covers the server configuration and the routing support in the in-repo .NET client.

Routing advice is a hint to the client, and it does not change how a statement executes.

- It is **not an instruction.** The operation that carried it already executed normally on the
  node that answered. A client that ignores it (or predates it) loses only a network hop.
- It does **not** change execution. Isolation, locking, authorization, causal tokens, commit
  handling and results are byte-for-byte identical with routing on or off.
- It does **not** remove replication or transaction-coordinator traffic. Multi-partition work also
  remains. It cannot spread the write load of one hot table, because a table has one data leader.

## 1. Negotiation

Routing metadata is strictly opt-in per request. Each transport has a field that the client sets.

- **gRPC:** set `SqlRequest.routing_accept_version = 1`.
- **REST:** set `"routingAcceptVersion": 1` on `/execute-sql-query` / `/execute-sql-non-query`.

A request that omits the field gets its exact historical response shape. Pre-routing clients all
send 0. A server that predates the feature ignores the field and attaches no metadata. Advice that
the client already has will then expire. Unknown metadata versions must be ignored by clients.

Metadata is emitted on autocommit statement responses **and** on statements in an explicit
transaction. A transaction's statements are pinned to the node and stream the transaction started
on. Advice for them does not move the current transaction. It names the statement's table leader,
and a client uses it to choose where its *next* transaction starts (§6).

## 2. The advice message

gRPC carries a `RoutingAdvice` message on the `QueryComplete` / `NonQueryReply` terminators (batch)
and as a trailing `QueryStreamMessage` (unary query, negotiated only). REST carries the same fields
in an optional `routing` object on the success envelope.

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

Each field has a fixed meaning.

1. `disposition: "prefer"` asks the client to remember `preferredNodeId` for future executions of
   this exact statement context; `"clear"` asks it to forget a previously learned destination.
2. `preferredNodeId` is an **opaque identity**. It is not an address. The client resolves it through
   the operator-configured map (§5) and does not dial it directly.
3. `reuseScope: "statementParametersIndependent"` explicitly permits reuse across parameter
   values. It is the only scope emitted today; a client must not treat an unknown scope as
   parameter-independent.
4. `dependencyToken` is an opaque change detector over the statement's resolved physical
   dependencies. It changes on schema change, `TRUNCATE`, materialized-view refresh, and database
   recreation. The token cannot be sorted. It also has no authority over routing and does not authenticate anything.
5. `maxAgeMs` bounds reuse, measured by the client with a **monotonic clock from receipt** and
   clamped further by client policy. Expiry heals a stale
   belief after a leader transfer; there is no invalidation protocol.
6. `provenance: "placementHint"` states that this is the answering node's local placement
   belief. It does not prove which node executed the underlying storage operations.
7. Advice is built only after the statement's normal authorization checks. It does not contain
   SQL literals, row values, encoded keys, credentials, or a cluster map.
8. Advice construction is best-effort and side-effect-free: once a statement committed, no routing
   failure may convert that success into an error.

`reason` values: `singleTableHash`, `singleRangeTable` (prefer), `ineligible`, `placementUnknown`,
`cacheAffinity` (clear).

## 3. Eligibility — which statements get a "prefer"

The server advertises a destination if it can prove that the statement's data footprint is **one
ordinary table with all of its key spaces on one partition**. If it cannot prove this, it does not
advertise one. This means that the destination is independent of parameter values. Under hash
routing, this is always true. All row and index key spaces of a table share one placement group.
Under key-range routing, it is true while the row space, the index spaces and the large-value space
are each one range on the same partition, which is how Kahuna seeds a new
table. The first split of any of them ends it.

| Statement | Advice |
| --- | --- |
| Simple `SELECT` / `INSERT` / `UPDATE` / `DELETE` over one ordinary table | `prefer`, reusable across parameter values |
| Empty point lookup, zero affected rows | Eligible — placement needs no returned row |
| Join, subquery (anywhere in the statement), derived table, view | `clear` (`ineligible`) |
| `AS OF SYSTEM TIME`, branch database, materialized view | `clear` (`ineligible`) |
| `{cache=…}`-hinted query (hit, miss or bypass) | `clear` (`cacheAffinity`) — cache locality and data locality may prefer different nodes |
| Key-range table whose spaces are each one range on one partition | `prefer` (`singleRangeTable`), reusable across parameter values until a split |
| Key-range table with a divided space, or a space on another partition | `clear` (`ineligible`) — the destination depends on the parameter |
| DDL, server-level statements, `SHOW`/information queries | No metadata at all |
| Placement not yet initialized on this node, or leader unknown | `clear` (`placementUnknown`) |
| Standalone node | No metadata at all — there is only one destination |

Eligibility is decided from the **bound** statement (resolved tables, effective storage identity).
A regex over the SQL text is not used. Anything the server cannot prove is answered conservatively.

## 4. Server configuration

Routing advice has two restart-class, node-scope settings (see `SHOW VARIABLES`).

```yml
sql_routing_advice_enabled: true   # kill switch; emission still requires client negotiation
sql_routing_advice_ttl_ms: 5000    # advertised maxAgeMs (1 … 600000)
```

Because emission is negotiated per request, leaving the server default on is safe. Clients
see no metadata until they ask for it.

## 5. Client support (`CamusDB.Grpc.Client`)

```csharp
CamusGrpcOptions options = new();
options.Routing.Mode = CamusRoutingMode.Learned;   // Off | Learned | Auto (default)
options.Routing.NodeAddresses["camus-a:7070"] = "https://db-a.internal:9090";
options.Routing.NodeAddresses["camus-b:7070"] = "https://db-b.internal:9090";
options.Routing.NodeAddresses["camus-c:7070"] = "https://db-c.internal:9090";

await using CamusConnection connection = CamusConnection.Connect(
    ["https://db-a.internal:9090"], options);
```

- **The trust map is the routing authority.** `NodeAddresses` maps a server-advertised identity to
  an operator-configured address. Advice naming an unmapped identity is ignored. The client does not
  dial an address that a response provides, and it does not derive one from a server identity. All mapped
  addresses join the endpoint pool with the connection's full TLS/credential/timeout policy.
- **Modes.** `Off` rotates over configured endpoints and does not negotiate. `Learned` negotiates
  and prefers learned destinations for unpinned work. `Auto` — the default — behaves as `Learned`
  if the map names at least two distinct addresses (a single load-balancer URL is not a
  node set). Otherwise, it behaves as `Off`. This means that the trust map is the opt-in. A
  configuration that omits `NodeAddresses` sends requests byte-identical to a pre-routing client, and `Off` remains
  the explicit kill switch.
- **What is learned.** Advice is cached per `(database, exact SQL text, operation kind)`. The text
  is used as is: no lowercasing, comment stripping, or literal normalization is applied. The TTL is
  `min(maxAgeMs, Routing.MaxHintAge)`, bounded by `RouteCacheMaxEntries` (default 4096) and
  `RouteCacheMaxBytes` (default 4 MiB). Late responses and late clears cannot overwrite a newer
  route (revision-guarded). Parameterized and prepared workloads benefit most from reuse. The cache
  does not generalize a hint from one SQL string to another.
- **Prepared statements** select their endpoint before each autocommit execution and register
  themselves there on demand. A handle does not move between endpoints or stream incarnations.
  Disposal sweeps all endpoints that the statement has registered on.
- **Transactions** pin to the endpoint and stream START landed on, and remain there until they end. Where
  START lands is decided once, by the first thing that names a route.
  - With the optional `affinity:` parameter, START is sent in `BeginTransactionAsync` on the
    warmed prepared statement's learned endpoint (rotation when cold).
  - With routing negotiated and no affinity, START is **deferred to the first statement** and sent
    on that statement's learned endpoint. No application change is needed for this. The server
    anchors the transaction's coordinator session to the first table the transaction touches.
    Starting on that table's leader makes all operation registrations and the commit
    local. Statements in the transaction carry advice too. That advice is learned for the *next*
    transaction and does not move this one. A session finalized before any statement sends START
    and then the finalize on a rotation endpoint. A failed START surfaces at the first statement
    with the same exception type an eager START would throw. `SET TRANSACTION` as the first
    statement carries no advice, and the transaction then starts on rotation. To avoid this, pass
    isolation, mode and locking to `BeginTransactionAsync`.
  - With routing off, START is sent in `BeginTransactionAsync` on rotation, exactly as before
    routing existed.
- **Failure handling.** A transport failure puts the endpoint on a short cooldown
  (`Routing.EndpointCooldown`, default 1 s) for future **unpinned** work only. A domain SQL error
  is a healthy server answering, and it has no effect on routing. Routing adds no retry of any kind.
  The existing retry taxonomy (§8 of the protocol doc) remains authoritative. A timeout after
  dispatch may hide a committed write. For this reason, a request is not replayed to a fallback
  endpoint under any condition.

## 6. Operational notes

- Stale advice is ordinary: after a leader transfer, a routed request lands on a follower, is
  forwarded exactly as an unrouted request would be, and the fresh response's advice (or expiry)
  converges the client. No correctness property ever depends on advice freshness.
- A node id may be advertised when the client has no mapping for it. The hint is then unusable on
  that client. This is not an error.
- Advice does not cross databases or connections, and it is not shared between credentials. The
  cache lives on the client
  connection and dies with it.
- A transaction's Kahuna coordinator session is placed by hashing its coordinator key with the
  same placement rule as data keys. A client-owned transaction (deferred session start) anchors
  that key in the placement group of the first table it touches. The session is then on that
  table's data leader. With the client starting the transaction there too, registrations and the
  commit are local. Under key-range sharding, no key string names a table's partition. In that case,
  the session is pinned to the partition of the table's one row range (Kahuna's
  `CoordinatorPartitionId`, Kahuna 1.13.4 or later on all nodes). The pin is fixed for the life
  of the session. A split during the transaction costs forwarded calls, and it cannot cause a wrong
  answer. A table whose row space is already divided gets an unpinned session. The right partition
  for it depends on the row key. An autocommit statement opens its session eagerly and is not anchored.
- Performance claims should come from the qualification benchmark (routing off vs learned vs a
  manually selected leader on identical concurrency and pool budgets). The
  win is bounded by the eligible fraction of the workload and by how often statements repeat.
