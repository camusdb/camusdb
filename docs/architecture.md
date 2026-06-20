# Architecture

This document describes how CamusDB is structured internally and how a SQL statement
flows through the system. It is a developer reference; for user-facing topics see the
other documents in `docs/` (query planner, transactions/locking, distributed schema,
configuration).

CamusDB is a NewSQL database: it presents a SQL interface and ACID transactions on top of
a Raft-replicated, transactional key-value store
([Kahuna](https://github.com/kahunakv/kahuna)). The same engine runs in two modes —
**standalone** (a single embedded process) and **cluster** (data partitioned across Raft
nodes) — by swapping only the storage node and its transport.

## Request lifecycle

A statement travels through the stack in roughly this order:

1. **HTTP API.** All operations arrive as JSON over HTTP. The request carries the SQL
   text (or a structured command), parameters, and an optional transaction handle and
   isolation/mode hints.
2. **Parser.** An LALR(1) parser (YaccLexTools) turns SQL text into an AST. Identifiers
   are normalized to lowercase at parse time. Parsed ASTs are cached per instance and
   swept periodically so repeated statements skip re-parsing.
3. **Binder.** The bound model resolves table aliases, derived-table output columns,
   projection aliases, ordinal `GROUP BY` / `ORDER BY` references, aggregate and `HAVING`
   scope, and subquery scope against the catalog.
4. **Planner.** A cost-based query planner builds a physical plan tree from the bound
   model, choosing between table and index scans, join strategies, and the aggregate /
   distinct / sort / limit nodes. See [query-planner.md](query-planner.md).
5. **Operators.** The plan executes as a tree of operators that read and write rows
   through the storage layer.
6. **Storage + transactions.** Operators read and write rows and index entries through
   the transactional KV layer, inside a transaction whose isolation level governs locking
   and visibility.
7. **Result.** Rows stream back up through projection and limiting to the HTTP response.

## Layers

### Parser

LALR(1) grammar compiled with YaccLexTools, producing the AST consumed by the binder.
Lowercasing of identifiers happens here, which is why identifier handling is
case-insensitive throughout the engine.

### Binder

Resolves names and scopes so that downstream stages never have to re-derive them: table
aliases, derived tables, projection aliases, ordinal references, aggregate/HAVING scope,
and subquery scope.

### Query planner

Builds a physical plan tree and chooses access paths using a small statistics-backed cost
model (row counts, per-index counts, per-column min/max). It applies predicate,
projection, and limit pushdown, index-based sort elision, join-order heuristics, and
semi-/anti-join rewrites of indexed `IN` / `NOT IN` subqueries. `EXPLAIN` exposes the
resulting plan. Full reference: [query-planner.md](query-planner.md) and
[explain.md](explain.md).

### Query operators

The plan executes as composable, storage-agnostic operators — scanning, filtering,
sorting, limiting, projection, aggregation, distinct, semi-join, and join execution.
Keeping these independent of the storage layout means filtering, sorting, aggregation, and
projection behave identically in standalone and cluster modes.

### Catalog

Table and index descriptors (schema) are held in memory and persisted through the KV
layer as DDL transactions. Schema changes are applied online and, in a cluster, replicated
and converged across nodes. The schema subsystem — replicated schema log, the staged
online-schema state machine, resumable index backfill, DDL forwarding, and positional row
encoding (which is why renames move no data) — is documented in
[distributed-schema-architecture.md](distributed-schema-architecture.md).

### Storage layer

Row data and index entries live in an embedded Kahuna transactional KV node. `KvTableStore`
maps each table row and index entry onto a Kahuna key using a prefix layout. The prefix is
chosen so that all rows of a table hash to the same Raft partition, keeping single-table
reads and writes within one partition's leader. Eligible secondary indexes can use
key-range routing with an order-safe encoding so that range scans stay ordered.

### Transaction layer

`KvTransactionsManager` coordinates `BEGIN` / `COMMIT` / `ROLLBACK` over Kahuna's
transaction API. Serializable is the default isolation level, implemented with pessimistic
range and predicate locks, wait-die deadlock avoidance, snapshot reads, and a session
read-your-writes causality token; read-committed is available per transaction or as a
process default. Cross-partition writes commit through Kahuna's two-phase commit protocol.
Full reference: [transactions-locking-and-isolation.md](transactions-locking-and-isolation.md)
and [serializable-retry-contract.md](serializable-retry-contract.md).

### Cluster transport

Durability and replication come from Raft, via Kommander. In **cluster mode** a single
process-level Kahuna node is shared across all databases and wired with real gRPC
inter-node and Raft transports (`GrpcCommunication` + `StaticDiscovery`); nodes join with
`--mode=cluster` and a static peer list. In **standalone mode** each process creates a node
with the embedded in-process transport, so the entire stack above is unchanged — only the
transport and discovery differ.

## Partitioning model

Data is partitioned across Raft groups. Each partition elects its own leader, and a
statement is routed to the partition that owns the keys it touches. Because a table's rows
share a key prefix that lands on one partition, most single-table operations are
single-partition and avoid distributed coordination; operations that span partitions (some
cross-table writes and schema changes) use two-phase commit and the schema convergence
protocol respectively.

## Standalone vs. cluster

| Aspect            | Standalone                          | Cluster                                            |
|-------------------|-------------------------------------|----------------------------------------------------|
| Kahuna node       | per-process, embedded               | one process-level node shared across databases     |
| Transport         | in-process                          | gRPC inter-node + Raft                              |
| Discovery         | none                                | `StaticDiscovery` with a static peer list          |
| Partitioning      | single partition                    | data partitioned across nodes, per-partition leader|
| Configuration     | none required                       | `--mode=cluster` + peer list                        |

The SQL surface, planner, operators, catalog, and transaction semantics are identical in
both modes.
