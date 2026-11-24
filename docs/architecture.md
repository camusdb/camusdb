# Architecture

This document describes how CamusDB is structured internally and how a SQL statement
flows through the system. It is a developer reference. For user-facing topics, see the
other documents in `docs/`, such as the query planner and transactions/locking guides.

CamusDB is a NewSQL database: it presents a SQL interface and ACID transactions on top of
a Raft-replicated, transactional key-value store
([Kahuna](https://github.com/kahunakv/kahuna)). The same engine supports two modes:
**standalone** (a single embedded process) and **cluster** (data partitioned across Raft
nodes). Only the storage node and its transport differ between them.

## Request lifecycle

A statement passes through seven stages, in roughly the order below.

1. **HTTP API.** All operations arrive as JSON over HTTP. The request carries the SQL
   text (or a structured command), parameters, and an optional transaction handle and
   isolation/mode hints.
2. **Parser.** An LALR(1) parser (YaccLexTools) turns SQL text into an AST. Identifiers
   are preserved verbatim in the exact case the user wrote them — the parser does not fold
   case. SQL keywords are matched case-insensitively. Parsed ASTs are cached per
   instance and swept periodically so repeated statements skip re-parsing.
3. **Binder.** The binder resolves names and scopes against the catalog, for example table
   aliases and ordinal `GROUP BY` / `ORDER BY` references. The result is the bound model.
4. **Planner.** A cost-based query planner builds a physical plan tree from the bound
   model. It chooses between table and index scans and between join strategies, and it adds
   the nodes the query needs, such as aggregate or sort nodes. See
   [query-planner.md](query-planner.md).
5. **Operators.** The plan executes as a tree of operators that read and write rows
   through the storage layer.
6. **Storage + transactions.** Operators read and write rows and index entries through
   the transactional KV layer. Each read and write belongs to a transaction whose
   isolation level governs locking and visibility.
7. **Result.** Rows stream back up through projection and limiting to the HTTP response.

## Layers

### Parser

LALR(1) grammar compiled with YaccLexTools, producing the AST consumed by the binder.
The parser does not fold identifier case. Identifiers are stored in the exact case the user
declared them (`CREATE TABLE`/`CREATE INDEX`/`CREATE DATABASE`), and references to them in
SQL match **case-insensitively**. The parser leaves identifier text unchanged. The
case-insensitive matching is done at lookup time by case-insensitive comparers on the
schema/catalog dictionaries and name comparisons. Because
identifiers are restricted to ASCII (`[A-Za-z_][A-Za-z0-9_]*`), `OrdinalIgnoreCase` is an
exact, culture-independent fold. Database names are also case-insensitive-unique
(`MyDb` and `mydb` are the same database) but stored and displayed in their original case.

### Binder

The binder resolves three kinds of names and scopes so that downstream stages do not have
to re-derive them.

- Table aliases and derived tables.
- Projection aliases and ordinal references.
- Aggregate/HAVING scope and subquery scope.

### Query planner

The planner builds a physical plan tree and chooses access paths with a small cost model.
The model reads row and per-index counts and the per-column min/max values from the table
statistics. Several optimizations reduce the rows a plan reads and the work it does.

- Predicate, projection, and limit pushdown.
- Index-based sort elision.
- Join-order heuristics.
- Semi-join and anti-join rewrites of indexed `IN` / `NOT IN` subqueries.

`EXPLAIN` exposes the resulting plan. Full reference: [query-planner.md](query-planner.md)
and [explain.md](explain.md).

### Query operators

The plan executes as a tree of composable, storage-agnostic operators, such as scan, sort
and join operators. Because the operators do not depend on the storage layout, they behave
identically in standalone and cluster modes. Blocking operators can optionally spill their
intermediate buffers to temporary files, which limits the memory they use. For more
information, see [spill-to-disk.md](spill-to-disk.md).

### Catalog

Table and index descriptors (schema) are held in memory and persisted through the KV
layer as DDL transactions. Schema changes are applied online and, in a cluster, replicated
and converged across nodes. The schema subsystem is documented in
[distributed-schema-architecture.md](distributed-schema-architecture.md). That document
covers the replicated schema log and the staged online-schema state machine. It also
describes resumable index backfill and DDL forwarding. Rows use a positional encoding,
which is why a rename moves no data.

### Storage layer

Row data and index entries live in the process-level Kahuna transactional KV node. `KvTableStore`
maps each table row and index entry onto a Kahuna key using a `{dbId}:{tableId}|…` prefix
layout. This prevents rows from different databases from colliding in the same node. Within
one database the prefix is chosen so that all rows of a table hash to the same Raft partition,
keeping single-table reads and writes within one partition's leader. Eligible secondary indexes
can use key-range routing with an order-safe encoding so that range scans stay ordered.

#### Keyspace map

| Key prefix                           | Contents                                                        |
|--------------------------------------|-----------------------------------------------------------------|
| `_system/dbregistry/…`               | Database name → id registry                                     |
| `{dbId}/meta/…`                      | Per-database schema: version, table definitions, coordinator jobs |
| `{dbId}:stats:{tableId}`             | Statistics for one table: row counts, histograms, distinct counts |
| `{dbId}:{tableId}|r/{rowId}`         | Row data                                                        |
| `{dbId}:{tableId}|i:{indexId}/{key}` | Secondary index entries                                         |

`dbId` is a compact opaque identifier allocated at CREATE DATABASE time and never reused
(rename-safe: the key prefix is stable across renames). `tableId` follows the same scheme.

The `|` after `{dbId}:{tableId}` is Kahuna's placement-group separator. Under hash routing Kahuna
places a key space by the prefix before its first `|`. This means that a table's rows and all of
its indexes land on the same partition. A transaction that reads an index entry and writes the row
it points to then touches one partition, which is the condition for Kahuna's one-phase commit path.
The layout is on disk; it changed with storage revision `v2`, and data crosses revisions only by
logical dump and reimport (see [logical-dump-and-reimport.md](logical-dump-and-reimport.md)).

### Transaction layer

`KvTransactionsManager` coordinates `BEGIN` / `COMMIT` / `ROLLBACK` over Kahuna's
transaction API. Serializable is the default isolation level. It is implemented with
pessimistic range and predicate locks and wait-die deadlock avoidance, and it also uses
snapshot reads and a session read-your-writes causality token. Read-committed is available
per transaction or as a process default. Cross-partition writes commit through Kahuna's two-phase commit protocol.
Full reference: [transactions-locking-and-isolation.md](transactions-locking-and-isolation.md)
and [serializable-retry-contract.md](serializable-retry-contract.md).

### Cluster transport

Durability and replication come from Raft, via Kommander. Both standalone and cluster share
the same unified storage model: **one Kahuna node per process** stores all databases in the
`{dbId}:{tableId}|…` keyspace. The two modes differ only in the transport.

- **Cluster mode** — the node uses gRPC inter-node and Raft transports
  (`GrpcCommunication` + `StaticDiscovery`); nodes join with `--mode=cluster` and a static
  peer list. Data is partitioned across nodes, and each partition elects a Raft leader.
- **Standalone mode** — the same node uses the embedded in-process transport. Single
  partition, no peer list required.

## Partitioning model

Data is partitioned across Raft groups. Each partition elects a leader, and a
statement is routed to the partition that owns the keys it touches. Because a table's rows
share a key prefix that lands on one partition, most single-table operations are
single-partition and avoid distributed coordination; operations that span partitions (some
cross-table writes and schema changes) use two-phase commit and the schema convergence
protocol respectively.

## Standalone vs. cluster

| Aspect            | Standalone                          | Cluster                                            |
|-------------------|-------------------------------------|----------------------------------------------------|
| Kahuna node       | one process-level node (embedded)   | one process-level node shared across databases     |
| Transport         | in-process                          | gRPC inter-node + Raft                              |
| Discovery         | none                                | `StaticDiscovery` with a static peer list          |
| Partitioning      | single partition                    | data partitioned across nodes, per-partition leader|
| Configuration     | none required                       | `--mode=cluster` + peer list                        |

The SQL surface, planner, operators, catalog, and transaction semantics are identical in
both modes.
