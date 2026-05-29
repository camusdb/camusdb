CamusDB
=======
CamusDB is an open-source NewSQL distributed database written in C# on .NET 9. It combines a familiar SQL interface with a Raft-based distributed storage layer, supports multi-node clusters with automatic leader election and partition routing, and exposes a JSON/HTTP API. The project is alpha-quality — APIs and storage formats may change between versions.

**This is an alpha project. Do not use it in production.**

[![Build Status](https://app.travis-ci.com/camusdb/camusdb.svg?branch=main)](https://app.travis-ci.com/camusdb/camusdb)

[Documentation](https://camusdb.github.io/docs/intro)

![camus-cli](https://media.giphy.com/media/vqs2XqX5mAxC4Ln0FO/giphy.gif)

Features
--------
- **SQL dialect** — SELECT, INSERT, UPDATE, DELETE, CREATE/DROP/ALTER TABLE, transactions (BEGIN / COMMIT / ROLLBACK), parameterized placeholders, and case-insensitive identifier handling.
- **Aggregation** — COUNT, SUM, AVG, MIN, MAX with GROUP BY.
- **Filtering and ordering** — WHERE clauses with =, !=, <, >, <=, >=, AND, OR, LIKE, ILIKE, IS NULL, IN; ORDER BY (ASC/DESC); LIMIT and OFFSET.
- **Indexes** — PRIMARY KEY, UNIQUE indexes, and multi-column indexes; ALTER TABLE ADD/DROP INDEX.
- **Schema management** — CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS, ALTER TABLE ADD/DROP COLUMN.
- **ACID transactions** — pessimistic locking with read-committed isolation; cross-partition writes use two-phase commit (2PC).
- **Multi-node cluster** — Raft consensus (via Kommander) partitions data across nodes; each partition elects its own leader. Nodes join a cluster with `--mode=cluster` and a static peer list.
- **Standalone mode** — runs as a single embedded process with no cluster configuration required.
- **HTTP API** — all database operations are accessible over a JSON/HTTP endpoint.
- **Multi-platform** — runs on any platform supported by .NET 9.

Column Types
------------
- `string`
- `int64`
- `float64`
- `bool`
- `objectId`

Running a cluster
-----------------
A three-node cluster can be started with Docker Compose:

```bash
docker compose -f docker/local.yml up --build
```

This starts three nodes on a private bridge network:

| Node   | HTTP API          | Raft port |
|--------|-------------------|-----------|
| camus1 | localhost:5095    | 7070      |
| camus2 | localhost:5096    | 7072      |
| camus3 | localhost:5097    | 7074      |

To run a single node without Docker:

```bash
# Standalone (default)
dotnet run --project CamusDB

# Cluster node
dotnet run --project CamusDB -- \
  --mode=cluster \
  --raft-nodename=camus-1 \
  --raft-host=192.168.1.10 \
  --raft-port=7070 \
  --initial-cluster-partitions=3 \
  --initial-cluster 192.168.1.10:7070 192.168.1.11:7072 192.168.1.12:7074
```

Architecture
------------
The engine is structured as a pipeline of composable operators:

- **SQL Parser** — LALR(1) parser (GPLEX/GPPG) that produces an AST. Identifiers are normalized to lowercase at parse time.
- **Query planner** — selects index scans or full table scans based on available indexes and the WHERE predicate.
- **Query operators** — `QueryScanner`, `QueryFilterer`, `QuerySorter`, `QueryLimiter`, `QueryProjector`, and `QueryAggregator` form a push-based execution pipeline.
- **Storage layer** — row data and index entries are stored in an embedded Kahuna KV node. `KvTableStore` maps table rows and index entries onto Kahuna keys using a prefix layout that keeps all rows of a table on the same Raft partition.
- **Transaction layer** — `KvTransactionsManager` coordinates BEGIN/COMMIT/ROLLBACK via Kahuna's transaction API; cross-partition writes go through Kahuna's 2PC protocol.
- **Catalog** — table and index descriptors are kept in memory and persisted through the KV layer.
- **Cluster mode** — a process-level Kahuna node is shared across all databases, wired with real gRPC inter-node and Raft transports (`GrpcCommunication` + `StaticDiscovery`). Standalone mode creates a per-database node with the embedded in-process transport.

## Requirements
- .NET 9 (SDK 9.0.100)
- Docker (optional, for cluster setup)

## License

This project is licensed under the [MIT license](LICENSE.txt).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in CamusDB by you, shall be licensed as MIT, without any additional terms or conditions.
