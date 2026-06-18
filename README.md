CamusDB
=======
CamusDB is an open-source NewSQL distributed database written in C# on .NET 10. It combines a familiar SQL interface with a Raft-based distributed storage layer, supports multi-node clusters with automatic leader election and partition routing, and exposes a JSON/HTTP API. The project is alpha-quality — APIs and storage formats may change between versions.

**This is an alpha project. Do not use it in production.**

[![Build Status](https://app.travis-ci.com/camusdb/camusdb.svg?branch=main)](https://app.travis-ci.com/camusdb/camusdb)

[Documentation](https://camusdb.github.io/docs/intro)

![camus-cli](https://media.giphy.com/media/vqs2XqX5mAxC4Ln0FO/giphy.gif)

Features
--------
- **SQL dialect** — SELECT, INSERT, UPDATE, DELETE, CREATE/DROP/ALTER TABLE, transactions (BEGIN / COMMIT / ROLLBACK), parameterized placeholders, table aliases, derived tables, simple inner joins, comma joins, row-level DISTINCT, and case-insensitive identifier handling.
- **Aggregation** — COUNT, SUM, AVG, MIN, MAX with GROUP BY and HAVING filters.
- **Filtering and ordering** — WHERE clauses with =, !=, <, >, <=, >=, AND, OR, LIKE, ILIKE, BETWEEN, IS NULL, IN, NOT IN, scalar subqueries, and EXISTS subqueries; ORDER BY (ASC/DESC), projection aliases, ordinal references, LIMIT, and OFFSET.
- **Scalar functions** — string, math, date/time, cast, object id, and JSON helpers including `json_valid`, `json_type`, `json_extract`, `json_value`, `json_array_length`, and `json_contains`.
- **Query planning** — physical plan trees for table scans, index scans, joins, aggregation, distinct, sorting, and limits, with predicate/projection/limit pushdown, index-based sort elision, join-order heuristics, index nested-loop joins for eligible equi-joins, semi/anti-join rewrite of indexed `IN`/`NOT IN` subqueries, index-driven value-list `IN`, and streaming `DISTINCT`. A small statistics-backed cost model (row counts, per-index counts, per-column min/max) chooses between index and full scans.
- **Query introspection** — `EXPLAIN`, `EXPLAIN (LOGICAL)`, `EXPLAIN (PHYSICAL)`, and `EXPLAIN (ANALYZE)` return the plan as result rows (node names, details, estimated rows/cost, and — for `ANALYZE` — actual row counts and KV access counters).
- **Indexes** — PRIMARY KEY, inline UNIQUE column constraints, UNIQUE indexes, multi-column indexes, CREATE INDEX IF NOT EXISTS, CREATE UNIQUE INDEX IF NOT EXISTS, and ALTER TABLE ADD/DROP INDEX.
- **Database management** — databases must be created explicitly (`CREATE DATABASE`, `DROP DATABASE [IF EXISTS]`, `RENAME DATABASE old TO new`); there is no magic creation. Each database is assigned an immutable internal id at creation time; the name is a display-only label that can be renamed without moving any data.
- **Schema management** — CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS, ALTER TABLE ADD/DROP COLUMN.
- **ACID transactions** — pessimistic locking; serializable isolation is the default (range/predicate locks with wait-die deadlock avoidance and snapshot reads), with read-committed available per transaction (`SET TRANSACTION` or the begin-request field) or as a process default; cross-partition writes use two-phase commit (2PC).
- **Multi-node cluster** — Raft consensus (via Kommander) partitions data across nodes; each partition elects its own leader. Nodes join a cluster with `--mode=cluster` and a static peer list.
- **Standalone mode** — runs as a single embedded process with no cluster configuration required.
- **HTTP API** — all database operations are accessible over a JSON/HTTP endpoint.
- **Multi-platform** — runs on any platform supported by .NET 10.

Column Types
------------
- `string`
- `int64`
- `float64`
- `bool`
- `objectId`

SQL examples
------------
```sql
CREATE TABLE IF NOT EXISTS app_users (
  id STRING PRIMARY KEY NOT NULL,
  email STRING UNIQUE NOT NULL,
  display_name STRING NOT NULL,
  password_hash STRING NOT NULL,
  role STRING NOT NULL,
  created_at_utc STRING NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS app_users_email_idx ON app_users (email);

SELECT DISTINCT role FROM app_users ORDER BY role;

SELECT role, COUNT(*) AS users
FROM app_users
GROUP BY role
HAVING users > 0
ORDER BY 2 DESC;

SELECT r.id, r.name, ur.amount
FROM robots r
JOIN user_robots ur ON r.id = ur.robots_id;

SELECT r.id, r.name, ur.amount
FROM robots r, user_robots ur
WHERE r.id = ur.robots_id;

SELECT *
FROM robots
WHERE id NOT IN (SELECT robots_id FROM user_robots);

SELECT json_value(payload, "$.name")
FROM robots
WHERE json_valid(payload) = true;

EXPLAIN SELECT * FROM app_users WHERE email = 'a@example.com';

EXPLAIN (ANALYZE) SELECT role, COUNT(*) FROM app_users GROUP BY role;
```

`SELECT DISTINCT` is row-level distinct. Aggregate-level distinct such as `COUNT(DISTINCT code)` is not supported yet.

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

- **SQL Parser** — LALR(1) parser (YaccLexTools) that produces an AST. Identifiers are normalized to lowercase at parse time.
- **Query planner** — builds a physical plan tree from the bound SELECT model, choosing table scans, index scans, index lookup scans, nested-loop joins, index nested-loop joins, aggregate nodes, distinct nodes, sort nodes, and limit nodes based on query shape and available indexes.
- **Query binder** — resolves table aliases, derived table output columns, projection aliases, ordinal GROUP BY/ORDER BY references, aggregate scope, HAVING scope, and subquery scope before execution.
- **Query operators** — `QueryScanner`, `QueryFilterer`, `QuerySorter`, `QueryLimiter`, `QueryProjector`, `QueryAggregator`, `QueryDistincter`, `SemiJoinExecutor`, and `QueryJoinExecutor` execute the plan while keeping filtering, sorting, aggregation, projection, and limiting storage-agnostic.
- **Storage layer** — row data and index entries are stored in an embedded Kahuna KV node. `KvTableStore` maps table rows and index entries onto Kahuna keys using a prefix layout that keeps all rows of a table on the same Raft partition.
- **Transaction layer** — `KvTransactionsManager` coordinates BEGIN/COMMIT/ROLLBACK via Kahuna's transaction API; cross-partition writes go through Kahuna's 2PC protocol.
- **Catalog** — table and index descriptors are kept in memory and persisted through the KV layer.
- **Cluster mode** — a process-level Kahuna node is shared across all databases, wired with real gRPC inter-node and Raft transports (`GrpcCommunication` + `StaticDiscovery`). Standalone mode creates a per-database node with the embedded in-process transport.

Query Planner
-------------

See [docs/query-planner.md](docs/query-planner.md) for a full developer reference: pipeline stages, physical plan nodes, predicate analysis, index scan selection, join execution, the cost model and statistics, optimization passes, file map, and a checklist for adding new SQL features. For the user-facing `EXPLAIN` output format (node names, columns, and worked examples) see [docs/explain.md](docs/explain.md).

Distributed Schema
------------------

See [docs/distributed-schema-architecture.md](docs/distributed-schema-architecture.md) for a full developer reference on how DDL works across a cluster: schema as a replicated state machine over an ordered Raft log, the schema-change delta and the two-version invariant, ack-based convergence, the staged online-schema state machine (`DeleteOnly → WriteOnly → Public`) with a convergence gate between steps, the resumable change coordinator and crash-safe index backfill, follower→leader DDL forwarding with idempotent dedup, positional row encoding (why renames are free), schema-version pinning, the checkpoint persist-failure policy, an invariants checklist, and known limitations.

Configuration
-------------

CamusDB reads `CamusDB/Config/config.yml` at startup and merges CLI flags and environment variables into a single resolved configuration (precedence: CLI flag > environment variable > `config.yml` > built-in default). See [docs/configuration.md](docs/configuration.md) for the full reference: the precedence model, the CLI ↔ YAML mapping, the isolation/locking and parser-cache tunables, the allow-listed `kahuna:` engine passthrough, and the validation error matrix.

## Requirements
- .NET 10 SDK
- Docker (optional, for cluster setup)

## Testing

The test suite is split into two assemblies:

- **`CamusDB.Tests`** — the fast unit/integration suite (run on every change):
  ```sh
  dotnet test CamusDB.Tests/CamusDB.Tests.csproj
  ```
- **`CamusDB.Cluster.Tests`** — the heavy in-process multi-node cluster suite (real Raft via
  Kahuna/Kommander). It is isolated in its own assembly so its accumulated in-process load
  (dozens of sequential clusters) stays out of the fast suite, where it caused load-induced
  bring-up flakiness. Run it separately / periodically:
  ```sh
  dotnet test CamusDB.Cluster.Tests/CamusDB.Cluster.Tests.csproj
  ```

## License

This project is licensed under the [MIT license](LICENSE.txt).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in CamusDB by you, shall be licensed as MIT, without any additional terms or conditions.
