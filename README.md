CamusDB
=======
CamusDB is an open-source NewSQL distributed database written in C# on .NET 10. It combines a familiar SQL interface with a Raft-based distributed storage layer, supports multi-node clusters with automatic leader election and partition routing, and exposes both a JSON/HTTP API and a gRPC API. The project is alpha-quality — APIs and storage formats may change between versions.

**This is an alpha project. Do not use it in production.**

[![Build Status](https://app.travis-ci.com/camusdb/camusdb.svg?branch=main)](https://app.travis-ci.com/camusdb/camusdb)

[Documentation](https://camusdb.github.io/docs/intro)

![camus-cli](https://media.giphy.com/media/vqs2XqX5mAxC4Ln0FO/giphy.gif)

Features
--------
- **SQL dialect** — SELECT (including `FROM`-less `SELECT <expr>`), INSERT, UPDATE, DELETE, CREATE/DROP/ALTER TABLE, transactions (BEGIN / COMMIT / ROLLBACK), parameterized placeholders, table aliases, derived tables, simple inner joins, comma joins, row-level DISTINCT, and case-insensitive identifier handling.
- **Aggregation** — COUNT, SUM, AVG, MIN, MAX with GROUP BY and HAVING filters.
- **Filtering and ordering** — WHERE clauses with =, !=, <, >, <=, >=, AND, OR, LIKE, ILIKE, regex match operators (~, ~*, !~, !~*), BETWEEN, IS NULL, IN, NOT IN, scalar subqueries, and EXISTS subqueries; ORDER BY (ASC/DESC), projection aliases, ordinal references, LIMIT, and OFFSET.
- **Scalar functions** — string, math, date/time, cast, object id, regex, and JSON helpers including `json_valid`, `json_type`, `json_extract`, `json_value`, `json_array_length`, and `json_contains`.
- **Query planning** — physical plan trees for table scans, index scans, joins, aggregation, distinct, sorting, and limits, with predicate/projection/limit pushdown, index-based sort elision, join-order heuristics, index nested-loop joins for eligible equi-joins, semi/anti-join rewrite of indexed `IN`/`NOT IN` subqueries, index-driven value-list `IN`, and streaming `DISTINCT`. A small statistics-backed cost model (row counts, per-index counts, per-column min/max) chooses between index and full scans.
- **Query introspection** — `EXPLAIN`, `EXPLAIN (LOGICAL)`, `EXPLAIN (PHYSICAL)`, and `EXPLAIN (ANALYZE)` return the plan as result rows (node names, details, estimated rows/cost, and — for `ANALYZE` — actual row counts and KV access counters).
- **Indexes** — PRIMARY KEY, inline UNIQUE column constraints, UNIQUE indexes, multi-column indexes, per-column ascending/descending ordered indexes, CREATE INDEX IF NOT EXISTS, CREATE UNIQUE INDEX IF NOT EXISTS, and ALTER TABLE ADD/DROP INDEX.
- **Database management** — databases must be created explicitly (`CREATE DATABASE`, `DROP DATABASE [IF EXISTS]`, `RENAME DATABASE old TO new`); there is no magic creation. Each database is assigned an immutable internal id at creation time; the name is a display-only label that can be renamed without moving any data.
- **Copy-on-write database branching** — fork a database instantly with `CREATE DATABASE feature_x BRANCH FROM prod`. The branch shares the parent's data until it diverges (no row data is copied), reads see the parent as of the fork instant, writes are private to the branch, and the parent keeps evolving and never sees the branch. Inspect the tree with `SHOW BRANCHES FROM db` and `SHOW ANCESTORS FROM db`. Ideal for cheap staging clones, schema-migration dry-runs, and per-PR ephemeral databases. See [Database Branching](#database-branching) below.
- **Schema management** — CREATE TABLE IF NOT EXISTS, DROP TABLE IF EXISTS, ALTER TABLE ADD/DROP COLUMN, ALTER TABLE RENAME TABLE/COLUMN/INDEX, column DEFAULT values (including function defaults such as `gen_uuid_v7()`), CHECK constraints, and `SHOW CREATE TABLE`.
- **ACID transactions** — pessimistic locking; serializable isolation is the default (range/predicate locks with wait-die deadlock avoidance and snapshot reads), with read-committed available per transaction (`SET TRANSACTION` or the begin-request field) or as a process default; cross-partition writes use two-phase commit (2PC).
- **Multi-node cluster** — Raft consensus (via Kommander) partitions data across nodes; each partition elects its own leader. Nodes join a cluster with `--mode=cluster` and a static peer list.
- **Standalone mode** — runs as a single embedded process with no cluster configuration required.
- **APIs** — all database operations are accessible over a JSON/HTTP endpoint and over a gRPC endpoint (streaming query results and a duplex batch-execute channel with per-transaction chains).
- **Recovering dropped objects** — `DROP DATABASE`/`DROP TABLE` defer physical deletion: the object is unlinked (orphaned) rather than immediately erased, so it can be recovered while its data still exists by relinking under a new name (`CREATE DATABASE new RELINK TO '<id>'`, `CREATE TABLE new RELINK TO '<id>'`). List recoverable objects with `SHOW ORPHAN DATABASES` / `SHOW ORPHAN TABLES`; a background reclaimer garbage-collects orphans once their retention window elapses.
- **Multi-platform** — runs on any platform supported by .NET 10.

Column Types
------------
| Type | SQL keyword(s) | Notes |
|------|----------------|-------|
| String | `string`, `string(N)`, `varchar`, `char`, `text` | UTF-8 text; `string(N)` bounds the length |
| Integer64 | `int64`, `int` | 64-bit signed integer |
| Float64 | `float64` | 64-bit IEEE-754 |
| Float32 | `float32`, `real` | 32-bit IEEE-754 |
| Bool | `bool`, `boolean` | |
| Id | `object_id`, `oid` | 24-hex ObjectId, the default primary-key type |
| Uuid | `uuid`, `guid` | native 128-bit UUID; `gen_uuid_v4()` / `gen_uuid_v7()` |
| Bytes | `bytes`, `blob` | binary; `0x…`-hex in SQL, base64 in JSON |
| Date | `date` | calendar date |
| DateTime | `datetime`, `timestamp` | date + time |
| Array | `array(<elem>)` | homogeneous array of a scalar element type (not nested) |

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

Running with Docker
-------------------
A single node can be started from the published image, mapping the JSON/REST and gRPC ports and
persisting data in a named volume:

```bash
docker run --rm \
        -p 5095:5095 \
        -p 5096:5096 \
        -v camus-data:/data \
        --name camusdb camusdb/camusdb:latest
```

Running a cluster
-----------------
A three-node cluster can be started with Docker Compose:

```bash
docker compose -f docker/local.yml up --build
```

This starts three nodes on a private bridge network:

| Node   | JSON/REST API     | gRPC API          | Raft port |
|--------|-------------------|-------------------|-----------|
| camus1 | localhost:5095    | localhost:6095    | 7070      |
| camus2 | localhost:5096    | localhost:6096    | 7072      |
| camus3 | localhost:5097    | localhost:6097    | 7074      |

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
A SQL statement enters over the HTTP API, is parsed into an AST, bound against the catalog, and turned into a physical plan by a cost-based query planner. The plan executes as a tree of storage-agnostic operators (scan, filter, join, aggregate, sort, project, limit) that read and write rows through a transactional key-value layer. That KV layer is an embedded [Kahuna](https://github.com/kahunakv/kahuna) node: table rows and index entries are mapped onto Kahuna keys with a prefix layout, transactions are coordinated via Kahuna's transaction API, and durability and replication are provided by Raft (Kommander). In a cluster, data is partitioned across nodes — each partition elects its own leader, statements are routed to the owning partition, and cross-partition writes use two-phase commit. Standalone mode runs the same stack against a per-process embedded node with no cluster configuration.

See [docs/architecture.md](docs/architecture.md) for a full developer reference: the request lifecycle, each layer (parser, binder, planner, operators, catalog, KV storage, transactions, cluster transport), the key layout and partitioning model, and how standalone and cluster modes differ.

Query Planner
-------------

See [docs/query-planner.md](docs/query-planner.md) for a full developer reference: pipeline stages, physical plan nodes, predicate analysis, index scan selection, join execution, the cost model and statistics, optimization passes, file map, and a checklist for adding new SQL features. For the user-facing `EXPLAIN` output format (node names, columns, and worked examples) see [docs/explain.md](docs/explain.md).

Distributed Schema
------------------

See [docs/distributed-schema-architecture.md](docs/distributed-schema-architecture.md) for a full developer reference on how DDL works across a cluster: schema as a replicated state machine over an ordered Raft log, the schema-change delta and the two-version invariant, ack-based convergence, the staged online-schema state machine (`DeleteOnly → WriteOnly → Public`) with a convergence gate between steps, the resumable change coordinator and crash-safe index backfill, follower→leader DDL forwarding with idempotent dedup, positional row encoding (why renames are free), schema-version pinning, the checkpoint persist-failure policy, an invariants checklist, and known limitations.

Database Branching
------------------

Fork a database the way you branch code. `CREATE DATABASE feature_x BRANCH FROM prod` mints an instant, **copy-on-write point-in-time fork**: it shares the parent's bytes until it diverges, so creating a branch copies no row data — only a small amount of schema metadata. Reads on the branch see the parent as of the fork instant, writes are private to the branch, and the parent keeps evolving and never sees the branch.

```sql
-- Instantly fork production into an isolated, writable clone (no data copied)
CREATE DATABASE staging BRANCH FROM prod;

-- Inspect the branch tree
SHOW BRANCHES  FROM prod;     -- every database forked (transitively) from prod, with depth
SHOW ANCESTORS FROM staging;  -- staging's fork chain, immediate parent up to the root

-- Throw it away when done — the parent is untouched
DROP DATABASE staging;
```

Use it for cheap staging clones of production, schema-migration dry-runs, per-PR ephemeral databases, and "what-if" analytics. Branches nest arbitrarily deep, and the fork's frozen view is kept durable by a Raft-replicated snapshot-floor hold — so a long-lived branch keeps reading its parent as of the fork instant even under heavy parent churn.

See [docs/database-branching.md](docs/database-branching.md) for a full developer/operator reference: the ancestry model and read lineage, the copy-on-write overlay and tombstones, frozen-view durability, crash-recovery, and operator guidance (metrics, config knobs, limitations).

Query Result Cache
------------------

Opt a read into an in-memory, per-node result cache with an inline hint: `SELECT * FROM orders {cache=recent_orders}`. An identical later query — same shape, same bound values, same schema — is served from memory without touching storage. The cache is correct before it is fast: a committed write on the same node evicts every dependent entry before it becomes visible to a later read, so a same-node reader never sees stale data. Writes on other nodes are bounded by a per-entry TTL, or eliminated per-hit with `{cache=…, strict}`. Options include `ttl=30s` (units `ms`/`s`/`m`/`h`) and `strict`; entries are dropped manually with `EVICT CACHE 'name'` or `EVICT CACHE ALL`. The feature is on by default (opt-in per query via the hint — nothing without a `{cache=…}` hint is cached) and applies to autocommit single-table `SELECT`s; set `query_result_cache_enabled: false` to disable it entirely.

```sql
-- Cache this result under the "recent_orders" family, expiring after 30 seconds
SELECT id, total FROM orders {cache=recent_orders, ttl=30s} WHERE status = 1;

-- Validate against live storage on every hit (no cross-node staleness window)
SELECT * FROM inventory {cache=stock, strict} WHERE sku = 'ABC-123';

EVICT CACHE 'recent_orders';   -- drop one family for the current database
EVICT CACHE ALL;               -- drop every result-cache entry for the current database
```

See [docs/query-result-cache.md](docs/query-result-cache.md) for a full operator/developer reference: the hint syntax and response metadata, the read/publish path, dependency capture and same-node invalidation, the commit-safe publish gate, TTL and strict validation, fingerprinting, the `query_result_cache_*` config knobs, and known limitations.

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
