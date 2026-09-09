# CamusDB: a distributed SQL database written in C#, and why that matters

> Perfection is achieved, not when there is nothing more to add, but when there is nothing left to take away.
>
> — Antoine de Saint-Exupéry

For a long time, the rule was unwritten but clear. If you build a database, you write it in C, C++, Go, Rust, or Java. Those are "systems languages." Everything else is for the application on top.

I want to challenge that rule with a working system, not with an opinion.

CamusDB is an open-source distributed SQL database. It is written in C# on .NET, from the SQL parser to the Raft log to the bytes on disk. As far as I know, it is the first distributed SQL database built on .NET. This article explains why I built it, what it can do today, and how you can try it in five minutes.

## Where it comes from

I spent years in the video game industry, and part of that time with Google Spanner. That is where distributed SQL clicked for me.

In games, the database is not boring infrastructure in the background. It holds inventories, economies, leaderboards, matchmaking state, and live event rewards. A duplicated item or a lost purchase is not a bug report. It is a broken promise to a player who spent time or money.

Spanner showed me that a database can solve those problems at the infrastructure level. Strong consistency and horizontal scale do not have to be a choice. Application teams do not have to reinvent queues, caches, and repair scripts for every feature.

In 2021 I started to ask a question: what would it take to build something like that myself?

The answer took years, and most of it was not the database. First came [Kommander](https://github.com/kahunakv/kommander), a Raft consensus library. Then came [Kahuna](https://github.com/kahunakv/kahuna), a distributed key/value store with locks and transactions on top of Raft. CamusDB is the SQL layer on top of that stack. All three are .NET.

## Why .NET

People ask this question more than any other. The honest answer has two parts.

The first part is personal. I know .NET well. When the tool fits your hand, you move faster and your code stays cleaner.

The second part is technical, and it is the point of this article. Modern .NET is not the .NET of fifteen years ago. The runtime, the JIT, and the garbage collector improved every year. `Span<T>`, `Memory<T>`, `ArrayPool<T>`, and `stackalloc` let you write allocation-free hot paths. `async`/`await` is mature all the way down to the socket. Native AOT exists. The networking stack is fast. Kestrel serves HTTP/2 and gRPC without a proxy in front.

A database is a good test for a runtime. It needs low latency, predictable memory, heavy concurrency, careful I/O, and long uptime. .NET passed that test for me. The whole engine runs in one process: the parser, the cost-based planner, the operators, the transaction layer, the Kahuna storage node, and Raft replication. RocksDB sits underneath for persistence.

Go and Java earned their place in infrastructure with real systems: etcd, CockroachDB, Kafka, Cassandra. I think .NET deserves a seat at that table, and the only way to earn it is to ship a real system. CamusDB is my attempt.

## What CamusDB does today

CamusDB makes a few opinionated choices. I want to be clear about them, because they shape the whole product.

**Serializable by default.** Most databases default to a weaker isolation level and ask you to opt in to correctness. CamusDB does the opposite. Every transaction is serializable unless you ask for less. Pessimistic two-phase locking with range locks blocks phantom reads. Wait-die scheduling avoids deadlocks. Reads run against consistent snapshots. Writes that span partitions use two-phase commit on top of Raft. If a workload does not need serializability, read-committed is available per transaction with `SET TRANSACTION`. But you have to say so.

**Database branching.** Fork a database the way you branch code:

```sql
CREATE DATABASE staging BRANCH FROM prod;
```

The branch is created instantly. It shares the parent's bytes until it diverges, so no row data is copied. Reads on the branch see the parent as of the fork instant. Writes stay private to the branch. The parent never sees the branch. Use it for a staging clone of production, a dry run of a schema migration, a per-pull-request database, or a "what if" experiment on live data.

**Recoverable drops.** `DROP TABLE` and `DROP DATABASE` do not destroy data. The object is unlinked, and a background reclaimer removes it only after a retention window. Until then you can bring it back:

```sql
SHOW ORPHAN TABLES;
CREATE TABLE users RELINK TO '<id>';
```

**Time travel.** The storage layer keeps prior versions of every key, so you can read a table as it was:

```sql
SELECT * FROM leaderboard AS OF SYSTEM TIME '-10s';
```

The whole statement reads one consistent historical snapshot, lock-free, without blocking writers. It is a debugging tool and a recovery tool at the same time.

**A result cache inside the database.** Any read can opt into an in-memory, per-node cache with an inline hint:

```sql
SELECT id, total FROM orders {cache=recent_orders, ttl=30s} WHERE status = 1;
```

A committed write on the same node evicts every dependent entry before the write becomes visible. There is no external cache tier to keep in sync.

Beyond those, you get the things you expect from a SQL database: a cost-based query optimizer, secondary and covering indexes, joins, views and materialized views, check constraints, prepared statements, `EXPLAIN`, row-level TTL, exact vector search over embeddings, and a gRPC API with a .NET client.

## Testing is the product

A database with clever features and a weak correctness story is a liability. So a large part of the work on CamusDB is not features. It is Jepsen-style testing, chaos engineering, fault injection, and degraded-state testing. Nodes get killed mid-commit. Leaders get replaced mid-schema-change. Disks fill up. The suite runs thousands of tests, many of them against a real embedded cluster.

I say this to set expectations. CamusDB runs in production for real workloads, and I am proud of that. But it is also young. Some features are alpha. APIs and storage formats can change between versions. If you try it, you will find rough edges, and I want to hear about them.

## Try it in five minutes

CamusDB ships as a .NET global tool. With the .NET 10 SDK or runtime installed:

```bash
dotnet tool install -g CamusDB.Server
camusdb
```

That is the whole setup. There is no clone, no build, and no configuration file. The node starts with built-in defaults, stores its data under your local application data directory, and serves the JSON/REST API on port 5095 and gRPC on port 5096. Open `http://localhost:5095` in a browser and you get a read-only dashboard.

Then install the SQL shell and talk to it:

```bash
dotnet tool install -g CamusDB.SqlSh
camus-cli -c "Endpoint=http://localhost:5095"
```

```sql
CREATE DATABASE shop;
CREATE TABLE items (id OID PRIMARY KEY, name STRING, price INT64);
INSERT INTO items (id, name, price) VALUES (gen_id(), 'sword', 100);
SELECT * FROM items;
```

If you prefer Docker:

```bash
docker run --rm -p 5095:5095 -p 5096:5096 -v camus-data:/data camusdb/camusdb:latest
```

And if you want to see the distributed part, a three-node cluster is one command from a source checkout:

```bash
docker compose -f docker/local.yml up --build
```

## Talk to it from C#

The .NET client is a multiplexing gRPC client. One connection is shared across your whole application, and many concurrent transactions pipeline over a small pool of streams.

```csharp
using CamusDB.Grpc.Client;

await using CamusConnection conn = CamusConnection.Connect("http://localhost:5096");

await conn.ExecuteNonQueryAsync(
    "shop", "INSERT INTO items (id, name, price) VALUES (gen_id(), 'shield', 80)");

QueryResult result = await conn.ExecuteQueryAsync("shop", "SELECT name, price FROM items");
foreach (ResultRow row in result.Rows)
    Console.WriteLine($"{row.Values[0].StringValue}: {row.Values[1].Int64Value}");
```

A transaction is a session object:

```csharp
CamusTransactionSession tx = await conn.BeginTransactionAsync("shop");
try
{
    await tx.ExecuteNonQueryAsync("UPDATE items SET price = price - 10 WHERE name = 'sword'");
    await tx.ExecuteNonQueryAsync("UPDATE items SET price = price + 10 WHERE name = 'shield'");
    await tx.CommitAsync();
}
catch (CamusGrpcException)
{
    await tx.RollbackAsync();
    throw;
}
```

That transaction is serializable. You did not ask for it. That is the point.

## Why you should care, even if you never run it

If you write .NET for a living, CamusDB is proof that the platform you already know reaches all the way down. You do not have to switch languages to build the storage engine, the consensus layer, or the query planner. You can read the source of your database in the same language as your application, step through it in the same debugger, and extend it with the same NuGet packages.

If you do not write .NET, CamusDB is one more data point that "systems language" is a moving target. The runtimes caught up. The interesting question is no longer which language, but which design.

## Come build it with me

CamusDB is open source and I build it in the open. There is a lot left to do. Databases are never done.

- Try it. Run the commands above and break something. Open an issue with what you found.
- Read the docs at [camusdb.github.io/docs](https://camusdb.github.io/docs/intro). The architecture guide explains the request lifecycle from SQL text to Raft log.
- Read the code at [github.com/camusdb/camusdb](https://github.com/camusdb/camusdb). Start with the query planner or the transaction layer. Both are plain C#.
- Tell me what you would build with database branching or time travel. The best features so far came from real pain, not from a roadmap.

I wanted to build the database I wish I had for the systems I worked on. I also wanted to show that .NET can carry that weight. Both goals are still in progress, and that is the fun part.

Welcome to CamusDB.
