# Browser playground

The browser playground runs the CamusDB engine inside a browser tab, so a newcomer can try SQL
without installing anything. It is the real engine: the same parser, planner, executor, and embedded
Kahuna node that the server runs, compiled to WebAssembly. Nothing is sent to a server.

The host project is `CamusDB.Wasm/`.

## What it is and what it is not

- It is **one node by default**, and a **cluster of three** when the visitor asks for it. The
  cluster runs inside the one tab over an in-memory transport; a tab still cannot open a socket, so
  it cannot reach another host or another tab.
- Data is **in memory**. A page reload, or the **Reset data** button, starts over with the sample
  data set.
- It runs on the **default single-threaded** .NET WebAssembly runtime. It needs no special HTTP
  headers, so any static host can serve it, for example GitHub Pages.
- Features that need a file system or the network are not available: backups, and anything that
  reads or writes files outside memory.

## How it works

`CamusDB.Core` has two target frameworks: `net10.0` and `net10.0-browser`. A project that references
it builds only the target that matches its own framework, so the server and the tests build
`net10.0` only. The browser target:

- resolves the browser build of `Kahuna.Core` (version 1.9.2 or later). That build has no
  ASP.NET Core, no gRPC transports, no RocksDB or SQLite, and no Blake3. It runs Raft scheduling as
  async continuations on the page's event loop, not on worker threads;
- defines `CAMUSDB_BROWSER`. Only two places use it: `EmbeddedKahuna.CreateCluster`, which builds
  the gRPC transports, and the RocksDB WAL sizing in `EmbeddedKahunaOptionsBuilder`. The
  `EmbeddedKahuna` constructor that **takes** transports is in the browser build, which is what
  lets several nodes in one tab share the in-memory ones;
- computes `md5()` with a managed implementation (`Util/Hashes/ManagedMd5.cs`), because .NET on
  WebAssembly has no MD5.

`CamusDB.Wasm/PlaygroundEngine.cs` is the JavaScript surface. It holds a `SingleNodeSession`, and a
`ClusterSession` once the visitor starts one. Every call goes through one gate, so a script never
interleaves with another script, and a node never starts or stops while a statement runs on it.

Single node:

| Call | What it does |
|---|---|
| `InitAsync()` | Starts the node, the registry, and the executor, and creates the `playground` database. |
| `ExecuteAsync(script)` | Splits the script at each top-level `;` (`SqlScriptSplitter`), runs each statement, and returns a JSON array with one result for each statement. The script stops at the first error. |
| `ExecuteInDatabaseAsync(script, database)` | The same, against another database. |
| `ResetAsync()` | Disposes the engine and starts a new, empty one. |

Cluster:

| Call | What it does |
|---|---|
| `StartClusterAsync(nodeCount)` | Starts that many engines over one pair of in-memory transports, waits for a leader on every partition, creates the `playground` database, and returns the status. A cluster that already runs is stopped first. At most 5 nodes. |
| `StopClusterAsync()` | Stops every node and frees the cluster. |
| `ExecuteOnNodeAsync(index, script, database)` | Runs the script on one node. |
| `ClusterStatusAsync()` | `{"partitionCount", "nodes":[{"index","name","endpoint","running","leads":[…],"blockedTo":[…]}]}`. |
| `StopNodeAsync(index)` / `StartNodeAsync(index)` | Stops a node as if its host had crashed, or starts it again empty so it catches up. A stop that would leave fewer than a majority running is refused. |
| `LeaderOfPartitionAsync(partitionId)` | Waits until some running node claims the partition, and returns its index. |
| `BlockLinkAsync(from, to)` / `RestoreLinksAsync()` | Drops the traffic in one direction, or restores every blocked link. A node whose links are cut keeps running and keeps campaigning, which is what makes a network partition different from a crash. |

Each statement is autocommitted in the same way as the REST API does it (`StatementRunner`).
`StatementScope.ReturnsRows` selects the query path or the non-query path.

The page is in `CamusDB.Wasm/wwwroot/`. `sample.js` holds the sample data set and the example
queries.

## The cluster in one tab

`ClusterSession` builds each member the way the cluster test harness builds one: an
`EmbeddedKahuna` over a shared Kahuna `MemoryInterNodeCommmunication` and a shared Kommander
`InMemoryCommunication`, with a `StaticDiscovery` roster of its peers. Every member is a full node,
so the cluster elects leaders, replicates DDL through Raft and fails over; only the transport is in
memory.

The three node-to-node seams are shipped classes in `CamusDB.Core`, not playground code:

| Seam | Interface | In the server | In one process |
|---|---|---|---|
| DDL forwarded to the schema leader | `ISchemaDdlForwarder` | `HttpSchemaDdlForwarder` | `InProcessSchemaDdlForwarder` |
| A follower's schema ack | `ISchemaAckSender` | the same HTTP class | the same in-process class |
| A query fragment on a peer | `IQueryFragmentTransport` | `HttpQueryFragmentTransport` | `InProcessQueryFragmentTransport` |

Both in-process classes resolve their target through `InProcessClusterNodes`, which maps a Raft
endpoint to that member's `CommandExecutor`. The cluster test suite uses the same three classes, so
the playground runs a path the tests cover. The fragment transport keeps the wire round trip on
purpose: it encodes each request as UTF-8 JSON and each returned frame through both codecs of
`QueryFragmentWireCodec`, so an encoding bug cannot hide behind an in-process shortcut.

The Raft timings are Kahuna's embedded defaults — a 100 ms heartbeat and a 500 to 1500 ms election
timeout — because all three nodes share the page's one event loop. The fast single-node timings
would start elections that no fault caused. The single-node session keeps its own fast timings,
which are safe only because one node wins its own election uncontested.

### What it costs

Measured in headless Chromium 146 on an Apple M-series laptop, over the published Release build.
Memory is the WebAssembly linear heap, which only grows: it is a high-water mark, not live data.

| | 1 node | 3 nodes |
|---|---|---|
| Time to start the engine | 0.7 s | 3.6 s |
| Heap after the runtime loads | 32–38 MB | 32–38 MB |
| Heap after the engine starts | 55 MB | 67 MB |
| Heap after the sample data set | 67 MB | 80 MB |
| `INSERT`, median of 20 | 9.1–9.4 ms | 10.2–11.3 ms |
| `SELECT COUNT(*)`, median of 20 | 4.6 ms | 4.7–5.0 ms |
| Leader changes over 60 idle seconds | — | 0 |

Two nodes therefore cost about 12 MB more than one, and a statement costs about 1 ms more. The
start is the real difference, so single-node stays the default and the cluster starts only when the
visitor presses the button. Kommander's `IRaft` does not expose the Raft term, so the stand-in for
a needless election is the leader of each partition: a leader that moves with no fault to explain
it.

## Build and run locally

```sh
dotnet publish CamusDB.Wasm/CamusDB.Wasm.csproj -c Release -o artifacts/wasm
cd artifacts/wasm/wwwroot && python3 -m http.server 8080
```

Then open <http://localhost:8080/>. To deploy, copy the `wwwroot` directory to any static host.

The download is about 4.4 MB with brotli compression, with trimming on. Trimming needs trim-safe
dependencies: Nixie 1.3.2 annotates the actor constructors it creates by reflection, and Kahuna.Core
1.9.2 gives Kommander and Kahuna source-generated JSON contexts. Before 1.9.2, a trimmed build either
failed to create an actor or hung for 60 s in `JoinCluster`, because a trimmed application turns off
reflection-based `System.Text.Json`.

A trimmed publish leaves trimmed framework files in `CamusDB.Wasm/obj`. Delete `obj/` and `bin/`
before a publish that turns trimming off again, or the runtime fails with a `TypeLoadException`.

## Tests

- `node CamusDB.Wasm/smoke/smoke.mjs artifacts/wasm/wwwroot` runs the published engine under Node,
  which uses the same single-threaded runtime as a browser tab. It checks query results, errors,
  the reset, the sample data set, and every example on the page. CI runs it in the
  `Browser-Playground-Smoke` job.
- `node CamusDB.Wasm/smoke/smoke-cluster.mjs artifacts/wasm/wwwroot` starts three nodes in that
  runtime, creates a table on one and reads it on the others, stops the leader of partition 0 and
  writes again, restarts the stopped node and reads every row through it, and then watches for 10 s
  that no leader moves. CI runs it in the same job.
- `TestSqlScriptSplitter`, `TestStatementScopeReturnsRows`, and `TestManagedMd5` in `CamusDB.Tests`
  cover the shared code on `net10.0`.

## Rules for engine code

The browser runtime has one thread. A blocking wait (`.Wait()`, `.Result`,
`GetAwaiter().GetResult()`, `SemaphoreSlim.Wait()`) cannot be released by another thread there, so it
throws or hangs. Use the async form. The platform compatibility analyzer (CA1416) reports these calls
when it builds the `net10.0-browser` target, so build `CamusDB.Core` itself to see them.
