# Browser playground

The browser playground is the CamusDB engine in a browser tab. A newcomer can try SQL before
installing anything. It is the real engine, compiled to WebAssembly. The code from the parser to the
executor is the server's code, and so is the embedded Kahuna node. No data is sent to a
server.

The host project is `CamusDB.Wasm/`.

## What it is and what it is not

- It is **one node by default**, and a **cluster of three** when the visitor asks for it. The
  cluster lives in the one tab and uses an in-memory transport. A tab cannot open a socket. This
  means that the playground cannot reach another host or another tab.
- Data is **in memory**. A page reload, or the **Reset data** button, starts over with the sample
  data set.
- It uses the **default single-threaded** .NET WebAssembly runtime. It needs no special HTTP
  headers. Any static host can serve it, for example GitHub Pages.
- Features that need a file system or the network are not available. Examples are backups and C#
  extensions. C# functions and triggers load assemblies from files, and so do `CALL`
  procedures. None of them work in the playground. The same applies to anything that reads or writes files outside memory.

## How it works

`CamusDB.Core` has two target frameworks: `net10.0` and `net10.0-browser`. A project that references
it builds the one target that matches its framework. The server and the tests therefore build
`net10.0` only. The browser target differs from the server target in three ways.

- It resolves the browser build of `Kahuna.Core` (version 1.9.2 or later). That build leaves out
  ASP.NET Core and the gRPC transports. It also leaves out RocksDB and SQLite. Blake3 is not
  included either. Raft scheduling is done
  by async continuations on the page's event loop. Worker threads are not used.
- It defines `CAMUSDB_BROWSER`. Two places use it: `EmbeddedKahuna.CreateCluster`, which builds
  the gRPC transports, and the RocksDB WAL sizing in `EmbeddedKahunaOptionsBuilder`. The
  `EmbeddedKahuna` constructor that **takes** transports is in the browser build. That constructor
  lets several nodes in one tab share the in-memory transports.
- It computes `md5()` with a managed implementation (`Util/Hashes/ManagedMd5.cs`). .NET on
  WebAssembly does not provide MD5.

`CamusDB.Wasm/PlaygroundEngine.cs` is the JavaScript surface. It owns a `SingleNodeSession`, and a
`ClusterSession` once the visitor starts one. All calls go through one gate. A script cannot
interleave with another script, and a node does not start or stop while a statement executes on it.

The single-node session exposes four calls.

| Call | What it does |
|---|---|
| `InitAsync()` | Starts the node with its registry and executor. It then creates the `playground` database. |
| `ExecuteAsync(script)` | Splits the script at each top-level `;` (`SqlScriptSplitter`). It executes each statement and returns a JSON array with one result for each statement. The script stops at the first error. |
| `ExecuteInDatabaseAsync(script, database)` | The same, against another database. |
| `ResetAsync()` | Disposes the engine and starts a new, empty one. |

The cluster session adds calls to start and stop nodes and to cut links between them.

| Call | What it does |
|---|---|
| `StartClusterAsync(nodeCount)` | Starts that many engines over one pair of in-memory transports and waits for a leader on each partition. It then creates the `playground` database and returns the status. A cluster that is already up is stopped first. At most 5 nodes. |
| `StopClusterAsync()` | Stops all nodes and frees the cluster. |
| `ExecuteOnNodeAsync(index, script, database)` | Executes the script on one node. |
| `ClusterStatusAsync()` | `{"partitionCount", "nodes":[{"index","name","endpoint","running","leads":[…],"blockedTo":[…]}]}`. |
| `StopNodeAsync(index)` / `StartNodeAsync(index)` | Stops a node as if its host had crashed, or starts it again empty so it catches up. A stop that would leave fewer than a majority running is not allowed. |
| `LeaderOfPartitionAsync(partitionId)` | Waits until some running node claims the partition, and returns its index. |
| `BlockLinkAsync(from, to)` / `RestoreLinksAsync()` | Drops the traffic in one direction, or restores all blocked links. A node whose links are cut continues to run and to campaign. That is the difference between a network partition and a crash. |

Each statement is autocommitted in the same way as the REST API does it (`StatementRunner`).
`StatementScope.ReturnsRows` selects the query path or the non-query path.

The page is in `CamusDB.Wasm/wwwroot/`. The sample data set and the example queries are in
`sample.js`.

## The cluster in one tab

`ClusterSession` builds each member the way the cluster test harness builds one. Each member is an
`EmbeddedKahuna` over a shared Kahuna `MemoryInterNodeCommmunication` and a shared Kommander
`InMemoryCommunication`, with a `StaticDiscovery` roster of its peers. Each member is a full node.
The cluster elects leaders and fails over. DDL is replicated through Raft. The transport is the one
part that is in memory.

The three node-to-node seams are shipped classes in `CamusDB.Core`. The playground does not add
separate code for them.

| Seam | Interface | In the server | In one process |
|---|---|---|---|
| DDL forwarded to the schema leader | `ISchemaDdlForwarder` | `HttpSchemaDdlForwarder` | `InProcessSchemaDdlForwarder` |
| A follower's schema ack | `ISchemaAckSender` | the same HTTP class | the same in-process class |
| A query fragment on a peer | `IQueryFragmentTransport` | `HttpQueryFragmentTransport` | `InProcessQueryFragmentTransport` |

Both in-process classes resolve their target through `InProcessClusterNodes`, which maps a Raft
endpoint to that member's `CommandExecutor`. The cluster test suite uses the same three classes.
This means that the playground exercises a path the tests cover. The fragment transport does the
wire round trip on purpose. It encodes each request as UTF-8 JSON and each returned frame through
both codecs of `QueryFragmentWireCodec`. An encoding bug therefore cannot hide behind an in-process
shortcut.

The Raft timings are Kahuna's embedded defaults: a 100 ms heartbeat and a 500 to 1500 ms election
timeout. All three nodes share the page's one event loop. The fast single-node timings would start
elections that no fault caused. The single-node session uses the fast timings. They are safe there
because one node wins its election uncontested.

### What it costs

Measured in headless Chromium 146 on an Apple M-series laptop, over the published Release build.
Memory is the WebAssembly linear heap, which only grows. The values are high-water marks of the
heap, and live data can be smaller.

| | 1 node | 3 nodes |
|---|---|---|
| Time to start the engine | 0.7 s | 3.6 s |
| Heap after the runtime loads | 32–38 MB | 32–38 MB |
| Heap after the engine starts | 55 MB | 67 MB |
| Heap after the sample data set | 67 MB | 80 MB |
| `INSERT`, median of 20 | 9.1–9.4 ms | 10.2–11.3 ms |
| `SELECT COUNT(*)`, median of 20 | 4.6 ms | 4.7–5.0 ms |
| Leader changes over 60 idle seconds | — | 0 |

Two extra nodes therefore cost about 12 MB more than one node, and a statement costs about 1 ms more. The
start is the real difference. For this reason, single-node is the default. The cluster starts only after
the visitor presses the button. Kommander's `IRaft` does not expose the Raft term. The stand-in
for a needless election is therefore the leader of each partition: a leader that moves when no
fault explains it.

## Build and run locally

```sh
dotnet publish CamusDB.Wasm/CamusDB.Wasm.csproj -c Release -o artifacts/wasm
cd artifacts/wasm/wwwroot && python3 -m http.server 8080
```

Then open <http://localhost:8080/>. To deploy, copy the `wwwroot` directory to any static host.

The download is about 4.4 MB with brotli compression, with trimming on. Trimming needs trim-safe
dependencies. Nixie 1.3.2 annotates the actor constructors it creates by reflection, and Kahuna.Core
1.9.2 gives Kommander and Kahuna source-generated JSON contexts. A trimmed application turns off
reflection-based `System.Text.Json`. Before 1.9.2, a trimmed build either failed to create an actor
or hung for 60 s in `JoinCluster`.

A trimmed publish leaves trimmed framework files in `CamusDB.Wasm/obj`. Delete `obj/` and `bin/`
before a publish that turns trimming off again. Otherwise the runtime throws a `TypeLoadException`.

## Tests

- `node CamusDB.Wasm/smoke/smoke.mjs artifacts/wasm/wwwroot` loads the published engine in Node,
  which uses the same single-threaded runtime as a browser tab. It checks query results and errors.
  It also checks the reset and the sample data set. Each example on the page is checked too. The CI job
  `Browser-Playground-Smoke` executes it.
- `node CamusDB.Wasm/smoke/smoke-cluster.mjs artifacts/wasm/wwwroot` starts three nodes in that
  runtime. It creates a table on one node and reads it on the others. It stops the leader of
  partition 0 and writes again. It then restarts the stopped node and reads all rows through it.
  Last, it watches for 10 s that no leader moves. The same CI job executes it.
- Three test classes in `CamusDB.Tests` cover the shared code on `net10.0`:
  `TestSqlScriptSplitter`, `TestStatementScopeReturnsRows`, and `TestManagedMd5`.

## Rules for engine code

The browser runtime has one thread. There, a blocking wait (`.Wait()`, `.Result`,
`GetAwaiter().GetResult()`, `SemaphoreSlim.Wait()`) cannot be released by another thread. It throws
or hangs. Use the async form. The platform compatibility analyzer (CA1416) flags these calls when it
builds the `net10.0-browser` target. To see them, build `CamusDB.Core` itself.
