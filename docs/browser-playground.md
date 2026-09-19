# Browser playground

The browser playground runs the CamusDB engine inside a browser tab, so a newcomer can try SQL
without installing anything. It is the real engine: the same parser, planner, executor, and embedded
Kahuna node that the server runs, compiled to WebAssembly. Nothing is sent to a server.

The host project is `CamusDB.Wasm/`.

## What it is and what it is not

- It is **one node**. There is no cluster mode, because a browser tab cannot open sockets.
- Data is **in memory**. A page reload, or the **Reset data** button, starts over with the sample
  data set.
- It runs on the **default single-threaded** .NET WebAssembly runtime. It needs no special HTTP
  headers, so any static host can serve it, for example GitHub Pages.
- Features that need a file system or the network are not available: cluster mode, backups, and
  anything that reads or writes files outside memory.

## How it works

`CamusDB.Core` has two target frameworks: `net10.0` and `net10.0-browser`. A project that references
it builds only the target that matches its own framework, so the server and the tests build
`net10.0` only. The browser target:

- resolves the browser build of `Kahuna.Core` (version 1.9.0 or later). That build has no
  ASP.NET Core, no gRPC transports, no RocksDB or SQLite, and no Blake3. It runs Raft scheduling as
  async continuations on the page's event loop, not on worker threads;
- defines `CAMUSDB_BROWSER`. Only two places use it: the cluster constructors of `EmbeddedKahuna`,
  and the RocksDB WAL sizing in `EmbeddedKahunaOptionsBuilder`;
- computes `md5()` with a managed implementation (`Util/Hashes/ManagedMd5.cs`), because .NET on
  WebAssembly has no MD5.

`CamusDB.Wasm/PlaygroundEngine.cs` starts the engine and gives JavaScript three calls:

| Call | What it does |
|---|---|
| `InitAsync()` | Starts the node, the registry, and the executor, and creates the `playground` database. |
| `ExecuteAsync(script)` | Splits the script at each top-level `;` (`SqlScriptSplitter`), runs each statement, and returns a JSON array with one result for each statement. The script stops at the first error. |
| `ResetAsync()` | Disposes the engine and starts a new, empty one. |

Each statement is autocommitted in the same way as the REST API does it. `StatementScope.ReturnsRows`
selects the query path or the non-query path.

The page is in `CamusDB.Wasm/wwwroot/`. `sample.js` holds the sample data set and the example
queries.

## Build and run locally

```sh
dotnet publish CamusDB.Wasm/CamusDB.Wasm.csproj -c Release -o artifacts/wasm
cd artifacts/wasm/wwwroot && python3 -m http.server 8080
```

Then open <http://localhost:8080/>. To deploy, copy the `wwwroot` directory to any static host.

The download is about 12 MB with brotli compression. Trimming is off, because Nixie creates actors
through reflection and the trimmer removes their constructors. Trimming will be turned on when the
dependencies are trim-safe.

## Tests

- `node CamusDB.Wasm/smoke/smoke.mjs artifacts/wasm/wwwroot` runs the published engine under Node,
  which uses the same single-threaded runtime as a browser tab. It checks query results, errors,
  the reset, the sample data set, and every example on the page. CI runs it in the
  `Browser-Playground-Smoke` job.
- `TestSqlScriptSplitter`, `TestStatementScopeReturnsRows`, and `TestManagedMd5` in `CamusDB.Tests`
  cover the shared code on `net10.0`.

## Rules for engine code

The browser runtime has one thread. A blocking wait (`.Wait()`, `.Result`,
`GetAwaiter().GetResult()`, `SemaphoreSlim.Wait()`) cannot be released by another thread there, so it
throws or hangs. Use the async form. The platform compatibility analyzer (CA1416) reports these calls
when it builds the `net10.0-browser` target, so build `CamusDB.Core` itself to see them.
