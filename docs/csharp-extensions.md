# C# extensions: functions, triggers and procedures

CamusDB can run your own C# code as SQL scalar functions, as row triggers, and as stored
procedures. The code ships as a .NET assembly (a DLL) that the operator deploys next to the server
and lists in the configuration. The server loads it when it starts.

The DLL is **never stored in the database** and never replicated between nodes. Each node loads
the files its own configuration names. Every node of a cluster must therefore deploy the same
files; the server checks this for triggers (see [Deployment](#deployment)).

- [Writing an extension](#writing-an-extension)
  - [The contract package](#the-contract-package)
  - [Scalar functions](#scalar-functions)
  - [Value mapping](#value-mapping)
  - [Triggers](#triggers)
  - [Procedures](#procedures)
  - [The SQL session](#the-sql-session)
- [Deployment](#deployment)
- [SAFE and UNSAFE assemblies](#safe-and-unsafe-assemblies)
- [Transactions, retries and side effects](#transactions-retries-and-side-effects)
- [Limits and guardrails](#limits-and-guardrails)
- [Observability](#observability)
- [Error codes](#error-codes)

## Writing an extension

### The contract package

An extension is a class library that references the `CamusDB.Extensions.Abstractions` NuGet
package. Target `netstandard2.0` or `net10.0`; the server loads both.

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>netstandard2.0</TargetFramework>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="CamusDB.Extensions.Abstractions" Version="0.16.*" />
  </ItemGroup>
</Project>
```

The package is **versioned with the server**: reference the version of the servers you deploy to. The
server ships its own copy of the contract, so the extension's output folder does not need one. A
server refuses an extension compiled against a newer contract than its own (`CADB0803`): while CamusDB
is 0.x a newer major.minor counts as newer, because a 0.x minor release may break the contract, so
pin `0.16.*` rather than `0.*`; from 1.0 on, a newer major counts. An extension built against the
same or an older version loads. Maintainers publish the package with `scripts/publish-nuget.sh`,
together with the server tool and at the same version; the pack refuses a package version that
differs from the contract assembly's.

The contract assembly holds every type an extension can see: the `[CamusFunction]` and
`[CamusProcedure]` attributes, the `ICamusTrigger` and `ICamusProcedure` interfaces, and the context
and row types. Its XML documentation is the reference for each member. The server always hands an
extension its own copy of the contract, so an extension can ship with any copy of it next to its DLL.
An extension compiled against a newer contract than the server carries is refused at startup (see
above for what "newer" means).

### Scalar functions

Mark a public static method with `[CamusFunction("name")]`:

```csharp
using CamusDB.Extensions;

public static class TextFunctions
{
    [CamusFunction("slugify")]
    public static string Slugify(string value) => /* ... */;

    [CamusFunction("greet", Aliases = new[] { "hello" })]
    public static string Greet(string? name, string greeting = "hello")
        => greeting + " " + (name ?? "stranger");

    [CamusFunction("random_token", IsVolatile = true)]
    public static string RandomToken() => Guid.NewGuid().ToString("N");
}
```

```sql
SELECT slugify(title) FROM posts WHERE slugify(title) = 'hello-world';
SELECT greet(NULL), hello('ann', 'hi');
```

- **Names** are case-insensitive. A name or alias that collides with a built-in function, an
  aggregate (`count`, `sum`, `min`, `max`, `avg`, and the other aggregate names the engine reserves),
  a table function, or a function of another loaded extension stops the server at startup. An extension
  never replaces a built-in.
- **NULL.** A parameter of type `Nullable<T>` (`long?`) or a reference type annotated nullable
  (`string?`, `byte[]?`) receives SQL NULL as `null`. For any other parameter, a NULL argument makes
  the result NULL, and the method is not called. Returning `null` returns SQL NULL.
- **Optional parameters** make a range of accepted argument counts. A missing argument takes the
  parameter's default value.
- **Volatility.** Set `IsVolatile = true` when the result can differ for the same arguments. A
  volatile function bypasses the query result cache and is refused in a CHECK constraint. A
  **non-volatile** function in a column `DEFAULT` is evaluated **once, when the table is created**,
  and stored as a constant; a volatile one is evaluated for each inserted row. Mark a function
  volatile if each row needs a new value.
- **Errors.** An exception from the method ends the statement with `CADB0809` and the function name
  and message. A wrong argument type is reported before the method runs, with the function name and
  argument position.
- **Cost.** The server compiles one typed adapter per method when it loads the extension. A call then
  costs one delegate invocation, the same as a built-in function: there is no reflection on the call
  path.
- **Thread safety.** Concurrent statements call the method at the same time.
- **Where a call runs.** A function call runs on the node that coordinates the statement. The server
  never ships an expression that calls an extension function to a peer node in a distributed scan,
  because a peer may not load the same file. A filter that calls one is evaluated on the
  coordinator.

### Value mapping

| SQL type | CLR type |
| --- | --- |
| `INT64` | `long` |
| `FLOAT64` | `double` (an `INT64` argument converts) |
| `FLOAT32` | `float` |
| `BOOL` | `bool` |
| `STRING` | `string` |
| `OID` (Id) | `string` (the ObjectId text) |
| `UUID` | `Guid` (a `STRING` argument in UUID form converts) |
| `BYTES` (also vectors) | `byte[]` |
| `DATE`, `DATETIME` | `DateTime` in UTC; a returned `DateTime` is a `DATETIME` |
| `NUMERIC` | `decimal` (an `INT64` argument converts) |

Arrays are not mapped in this version; a parameter or return type outside the table stops the server
at startup with the method name. JSON values are `STRING`.

**NUMERIC is never truncated.** NUMERIC holds 38 digits with 9 after the point; a .NET `decimal`
holds 28 or 29 significant digits. A NUMERIC value whose digits do not fit a `decimal` fails with
`CADB0810` instead of losing digits. A returned `decimal` with more than 9 fractional digits rounds
half away from zero, the same as text cast to NUMERIC.

### Triggers

A trigger is a public class with a public parameterless constructor that implements
`ICamusTrigger`:

```csharp
public sealed class AuditTrigger : ICamusTrigger
{
    public Task FireAsync(ITriggerContext context)
    {
        IRow? row = (IRow?)context.New ?? context.Old;

        return context.Sql.ExecuteAsync(
            "INSERT INTO audit_log (op, tbl, name) VALUES (@op, @tbl, @name)",
            new Dictionary<string, object?>
            {
                ["op"] = context.Operation.ToString(),
                ["tbl"] = context.TableName,
                ["name"] = row?.GetString("name"),
            });
    }
}
```

```sql
CREATE TRIGGER audit ON orders AFTER INSERT, UPDATE, DELETE EXTERNAL NAME 'MyCompany.Ext.AuditTrigger';
CREATE TRIGGER normalize ON orders BEFORE INSERT, UPDATE EXTERNAL NAME 'MyCompany.Ext.NormalizeTrigger';
DROP TRIGGER audit ON orders;
```

`EXTERNAL NAME` is the full CLR type name. The words `TRIGGER`, `BEFORE`, `AFTER`, `EXTERNAL` and
`NAME` are not reserved: they stay usable as table and column names.

**Firing.**

- A trigger fires **once per affected row**, inside the statement's transaction, on the node that
  runs the statement.
- A **BEFORE** trigger fires before the row is validated and written. In INSERT and UPDATE it can
  change `context.New`; the changed row is converted to the column types and must still pass NOT
  NULL, length and CHECK rules, exactly as if the statement had supplied the values. In BEFORE
  INSERT the row has no row id yet. A BEFORE DELETE trigger cannot change the row, but it can veto
  the delete by throwing.
- An **AFTER** trigger fires after the whole statement wrote its rows and passed its foreign-key
  checks, once per row, in statement order. It sees the final rows and cannot change them. A
  statement that writes in several chunks (a large `INSERT … SELECT`) still fires its AFTER triggers
  only after the last chunk. The statement keeps those rows in memory until then, charged against the
  per-operator budget `spill_threshold_bytes`; past it the statement fails with `CADB0826`.
- A BEFORE UPDATE trigger can change any column, so the update prepares every foreign-key check of the
  table, and a NULL that the statement assigns to a NOT NULL column is checked after the trigger ran:
  a trigger can repair it. The final row must still pass every rule.
- **A BEFORE trigger's SQL must not change a row its statement already holds.** UPDATE and DELETE
  read a chunk of rows, run the BEFORE triggers row by row, then write the chunk. If a BEFORE
  trigger's SQL updates or deletes another row of that same chunk, the statement fails with
  `CADB0825` instead of writing from a stale copy of that row. A change to a row of an earlier chunk,
  or any change from an AFTER trigger, is allowed.
- `OLD` and `NEW` carry **every column** of the row, large values included.
- Triggers fire in creation order.

**What fires.** SQL `INSERT` (with `VALUES` or `SELECT`), `UPDATE`, `DELETE` (with or without
`RETURNING`), DML run by a procedure or by another trigger, and the typed insert, update and delete
APIs over HTTP and gRPC.

**What does not fire.** Row work that the engine does for itself: `DROP TABLE`, `TRUNCATE`, TTL
expiry, the backfill of `ALTER TABLE … ADD COLUMN`, the rewrite of `DROP COLUMN` and
`ALTER TABLE … REWRITE STORAGE`, `CREATE TABLE … AS SELECT`, materialized-view refresh, and restore.

**Where triggers can be created.** On plain tables only, not on views or materialized views. A dropped
table that is recovered comes back without its triggers, the same as without its foreign keys;
re-create them.

**Threading and state.** The server creates **one instance per engine** and shares it between
concurrent statements. Keep no per-statement state in fields. Static state is per engine too: each
engine loads each extension into its own load context.

**Privileges.** `CREATE TRIGGER` and `DROP TRIGGER` need the `ALTER` privilege on the table, the same
as `ALTER TABLE`.

**Cluster mode.** Trigger DDL replicates through the schema log to every node. It must run on the
database's **schema leader**, the same as view and sequence DDL; a follower refuses it with an error
that names the leader.

### Procedures

A procedure is a public class with a public parameterless constructor, marked
`[CamusProcedure("name")]`, that implements `ICamusProcedure`:

```csharp
[CamusProcedure("transfer", MinArguments = 3, MaxArguments = 3)]
public sealed class Transfer : ICamusProcedure
{
    public async Task ExecuteAsync(IProcedureContext context)
    {
        string from = (string)context.Arguments[0]!;
        string to = (string)context.Arguments[1]!;
        long amount = (long)context.Arguments[2]!;

        await context.Sql.ExecuteAsync("UPDATE accounts SET balance = balance - @a WHERE name = @n",
            new Dictionary<string, object?> { ["a"] = amount, ["n"] = from });
        await context.Sql.ExecuteAsync("UPDATE accounts SET balance = balance + @a WHERE name = @n",
            new Dictionary<string, object?> { ["a"] = amount, ["n"] = to });
    }
}
```

```sql
CALL transfer('alice', 'bob', 30);
CALL transfer(@from, @to, 10 * 3);
```

- Arguments are evaluated before the procedure runs (parameters, expressions and functions are
  allowed) and arrive as values of the [value mapping](#value-mapping), or `null` for SQL NULL. A
  call outside `MinArguments`..`MaxArguments` fails before the procedure runs.
- The procedure runs inside the statement's transaction. Everything it writes commits or rolls back
  with the `CALL`; an exception rolls back all of it and fails with `CADB0822`.
- `CALL` reports the total number of rows its SQL changed. It returns no result set in this version.
- `CALL` is a no-rows statement: send it to `/execute-sql-non-query` or to gRPC `ExecuteNonQuery`.
  It is not preparable.
- `CALL` requires a **superuser** in this version, because there is no grant model for procedures
  yet. `CALL` is a plain identifier, not a reserved word.

### The SQL session

`context.Sql` runs SQL inside the transaction of the statement that called your code:

- `ExecuteAsync` runs `INSERT`, `UPDATE` or `DELETE` and returns the number of changed rows.
  `QueryAsync` runs `SELECT` and returns copies of the rows, which stay valid after the call.
- DDL and transaction control (`BEGIN`, `COMMIT`, `ROLLBACK`) are refused with `CADB0820`.
- The SQL runs **with the privileges of the user** whose statement fired the trigger or called the
  procedure.
- A placeholder `@name` takes the dictionary entry `name` (with or without the `@`).
- Writes count against the transaction's mutation limit (`max_mutations_per_transaction`), and fire
  the triggers of the tables they write.
- An error raised by the session's own SQL keeps its error code when your code lets it escape: a
  duplicate key stays a duplicate key.
- **An error from the session ends the calling statement, even if your code catches it.** There are
  no savepoints: a failed statement can leave part of its writes staged (a foreign-key check runs
  after the row writes). So the session marks the transaction rollback-only, and a trigger or
  procedure that catches the error and returns still fails its statement with `CADB0823`. A commit of
  that transaction is refused with `CADB0824`.
- Byte arrays are copies in both directions. Changing an array that `GetBytes`, a function argument
  or a procedure argument gave you changes nothing stored, and the server copies every array you
  hand it.

## Deployment

Two configuration lists name the files to load:

```yaml
extension_assemblies:          # SAFE: checked before loading
  - /opt/camusdb/extensions/MyCompany.Ext.dll
unsafe_extension_assemblies:   # UNSAFE: loaded without the check
  - /opt/camusdb/extensions/MyCompany.Reviewed.dll
```

The command-line flags are `--extension-assemblies` and `--unsafe-extension-assemblies` (space
separated). See [configuration.md](configuration.md).

- Paths must be **absolute**. A relative path, a missing file, a file that is not a .NET assembly, a
  path listed twice, and two files with the same assembly name each stop the server at startup with
  a `CADB080x` error that names the path. A node never starts without code it was told to load.
- Both settings are restart-class: the server reads them once, when it starts. To change an
  extension, replace the file and restart the node.
- A private dependency of an UNSAFE extension resolves from the extension's own folder (through its
  `.deps.json` when there is one). Each extension loads into its own load context, so two
  extensions can carry different versions of one dependency.
- **Every node must deploy the same files.** A trigger records the SHA-256 of the assembly that
  defined it on the node that created it. A node that does not load that type, or loads a build with
  another hash, refuses DML on the table with `CADB0817` instead of firing different code. **The hash
  covers that one assembly file only.** The private dependencies of an UNSAFE extension, which load
  from its folder, are not hashed: two nodes with the same extension file and different dependency
  files are not told apart. Deploy the whole extension folder as one unit. To upgrade
  a trigger's extension, deploy the new file on every node, restart them, then drop and re-create the
  trigger. Functions and procedures have no such check in this version: a function never runs on a
  peer node, but two nodes with different builds give different answers to the statements they
  coordinate.
- The browser playground has no extensions.
- The logical dump does not carry triggers; see
  [logical-dump-and-reimport.md](logical-dump-and-reimport.md).

## SAFE and UNSAFE assemblies

A file in `extension_assemblies` is **SAFE**. Before the server loads it, it reads the assembly's
metadata tables (no extension code runs) and refuses the file when it finds any of these:

- a reference to an assembly outside the allow-list: the contract and the compute-only framework
  facades (`netstandard`, `System.Runtime`, `System.Collections`, `System.Linq`, `System.Memory`,
  `System.Threading`, `System.Text.RegularExpressions`, and a few more);
- a type in `System.Net`, `System.Runtime.Loader`, `System.Runtime.InteropServices` or
  `System.Reflection.Emit`;
- the types `File`, `FileStream`, `Directory`, `FileInfo`, `DirectoryInfo`, `Process`, `AppDomain`,
  `Activator` and `Assembly`;
- the members `Environment.Exit`, `Environment.SetEnvironmentVariable`, `Type.GetType`,
  `MethodBase.Invoke` and `GC.Collect`;
- a P/Invoke declaration (`DllImport`), or code compiled with `/unsafe`.

The error lists every finding at once. At run time, the SAFE extension's load context also refuses
any assembly outside the allow-list, so a dynamic load cannot go around the check.

A file in `unsafe_extension_assemblies` skips both checks. The server logs a warning for it at
startup, and `SHOW EXTENSIONS` reports its trust as `unsafe`.

**SAFE is a guardrail, not a sandbox.** .NET has no in-process isolation: an extension runs inside
the server process with the server's rights. The SAFE check catches accidental use of a dangerous
API and gives a reviewer a short list to read; it does not stop code that loops forever or allocates
without limit. Deploy only code you have reviewed. Your control over which files the server loads is
the real security boundary.

## Transactions, retries and side effects

- What your code writes **through `context.Sql`** is part of the statement's transaction. It commits
  with the statement and rolls back with it, exactly once in effect.
- The server can run a statement **more than once**. In autocommit mode a serializable conflict
  replays the statement from a new transaction, and a schema change in progress can make a
  transaction retry. A statement is retried inside the same transaction only when the failed attempt
  wrote nothing; once a statement or its triggers wrote, the retry starts from a new transaction
  (`CADB0504`). Each failed attempt rolls back its in-database effects, so they occur once. Any
  effect **outside the database** (a network call, a file, a log line, a counter in memory) occurs
  **at least once**. Make such effects idempotent, or do them after the transaction commits.
- A trigger or procedure error inside an explicit transaction (`BEGIN … COMMIT`) rolls back the whole
  transaction, as any statement error does; there are no savepoints.

## Limits and guardrails

- **Nesting.** Triggers and procedures can nest — a trigger's SQL can fire another trigger — up to 16
  levels. Deeper nesting, for example a trigger that writes to its own table, fails the statement
  with `CADB0819`.
- **Mutation limit.** Trigger and procedure writes count against `max_mutations_per_transaction`. The
  rows an AFTER trigger waits for are bounded by the same limit.
- **Cancellation is cooperative.** The contexts carry a `CancellationToken`. A statement that writes
  is not cancellable once it starts writing, so the token does not fire in this version; observe it
  anyway.
- **Runaway code cannot be stopped.** A loop that never returns holds its statement forever, and a
  stack overflow ends the server process. Neither can be caught.
- **Aggregates, table functions and arrays** are not supported as extensions in this version.

## Observability

`SHOW EXTENSIONS` lists the assemblies the node loaded, one row per assembly: `name`, `version`,
`path`, `sha256`, `trust` (`safe` or `unsafe`), and the comma-separated `functions`, `triggers` and
`procedures` it registered. A node without extensions returns no rows. The statement is node-local
(compare the `sha256` column across nodes to find a node with another build) and requires a superuser.

The triggers of a table are part of its schema record. The server also logs each loaded extension
and its members at startup.

## Error codes

| Code | Meaning |
| --- | --- |
| `CADB0800` | An extension path is relative or listed twice (startup) |
| `CADB0801` | An extension file does not exist (startup) |
| `CADB0802` | An extension file is not a loadable .NET assembly (startup) |
| `CADB0803` | An extension was built against a newer contract than the server's (startup) |
| `CADB0804` | Two extension files have the same assembly name (startup) |
| `CADB0805` | A function or procedure name collides with a built-in or another extension (startup) |
| `CADB0806` | A SAFE extension failed the SAFE check; the message lists every finding (startup) |
| `CADB0807` | A SAFE extension tried to load an assembly outside the allow-list (run time) |
| `CADB0808` | An extension member has a shape the server cannot bind (startup) |
| `CADB0809` | An extension function threw (HTTP 400) |
| `CADB0810` | A NUMERIC value does not fit a `decimal`, or the reverse (HTTP 400) |
| `CADB0811` | `CREATE TRIGGER` named a type that no loaded extension defines (HTTP 400) |
| `CADB0812` | `CREATE TRIGGER` named a type that two extensions define (HTTP 400) |
| `CADB0813` | `CREATE TRIGGER` named a type that is not a trigger (HTTP 400) |
| `CADB0814` | The table already has a trigger with that name (HTTP 409) |
| `CADB0815` | `DROP TRIGGER` named a trigger the table does not have (HTTP 404) |
| `CADB0816` | The target of `CREATE TRIGGER` is a view or a materialized view (HTTP 400) |
| `CADB0817` | This node does not load the trigger's extension, or loads another build (HTTP 503) |
| `CADB0818` | A trigger threw; nothing the statement wrote persists (HTTP 400) |
| `CADB0819` | Triggers and procedures nested more than 16 levels (HTTP 400) |
| `CADB0820` | The SQL session refused a statement that is not SELECT, INSERT, UPDATE or DELETE (HTTP 400) |
| `CADB0821` | `CALL` named a procedure that no loaded extension defines (HTTP 404) |
| `CADB0822` | A procedure threw; everything the `CALL` wrote rolls back (HTTP 400) |
| `CADB0823` | A trigger or procedure caught an error from its own SQL and returned; the statement fails (HTTP 400) |
| `CADB0824` | A commit of a transaction in which an extension's SQL failed; roll it back (HTTP 409) |
| `CADB0825` | A BEFORE trigger's SQL changed a row its statement already read and is still writing (HTTP 409) |
| `CADB0826` | The rows kept for AFTER triggers exceed `spill_threshold_bytes` (HTTP 400) |
