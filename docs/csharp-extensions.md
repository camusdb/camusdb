# C# extensions: functions, aggregates, table functions, triggers and procedures

CamusDB can run your own C# code as SQL scalar functions, as aggregate functions, as table
functions in `FROM`, as row triggers, and as stored procedures. The code ships as a .NET assembly (a DLL) that the operator deploys next to the server
and lists in the configuration. The server loads it when it starts.

The DLL is **never stored in the database** and never replicated between nodes. Each node loads
the files its own configuration names, which makes the extension **available** on that node. A
database uses an extension only after `CREATE EXTENSION` **installs** it there; the installed record
names the version and the SHA-256 of the build, and every node runs that exact build or refuses the
statements that need it (see [Installing an extension in a database](#installing-an-extension-in-a-database)).

- [Writing an extension](#writing-an-extension)
  - [The contract package](#the-contract-package)
  - [The manifest](#the-manifest)
  - [Scalar functions](#scalar-functions)
  - [Value mapping](#value-mapping)
  - [Table functions](#table-functions)
  - [Aggregate functions](#aggregate-functions)
  - [Triggers](#triggers)
  - [Typed trigger rows](#typed-trigger-rows)
  - [Procedures](#procedures)
  - [The SQL session](#the-sql-session)
- [Installing an extension in a database](#installing-an-extension-in-a-database)
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

### The manifest

Every extension assembly declares its name and version once, at assembly level:

```csharp
[assembly: CamusExtension("geo", "1.2.0", Requires = new[] { "units" })]
```

- The **name** is how SQL refers to the extension: `CREATE EXTENSION geo`, and the qualified call
  `geo.distance(a, b)`. It must be a plain identifier (a letter or underscore, then letters, digits
  and underscores). Names are case-insensitive.
- The **version** is SemVer: `major.minor.patch` with an optional `-prerelease` suffix and no
  `+build` metadata. A node can load two versions of one extension side by side; each database
  picks one.
- **Requires** names extensions that must be installed in a database before this one can be.

The server reads the manifest from the assembly's metadata before it loads the assembly, so no
extension code runs to read it. An assembly without a manifest, with a name that is not an
identifier, or with a version that is not SemVer stops the server at startup (`CADB0827`).

### Scalar functions

Mark a public static method with `[CamusFunction("name")]`:

```csharp
using CamusDB.Extensions;

[assembly: CamusExtension("text_utils", "1.0.0")]

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
CREATE EXTENSION text_utils;  -- once per database; see "Installing an extension in a database"
SELECT slugify(title) FROM posts WHERE slugify(title) = 'hello-world';
SELECT greet(NULL), hello('ann', 'hi');
```

- **Names** are case-insensitive. A name or alias that collides with a built-in function, an
  aggregate (`count`, `sum`, `min`, `max`, `avg`, and the other aggregate names the engine reserves),
  or a built-in table function stops the server at startup. An extension never replaces a built-in.
  Scalar functions, aggregates and table functions share **one name space**: one extension cannot
  use a name for two of them, and `CREATE EXTENSION` refuses a name that another installed
  extension uses for any of the three.
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
| `ARRAY(E)` | `IReadOnlyList<T>`, where `T` is the mapping of `E` |

A parameter or return type outside the table stops the server at startup with the method name and
the reason. JSON values are `STRING`.

**Arrays.** Declare an array as `IReadOnlyList<T>`, for example `IReadOnlyList<long>` or
`IReadOnlyList<string>`; a CLR array type such as `long[]` in a signature is refused (you can still
*return* a `long[]` from a method declared `IReadOnlyList<long>`).

- A NULL **array** follows the parameter rule: it makes the result NULL for `IReadOnlyList<long>`, and
  arrives as `null` for a parameter declared `IReadOnlyList<long>?`.
- A NULL **element** arrives as `null` in a reference element (`IReadOnlyList<string>`), and in a value
  element only when it is declared nullable (`IReadOnlyList<long?>`). A NULL element in an
  `IReadOnlyList<long>` argument fails the statement with the function name, the argument and the
  position.
- Each element follows the scalar rules, so an `INT64` element reaches an `IReadOnlyList<double>` as
  a double.
- **Copies, both ways.** The function receives a new read-only list, never the engine's storage. A
  returned list is copied into the engine value before the call returns, so a later change to it has
  no effect.
- A function that returns `IReadOnlyList<T>` declares its element type, so `CREATE TABLE … AS SELECT`
  and a derived table over it get an `ARRAY(E)` column with the right element type.
- **Not mapped:** nested arrays (`IReadOnlyList<IReadOnlyList<long>>`) and arrays of `byte[]`. Both
  stop the server at startup.
- `IRow.GetValue` and procedure arguments box an array as a list with nullable elements
  (`IReadOnlyList<long?>`, `IReadOnlyList<string?>`). `IRow.GetArray<T>` reads it with the element
  type you choose, and `IWritableRow.SetArray<T>` writes one in a BEFORE trigger. A session parameter
  can be a `long[]`, a `List<string>`, or any other list of a mapped element type.

**NUMERIC is never truncated.** NUMERIC holds 38 digits with 9 after the point; a .NET `decimal`
holds 28 or 29 significant digits. A NUMERIC value whose digits do not fit a `decimal` fails with
`CADB0810` instead of losing digits. A returned `decimal` with more than 9 fractional digits rounds
half away from zero, the same as text cast to NUMERIC.

### Table functions

A table function is a public static method marked `[CamusTableFunction("name")]` that returns
`IEnumerable<T>`. It stands as a relation in `FROM`, the same as the built-in `generate_series`:

```csharp
public static class TextTables
{
    [CamusTableFunction("split_words", Column = "word")]
    public static IEnumerable<string> SplitWords(string text)
    {
        foreach (string word in text.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            yield return word;
    }

    [CamusTableFunction("count_to", CountRowsMethod = nameof(CountToRows))]
    public static IEnumerable<long> CountTo(long n)
    {
        for (long i = 1; i <= n; i++)
            yield return i;
    }

    public static long? CountToRows(long n) => n < 0 ? 0 : n;
}
```

```sql
SELECT word FROM split_words('a b c');
SELECT w.word, d.weight FROM split_words(@text) AS w JOIN dictionary d ON d.term = w.word;
SELECT n FROM count_to(1000) AS n WHERE n % 7 = 0 LIMIT 10;
SELECT t FROM split_words('x y') AS s(t);   -- a column alias renames the column
```

- **One output column.** `T` is one mapped type, arrays included. `Column` names the column; without
  it, the function name (or the relation alias) names it. A row class with several columns is not
  supported in this version.
- **Arguments** follow the scalar-function rules, but a call reads no column of another relation in
  the same `FROM` (there is no `LATERAL`). A NULL argument for a parameter that does not accept NULL
  gives zero rows, and so does a `null` return value. A `null` element is a NULL row value.
- **Laziness and disposal.** The server pulls one value at a time and can stop at any point: a
  `LIMIT`, a cancel, an error elsewhere in the query. It then disposes the enumerator, so a `finally`
  block or a `using` in an iterator method runs. Do not build the whole output first. The server
  checks the statement's cancellation between two values; a value that takes long to compute delays
  a cancel, and the server cannot stop code that never yields. There is no row cap.
- **Re-reads and volatility.** A join can read a relation more than once. A function that is not
  `IsVolatile` is called again for each read, so it must give the same values for the same
  arguments. The output of an `IsVolatile = true` function is kept for the statement after its first
  read (it spills like other kept rows), so every read in one statement sees the same rows; the next
  statement calls it again.
- **Row estimate.** `CountRowsMethod` names a public static method of the same class, with the same
  parameters, that returns `long?`. The planner calls it when the arguments are constants and uses
  the result as the leaf's estimate; `EXPLAIN` shows it on the `function-scan` node. Without it the
  leaf gets the default estimate. Keep the method cheap and free of effects.
- **Errors.** An exception from the method or from the enumerator ends the statement with `CADB0809`
  and the function name.
- **Where it runs.** On the node that coordinates the statement; a table function never ships to a
  peer.

### Aggregate functions

An aggregate is a public class marked `[CamusAggregate("name")]` that implements
`ICamusAggregate<TState>`:

```csharp
[CamusAggregate("geomean")]
public sealed class GeoMean : ICamusAggregate<GeoMean.State>
{
    public sealed class State { public double LogSum; public long Count; }

    public State Create() => new State();

    public void Accumulate(State state, double x)
    {
        if (x > 0) { state.LogSum += Math.Log(x); state.Count++; }
    }

    public double? Finish(State state) => state.Count == 0 ? null : Math.Exp(state.LogSum / state.Count);
}
```

```sql
SELECT geomean(price) FROM items;
SELECT category, geomean(price) AS g, count(*) FROM items GROUP BY category HAVING geomean(price) > 10;
INSERT INTO summary (category, g) SELECT category, geomean(price) FROM items GROUP BY category;
```

- **The methods.** `Create` comes from the interface. `Accumulate(TState state, ...)` takes the SQL
  arguments after the state and returns `void`; it must be the only public method of that name.
  `Finish(TState state)` returns the result, of a mapped type (an array too); `null` is SQL NULL. The
  argument and result types are the aggregate's SQL signature, which is why the server finds these
  two methods by name. An optional `Merge(TState into, TState other)` is accepted and not called in
  this version (see below).
- **NULL.** A row whose argument is NULL for a parameter that does not accept NULL is skipped, the
  SQL rule for built-in aggregates. A `Nullable<T>` or nullable reference parameter receives the NULL.
- **Groups and zero rows.** The server calls `Create` once per group, `Accumulate` once per row of the
  group, and `Finish` once. A global aggregate over zero rows calls `Create` and then `Finish`. A
  `GROUP BY` over zero rows has no group, so it calls nothing.
- **State.** One instance of the class exists per engine and is shared by every statement; keep all
  per-group data in the state object. One state is used by one thread at a time.
- **Where it is allowed.** In the select list (bare or inside an expression, such as
  `geomean(x) * 2`) and in `HAVING`, like a built-in aggregate. `WHERE`, `JOIN ON` and `GROUP BY`
  refuse it, and so do a CHECK constraint and a column `DEFAULT`. There are no window functions and
  no `DISTINCT` inside an aggregate call, for built-in and extension aggregates alike.
- **Memory.** A spilling `GROUP BY` charges each group's state against the memory budget, but the
  server cannot measure your state, so it charges `StateSizeHint` bytes (64 when you give none).
  Give a larger hint for a state that holds a collection, so that a `GROUP BY` with many groups
  spills to disk before it runs out of memory. A spilling `GROUP BY` writes raw rows to disk, never
  your state, and gives the same result as one that does not spill.
- **Not distributed in this version.** An extension aggregate always runs on the node that runs the
  statement, over the gathered rows: the partial aggregation that splits a built-in `count`, `sum`,
  `min`, `max` or `avg` across the nodes never splits an extension aggregate.
- **Errors.** An exception from `Create`, `Accumulate` or `Finish` ends the statement with `CADB0809`
  and the aggregate name. A wrong argument count fails before any row is read.
- **Cost.** The server compiles the three calls once, when it loads the extension. A row costs one
  delegate call and its argument conversions. A `GROUP BY` of 200,000 rows into 100 groups took the
  same time with an extension aggregate as with `SUM`.

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

### Typed trigger rows

A trigger can receive OLD and NEW as instances of its own row class instead of `IRow`. Implement
`ICamusTrigger<TRow>`:

```csharp
public sealed class OrderRow
{
    public string? Id { get; set; }
    public string Name { get; set; } = "";
    [CamusColumn("total_cents")] public long? Total { get; set; }
    public IReadOnlyList<string>? Tags { get; set; }
}

public sealed class TrimOrderName : ICamusTrigger<OrderRow>
{
    public Task FireAsync(ITriggerContext<OrderRow> context)
    {
        if (context.New is { } row)
            row.Name = row.Name.Trim();   // BEFORE: written back
        return Task.CompletedTask;
    }
}
```

```sql
CREATE TRIGGER trim ON orders BEFORE INSERT, UPDATE EXTERNAL NAME 'MyCompany.Ext.TrimOrderName';
```

- **Mapping.** Each public property with a public getter and a public setter maps to the column of the
  same name (case-insensitive), or to the name in its `[CamusColumn("...")]`. Init-only properties,
  fields and indexers are not mapped. Columns the class does not map are ignored, so a class can
  model part of a row.
- **Types.** A property type must be the [value mapping](#value-mapping) of its column, exactly:
  `long` (or `long?`) for `INT64`, `DateTime` for `DATE` and `DATETIME`, `IReadOnlyList<string>` for
  `ARRAY(STRING)`, and so on. `CREATE TRIGGER` refuses a property with no column and a property of
  another type with `CADB0836`, and the message lists every problem and the type that fits.
- **NULL.** A `Nullable<T>` property, or a reference property annotated nullable (`string?`),
  receives NULL as `null`. A NULL in any other property fails the statement with `CADB0818` and the
  column name. A column that has no value when the trigger fires is NULL too: a column the INSERT
  omits, and the row id in BEFORE INSERT. Map such columns to nullable properties.
- **BEFORE triggers write back changed properties only.** After the call the server compares each
  property of NEW with the value it started with (byte arrays and lists by content) and writes back
  only the ones that differ, through the same path as `IWritableRow.Set`: the values are converted to
  the column types and the row must pass NOT NULL, length and CHECK rules. An unchanged property
  costs no write. An AFTER trigger's changes to its objects are ignored. The untyped rows are still
  there: cast the context to `ITriggerContext`.
- **Schema changes.** The trigger record keeps the columns its class maps. While the trigger exists,
  `ALTER TABLE … DROP COLUMN` and `RENAME COLUMN` of a mapped column are refused with `CADB0530`;
  drop the trigger first. Other columns change freely. If `ALTER EXTENSION … UPDATE` brings a row
  class that no longer fits the table, the first DML on each node that fires the trigger fails with
  `CADB0836`.
- **Cost.** Each fired row creates one object per image (OLD, NEW) and converts each mapped column.
  The untyped `ICamusTrigger` reads the row without a copy; prefer it on a hot table. The rows that
  wait for AFTER triggers are kept in the engine's form, so the `CADB0826` budget is the same for
  both kinds. The mapping is compiled once per row class: no reflection runs per row.

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

## Installing an extension in a database

Loading an extension makes it **available** on a node. Its functions, aggregates, table functions,
triggers and procedures resolve only in a database where it is **installed**:

```sql
CREATE EXTENSION geo;                  -- the highest version this node loads
CREATE EXTENSION geo VERSION '1.2.0';  -- an exact version
ALTER EXTENSION geo UPDATE;            -- move to the highest loaded version
ALTER EXTENSION geo UPDATE TO '1.3.0'; -- or to an exact one
DROP EXTENSION geo;

CREATE EXTENSION IF NOT EXISTS geo;    -- a no-op when geo is installed, at any version
DROP EXTENSION IF EXISTS geo;          -- a no-op when geo is not installed

SELECT distance(a, b) FROM routes;     -- plain name
SELECT geo.distance(a, b) FROM routes; -- qualified with the extension name
```

The three statements are schema DDL: they replicate through the schema log, so every node of a
cluster changes at the same schema version, and they survive a restart. In cluster mode they run on
the database's schema leader, the same as trigger DDL. Only a superuser may run them.

- **The IF forms.** `CREATE EXTENSION IF NOT EXISTS` does nothing when the extension is installed,
  and it does not change the installed version, even when it names another version: use
  `ALTER EXTENSION ... UPDATE` for that. When the extension is not installed, it acts as the plain form.
  `DROP EXTENSION IF EXISTS` does nothing when the extension is not installed; when it is installed, it
  acts as the plain form, so dependents still refuse it (`CADB0833`). Without the IF forms, the
  statements refuse with `CADB0829` and `CADB0830`.

- **One name, one member.** A plain name resolves against the built-ins and the extensions
  installed in the current database. No extension can shadow a built-in (the boot refuses it), and
  `CREATE EXTENSION` refuses an extension that defines a procedure name, or a function, aggregate or
  table-function name, that another installed extension already defines (`CADB0832`). Two extensions that share a name can both be
  loaded on a node and installed in different databases.
- **Requirements.** `CREATE EXTENSION` refuses while an extension in the manifest's `Requires` list
  is not installed (`CADB0831`). `DROP EXTENSION` refuses while another installed extension requires
  this one.
- **Dependencies.** `DROP EXTENSION` refuses while a column default, a CHECK constraint, a view, a
  materialized view or a trigger uses one of the extension's members (a view or a materialized view
  can also call its aggregates and table functions), and the error lists them
  (`CADB0833`). Drop those objects first; there is no `CASCADE`. `ALTER EXTENSION ... UPDATE` is
  refused the same way when the target version does not define a member that such an object uses.
- **Triggers follow the extension.** A trigger names its extension, not a build. The version and the
  hash live in the extension's record only, so `ALTER EXTENSION ... UPDATE` moves every function,
  aggregate, table function, procedure and trigger of the extension to the new build in one step. No trigger is dropped or
  re-created.
- **Caches.** Cached results, bound statements and cached plans are keyed on the database's
  installed extensions, so none of them is served after an install, an update or a drop changes what
  a function call means.

### Rolling upgrade

1. Deploy the new file on every node next to the old one, add it to `extension_assemblies`, and
   restart each node in turn. Both versions are now loaded; databases still use the old one.
2. Run `ALTER EXTENSION geo UPDATE TO '2.0.0'` in each database. All nodes switch at the same
   schema version.
3. Later, remove the old file from the configuration and restart the nodes again.

In cluster mode the node that runs `CREATE EXTENSION` or `ALTER EXTENSION ... UPDATE` first asks
every other node in the cluster which builds it loaded, and refuses (`CADB0835`) while a node that
answers does not load the target build: the same name, version and SHA-256. The error names those
nodes. A node that does not answer is skipped; if it comes back without the file, see the next
section.

### A node without the installed build

A node that does not load the installed build of an extension (no file with that name and version,
or a file with another SHA-256) still starts and still serves every statement that does not use the
extension. It logs an error that names the database and the extension when it opens the database.
A statement there that calls one of the extension's functions or procedures, or writes to a table
with one of its triggers, fails with `CADB0834`. `SHOW EXTENSIONS` lists the extension with the
status `missing`.

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
  path listed twice, a file without a manifest, and two files with the same extension name and
  version each stop the server at startup with a `CADB080x` or `CADB0827` error that names the path.
  A node never starts without code it was told to load.
- Both settings are restart-class: the server reads them once, when it starts. To change an
  extension, replace the file and restart the node.
- A private dependency of an UNSAFE extension resolves from the extension's own folder (through its
  `.deps.json` when there is one). Each extension loads into its own load context, so two
  extensions can carry different versions of one dependency.
- **Every node must deploy the same files.** An installed extension records the SHA-256 of its
  build. A node that loads another build of the same version refuses the statements that use the
  extension (`CADB0834`) instead of running different code; this covers functions, procedures and
  triggers alike. **The hash covers that one assembly file only.** The private dependencies of an
  UNSAFE extension, which load from their folder, are not hashed: two nodes with the same extension
  file and different dependency files are not told apart. Deploy the whole extension folder as one
  unit. To upgrade an extension, follow the [rolling upgrade](#rolling-upgrade).
- The browser playground has no extensions.
- The logical dump carries `CREATE EXTENSION name VERSION 'v'` for each installed extension, not
  its members, and does not carry triggers; see
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
- **Not supported in this version:** window functions, `DISTINCT` inside an aggregate call, table
  functions with more than one column, and the distributed partial aggregation of an extension
  aggregate.

## Observability

`SHOW EXTENSIONS` lists the builds the node loaded, one row per build: `name` and `version` (from
the manifest), `status`, `path`, `sha256`, `trust` (`safe` or `unsafe`), `requires`, the
comma-separated `functions`, `triggers`, `procedures`, `aggregates` and `table_functions` it
registered, and `signatures`: the SQL signature of each function, aggregate and table function,
joined by `; ` (for example `split_words(STRING) -> SETOF STRING` and
`sum_list(ARRAY(INT64)) -> INT64`; an optional parameter is in brackets). When the statement runs in a
database, `status` is `installed` for the build that database uses and `available` for the others,
and a row with status `missing` (and no path or trust) reports an extension the database installed
that this node does not load. A node without extensions, in a database without installed
extensions, returns no rows. The statement is node-local (compare the `sha256` column across nodes
to find a node with another build) and requires a superuser.

The triggers of a table are part of its schema record. The server also logs each loaded extension
and its members at startup.

## Error codes

| Code | Meaning |
| --- | --- |
| `CADB0800` | An extension path is relative or listed twice (startup) |
| `CADB0801` | An extension file does not exist (startup) |
| `CADB0802` | An extension file is not a loadable .NET assembly (startup) |
| `CADB0803` | An extension was built against a newer contract than the server's (startup) |
| `CADB0804` | Two extension files declare the same extension name and version (startup) |
| `CADB0805` | A function, aggregate or table-function name collides with a built-in, or one extension defines a name twice (startup) |
| `CADB0806` | A SAFE extension failed the SAFE check; the message lists every finding (startup) |
| `CADB0807` | A SAFE extension tried to load an assembly outside the allow-list (run time) |
| `CADB0808` | An extension member has a shape the server cannot bind (startup) |
| `CADB0809` | An extension function, aggregate or table function threw (HTTP 400) |
| `CADB0810` | A NUMERIC value does not fit a `decimal`, or the reverse (HTTP 400) |
| `CADB0811` | `CREATE TRIGGER` named a type that no installed extension defines (HTTP 400) |
| `CADB0812` | `CREATE TRIGGER` named a type that two installed extensions define (HTTP 400) |
| `CADB0813` | `CREATE TRIGGER` named a type that is not a trigger (HTTP 400) |
| `CADB0814` | The table already has a trigger with that name (HTTP 409) |
| `CADB0815` | `DROP TRIGGER` named a trigger the table does not have (HTTP 404) |
| `CADB0816` | The target of `CREATE TRIGGER` is a view or a materialized view (HTTP 400) |
| `CADB0817` | The installed version of a trigger's extension does not define its type (HTTP 503) |
| `CADB0818` | A trigger threw; nothing the statement wrote persists (HTTP 400) |
| `CADB0819` | Triggers and procedures nested more than 16 levels (HTTP 400) |
| `CADB0820` | The SQL session refused a statement that is not SELECT, INSERT, UPDATE or DELETE (HTTP 400) |
| `CADB0821` | `CALL` named a procedure that no installed extension defines (HTTP 404) |
| `CADB0822` | A procedure threw; everything the `CALL` wrote rolls back (HTTP 400) |
| `CADB0823` | A trigger or procedure caught an error from its own SQL and returned; the statement fails (HTTP 400) |
| `CADB0824` | A commit of a transaction in which an extension's SQL failed; roll it back (HTTP 409) |
| `CADB0825` | A BEFORE trigger's SQL changed a row its statement already read and is still writing (HTTP 409) |
| `CADB0826` | The rows kept for AFTER triggers exceed `spill_threshold_bytes` (HTTP 400) |
| `CADB0827` | An extension assembly has no manifest, or an invalid one (startup) |
| `CADB0828` | `CREATE` or `ALTER EXTENSION` named an extension or a version this node does not load (HTTP 400) |
| `CADB0829` | The extension is already installed in the database (HTTP 409) |
| `CADB0830` | The extension is not installed in the database, or `CREATE TRIGGER` named a type of an extension that is loaded but not installed (HTTP 404) |
| `CADB0831` | An extension the manifest requires is not installed (HTTP 400) |
| `CADB0832` | Another installed extension defines one of the extension's procedure, function, aggregate or table-function names (HTTP 409) |
| `CADB0833` | Objects depend on the extension, or on a member the target version lacks; the message lists them (HTTP 409) |
| `CADB0834` | The database installed an extension build that this node does not load (HTTP 503) |
| `CADB0835` | A live node does not load the target build of `CREATE` or `ALTER EXTENSION` (HTTP 409) |
| `CADB0836` | A typed trigger's row class does not fit the table; the message lists every problem (HTTP 400) |
