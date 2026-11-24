# SQL fuzzer

The SQL fuzzer finds **logic bugs**: a query that returns wrong rows and gives no error. It also finds
statements that fail with an unclassified exception and statements that do not finish. It also finds
writes that leave the stored forms of a table in disagreement.

It does not need a second database as a reference. A seeded generator makes a random schema, random
data and random statements. Then an **oracle** changes one query into other queries that must give the
same result. It executes all the queries and compares the results. The oracles are the ones of
[SQLancer](https://github.com/sqlancer/sqlancer), written in C#. They are executed in-process against
`CommandExecutor` with `ExecuteSQLTicket`. This means that no driver and no network are necessary.

The fuzzer is the test project `CamusDB.SqlFuzz.Tests`. It is not part of the `CamusDB.Tests` suite.
It is a separate CI job, `.github/workflows/sql-fuzz-nightly.yml`.

## The oracles

| Oracle | Rule | Typical bugs it finds |
|---|---|---|
| Error classification | Each error is a `CamusDBException` with a specific code. Each statement finishes within the statement time limit. The rows of each successful read serialize to the HTTP JSON row format. | A raw `OverflowException`, `ArgumentException` or `NullReferenceException`. The internal-error code `CADB0099`. A hang. A value that a client cannot receive. |
| TLP (ternary logic partitioning) | For a predicate `p`, the queries with `WHERE p`, `WHERE NOT p` and `WHERE p IS NULL` together return exactly the rows of the query that has no WHERE. Variants cover aggregates (MIN of the MINs, MAX of the MAXs, SUM of the SUMs, SUM of the COUNTs) and DISTINCT. Other variants cover GROUP BY, HAVING, and the ON predicate of an INNER join. | NULL handling, three-valued logic, NaN and -0.0, a comparison that is neither TRUE, FALSE nor NULL in a consistent way. |
| NoREC | `SELECT COUNT(*) FROM … WHERE p` equals `SELECT SUM(CASE WHEN p THEN 1 ELSE 0 END) FROM …`. The first form lets the planner use indexes and pushdown. The second form evaluates `p` on each row of a full scan. A variant compares the rows too. | A seek with wrong bounds, a probe that loses a key, a predicate pushed to the wrong side of a join. |
| Plan equivalence | One query returns the same rows, with the same output shape, under each plan. The plans are each `@{FORCE_INDEX=…}` choice of each source, each engine configuration and forced join operator (see below), for a one-table query a full scan of the primary rows, and for a join the form with bare column names (see below). | An index or a join operator that returns other rows than a scan. A bare column name that the join planner does not use as a key. |
| DML integrity | After each write, the table is exactly the table the rule expects. The affected-row count and each proposed and assigned value are checked cell by cell. No other row is added, changed or lost, and a failure changes nothing at all. The full scan, each index (its keys and its INCLUDE payload) and the large-value cells agree. In an explicit transaction, each write has its effect in the transaction. A failed statement aborts the transaction: later statements and COMMIT fail with `CADB0824`, and the committed rows are as they were before. A transaction that has no failure commits exactly the state it saw at its end. | Twin rows, orphan index entries, rows missing from an index, stale covering payloads, orphan or lost large-value cells, a wrong value or affected-row count, an UPSERT that skipped its insert or its update, an `UPDATE … FROM` that changed a row its list matched twice, a failed statement that changed data. |

### FLOAT64 sums

A FLOAT64 `SUM` depends on the order of its additions: `1e300 + -1e300 + 1.0` is 1, but
`1e300 + 1.0 + -1e300` is 0. This means that the full query and the parts of a TLP partition can
differ by much more than a small fraction of the result. The aggregate and GROUP BY forms of TLP
therefore also read `SUM(ABS(x))` (the `abs_sum<i>` columns), and the comparison allows a difference
of up to 1e-9 times that value. A lost row of ordinary size is a finding. A sum whose inputs reach the
overflow range is not compared. For such a sum, the order also decides if a partial sum overflows.
Such a check counts as a skipped check and does not count as a pass. All other values, MIN and MAX of
FLOAT64 included, compare exactly.

### The plan-equivalence configurations

A configuration is fixed when an engine is built. For this reason, each configuration is a separate
engine on the same node and the same database. A setting changed on an engine after it is built has
no effect, and the oracle then compares a result with itself.

| Arm | Options |
|---|---|
| `heuristic-planner` | `CostBasedAccessPathEnabled = false, CostBasedJoinOrderEnabled = false` |
| `no-hash-join` | `SpillEnabled = false, HashJoinMaxBuildRows = 0`: hash joins are replaced by nested loops. The cap applies only with spill off, and the arm turns spill off for this reason |
| `grace-hash-join` | `SpillEnabled = true, ForceSpillThresholdRows = 1` |
| `index-batch-1` | `IndexScanFetchBatchSize = 1` |
| `plan-cache` | `PlanCacheEnabled = true` |
| `row-decode` | `SlotBackedDecode = false` |
| `no-query-caches` | `BoundQueryCacheEnabled = false, PointReadRowIdCacheEnabled = false` |
| `force-merge-join` | the test seam `Statistics.ForceMergeJoinForTesting = true` on the arm's own executor |
| `force-hash-join` | the test seam `Statistics.ForceHashJoinForTesting = true` |
| `force-nested-loop` | the test seam `Statistics.ForceNestedLoopForTesting = true` |
| `force-index-nested-loop` | the test seam `Statistics.ForceIndexNestedLoopForTesting = true` |

The list is `PlanEquivalenceOracle.Arms`. To add an arm, add an entry with the option text that the
printed regression test uses, and for a test seam the statement that sets it (`PrepareText`). The
plan-cache arm executes each query twice. The second run uses the stored plan. In some rounds, one
table has 100 to 200 rows. The planner picks a merge join, and weighs a hash join against an index
nested loop, only for a larger outer side. A FROM clause of such a round has at most two sources and
names that table once. A cross join of the table with itself gives some 40,000 pairs, and a probe for
each pair passes the statement time limit even when there is no defect.

A fifth of the join equalities compare a column of one side with an expression of the columns of the
other side. The planner keys a hash join on such an expression (see
[Expression keys](joins.md#expression-keys)). The oracles compare that join with the nested loop
of the `force-nested-loop` arm.

About a third of the joins that lack an equality key take a range form: a numeric column of the right
table between two numeric columns of the left side, written with `BETWEEN` or with two comparisons.
The planner can serve it with a range seek on an index of that column (see
[Range seek](joins.md#range-seek)). The `force-nested-loop` arm does not seek. This means that plan
equivalence compares the seek with a nested loop, and the `index-batch-1` and `plan-cache` arms
execute the seek under their settings.

A join whose `ON` or `WHERE` names a column that only one source of its `FROM` has is also executed
in a second form, where each such column is unqualified (`plan:bare-names`). The engine gives a bare
name its source when it binds the statement (see
[Column names in join predicates](joins.md#column-names-in-join-predicates)). This means that the two
forms must return the same rows and show the same `EXPLAIN`, shape id included. A bare name that lost
its join key returns the same rows through a nested loop. For this reason, the `EXPLAIN` comparison is
the part that finds it (`plan:bare-names:plan`). The oracle makes the check only for a query that has
such a column. The generated tables share the names `c0`, `c1`, …. This means that the column is one
that the wider of two tables has, or a column of a `VALUES` list.

A configuration name is no evidence that a check compared two different plans. For this reason, each
passed check also takes the EXPLAIN of both plans. The report gives the count of checks whose second
plan had another access path, and how often each operator was the second plan. The probe memo of the
index nested loop can't be switched off, and no arm turns it off.

## What the fuzzer cannot find

A metamorphic oracle finds **inconsistent** results. It does not find a result that is wrong in the
same way on all paths. Examples are a silent INT64 wraparound or a CAST that truncates. All queries of
the oracle give the same wrong value, and the comparison passes. These bugs need a reference, for
example PostgreSQL or an in-memory model. The generator is built so that such a reference oracle can
use it (see "The generator").

The fuzzer executes one session at a time. It does not find concurrency bugs or isolation anomalies,
or bugs that need a fault. Other harnesses own them.

The row oracles compare only queries whose rows do not depend on order. This excludes LIMIT, window
functions and set operations. The error-classification oracle executes those forms, but their rows
are not checked. The generator does not make FLOAT32, BYTES, UUID or array columns. It also does not
make CHECK constraints or composite primary keys, and it pairs only comparable types.

INTERVAL is an expression type only. No CamusDB table stores an interval. The generator writes
interval values in several forms.

- Interval literals in the PostgreSQL and ISO-8601 forms.
- `date ± n`, `date - date`, `point ± interval` and `datetime - datetime`.
- Interval arithmetic with numbers, and interval comparisons.
- The functions `age`, `make_interval`, `justify_*`, `date_bin` and `date_part`.

It does not write an interval column, an interval value in a write, or `SUM` or `AVG` of an interval.
The row oracles compare an interval by its order key, where a month is 30 days. `1 mon` and `30 days`
are one value. This means that a DISTINCT or a UNION can return either one.

## Run it locally

```sh
# The fuzzer's own tests: the generator, each oracle against a planted fault, the shrinker.
dotnet test CamusDB.SqlFuzz.Tests/CamusDB.SqlFuzz.Tests.csproj --filter "FullyQualifiedName~CamusDB.SqlFuzz.Tests.Unit."

# All five campaigns, 30 s each, with a seed from the clock.
dotnet test CamusDB.SqlFuzz.Tests/CamusDB.SqlFuzz.Tests.csproj --filter "FullyQualifiedName~.FuzzCampaignTests."

# One oracle for 5 minutes with a fixed seed.
SQLFUZZ_SEED=42 SQLFUZZ_BUDGET_SECONDS=300 \
  dotnet test CamusDB.SqlFuzz.Tests/CamusDB.SqlFuzz.Tests.csproj --filter "FullyQualifiedName~.FuzzCampaignTests.Tlp"
```

A campaign test fails when its oracle found a new disagreement. It also fails when its checks did not
compare enough. Outside a replay, at least half of the checks must complete a comparison, and a
skipped check does not compare anything. Each variant with 20 attempts must complete at least one.
The test output names the seed and the round. The output folder contains three kinds of files.

- `TestSqlFuzz<Oracle><hash>Original.md` and `.cs`: the first example of each distinct finding (up
  to 25). It is written as soon as it is found, before any shrinking. This means that a later hang or
  a stopped job cannot lose it.
- `TestSqlFuzz<Oracle><hash>.md`: the seed, the disagreement, the plans of the reduced case, and the
  shrunk statements (up to `SQLFUZZ_MAX_FINDINGS` findings for each oracle). It says if the finding
  reproduced on a new database. For a finding that did not reproduce, the file shows the original
  statements.
- `TestSqlFuzz<Oracle><hash>.cs`: a ready-to-paste NUnit test for `CamusDB.Tests`.

### What a round checks besides its oracle

- **The setup.** All outcomes of the schema, the data load and ANALYZE are classified. A defect (an
  internal code, an unclassified exception, a hang) is a finding with the setup before it. A refused
  CREATE TABLE or CREATE INDEX is a finding too. The round's checks assume the schema. A refused data
  load is counted in the report as `setup statements refused`. It leaves a table with fewer rows.
- **The time limit** applies to each operation on the engine, such as a statement or an integrity
  scan. BEGIN, COMMIT and ROLLBACK are operations too. An operation past it is a hang finding. An operation that does not stop after its
  cancellation poisons the engine. The campaign does not use that engine again, even to drop a
  database, and starts a new one. The shrinker does the same when a reduction step makes a statement
  hang.
- **The read history.** A read can change what a later statement does, through the bound-query, plan
  and probe caches. For this reason, the reads of each check that did not fail join the setup history,
  with the arm each one ran on, and a finding replays them. The shrinker removes the ones a finding
  does not need.

| Variable | Meaning | Default |
|---|---|---|
| `SQLFUZZ_SEED` | The base seed. | `GITHUB_RUN_NUMBER` in CI, else the clock |
| `SQLFUZZ_ROUND` | Replay mode: run only this round of the seed. | none |
| `SQLFUZZ_ORACLE` | Run one oracle only: `tlp`, `norec`, `plan`, `error`, `dml`. | all |
| `SQLFUZZ_BUDGET_SECONDS` | The time budget of each oracle. | 30 |
| `SQLFUZZ_SHRINK_SECONDS` | The time to shrink one finding. | 60 |
| `SQLFUZZ_MAX_FINDINGS` | The distinct findings to shrink and print for each oracle. | 3 |
| `SQLFUZZ_STATEMENT_TIMEOUT_SECONDS` | The time limit of one statement (the hang oracle). | 10 |
| `SQLFUZZ_OUTPUT_DIR` | Where the finding files go. | `sqlfuzz-findings` in the test output folder |

Run one `dotnet test` at a time. Each campaign boots a separate embedded node. The hang oracle is not
reliable on a machine that is executing another suite at the same time.

## Replay a finding

Each round takes its seed from the base seed and the round number. This means that one round can be
replayed alone.

```sh
SQLFUZZ_SEED=1 SQLFUZZ_ROUND=172 SQLFUZZ_ORACLE=dml \
  dotnet test CamusDB.SqlFuzz.Tests/CamusDB.SqlFuzz.Tests.csproj --filter "FullyQualifiedName~.FuzzCampaignTests."
```

The report and the printed test contain this command. A replay gives the same statements only on the
same generator code. A change to the generator changes the statements of all seeds. For a CI finding,
use the printed test or replay it on the commit that CI tested.

In CI, dispatch `sql-fuzz-nightly` with the `seed`, `oracle` and `round` inputs.

## From a finding to a fix

1. Paste the printed test into `CamusDB.Tests` and run it. It fails on the code that gave the finding.
   It replays the shrunk statements with `CamusDB.Tests.SqlFuzz.SqlFuzzReplay`, and asserts the
   relation of the oracle with the same code the campaign used: the write and transaction rules
   (`WriteEffect`, `TransactionEffect`), the internal-code list (`SqlFuzzErrorRules`), the JSON wire
   check (`SqlFuzzWire`) on a read, and the entry point of the statement (a read, ANALYZE in a
   read-write transaction, or a write). The setup replays as the campaign ran it: a refused statement
   is history, and a transaction goes on after one.
2. Give the test a name and a summary that state the cause, or write a smaller hand-made test, and fix
   the bug.
3. If the fix waits, file it. Then add a `KnownIssue` entry to `ErrorClassifier.KnownIssues` that
   matches its error message. A known issue is counted and named in each report, but it does not fail
   the campaign. This means that the job is useful for new findings. Remove the entry in the commit
   that fixes the bug.

The shrinker removes setup statements, rows of multi-row INSERT statements and table columns. It also
asks the check for simpler forms of itself, such as an expression subtree in place of the predicate or
fewer output columns. Other forms drop an index hint or a join. A step is kept only if the replay
fails again with the same signature. A hang is not shrunk: each replay would hang again.

## The expected-error list and the gaps

`ErrorClassifier.Expected` lists the errors that the generator can cause on purpose. Each entry names
its code and the generator rule that causes it, for example a division by zero or a duplicate key. An
expected error is not a finding of the error-classification oracle. However, it is an outcome: the
DML-integrity oracle checks that a failed write changed no data.

An error with a specific code that is not on the list is counted in the report as `unlisted-code`. It
is not reported as a finding. A code is specific, and this means that the engine meant the refusal.
The report shows the most frequent ones. When a valid statement gets such a code, the cause is either
a generator rule or a CamusDB gap.

`ErrorClassifier.Gaps` lists the forms CamusDB does not support, with the outcome of each. The generator
does not write them. To add an entry, name the form and the outcome; then change the generator so that
it does not write the form.

`ErrorClassifier.InternalCodes` lists the codes that signal a defect: `CADB0099` and the codes for
corrupt storage. A refusal is not on this list. An outcome with one of these codes is always a
finding, unless it matches a known issue.

## The generator

The generator makes **statement trees** (`Generator/Ast.cs`). It does not make SQL text. A printer
turns a tree into the SQL of one dialect (`Printing/CamusSqlPrinter.cs`). Everything that is specific
to CamusDB is in the printer: the type names, the `@{FORCE_INDEX=…}` hint, the typed literals and the
forms an IN-list item allows. A differential test against another database adds a printer for that
dialect and uses the same trees.

A **profile** (`GeneratorProfile`) limits the types, operators, functions, expression forms and statement
forms that the generator may select. `GeneratorProfile.CamusDB` is the full set. A consumer that compares
CamusDB with a reference makes a subset with the `Without…` methods, for example the forms that both
CamusDB and PostgreSQL support. A profile is a contract for the complete tree. The helper expressions
that the generator adds by itself obey it too. Examples are the key offset and the ORDER BY … LIMIT of
INSERT … SELECT, or the COALESCE of a NOT NULL column. `ProfileValidator` walks a finished tree and
lists what the profile does not allow. `TestGenerator.EveryProfileExclusionHoldsOnTheWholeTree` uses
it to test each single exclusion and some combinations.

About three UPDATE and DELETE statements in ten are joined forms. These are `UPDATE … FROM` and
`DELETE … USING`, as well as a target alias that has no list (see
[UPDATE … FROM and DELETE … USING](update-from-delete-using.md)). The list is one or two tables. All
the columns of the statement are qualified, because a bare name that the target and a source both have
is ambiguous. The WHERE always compares a target column with a column of the list, most of the time
with the primary key of a source. As a result, most target rows match one row. The DML-integrity
oracle predicts a joined write with a SELECT over `list CROSS JOIN target` and the same WHERE. A target
row that the prediction names twice matched two rows of the list. An UPDATE must then fail and change
no data, and a DELETE deletes the row once. The generator does not write a comma list, a derived table
other than a `VALUES` list (below) or a set-returning function in the list. It does not write
`RETURNING` either.

About one SELECT in five reads some of its tables through a WITH (see
[WITH queries](common-table-expressions.md)): `WITH cte_t0 AS (SELECT * FROM t0) SELECT … FROM cte_t0 AS
a0`. The rows are the table's rows. This means that all oracles check the WITH path. In a TLP check,
the WITH is only in the full query, and the three parts read the tables. In a NoREC check, only the
optimized query has it. As a result, each check also compares a WITH with the tables it reads. A self
join through a WITH reads one CTE two times, which CamusDB computes once. The source of
INSERT … SELECT takes the same form. A reference with an index hint reads the table directly, because
a CTE does not have an index. The generator does not write a column list or `MATERIALIZED`. It also
does not write a CTE that reads another CTE, or a WITH in front of a write.

About one SELECT in five with an inner or LEFT join prints some of those joins as a `LATERAL` derived
table (see [LATERAL](joins.md#lateral)): `JOIN t1 AS a1 ON p` as `CROSS JOIN LATERAL (SELECT * FROM t1 AS
a1 WHERE p) AS a1`, and a LEFT JOIN as `LEFT JOIN LATERAL (…) AS a1 ON TRUE`. The rows are the same.
This means that all oracles check the LATERAL path. In a TLP check, only the full query has the LATERAL
form, and in a NoREC check only the optimized query has it. A join is printed as written when the right
table has an index hint or the ON contains a subquery. The JOIN ON variant of TLP prints all joins as
written, because it adds its partition to the last ON. The generator does not write a LATERAL derived
table that does more than filter one table, or a function in FROM that reads a column.

About one TLP check in five, and one NoREC check of rows in five, also prints the select list of the
full or optimized query in one `LATERAL` derived table. That derived table is a SELECT of expressions
only, placed after the main FROM: `SELECT lx.e0 AS o0
FROM … CROSS JOIN LATERAL (SELECT expr AS e0) AS lx`. The rows and the output names are the same. This
means that the checks compare the evaluation of a LATERAL body of expressions with the same expressions
in a select list. A select list with an aggregate or a window call is printed as written. The same is
true for a SELECT with GROUP BY, HAVING or a set operation.

About one FROM list in twelve starts with a `VALUES` list (see [VALUES lists in FROM](values-lists.md)):
`(VALUES (1, 'x'), (CAST(NULL AS int64), 'y')) AS a0 (v0, v1)`. It has 1 to 4 rows and 1 to 3 columns
of type `int64`, `float64`, `string` or `bool`. Each item is a literal of its column's type, and the
first row contains no NULL. This means that the column types follow from the literals. The list joins
the tables after it, and it is also the start of some `UPDATE … FROM` and `DELETE … USING` lists. As a
result, all oracles read it. The generator does not put a `VALUES` list on the right side of a join or
after `LATERAL`.

The statement list carries BEGIN, COMMIT and ROLLBACK markers. A consumer can use them to group
statements into explicit transactions, and no new format is needed. Each list records its seed.

`FuzzRandom` is xoshiro256** seeded through SplitMix64. Its sequence is part of the replay contract.
For this reason, `TestGenerator.TheRandomSourceIsFixed` pins it.

At least 90 % of the generated statements succeed on a fresh database
(`TestGenerator.NinetyPercentOfGeneratedStatementsRunWithoutAnError`, measured over 10,000 statements).
The same test fails on any finding of the error-classification rule in those statements or in their
setup, whatever the rate.

## Where the parts are

| Part | Location |
|---|---|
| Engine setup (one node, one database per round, configuration arms) | `CamusDB.SqlFuzz.Tests/Engine/FuzzEngine.cs` |
| Generator, profiles, tree | `CamusDB.SqlFuzz.Tests/Generator/` |
| CamusDB printer | `CamusDB.SqlFuzz.Tests/Printing/` |
| Runner, outcomes, error classification | `CamusDB.SqlFuzz.Tests/Runner/` |
| Oracles | `CamusDB.SqlFuzz.Tests/Oracles/` |
| Shrinker, regression-test printer | `CamusDB.SqlFuzz.Tests/Shrinking/` |
| Campaign loop and settings | `CamusDB.SqlFuzz.Tests/Campaign/` |
| Value rules, write and transaction rules, outcome rules, wire check, integrity checker, replay helper (shared with the printed tests) | `CamusDB.Tests/SqlFuzz/`, linked into the fuzzer project |
