# SQL fuzzer

The SQL fuzzer finds **logic bugs**: a query that returns wrong rows and gives no error. It also finds
statements that fail with an unclassified exception, statements that do not finish, and writes that
leave the stored forms of a table in disagreement.

It does not need a second database as a reference. A seeded generator makes a random schema, random
data and random statements. Then an **oracle** changes one query into other queries that must give the
same result, runs them all, and compares the results. The oracles are the ones of
[SQLancer](https://github.com/sqlancer/sqlancer), written in C#. They run in-process against
`CommandExecutor` with `ExecuteSQLTicket`, so no driver and no network are necessary.

The fuzzer is the test project `CamusDB.SqlFuzz.Tests`. It is not part of the `CamusDB.Tests` suite.
It runs as its own CI job, `.github/workflows/sql-fuzz-nightly.yml`.

## The oracles

| Oracle | Rule | Typical bugs it finds |
|---|---|---|
| Error classification | Every error is a `CamusDBException` with a specific code. Every statement finishes within the statement time limit. The rows of every successful read serialize to the HTTP JSON row format. | A raw `OverflowException`, `ArgumentException` or `NullReferenceException`. The internal-error code `CADB0099`. A hang. A value that a client cannot receive. |
| TLP (ternary logic partitioning) | For a predicate `p`, the queries with `WHERE p`, `WHERE NOT p` and `WHERE p IS NULL` together return exactly the rows of the query with no WHERE. Variants: aggregates (MIN of the MINs, MAX of the MAXs, SUM of the SUMs, SUM of the COUNTs), DISTINCT, GROUP BY, HAVING, and the ON predicate of an INNER join. | NULL handling, three-valued logic, NaN and -0.0, a comparison that is neither TRUE, FALSE nor NULL in a consistent way. |
| NoREC | `SELECT COUNT(*) FROM … WHERE p` equals `SELECT SUM(CASE WHEN p THEN 1 ELSE 0 END) FROM …`. The first form lets the planner use indexes and pushdown. The second form evaluates `p` on every row of a full scan. A variant compares the rows too. | A seek with wrong bounds, a probe that loses a key, a predicate pushed to the wrong side of a join. |
| Plan equivalence | One query returns the same rows, with the same output shape, under every plan: each `@{FORCE_INDEX=…}` choice of each source, each engine configuration and forced join operator (see below), and, for a one-table query, a full scan of the primary rows. | An index or a join operator that returns other rows than a scan. |
| DML integrity | After each write, the table is exactly the table the rule expects: the affected-row count, every proposed and assigned value cell by cell, no other row added, changed or lost, and no change at all after a failure. The full scan, every index (its keys and its INCLUDE payload) and the large-value cells agree. In an explicit transaction, every write has its effect inside the transaction; a failed statement aborts the transaction (later statements fail with `CADB0824`, COMMIT fails with `CADB0824`, and the committed rows are as they were before); a transaction with no failure commits exactly the state it saw at its end. | Twin rows, orphan index entries, rows missing from an index, stale covering payloads, orphan or lost large-value cells, a wrong value or affected-row count, an UPSERT that skipped its insert or its update, a failed statement that changed data. |

### FLOAT64 sums

A FLOAT64 `SUM` depends on the order of its additions: `1e300 + -1e300 + 1.0` is 1, but
`1e300 + 1.0 + -1e300` is 0. So the whole query and the parts of a TLP partition can differ by much more
than a small fraction of the result. The aggregate and GROUP BY forms of TLP therefore also read
`SUM(ABS(x))` (the `abs_sum<i>` columns), and the comparison allows a difference of up to 1e-9 times that
value. A lost row of ordinary size is still a finding. A sum whose inputs reach the overflow range is not
compared, because the order also decides if a partial sum overflows. Such a check counts as a skipped
check, not as a pass. All other values, MIN and MAX of FLOAT64 included, compare exactly.

### The plan-equivalence configurations

A configuration is fixed when an engine is built, so each one is its own engine on the same node and
the same database. A setting changed on an engine after it is built does nothing, and the oracle then
compares a result with itself.

| Arm | Options |
|---|---|
| `heuristic-planner` | `CostBasedAccessPathEnabled = false, CostBasedJoinOrderEnabled = false` |
| `no-hash-join` | `HashJoinMaxBuildRows = 0`: hash joins fall back to nested loops |
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
plan-cache arm runs each query twice, so the second run uses the stored plan. In some rounds one table
has 100 to 200 rows, because the planner picks a merge join, and weighs a hash join against an index
nested loop, only for a larger outer side. A FROM clause of such a round has at most two sources and
names that table once: a cross join of the table with itself gives some 40,000 pairs, and a probe for
each pair passes the statement time limit with no defect.

A configuration name is no evidence that a check compared two different plans. So each passed check
also takes the EXPLAIN of both plans, and the report gives the count of checks whose second plan had
another access path, and how often each operator was the second plan. The probe memo of the index
nested loop has no switch, so no arm turns it off.

## What the fuzzer cannot find

A metamorphic oracle finds **inconsistent** results. It does not find a result that is wrong in the
same way on every path. Examples are a silent INT64 wraparound or a CAST that truncates: every query
of the oracle gives the same wrong value, so the comparison passes. These bugs need a reference, for
example PostgreSQL or an in-memory model. The generator is built so that such a reference oracle can
use it (see "The generator").

The fuzzer runs one session at a time. It does not find concurrency bugs, isolation anomalies or bugs
that need a fault. Other harnesses own them.

The row oracles compare only queries whose rows do not depend on order: no LIMIT, no window function
and no set operation. The error-classification oracle runs those forms, but nothing checks their
rows. The generator has no FLOAT32, DATETIME, BYTES, UUID or array column, no CHECK constraint and no
composite primary key, and it pairs only comparable types.

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
compare enough: outside a replay, at least half of the checks must complete a comparison (a skipped
check compares nothing), and every variant with 20 attempts must complete at least one. The test
output names the seed and the round, and the output folder holds these files:

- `TestSqlFuzz<Oracle><hash>Original.md` and `.cs`: the first example of each distinct finding (up
  to 25), written as soon as it is found, before any shrinking, so a later hang or a stopped job
  cannot lose it.
- `TestSqlFuzz<Oracle><hash>.md`: the seed, the disagreement, the plans of the reduced case, and the
  shrunk statements (up to `SQLFUZZ_MAX_FINDINGS` findings for each oracle). It says if the finding
  reproduced on a new database. A finding that did not reproduce keeps its original statements.
- `TestSqlFuzz<Oracle><hash>.cs`: a ready-to-paste NUnit test for `CamusDB.Tests`.

### What a round checks besides its oracle

- **The setup.** Every outcome of the schema, the data load and ANALYZE is classified. A defect (an
  internal code, an unclassified exception, a hang) is a finding with the setup before it. A refused
  CREATE TABLE or CREATE INDEX is a finding too, because the round's checks assume the schema. A
  refused data load is counted in the report as `setup statements refused`, because it leaves a
  table with fewer rows.
- **The time limit** applies to every operation on the engine: a statement, BEGIN, COMMIT, ROLLBACK
  and an integrity scan. An operation past it is a hang finding. An operation that does not stop
  after its cancellation poisons the engine: the campaign does not use it again, not even to drop a
  database, and starts a new one. The shrinker does the same when a reduction step makes a statement
  hang.
- **The read history.** A read can change what a later statement does, through the bound-query, plan
  and probe caches. So the reads of every check that did not fail join the setup history, with the
  arm each one ran on, and a finding replays them. The shrinker removes the ones a finding does not
  need.

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

Run one `dotnet test` at a time. Each campaign boots its own embedded node, and the hang oracle is not
reliable on a machine that runs another suite at the same time.

## Replay a finding

Each round takes its seed from the base seed and the round number, so one round replays alone:

```sh
SQLFUZZ_SEED=1 SQLFUZZ_ROUND=172 SQLFUZZ_ORACLE=dml \
  dotnet test CamusDB.SqlFuzz.Tests/CamusDB.SqlFuzz.Tests.csproj --filter "FullyQualifiedName~.FuzzCampaignTests."
```

The report and the printed test contain this command. A replay gives the same statements only on the
same generator code: a change to the generator changes the statements of every seed. For a CI finding,
use the printed test, or replay it on the commit that CI tested.

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
3. If the fix waits, file it, and add a `KnownIssue` entry to `ErrorClassifier.KnownIssues` that matches
   its error message. A known issue is counted and named in every report, but it does not fail the
   campaign, so the job stays useful for new findings. Remove the entry in the commit that fixes the bug.

The shrinker removes setup statements, rows of multi-row INSERT statements and table columns, and it
asks the check for simpler forms of itself (an expression subtree in place of the predicate, fewer
output columns, no index hint, one join less). It keeps a step only when the replay fails again with the
same signature. A hang is not shrunk: each replay would hang again.

## The expected-error list and the gaps

`ErrorClassifier.Expected` lists the errors that the generator can cause on purpose. Each entry names
its code and the generator rule that causes it, for example a division by zero or a duplicate key. An
expected error is not a finding of the error-classification oracle, but it is still an outcome: the
DML-integrity oracle still checks that a failed write changed nothing.

An error with a specific code that is not on the list is counted in the report as `unlisted-code`, not
reported as a finding. A code is specific, so the engine meant the refusal. The report shows the most
frequent ones: when a valid statement gets such a code, the cause is either a generator rule or a
CamusDB gap.

`ErrorClassifier.Gaps` lists the forms CamusDB does not support, with the outcome of each. The generator
does not write them. To add an entry, name the form and the outcome; then change the generator so that
it does not write the form.

`ErrorClassifier.InternalCodes` lists the codes that report a defect, not a refusal: `CADB0099` and the
codes for corrupt storage. An outcome with one of them is always a finding, unless it matches a known
issue.

## The generator

The generator makes **statement trees**, not SQL text (`Generator/Ast.cs`). A printer turns a tree into
the SQL of one dialect (`Printing/CamusSqlPrinter.cs`). Everything that is specific to CamusDB is in the
printer: the type names, the `@{FORCE_INDEX=…}` hint, the typed literals and the forms an IN-list item
allows. A differential test against another database adds a printer for that dialect and uses the same
trees.

A **profile** (`GeneratorProfile`) limits the types, operators, functions, expression forms and statement
forms that the generator may select. `GeneratorProfile.CamusDB` is the full set. A consumer that compares
CamusDB with a reference makes a subset with the `Without…` methods, for example the forms that both
CamusDB and PostgreSQL support. A profile is a contract for the whole tree: the helper expressions the
generator adds on its own (the key offset and the ORDER BY … LIMIT of INSERT … SELECT, the COALESCE of
a NOT NULL column) obey it too. `ProfileValidator` walks a finished tree and lists what the profile does
not allow; `TestGenerator.EveryProfileExclusionHoldsOnTheWholeTree` runs it for every single exclusion
and some combinations.

The statement list carries BEGIN, COMMIT and ROLLBACK markers, so a consumer can group statements into
explicit transactions without a new format. Every list keeps its seed.

`FuzzRandom` is xoshiro256** seeded through SplitMix64. Its sequence is part of the replay contract, so
`TestGenerator.TheRandomSourceIsFixed` pins it.

At least 90 % of the generated statements run with no error on a fresh database
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
