# Plan-quality harness

The plan-quality harness finds planner changes that make queries do more work. It runs a fixed
corpus of tagged queries on seeded data. For each query it measures the plan with counters that are
the same on every run. Then it compares the counters with a baseline file in git.

The main test suite checks that results are correct. It does not check that a plan is good. A cost
change can make a query read ten times more rows and still return the right rows. The harness
catches that.

The code is in `CamusDB.PlanQuality.Tests/`. It is a separate test project, so it does not add time
to the main suite.

## What it measures

The harness runs each query two times: one time as `EXPLAIN (ANALYZE)`, for the plan and its
counters, and one time as a plain query, for the result.

| Number | Where it comes from | Role |
|---|---|---|
| Result hash | The result rows, sorted, with each row's columns sorted by name. A float has 6 decimals. | **Gate.** Any change fails. |
| Work | The sum of `rows_read`, `kv_lookups` and `kv_scan_entries` over all plan nodes. | **Gate.** The main quality number. |
| Statement rows read | The rows the whole statement read, from the slow query log (`rows_read` in `SHOW SLOW QUERIES`). | **Gate.** It also counts reads that are on no plan node, such as the materialization of a view or a derived table. |
| Join steps costed | `join-steps-costed=` on the `plan-info` row of `EXPLAIN (ANALYZE)`. | **Gate.** It shows the work of the join-order search. |
| Plan fingerprint | One line for each plan node: the node name and its detail, without estimates or costs. | Report. It shows what changed. |
| Join order | `join-order=` on the `plan-info` row: `cost-based`, `heuristic` or `cached`. | Report. |
| Q-error | For each node, the larger of estimated/actual and actual/estimated (both values at least 1). A value of 1 is a perfect estimate. | Report: the median and the maximum for each query. |
| Planning time, execution time | `planning-ms=` and the root node's `actual_time_ms`. | Report only. They never go into a baseline. |

The harness does not use `estimated_cost`. The cost model is the thing that changes, so its
numbers do not compare across versions.

For a join that the join-order search costed, q-error uses the search's estimate
(`join-order-est-rows=`). The plan annotation estimates an index-nested-loop join as one row for
each outer row, which is a different quantity.

## The gate

A run fails when one of these conditions is true:

1. A query fails or passes the time limit (60 s by default).
2. A result hash changes.
3. The work, the statement rows read or the join steps costed of one query grow by more than
   1.5×, **and** by more than 100. The second limit stops a tiny query (3 reads to 6) from failing.
4. The geometric mean of all work ratios grows by more than 5%. This catches a small slowdown
   spread over many queries.
5. The corpus has a query that the baseline does not have, or the baseline has a query that the
   corpus does not have.
6. For the JOB tier: a result differs from SQLite on the same rows.

These are reported, but they do not fail the run: less work, a changed plan with the same work, a
change of join-order path, and a change of q-error by more than 2×.

The limits are in `GateThresholds` (`Baselines/BaselineComparer.cs`), each with its reason. They are
first values, to tune after the harness has run for some weeks.

## Tiers

| Tier | Data | Corpus | When it runs |
|---|---|---|---|
| Small | Retail data set, scale 1 (about 42,000 rows) | `Corpus/small/*.sql`, 99 queries | Each pull request and each push to `main` (`Plan-Quality` job in `run-tests.yml`) |
| Small, scale 4 | Retail data set, scale 4 | The same 99 queries | Nightly (`plan-quality-nightly.yml`) |
| JOB | Synthetic IMDB data from `scripts/plan-quality/job/gen_job.py`, scale 0.003 (about 225,000 rows) | The 113 Join Order Benchmark queries | Nightly |

The **retail data set** (`Data/RetailDataSet.cs`) has 19 tables. It is made to show estimate errors:

- Foreign keys follow a Zipf distribution, so a few customers, products and cities are very popular.
- Columns are correlated. Most orders go to a store in the customer's own city, and a product's
  price depends on its category.
- Join keys and filter columns have NULLs, and indexed columns have many duplicate values.
- Some foreign keys have an index and some do not.

The **JOB queries** come from `gregrahn/join-order-benchmark` at a pinned commit. That repository
has no license, so `scripts/plan-quality/job/prepare.sh` downloads the files at run time. They are
never committed.

## Run it locally

Run the pull-request gate (the small tier, the determinism check, the planted regression and the
unit tests):

```sh
dotnet test CamusDB.PlanQuality.Tests/CamusDB.PlanQuality.Tests.csproj --filter TestCategory=PlanQuality
```

It takes about 2 minutes. The report is in
`CamusDB.PlanQuality.Tests/bin/Debug/net10.0/plan-quality/small-sf1-report.md`. Set
`PLAN_QUALITY_REPORT_DIR` to write it somewhere else.

Run the small tier at another scale:

```sh
PLAN_QUALITY_SCALE=4 dotnet test CamusDB.PlanQuality.Tests/CamusDB.PlanQuality.Tests.csproj --filter FullyQualifiedName~SmallTierGate
```

Run the JOB tier. It needs `python3` and `curl`:

```sh
scripts/plan-quality/job/prepare.sh /tmp/job 0.003
JOB_DIR=/tmp/job JOB_SCALE=0.003 dotnet test CamusDB.PlanQuality.Tests/CamusDB.PlanQuality.Tests.csproj --filter FullyQualifiedName~JobTierGate
```

Without `JOB_DIR`, the JOB test is skipped. Baselines exist for scale 0.003 (the nightly scale, about
4 minutes) and for scale 0.001 (about 40 seconds, for a quick local check). A baseline update of the
JOB tier also runs the reference engine, which takes about 13 minutes at scale 0.003.

To look at some queries only, set `PLAN_QUALITY_ONLY` to a comma list of query ids. A partial run
does not report the queries it left out, and it never writes a baseline.

## Read the report

The report starts with the gate result and a summary table. Then it has these sections:

- **Failures**: each failure, with the query id, its tags and the numbers.
- **Notes**: changes that did not fail the gate.
- **Work by tag**: the work of each tag, before and now. Use it to see which query shapes moved.
- **Plan changes**: for each plan that changed, the old plan (`-`) and the new plan (`+`).
- **Worst estimates**: the 10 queries with the largest q-error.
- **Timings**: total planning and execution time, and the 10 slowest queries. This section is not
  graded.

The run also writes `<tier>-sf<scale>-run.json`, with every counter of every plan node.

## Update a baseline

When a planner change moves plans on purpose, update the baseline:

```sh
PLAN_QUALITY_UPDATE_BASELINE=1 dotnet test CamusDB.PlanQuality.Tests/CamusDB.PlanQuality.Tests.csproj --filter FullyQualifiedName~SmallTierGate
```

The update first runs each query on a **reference engine**, with `CostBasedJoinOrderEnabled` and
`CostBasedAccessPathEnabled` off. If a result is different, it writes nothing. It also writes
nothing when a query fails.

**Put the baseline diff in the same commit as the planner change.** In the pull request, say why each
plan moved. The diff shows each changed query on its own lines, because the entries are sorted by
id and each plan node is one line. A baseline that people update without a look measures nothing.

Each scale has its own file: `Baselines/small-sf1.json`, `Baselines/small-sf4.json`,
`Baselines/job-sf0.003.json` and `Baselines/job-sf0.001.json`. Update each tier that the change
moves.

## Add a query

Add it to a file in `Corpus/small/`:

```sql
-- query: jc-3-example
-- tags: join, join:chain, tables:3
SELECT ... ;
```

- The id must be unique. It is the key in the baseline, so a rename is a removal and an addition.
- Give at least one tag. Use the tags that exist (`single-table`, `join:chain`, `join:star`,
  `join:cycle`, `join:clique`, `join:snowflake`, `tables:N`, `comma-join`, `outer-join`,
  `subquery`, and so on), so the report groups the query with similar ones.
- End each `ORDER BY ... LIMIT` with a unique column, so the kept rows have one right answer.
- Keep each query under about 1 s. The whole small tier must stay short.

Then run the baseline update, and commit the new entry with the query.

## Why the numbers repeat

The gate is useful only if the same plan on the same data gives the same numbers on every run.
`PlanCountersRepeat` proves this on a sample of the corpus: it builds two engines, each with its own
load of the data, and compares every counter of every node over two runs and two engines. It also
compares the statistics.

The harness builds each engine with these options off, because each one can change a counter
between two runs:

| Option | Why it is off |
|---|---|
| `AutoAnalyzeEnabled` | Its reservoir sample is seeded by the time of the run. The harness runs `ANALYZE` explicitly instead, which reads each table in key order. |
| `PointReadRowIdCacheEnabled` | A repeated primary-key lookup skips its index read, so a query's counters depend on the queries that ran before it. |
| `QueryResultCacheEnabled`, `PlanCacheEnabled` | A cache hit skips the work that the harness measures. |
| `TtlEnabled` | A background job that could delete rows. |

The slow query log is **on**, with a threshold of 0, because it is the source of the statement
rows read.

All data comes from seeded generators. The retail generator uses its own SplitMix64 generator,
because the sequence of `System.Random` for a seed is not part of the .NET contract. The tables are
created and loaded in a fixed order, because table ids come from a sequence and row ids keep
insertion order. The JOB generator uses Python's seeded `random` module, and the nightly workflow
pins one Python version.

## Known gaps

- **Correlated `EXISTS` on an unindexed key.** The subquery scans the inner table for each outer
  row, through the store directly. Neither the plan nodes nor the statement probe count those reads,
  so the work of `sq-exists-correlated` is much lower than the real cost. Its execution time in the
  report shows the cost.
- **Subqueries that the bind stage folds into literals.** An `IN (SELECT ...)` or a scalar subquery
  runs before the plan, and its reads are not on any plan node.
- **Q-error under a `LIMIT`.** A scan below a limit stops early, so its actual rows are lower than
  its estimate. A large q-error on such a node is expected.
- **Row-id order.** A row id holds a counter that starts at a random value. If the counter wraps
  inside one second during a load, two rows change order in a full scan. No counter in the corpus
  depends on scan order, so this matters only for a plan whose early exit depends on it.
- **Cluster plans.** The harness runs one node. Broadcast joins, gathers and placement are not
  covered.
- **Wall time** is reported but not graded. The hosted runners are not stable enough to compare times.
