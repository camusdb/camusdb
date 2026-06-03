# Queries

Query planning and pipeline execution for `SELECT` statements.

## Planning

| File | Role |
|------|------|
| `QueryBinder` | Name resolution: maps table/column names in the AST to open `TableDescriptor` objects, producing a `BoundSelectQuery` |
| `QueryPlanner` | Builds a physical plan tree for single-table queries (scan node → filter → project → sort → limit → aggregate) |
| `JoinQueryPlanner` | Builds a physical plan tree for multi-source (join) queries |
| `IndexScanSelector` | Chooses the best index scan for a predicate using a scoring model (unique lookup > range scan > order-by scan > full scan) |
| `IndexScanBoundAnalysis` | Extracts `FROM`/`TO` key bounds from a predicate for index range scans |
| `PredicateAnalyzer` | Decomposes a WHERE AST into indexable comparisons, column-vs-column comparisons, and residual conjuncts |
| `JoinEquiJoinAnalyzer` | Identifies equi-join predicates between two tables for index nested-loop join planning |
| `JoinEquiJoinIndexMatch` | Matches an equi-join predicate to an available index on the inner table |
| `JoinPredicatePushdown` | Splits a join WHERE clause into per-table predicates that can be evaluated before the join |
| `ProjectionPushdownPlanner` | Computes `RequiredColumns` for each plan node so scans fetch only needed columns |
| `RequiredColumnAnalyzer` | Walks an expression AST to collect the set of column names it references |
| `CommaJoinNormalizer` | Rewrites comma-separated `FROM` lists into explicit `INNER JOIN` syntax |
| `BoundSourceCatalog` | Resolves column references when multiple sources are in scope |

## Execution pipeline

| File | Role |
|------|------|
| `QueryScanner` | Leaf executor: full table scan or index range scan over `KvTableStore`, yields `QueryResultRow` |
| `QueryFilterer` | Evaluates a WHERE/filter AST expression against a row |
| `QueryProjector` | Evaluates the `SELECT` projection list and builds output `ColumnValue` dictionaries |
| `QueryProjectionResolver` | Resolves wildcard (`*`) projections to concrete column lists |
| `QuerySorter` | Materializes and sorts a row stream for `ORDER BY` |
| `QueryLimiter` | Applies `LIMIT` / `OFFSET` to a row stream |
| `QueryAggregator` | Executes `GROUP BY` + aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) |
| `QueryHavingEvaluator` | Filters aggregated groups using the `HAVING` clause |
| `QueryDistincter` | Deduplicates rows for `SELECT DISTINCT` |
| `QueryJoinExecutor` | Executes nested-loop and index nested-loop joins |
| `QueryPostScanPipeline` | Chains the post-scan pipeline stages (filter → project → sort → limit → aggregate) |
| `CorrelatedRowMerger` | Merges correlated outer rows with inner rows for joined results |
| `QueryRowMerger` | Merges two row dictionaries from different table sources |

## Subqueries

| File | Role |
|------|------|
| `SubqueryRewriter` | Detects subquery expressions in a SELECT AST and rewrites them for execution |
| `ExistsSubqueryAnalyzer` | Identifies `EXISTS (...)` subquery nodes |
| `ExistsSubqueryPreparer` | Prepares an `EXISTS` subquery plan before the outer query runs |
| `ExistsSubqueryExecutor` | Executes an `EXISTS` subquery check for each outer row |
| `ExistsSubqueryRegistry` | Holds all prepared `EXISTS` subqueries for a query execution |
| `ExistsSubqueryValidator` | Validates an `EXISTS` subquery structure |
| `InSubqueryAnalyzer` | Identifies `col IN (subquery)` nodes |
| `InSubqueryExecutor` | Executes an `IN` subquery per outer row |
| `InSubqueryMaterialization` | Pre-materializes an `IN` subquery result set |
| `ScalarSubqueryExecutor` | Executes a scalar subquery (one that returns a single value) |
| `SubqueryQueryExecutor` | Drives the execution of any subquery form |

## Derived tables

| File | Role |
|------|------|
| `DerivedTableExecutor` | Executes a subquery used as a table source (`FROM (SELECT ...) AS alias`) |
| `DerivedTableSchemaBuilder` | Infers the output schema of a derived table |

## Helpers

| File | Role |
|------|------|
| `ColumnValueAstBuilder` | Evaluates a literal or expression AST node to a `ColumnValue` |
| `QueryExpressionClassifier` | Classifies an expression as a literal, column reference, aggregate call, subquery, etc. |
| `QueryExpressionWalker` | Recursively walks an expression AST calling a visitor |
| `QueryAstComparer` | Structural equality comparison between two AST expression nodes |
| `QueryHavingWorkspace` | Working state for `HAVING` evaluation across a group |
| `QueryPlanStepAdapter` | Converts a `QueryPlanStep` into the matching `PhysicalPlanNode` |
| `PreparedExistsSubquery` | Immutable holder for a compiled `EXISTS` subquery plan |
| `SubqueryValueListAst` | Builds a synthetic `IN (v1, v2, …)` AST from a materialized subquery result |
