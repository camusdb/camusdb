# Predicates

Output types from `PredicateAnalyzer` — the WHERE clause decomposition used by the query planner to select an index and build an execution filter.

| Type | Description |
|------|-------------|
| `PredicateAnalysis` | Top-level result: three lists — indexable comparisons, column comparisons, and residual conjuncts |
| `AnalyzedComparison` | A single column-vs-constant comparison (`column op value`) that can drive an index scan — carries the column name, operator, and bound value |
| `AnalyzedColumnComparison` | A column-vs-column comparison (`col1 op col2`) used for join planning and post-scan filtering |

`IndexScanSelector` scores available indexes against the `PredicateAnalysis` to pick the best access path. Residual conjuncts that cannot be satisfied by the chosen index are compiled into a `FilterNode` that runs after the scan.
