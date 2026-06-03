# Plans

Physical plan node tree produced by `QueryPlanner` and `JoinQueryPlanner`. Nodes are composed into a tree; leaf nodes read from storage, inner nodes transform the row stream.

`PhysicalPlanNode` is the abstract base. Each node optionally holds a `RequiredColumns` set (computed by `ProjectionPushdownPlanner`) that tells the scan layer which columns to decode.

| Node | Description |
|------|-------------|
| `TableScanNode` | Full scan over all rows or a forced index bucket |
| `IndexRangeScanNode` | Bounded or half-bounded scan over a sorted index key range |
| `IndexLookupNode` | Point lookup by an exact index key |
| `FilterNode` | Evaluates a predicate AST and drops non-matching rows |
| `ProjectNode` | Evaluates the SELECT projection list |
| `SortNode` | Materializes and sorts the row stream for ORDER BY |
| `LimitNode` | Applies LIMIT / OFFSET |
| `AggregateNode` | GROUP BY + aggregate functions |
| `HavingFilterNode` | HAVING predicate applied after aggregation |
| `DistinctNode` | Deduplicates rows for SELECT DISTINCT |
| `NestedLoopJoinNode` | Cross or inner join by iterating all combinations of two row streams |
| `IndexNestedLoopJoinNode` | Inner join using an index on the inner table for each outer row |
| `DerivedTableScanNode` | Reads rows from a previously executed derived table (subquery in FROM) |
