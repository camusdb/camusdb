# Statistics

Collects and exposes runtime statistics for tables.

`StatisticsManager` is the central registry. `TableStatistics` holds per-table counters (row count, index size, etc.) that the query planner and monitoring surfaces can consume.

This module is currently minimal — the infrastructure is in place for the query planner to use table cardinality estimates for index-selection decisions.
