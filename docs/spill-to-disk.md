# Spill-to-Disk

Blocking query operators, such as sort, hash join and `GROUP BY`, must buffer intermediate rows in
memory while they work. The table in the next section lists them all. On a large input that buffer
can grow until it exhausts the process heap. Spill-to-disk lets each of these operators offload its
buffer to temporary files once it grows past a configured threshold. This trades memory for disk.
A large statement then completes and the node does not run out of memory.

The feature is **on by default** and is gated by a single flag. When an operator's input is under
the caps, that operator works entirely in memory. The spill machinery engages only once a buffer
overflows. When the flag is off, operators buffer in memory and their buffers are not capped. The
[design notes](#design-notes) list the other differences between the two settings.

## What spills, and what doesn't

CamusDB's query results are streamed to the caller row-by-row. This means that the final result set
is not fully materialized. The risk is only in **intermediate** buffers that an operator must fill
before it can produce its first output row. The following table lists these spill targets.

| Operator | Why it buffers | Spill strategy |
|---|---|---|
| Sort (`ORDER BY`) | needs all rows before emitting the first | external merge sort |
| Top-k (`ORDER BY … LIMIT`) | retains the `LIMIT + OFFSET` best rows | a spilling selection that writes at most about k rows to disk, when the retained rows pass the byte cap |
| Hash join | builds a hash table from one side | Grace / hybrid hash join (partition both sides) |
| `GROUP BY` | one accumulator per group | partition rows by group key, aggregate per partition |
| `DISTINCT` | one row per distinct tuple | partition rows by tuple hash, dedup per partition |
| `UNION` | one row per distinct tuple | the `DISTINCT` path |
| `INTERSECT [ALL]`, `EXCEPT [ALL]` | one count per distinct row of the right operand | partition both operands by row hash, combine each pair of partitions in memory ([set-operations.md](./set-operations.md#spill-to-disk)) |
| Derived tables / subqueries (`FROM (SELECT …)`) | materialized so they can be re-scanned | re-enumerable spill buffer |
| Recursive CTE (`WITH RECURSIVE`) | the rows of one step, which the next step reads | re-enumerable spill buffer for each step; the copy of each returned row that `UNION` retains is in memory ([common-table-expressions.md](./common-table-expressions.md#memory)) |
| Window functions (`OVER (…)`) | a sort per `(PARTITION BY, ORDER BY)`, and the rows of one partition | external merge sort, and a re-enumerable spill buffer per partition; the window inputs of the current partition are in memory ([window-functions.md](./window-functions.md#spill-to-disk)) |
| `DELETE` / `UPDATE` match set | all matching rows are collected before any mutation (see below) | re-enumerable spill buffer |

Operators that are already streaming or already bounded do **not** spill. These are `UNION ALL`,
global aggregates (`COUNT(*)` with no `GROUP BY`), the streaming `DISTINCT` fast path over an
ordered index, and the streaming merge join, which buffers a single equal-key run at most.

### Why `DELETE` and `UPDATE` buffer

A `DELETE`/`UPDATE` locates all matching rows before it mutates any of them. It cannot mutate as it
scans. Changing a row can alter the very index the scan is walking, and a row could then be visited
twice or skipped (the *Halloween problem*). For this reason, the match set is collected in full and
sealed **before** the mutation phase begins. That collection is the buffer that spills.

## How it works

### Threshold and runs

Each operator accumulates rows in memory up to two caps: `SpillThresholdRows` rows and
`SpillThresholdBytes` estimated bytes. When the buffer reaches either cap, the operator serializes
its current buffer to a spill file and continues. A sort writes each buffer as a **sorted run**. The
other operators write partitions or a single overflow file, depending on their strategy. The caps
apply **per operator instance**. Two operators in the same query each get a separate budget.

A sort on an `ORDER BY` expression computes each key once per row. While its buffer is under both
caps, the keys are stored beside the rows. When the buffer reaches a cap, the keys become extra
columns of their rows. They go to the spill files with the rows and are not computed again. A sort
that does not reach the caps does the same work with spill on as with spill off.

The byte cap exists because a row count does not bound the heap. Ten thousand narrow rows and ten
thousand rows that each contain a 64 KiB string differ by three orders of magnitude. The bytes are
an estimate. Each buffered row is charged for the values it already references, as listed below.

- The cell count and the length of each string, bytes and array value.
- For a row read straight from the storage buffer (a borrowed row), the size of that buffer. The
  buffer is alive as long as the row is, even when the query reads one small cell.
- For a joined row, both input rows.

The estimate does not decode values or walk an object graph. It errs high. A high estimate spills
earlier than needed, which costs time but never changes a result.

A **top-k** (`ORDER BY … LIMIT k`, where k is `LIMIT + OFFSET`) retains its `k` best rows and
normally does not spill. When those `k` rows pass the byte cap, the top-k hands its rows, and then
the rest of the input, to a spilling selection. The external merge sort would write all of the
input to disk. The selection works in the following steps.

1. Rows collect in a buffer. When the buffer reaches the byte cap, it is sorted and merged with the
   kept run (the best rows so far, at most `k`, on disk) into a new kept run. The old run's file is
   deleted at once.
2. When the kept run contains `k` rows, its last row is a cutoff. An input row that does not rank
   strictly before the cutoff cannot be in the result. It is dropped.
3. At the end, the kept run and the buffer are merged, and the first `k` rows are returned.

This means that the selection writes at most about `k` rows to disk. A top-k over a large table does
not need `spill_max_total_bytes` to fit the table. Keys computed by an `ORDER BY` expression are not
computed again at the handover. Among rows with equal keys, the selection can return a different
subset than the in-memory top-k does. Both are correct. SQL does not define which of the equal rows
a `LIMIT` returns.

### Merging

The external merge sort combines the sorted runs with a k-way merge backed by a priority queue.
If there are more than `SpillMergeFanIn` of them, the merge takes multiple passes. Each pass merges
groups of `SpillMergeFanIn` files, until a single ordered output remains.

The Grace hash join partitions both the build and probe sides by a hash of the join key. A partition
whose build side is too large is recursively re-partitioned. At a bounded recursion depth, the
partition is loaded completely. This backstop is for the pathological case of a single
over-represented key.

### GROUP BY and DISTINCT

`GROUP BY` and `DISTINCT` count **distinct keys**. A million input rows that collapse into
three groups or three distinct tuples need three entries in memory and do not spill. Below the
threshold, `DISTINCT` returns each new tuple the moment it first arrives, in first-seen order. This
means that a `LIMIT` stops the scan early, exactly as with spill off.

When admitting one more key would pass `SpillThresholdRows`, or the estimated bytes of the keys
admitted so far (with their `GROUP BY` state) reach `SpillThresholdBytes`, the operator writes the
rest of its input to `SpillMergeFanIn` partition files by a hash of the key. The state built so far
moves in memory to the partition its key hashes to. This state is the `GROUP BY` accumulators, or
the tuples `DISTINCT` has already returned. All later rows with that key land in the same
partition. Each partition is then processed separately. A `DISTINCT` partition seeds its set with
the tuples it carried and returns only the tuples that are new. This means that no tuple is returned
twice. A partition that again reaches the threshold is re-partitioned under a new hash seed, up to a
fixed depth. Past that depth it finishes in memory.

After a spill, `DISTINCT` returns the rows that came before the overflow. The rest follow partition
by partition. SQL does not define an order for `DISTINCT` unless the query has an `ORDER BY`.

**Top-k groupings skip partitions.** For `GROUP BY ... ORDER BY COUNT(...) DESC LIMIT n` (the
`top_k` detail of the aggregate row in `EXPLAIN`), the spill code records 8 slot totals for each
partition while it writes the partition files. A slot total is the sum of the counts of the groups
that hash to that slot. This means that no group in the slot has a larger count. The partitions are
then read back from the largest slot total down. The bounded sort above the grouping tracks the
current n-th count. A partition whose largest slot total is at or below that count is not read
back, since none of its groups can enter the result. Its file is deleted with the others. The skip
applies only if all three of the following conditions are true.

- It applies to the first level of partitions only.
- The sort must be bounded (`n` at or below `SpillThresholdRows`).
- The aggregate must be `COUNT`. A `SUM` argument can be negative.

Data where all groups have similar counts gives no saving.

### The row codec

Intermediate rows can be synthetic. Post-join and post-projection shapes do not correspond to any
table schema. The normal row serializer needs a `TableSchema` and cannot encode them. Spill files
use a dedicated **schema-less binary codec**. It frames each row as a length-prefixed record that
carries the row id and the column count, and then the name, type tag and value of each column. The
row id is preserved end-to-end, which matters for `DELETE`/`UPDATE`. After a spilled match set is
read back, each row is keyed by its original id when the mutation is applied.

### File layout

All spill files are stored under the data directory, in a directory for each process and a
subdirectory for each spilling operation.

```
{data_dir}/tmp/spill/{instanceId}/{scopeId}/
```

Each process owns a unique `{instanceId}` directory for its lifetime. Each spilling operation gets a
separate `{scopeId}` subdirectory (a *spill scope*). When the operation finishes, its scope and the
files in it are deleted.

### Disk limits

Spill files share the volume with the storage engine. A storage engine that hits a full disk enters
an error state that a restart does not clear. For this reason, a spill must fail its statement
before it can take that space. Both of the following limits apply to each spill write.

- **Total spill bytes.** `spill_max_total_bytes` limits the bytes of all live spill files on the
  node, summed over all queries that spill at the same time. A write that would pass it fails the
  statement with **CADB0549** (`SpillLimitExceeded`).
- **Free-space floor.** A spill never takes the volume below `min_free_disk_bytes`, the same
  watermark that stops DML. A write that would pass it fails the statement with **CADB0536**
  (`InsufficientDiskSpace`). This means that a spilling query cannot be the cause that stops writes
  on the node.

Each write reserves its bytes before it reaches the file. A write that is refused puts no bytes on
disk. The scope gives its bytes back when it is disposed, which is also when its files are deleted.
The free space is probed at intervals of at least two seconds. Between two probes, the estimate is the
last reading less the spill bytes written since. A fast writer cannot pass the floor between two
probes. If the probe fails, the floor check passes. The total limit applies in that case too.

## Cleanup and crash recovery

A spill scope is disposed in a `try`/`finally` around the operator. Its files are removed when the
operator completes normally, when it is cancelled and when it throws. Buffers that do not overflow
create no files at all.

Files can be orphaned if a process is killed mid-query. To recover that space, each process
locks an exclusive `.lock` file in its `{instanceId}` directory for as long as it is alive. At
startup, the process sweeps the spill root. It tries to open the `.lock` file of each instance
directory exclusively. A lock it can open belonged to a process that has exited or crashed. That
directory is stale and is deleted. A lock that is held by another process belongs to a live
concurrent process, and that directory is left alone. The directory of the current process is skipped.
On a graceful shutdown the lock is released explicitly.

## Configuration

Spill is controlled by the knobs in the following table. Each one can be set in `config.yml` under
its YAML key (see [configuration.md](configuration.md)).

| Knob | YAML key | Default | Meaning |
|---|---|---|---|
| `SpillEnabled` | `spill_enabled` | `true` | Master switch. When `false`, no operator spills. A hash-join build past `HashJoinMaxBuildRows` rows or `HashJoinMaxBuildBytes` estimated bytes is executed as a nested-loop join. |
| `SpillThresholdRows` | `spill_threshold_rows` | `500_000` | Per-operator in-memory row cap before that operator begins spilling. |
| `SpillThresholdBytes` | `spill_threshold_bytes` | `268435456` (256 MiB) | Per-operator cap on the estimated bytes in memory before that operator begins spilling. Checked beside the row cap. `<= 0` removes it. |
| `SpillMergeFanIn` | `spill_merge_fan_in` | `16` | Maximum number of spill files that one merge pass opens at once. Past that number, the merge takes multiple passes. |
| `SpillMaxTotalBytes` | `spill_max_total_bytes` | `8589934592` (8 GiB) | Limit on the bytes of all live spill files on the node. `<= 0` removes it. The `min_free_disk_bytes` floor applies in that case too. |

When `SpillEnabled` is `false`, the threshold, fan-in and total-bytes knobs are ignored. This includes
the byte cap. With spill off, an operator has nowhere to put its rows. This means that the
spill-off behavior does not change.

Two settings apply only if `SpillEnabled` is `false`. They limit the hash-join build. That operator
has a fallback that needs no disk.

| Knob | YAML key | Default | Meaning |
|---|---|---|---|
| `HashJoinMaxBuildRows` | `hash_join_max_build_rows` | `1_000_000` | Rows a hash-join build may contain. Past it, the join is executed as a nested-loop join. |
| `HashJoinMaxBuildBytes` | `hash_join_max_build_bytes` | `1073741824` (1 GiB) | Estimated bytes a hash-join build may contain. Past it, the join is executed as a nested-loop join. `<= 0` removes it. |

A nested-loop join buffers its right side in memory under the same two limits, and reads a larger
right side again for each left row. The byte limit is high enough that a build of narrow rows
(about 500 estimated bytes each) reaches the row limit first. It stops a build of rows with a
4,000-character string near 124,000 rows.

The browser playground sets `SpillEnabled = false`. Its file system is in memory. A spill file there
frees no memory and adds only the cost to encode and decode the rows.

## Errors

### CADB0507

If an operator needs to spill but the temp store is unwritable (permissions, a full disk, a
missing data directory), it raises **CADB0507** (`SpillStorageUnavailable`) and the caller sees
the failure. The operator does not switch to unbounded memory. That would risk the out-of-memory
failure that the feature exists to prevent. CADB0507 is **not retryable**. Retrying cannot make the
temp store writable, and the underlying storage problem must be fixed.

### CADB0549 and CADB0536

A spill write that would pass `spill_max_total_bytes` raises **CADB0549** (`SpillLimitExceeded`).
A spill write that would take the volume below `min_free_disk_bytes` raises **CADB0536**
(`InsufficientDiskSpace`). See [Disk limits](#disk-limits). In both cases the statement fails, its
spill files are deleted, and a `DELETE` or `UPDATE` changes no row. Both map to HTTP 507 and gRPC
`RESOURCE_EXHAUSTED`. Neither is retried automatically. The same statement can succeed later, when
other queries release their spill files or when disk space is freed.

## Known limitations

These limits apply with `SpillEnabled` on. With it off, no operator spills and operator memory is
not capped.

- **The caps are per operator. A statement does not have a separate cap.** A statement with several retaining operators,
  and several statements at once, can use a multiple of `SpillThresholdBytes`.
- **Some state is not charged.** `GROUP BY` charges a group when it admits it. An aggregate whose
  state grows within one group (a `MIN` or `MAX` over strings) is charged at its starting size only.
  A `GROUP BY`, `DISTINCT` or Grace hash join partition that passes a cap at the deepest
  re-partition level finishes in memory (`GROUP BY`, `DISTINCT`) or as a nested-loop join over the
  partition files (hash join). The streaming merge join buffers one equal-key run, and that buffer
  is not capped.
- **Some IN / NOT IN value sets are held in memory.** When the inner subquery has an index on its
  projected column, it is rewritten to a bounded semi/anti join. This includes an inner `DISTINCT`,
  which is a no-op for membership. The value set is materialized when the inner column is not
  indexed, when the subquery has an inner `GROUP BY`/`HAVING` or is correlated, and when it projects
  several columns. That collection spills to disk. However, the membership test needs the values in
  memory for the duration of the query.

## Design notes

- **Under the caps, spill on returns the same rows as spill off.** An operator whose input is under
  both caps buffers its rows in memory and writes no file. Below the caps, the two settings differ
  in the following ways.
  - Each retaining operator estimates the bytes of each row that it buffers, to check
    `SpillThresholdBytes`. The measured cost is within the noise.
  - The hash-join build goes to the Grace hash join at the row cap or the byte cap. With spill off,
    it buffers up to `HashJoinMaxBuildRows` rows (1,000,000) or `HashJoinMaxBuildBytes` estimated
    bytes (1 GiB), and is then executed as a nested-loop join. Each
    such fallback adds one to `join.nested_loop_fallbacks` in
    [`SHOW ENGINE STATS`](engine-stats.md#join-fallbacks) and writes a warning to the server log, at
    most one each minute.
    The byte cap stops a build of three short columns (about 500 estimated bytes for each row) near
    530,000 rows, and a build of rows with a 4,000-character string near 31,000 rows. In a
    measurement, the Grace path took 1.25 times (750,000 short rows) to 2.3 times (36,000 wide rows)
    as long as the in-memory join.
  - A nested-loop join buffers its right side, and that buffer spills. It does not have a row cap.
    With spill off, it buffers at most `HashJoinMaxBuildRows` rows and `HashJoinMaxBuildBytes`
    estimated bytes, and reads a larger right side again for each left row.
  - The planner does not use the bounded top-k when `LIMIT + OFFSET` is above the row threshold. The
    external merge sort is used. In a measurement it was not slower than the top-k.
  - The EXPLAIN labels of `UNION`, `INTERSECT` and `EXCEPT` end in `, spill`.
- **Correctness first.** `GROUP BY` never writes or merges partial accumulators. The
  accumulators built before the overflow remain in memory and move with their partition. Only the
  rows that arrive after the overflow go to disk. This means that each group is accumulated once, in
  input order. The hash join re-scans its build side on overflow. That costs extra I/O on the rare
  overflow path in exchange for a simpler, obviously-correct implementation.
- **Fail loud.** A required spill that cannot be performed is an error (CADB0507, CADB0549 or
  CADB0536). The operator does not return quietly to unbounded buffering.
