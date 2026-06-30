# Spill-to-Disk

Blocking query operators — sort, hash join, `GROUP BY`, `DISTINCT`, derived-table and subquery
materialization, and the `DELETE`/`UPDATE` row buffers — must hold intermediate rows in memory
while they work. On a large input that buffer can grow without bound and exhaust the process
heap. Spill-to-disk lets each of these operators offload its buffer to temporary files once it
grows past a configured threshold, trading memory for disk so a large statement completes instead
of OOM-ing the node.

The feature is **off by default** and is gated by a single flag. When it is off, every operator
keeps its original in-memory buffer and behaves exactly as before. When it is on but an operator's
input stays under the threshold, that operator also stays entirely in memory — the spill machinery
only engages once a buffer actually overflows.

## What spills, and what doesn't

CamusDB's query results are streamed to the caller row-by-row, so the final result set is never
fully materialized. The risk is only in **intermediate** buffers that an operator must hold before
it can produce its first output row. Those are the spill targets:

| Operator | Why it buffers | Spill strategy |
|---|---|---|
| Sort (`ORDER BY`) | needs all rows before emitting the first | external merge sort |
| Hash join | builds a hash table from one side | Grace / hybrid hash join (partition both sides) |
| `GROUP BY` | one accumulator per group | partition rows by group key, aggregate per partition |
| `DISTINCT` | must detect duplicates across all rows | sort, then drop adjacent duplicates |
| Derived tables / subqueries (`FROM (SELECT …)`) | materialized so they can be re-scanned | re-enumerable spill buffer |
| `DELETE` / `UPDATE` match set | all matching rows are collected before any mutation (see below) | re-enumerable spill buffer |

Operators that are already streaming or already bounded do **not** spill: global aggregates
(`COUNT(*)` with no `GROUP BY`), the streaming `DISTINCT` fast path over an ordered index, and the
streaming merge join, which only ever buffers a single equal-key run.

### Why `DELETE` and `UPDATE` buffer

A `DELETE`/`UPDATE` first locates every matching row, then mutates. It cannot mutate as it scans,
because changing a row can alter the very index the scan is walking — a row could be visited twice
or skipped (the *Halloween problem*). So the match set is collected in full and sealed **before**
the mutation phase begins. That collection is the buffer that spills.

## How it works

### Threshold and runs

Each operator accumulates rows in memory up to `SpillThresholdRows`. When the next row would
exceed the cap, the operator serializes its current buffer to a spill file and continues. A sort
writes each buffer as a **sorted run**; the other operators write partitions or a single overflow
file depending on their strategy. The threshold is applied **per operator instance**, not globally
— two operators in the same query each get their own budget.

### Merging

The external merge sort combines the sorted runs with a k-way merge backed by a priority queue.
If the number of runs exceeds `SpillMergeFanIn`, the merge runs in multiple passes — runs are
merged in groups of `SpillMergeFanIn` until a single ordered output remains. `DISTINCT` reuses the
sort and then drops adjacent equal rows in constant memory. The Grace hash join partitions both
the build and probe sides by a hash of the join key; a partition whose build side is still too
large is recursively re-partitioned, with a load-everything backstop at a bounded recursion depth
for the pathological case of a single over-represented key.

### The row codec

Intermediate rows can be synthetic — post-join, post-projection shapes that do not correspond to
any table schema — so the normal row serializer (which needs a `TableSchema`) cannot encode them.
Spill files use a dedicated **schema-less binary codec** that frames each row as a length-prefixed
record carrying its row id, column count, and each column's name, type tag, and value. The row id
is preserved end-to-end, which matters for `DELETE`/`UPDATE`: after a spilled match set is read
back, each row is still keyed by its original id when the mutation is applied.

### File layout

All spill files live under the data directory:

```
{data_dir}/tmp/spill/{instanceId}/{scopeId}/
```

Each process owns a unique `{instanceId}` directory for its lifetime; each spilling operation gets
its own `{scopeId}` subdirectory (a *spill scope*). When the operation finishes, its scope — and
every file in it — is deleted.

## Cleanup and crash recovery

A spill scope is disposed in a `try`/`finally` around the operator, so its files are removed on
**normal completion, cancellation, and exceptions** alike. Buffers that never overflow create no
files at all.

Files can still be orphaned if a process is killed mid-query. To recover that space, each process
holds an exclusive `.lock` file inside its `{instanceId}` directory for its whole lifetime, and
runs a **startup sweep**: it scans every instance directory under the spill root and tries to open
each `.lock` exclusively. A lock it can open belonged to a process that has exited or crashed, so
that directory is stale and is deleted; a lock that is still held belongs to a live concurrent
process and is left alone. The current process's own directory is skipped. On a graceful shutdown
the lock is released explicitly.

## Configuration

Spill is controlled by these knobs, settable in `config.yml` as `spill_enabled` /
`spill_threshold_rows` / `spill_merge_fan_in` (see [configuration.md](configuration.md)):

| Knob | YAML key | Default | Meaning |
|---|---|---|---|
| `SpillEnabled` | `spill_enabled` | `false` | Master switch. When `false`, no operator spills and behavior is identical to pre-spill CamusDB. |
| `SpillThresholdRows` | `spill_threshold_rows` | `500_000` | Per-operator in-memory row cap before that operator begins spilling. |
| `SpillMergeFanIn` | `spill_merge_fan_in` | `16` | Maximum simultaneously-open spill runs during a merge pass; more runs trigger a multi-pass merge. |

When `SpillEnabled` is `false`, the threshold and fan-in knobs are ignored.

## Error CADB0507

If an operator needs to spill but the temp store is unwritable (permissions, a full disk, a
missing data directory), it raises **CADB0507** (`SpillStorageUnavailable`) rather than silently
falling back to unbounded memory. The failure is surfaced to the caller instead of risking the OOM
the feature exists to prevent. CADB0507 is **not retryable** — retrying cannot make the temp store
writable; the underlying storage problem must be fixed.

## Known limitations

With `SpillEnabled` off, this does not apply — behavior is unchanged from a build without spill.

- **Some IN / NOT IN value sets stay in memory.** When the inner subquery has an index on its
  projected column it is rewritten to a bounded semi/anti join (including inner `DISTINCT`, which is a
  no-op for membership). The remaining cases — no index on the inner column, inner `GROUP BY`/`HAVING`,
  correlated, or multi-column projections — fall back to materializing the value set; that collection
  spills to disk, but the membership test still holds the values in memory for the duration of the query.

## Design notes

- **Off is byte-identical.** With `SpillEnabled = false`, and with it on but under threshold, each
  operator keeps its exact original in-memory path. Spill is purely additive.
- **Correctness over cleverness.** `GROUP BY` spills raw rows rather than partial accumulators, and
  the hash join re-scans its build side on overflow. These cost extra I/O on the rare overflow
  path in exchange for a simpler, obviously-correct implementation.
- **Fail loud, never silent.** The cardinal rule is that a required spill which cannot be performed
  is an error (CADB0507), not a quiet return to unbounded buffering.

A byte-budget threshold (capping spill by estimated row width rather than row count) is not yet
implemented; the current threshold bounds row **count** only.
