# Transaction Limits

CamusDB enforces resource bounds on transactions to protect nodes from unbounded memory
growth, oversized 2PC commit payloads, and runaway statements that pin locks across
the whole keyspace.

## Per-transaction mutation cap

### What it is

Every read-write transaction is subject to a hard limit on the number of KV mutations it
may accumulate. A transaction that would exceed the cap is rejected immediately with error
**CADB0506** (`TransactionMutationLimitExceeded`) before any of the offending writes are
sent to Kahuna. The error is **permanent and non-retryable**: the caller must split the
work into smaller transactions.

### What counts as a mutation

CamusDB stores each row as a single KV blob (not column-per-cell). The mutation unit is:

> **One mutation = one row-blob write/delete, OR one index entry write/delete.**

**The primary key has an index entry of its own, and it counts.** A row is stored under a generated
row id, and the primary key is an index mapping the key value to that row id — so writing a row to a
table with no secondary index at all costs **two** mutations, not one. The insert figures are
measured: an `INSERT` reserves 2 mutations per row on a primary-key-only table and 3 with one
secondary index. `DELETE` collects its keys through the same index set, so it costs the same; the
`UPDATE` row below predates this note and has not been re-checked.

Below, **K is the number of secondary indexes** — the primary key is already counted separately.

| Operation | Mutations |
|---|---|
| `INSERT` one row into a table with K secondary indexes | `2 + K` |
| `UPDATE` one row, changing M indexed columns | `1 + 2M` (row rewrite + old-entry delete + new-entry insert per changed index) |
| `DELETE` one row from a table with K secondary indexes | `2 + K` |
| Touching the same row N times in one transaction | N × (per-row cost) |

**An out-of-line value is one more mutation.** A `string`, `bytes` or array value stored outside its
row (see [Large values](storage-layout.md)) lives under its own key. An `INSERT` or `DELETE` of a row
with `k` out-of-line values therefore costs `k` more mutations: `2 + K + k`. An `UPDATE` that does not
change a large value costs nothing extra, because the value's key is not rewritten. A table with 4
large columns stored out of line and no secondary index costs 6 mutations per inserted row, so the
default budget admits 3,333 rows per statement instead of 10,000.

So the largest single-statement row count the default budget admits is
`floor(20000 / (2 + K))`: **10,000** rows with no secondary index, **6,666** with one. A covering
index costs the same as a plain one — its stored columns ride inside the entry it already writes.

The counter is **monotonic** — updating the same row twice counts twice, not once.
This reflects the real cost of holding the corresponding write intents in Kahuna until
the transaction commits.

### Default limit

`max_mutations_per_transaction: 20000` in the configuration file, which maps to
`CamusDBOptions.MaxMutationsPerTransaction`.

Setting this to `<= 0` disables the limit entirely (the reservation becomes a no-op and
the transaction behaves identically to pre-cap CamusDB).

### Error CADB0506

```
Transaction <id> would exceed the maximum of <limit> mutations
(already <n>, requested <k>); split the work into smaller transactions
```

HTTP status: **400 Bad Request** (caller error — the same class as a constraint violation).

CADB0506 is **not retryable**. It is not listed in `SerializableRetryHelper.RetryableCodes`,
and the autocommit retry wrapper propagates it to the caller unchanged. Retrying the same
transaction cannot succeed — the transaction must be redesigned to stay within the budget.

### How to handle it

Split the workload into multiple smaller transactions. For example, instead of:

```sql
-- may exceed 20 000 mutations for large tables
UPDATE orders SET status = 'shipped' WHERE shipped_at IS NOT NULL;
```

process in batches:

```sql
-- batch 1
UPDATE orders SET status = 'shipped'
WHERE shipped_at IS NOT NULL AND id >= :start AND id < :end;
-- repeat with advancing :start/:end until done
```

### Isolation-level independence

The cap applies to **every read-write transaction** regardless of isolation level (Read
Committed and Serializable alike) and regardless of `KeyRangeShardingEnabled`.

Read-only and zero-snapshot transactions never mutate and are never subject to the cap.

### DDL and system exemptions

Schema DDL operations (CREATE INDEX, ALTER TABLE ADD COLUMN, DROP INDEX, and associated
backfill jobs) run under transactions with the mutation limit disabled (`mutationLimit = 0`).
These system-internal operations may legitimately touch millions of index entries; they are
intentionally uncounted. Statistics and database-registry writes are also exempt.

User DML (INSERT / UPDATE / DELETE) is always counted, regardless of isolation level.

### Design notes

CamusDB counts mutations at the **KV-key** grain: one row blob plus one entry per
secondary index. Because a row is stored as a single blob (not column-per-cell), a
multi-column update costs **1 row mutation** plus its changed index entries — not one
mutation per column. The default budget of 20 000 is therefore measured in
row-equivalent units.

A separate cap on **mutation bytes** (total commit payload size) is not yet implemented;
the current limit bounds mutation *count* only.
