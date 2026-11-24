# Transaction Limits

CamusDB enforces resource bounds on transactions to protect nodes from unbounded memory
growth, oversized 2PC commit payloads, and runaway statements that pin locks across
the keyspace.

## Per-transaction mutation cap

### What it is

A read-write transaction is subject to a hard limit on the number of KV mutations it
may accumulate. A transaction that would exceed the cap is rejected immediately with error
**CADB0506** (`TransactionMutationLimitExceeded`) before any of the offending writes are
sent to Kahuna. The error is **permanent and non-retryable**: the caller must split the
work into smaller transactions.

### What counts as a mutation

CamusDB stores each row as a single KV blob (not column-per-cell). The mutation unit is:

> **One mutation = one row-blob write/delete, OR one index entry write/delete.**

**The primary key's index entry also counts.** A row is stored under a generated row id, and the
primary key is an index that maps the key value to that row id. This means that writing a row to a
table that has only a primary key costs **two** mutations. The insert figures are measured: an
`INSERT` reserves 2 mutations per row on a primary-key-only table and 3 with one secondary index.
`DELETE` collects its keys through the same index set and costs the same. An `UPDATE` reserves the row key plus the old and the new entry of each index it changes.

Below, **K is the number of secondary indexes** — the primary key is already counted separately.

| Operation | Mutations |
|---|---|
| `INSERT` one row into a table with K secondary indexes | `2 + K` |
| `UPDATE` one row, changing M indexed columns | `1 + 2M` (row rewrite + old-entry delete + new-entry insert per changed index) |
| `DELETE` one row from a table with K secondary indexes | `2 + K` |
| Touching the same row N times in one transaction | N × (per-row cost) |

**An out-of-line value is one more mutation.** A `string`, `bytes` or array value stored outside its
row (see [Large values](storage-layout.md)) is stored under a separate key. An `INSERT` or `DELETE` of
a row with `k` out-of-line values therefore costs `k` more mutations: `2 + K + k`. An `UPDATE` that
leaves a large value unchanged does not rewrite the value's key and costs no extra mutation. A table
with 4 large columns stored out of line and no secondary index costs 6 mutations per inserted row.
The default budget then admits 3,333 rows per statement, compared with 10,000 for a table that has
only a primary key.

The largest single-statement row count the default budget admits is `floor(20000 / (2 + K))`:
**10,000** rows for K = 0 and **6,666** for K = 1. A covering index costs the same as a regular
index. Its stored columns are part of the entry that the index already writes.

The counter is **monotonic**: updating the same row twice is counted twice.
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

Split the workload into multiple smaller transactions. On a large table, the following statement
can exceed the budget.

```sql
-- may exceed 20 000 mutations for large tables
UPDATE orders SET status = 'shipped' WHERE shipped_at IS NOT NULL;
```

The batched version limits each statement to a range of ids.

```sql
-- batch 1
UPDATE orders SET status = 'shipped'
WHERE shipped_at IS NOT NULL AND id >= :start AND id < :end;
-- repeat with advancing :start/:end until done
```

### Isolation-level independence

The cap applies to **all read-write transactions** regardless of isolation level (Read
Committed and Serializable alike) and regardless of `KeyRangeShardingEnabled`.

Read-only and zero-snapshot transactions do not mutate data, and the cap does not apply to them.

### DDL and system exemptions

Schema DDL operations and their backfill jobs run under transactions with the mutation limit
disabled (`mutationLimit = 0`). This covers CREATE INDEX and DROP INDEX, as well as ALTER TABLE ADD
COLUMN.
These system-internal operations may legitimately touch millions of index entries; they are
intentionally uncounted. Statistics and database-registry writes are also exempt.

User DML (INSERT / UPDATE / DELETE) is always counted, regardless of isolation level.

### Design notes

CamusDB counts mutations at the **KV-key** grain: one row blob plus one entry per
index, the primary key included. Because a row is stored as a single blob (not column-per-cell), a
multi-column update costs **1 row mutation** plus its changed index entries — not one
mutation per column. The default budget of 20 000 is therefore measured in
row-equivalent units.

A separate cap on **mutation bytes** (total commit payload size) is not yet implemented;
the current limit bounds mutation *count* only.
