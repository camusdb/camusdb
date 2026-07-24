# Covering Indexes — `INCLUDE` / Stored Columns

> **Audience:** developers writing SQL against CamusDB who want more queries answered from an
> index alone, and engineers maintaining the index schema, KV value format, DML maintenance, and
> query planner/scanner.
> **Scope:** the SQL surface, what a covering index stores, how a covered query avoids the
> primary-row fetch, how updates keep the payload fresh, and how the whole thing persists and
> replicates.

CamusDB lets a secondary index carry extra **non-key** columns in its stored entries so more
queries can be answered **index-only** — without a second lookup of the primary row. This matches
the `INCLUDE` syntax of SQL Server and PostgreSQL.

---

## Part I — Using covering indexes

### Declaring included columns

```sql
CREATE INDEX idx_orders_customer
    ON orders (customer_id)
    INCLUDE (status, total, created_at);
```

`customer_id` is the **key**: it drives ordering, range scans, and (for a `UNIQUE` index)
uniqueness. `status, total, created_at` are **stored/payload** columns — materialized inside each
index entry's value but never part of the key. You can also declare an inline covering index in
`CREATE TABLE`:

```sql
CREATE TABLE orders (
    id oid PRIMARY KEY,
    customer_id int64 NOT NULL,
    status string(32) NOT NULL,
    total float64 NOT NULL,
    KEY `idx_orders_customer` (customer_id) INCLUDE (status, total)
);
```

`ALTER TABLE … ADD [UNIQUE] INDEX … INCLUDE (…)` works too. `UNIQUE` applies to the **key columns
only**: `CREATE UNIQUE INDEX ix ON t (a) INCLUDE (b)` enforces uniqueness on `a`; `b` does not
participate.

### When a query is *covered*

A query is answered index-only when its predicate is served by the index key **and** every column
it projects is either a key column or an included column. Then no primary row is read:

```sql
-- Covered by idx_orders_customer (status, total are included):
SELECT customer_id, status, total FROM orders WHERE customer_id = 42;

-- NOT covered — created_at… is fine, but a column that is neither key nor included forces a
-- primary-row fetch:
SELECT customer_id, note FROM orders WHERE customer_id = 42;   -- 'note' not indexed
```

`EXPLAIN (ANALYZE)` proves it: the index scan node reports **`rows_read = 0`** for a covered query
(no primary rows fetched) and `rows_read > 0` when the row must be fetched.

### Rules

- An included column must exist and be a normal (public) column.
- An included column **cannot** also be a key column of the same index.
- Included columns are **unordered** payload — no `ASC`/`DESC` is allowed in `INCLUDE (…)`.
- Included columns may be any storable type, including `Array`/`Bytes` (they go into the value, not
  the order-preserving key).
- A predicate *on* an included column is still evaluated as an ordinary filter over the index-only
  row — an included column is not itself searchable via the index.
- An index may span at most `MaxIndexColumns` columns counting key **and** included columns
  (default 32), rejected at DDL time. Each written entry's encoded INCLUDE payload may be at most
  `MaxIndexIncludeTupleBytes` (default 4 KiB); an INSERT/UPDATE producing a larger payload is rejected
  and rolled back. Both guard against a covering index duplicating unbounded row data into every entry.

### Not yet supported

- `INCLUDE` on the primary key (the PK already covers whole rows).
- In-place `ALTER INDEX … ADD INCLUDE` — widen an existing index by drop + recreate instead.
- Dropping a table column that an index includes is rejected; drop or recreate the index first.

---

## Part II — How it works

### Storage format

INCLUDE never changes the index **key**, so ordering, uniqueness, and routing are identical to a
plain index. Only the entry **value** grows:

```
value payload = rowId (24 bytes, fixed) ‖ includeTuple
```

The rowId stays a fixed 24-byte prefix so a reader splits rowId from tuple without a length prefix.
The `includeTuple` serializes the included column values positionally, in include order, reusing the
same self-describing per-column format as the row encoder (a type marker + payload per column; a
`NULL` is a single marker byte). An index with **zero** included columns writes an empty tuple, so
its value is byte-identical to the historical rowId-only form — existing indexes need no migration.
Decoding is schema-driven and tolerant of a short/absent tuple (missing trailing columns read as
`NULL`), which keeps a partially-widened index readable.

### Keeping the payload fresh on writes

- **INSERT / backfill** materialize the included values into each new entry (included values may be
  `NULL`, unlike key columns).
- **DELETE** removes the entry by key; its value goes with it.
- **UPDATE** is the subtle case. An index entry is rewritten when the **key** changes (delete old
  key + insert new key) — and now **also** when an *included* value changes even though the key does
  not. In that case the key bytes are identical, so the entry value is **overwritten in place** on
  the same key (a plain `Set`, not `SetIfNotExists`, so a unique index's existing entry is refreshed
  rather than skipped). Miss this and a covered read would return stale payload.

### Query path

The planner marks a scan index-only when every required column is a key **or** included column. The
scanner then synthesizes each output row from the decoded key (key columns) and the decoded include
tuple (included columns), and never fetches the primary row — that is the `rows_read = 0` you see in
`EXPLAIN (ANALYZE)`. Both the range-scan and the unique-lookup paths support covering.

### Persistence & replication

Included columns are stored in the per-table schema as immutable column **ids** (`IncludeColumnIds`),
resolved to names at table open exactly like key columns. In a cluster the widened index definition
rides the existing `AddIndex` schema-log entry, so followers pick up the include metadata and serve
covered reads from the shared KV with no extra protocol.

### Surfacing

`SHOW CREATE TABLE` renders the clause (`… KEY \`ix\` (a) INCLUDE (\`b\`)`) so the DDL round-trips
through re-parse, and `SHOW INDEXES` adds an `Include` column listing the payload columns.
