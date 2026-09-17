# Large values: compression and out-of-line storage

CamusDB stores each row as one key-value entry. A `string`, `bytes` or array value can be large: a
document body, an image, an embedding. This page explains how CamusDB compresses such a value, how
it moves the value out of its row, how you control that per column, and how data that existed before
this feature reaches the new form.

Nothing on this page changes a query result. Compression and out-of-line storage change only the
physical form of stored bytes, the I/O a query does, and the size of each stored version.

## Why it exists

A row is also the unit of multi-version storage. Two problems follow when a row carries a large
value:

1. **Read I/O.** A query that reads only small columns still pulls the large value from storage.
2. **Write amplification.** An update of any column rewrites the whole row. A change to a 4-byte
   `status` column on a row with a 10 MB value writes 10 MB and adds a 10 MB version to the history.

Out-of-line storage moves the large value under its own key. The row keeps a 16-byte pointer. A
query that does not name the column never fetches the value, and an update of another column does
not rewrite it. Compression makes each stored copy smaller, for values inside the row and outside it.

## How a value is stored

The writer takes these steps for each non-null `string`, `bytes` or array value, in this order:

1. Encode the value to its raw bytes.
2. If the column strategy allows compression and the value has at least 256 bytes, compress it with
   LZ4. Keep the compressed form only if it saves at least
   `large_value_compression_min_saving_percent` (default 12%).
3. If the column strategy allows out-of-line storage, compare the **result** of step 2 with
   `large_value_threshold_bytes` (default 2048).
4. If the result is at or above the threshold, store it under its own key and put a pointer in the
   row. If not, store it inside the row.

Compression runs first on purpose. A value that compresses below the threshold stays in the row, so a
read of that column costs no second key.

The minimum-saving rule protects data that does not compress, such as float32 embeddings, JPEG images
and archives. Without the rule, such data would pay a decompression on every read and save nothing.

Every stored row records, for each variable-length cell, whether the cell is compressed and whether it
holds a pointer. A reader follows these marks. It never consults the current settings or the column
strategy. So no change to a setting or a strategy can make an existing row unreadable.

## Choosing a storage strategy

Each `string`, `bytes` and array column has a storage strategy. The names and the behaviour follow
PostgreSQL.

| Strategy | Compresses | Moves out of the row | Use it for |
| --- | --- | --- | --- |
| `EXTENDED` (default) | Yes, when it pays | Yes, at or above the threshold | Text, JSON, documents — most large columns |
| `MAIN` | Yes, when it pays | Never | Compressible values that a typical query reads, where a second key per row costs more than it saves |
| `EXTERNAL` | Never | Yes, at or above the threshold | Large values that do not compress, which queries often skip: images, archives, embeddings you read rarely |
| `PLAIN` | Never | Never | Values that every query reads and that do not compress, such as an embedding a KNN query reads for every row |

Set the strategy when you create the column:

```sql
CREATE TABLE docs (
    id         oid PRIMARY KEY,
    title      string,
    body       string STORAGE EXTENDED,
    thumbnail  bytes  STORAGE EXTERNAL,
    embedding  bytes(3072) STORAGE PLAIN
);
```

Change it later:

```sql
ALTER TABLE docs ALTER COLUMN thumbnail SET STORAGE PLAIN;
```

`SHOW CREATE TABLE` shows every strategy that was set explicitly. A strategy on a column of any other
type is refused with `CADB0414` (`ColumnStorageNotApplicable`).

### Examples

- **An article body that queries list by title.** Keep the default `EXTENDED`. A listing query
  (`SELECT id, title FROM docs`) never fetches the bodies. Text usually compresses to a third of its
  size or less, so bodies of up to about 6 KB often stay inside the row in compressed form.
- **A product image of 50 KB.** Use `EXTERNAL`. JPEG data does not compress, so an attempt only costs
  CPU. The image moves out of the row, and a price update does not rewrite it.
- **A 768-dimension embedding that a KNN query scans.** Use `PLAIN`. The embedding is 3072 bytes,
  which is above the threshold, and a KNN query reads it on every row. Out of line, each scanned batch
  would cost one extra fetch. See [Vector search](vector-search.md#storage-strategy-for-embeddings).
- **An embedding that a KNN query does not scan.** If a separate process reads the vectors rarely, use
  `EXTERNAL`: the rows stay small for every other query.

## What a query costs

- A query that does not name a large column never fetches its out-of-line values. `SELECT *` names
  every column, so it fetches all of them.
- A query that reads a large column fetches the out-of-line values of a batch of rows in one batched
  read, not one read per row.
- A compressed value is decompressed only when a query reads its column.
- An update that does not assign a large column, and does not use it in an index or a `CHECK`
  constraint, does not fetch or rewrite the value. It rewrites only the small row. This applies to a
  row written under the table's current layout. The first update of a row written before an
  `ADD COLUMN` or `DROP COLUMN` reads and rewrites its values once, in the new layout.

## Transaction limits

Each out-of-line value is one KV key, so it is one mutation. An insert or a delete of a row with `k`
out-of-line values costs `k` more mutations than the same row inline. An update that does not change
a large value costs nothing extra.

Example: a table with a primary key, no secondary index, and 4 large columns stores all 4 out of line.
Each inserted row costs `1 + 1 + 4 = 6` mutations, so the default limit of 20,000 admits 3,333 rows in
one transaction, instead of 10,000. See [Transaction limits](transaction-limits.md).

## Settings

All five settings are runtime settings. The first four have cluster scope, and
`large_value_resolve_batch_bytes` has node scope. A change applies to the next statement.

| Setting | Default | Meaning |
| --- | --- | --- |
| `large_value_threshold_bytes` | `2048` | Stored size at or above which a value moves out of the row. `<= 0` keeps every new value inline. |
| `large_value_compression_enabled` | `true` | Whether the writer may compress. Reads ignore this setting. |
| `large_value_compression_min_saving_percent` | `12` | Minimum saving for a compressed value to be kept. |
| `large_value_rewrite_batch_rows` | `200` | Rows per transaction for `ALTER TABLE ... REWRITE STORAGE`. At most half of `max_mutations_per_transaction`. |
| `large_value_resolve_batch_bytes` | `67108864` (64 MiB) | Decoded bytes of compressed and out-of-line values that one read, or one rewrite batch, resolves at a time. `<= 0` removes the bound. |

A compressed value can be up to 255 times larger than its stored form, so the number of rows in a read
says little about its memory. A scan, a batch read and a rewrite batch close at
`large_value_resolve_batch_bytes`, measured with the lengths each row records, before any value is
fetched. A single row that is larger than the bound is read alone. The setting changes memory use and
the number of fetches only. It never changes a result.

A setting change never alters an existing row. It decides the form of rows written after the change.

## Data written before this feature

Every row written before this feature is inline and uncompressed. It stays readable, with no operator
action, for the life of the database. Migration is an optimisation, never a requirement.

Existing data reaches the new form in three ways:

1. **Writes convert rows.** An `INSERT` or an `UPDATE` stores the row under the current rules. A table
   that receives writes converts itself over time.
2. **`SET STORAGE` does not rewrite rows.** It changes the form of future writes only, and it returns
   at once whatever the table size. This is deliberate: a table rewrite hidden inside an `ALTER` would
   make a metadata change take time proportional to the table.
3. **An explicit rewrite converts cold data.** Run it when a table receives few writes and you want its
   existing rows in the current form now:

   ```sql
   ALTER TABLE docs REWRITE STORAGE;
   ```

### How the rewrite behaves

- It processes the table in transactions of `large_value_rewrite_batch_rows` rows, in row-id order.
- It keeps each row's stored schema version, its values, its row id and every index entry. Only the
  physical form of the row and of its out-of-line values changes.
- It is **idempotent**. A row that is already in the target form is not written. A second run over a
  converted table reads the table and writes nothing.
- It is **resumable**. It records the last committed row id with each batch. If the statement stops,
  run it again: it continues after the last committed batch.
- It **never overwrites a user write**. Each batch is an optimistic transaction that takes no lock
  while it reads. If a user transaction commits a change to a row the batch read, the batch fails at
  commit and the user's write stands. The run retries those rows at its end. Rows that keep changing are
  left as they are; a write converts them anyway.
- It **can make a concurrent user write retry**. While a batch commits, the rows it writes are locked for
  a short time. A user transaction that writes one of those rows in that moment fails with the retryable
  `CADB0502` (`TransactionConflict`). An autocommit statement sent over HTTP or gRPC is retried up to 5
  times for you; an explicit transaction must retry from its start, as it must for any write conflict. Run the rewrite when writes to the table
  are few, and keep `large_value_rewrite_batch_rows` small on a busy table.
- It writes its progress to the server log: rows scanned, rows converted, rows already converted,
  rows deferred, bytes moved out of line and bytes saved by compression.
- It is **refused inside an explicit transaction** with `CADB0538` (`StatementNotAllowedInTransaction`),
  as `TRUNCATE` is. Each batch commits in a transaction of its own, so a `ROLLBACK` of your transaction
  could not undo it. There is a second reason. A Serializable transaction holds a lock on every row it
  read or wrote. A batch over those rows waits for the lock, fails, and is deferred. On a large table
  the statement would spend minutes on that, convert none of those rows, and still report success.
  Commit or roll back first, then run it.

A rewrite is safe for time-travel reads, branches and backups. It writes a new version of each row and
never modifies an old one. An `AS OF SYSTEM TIME` read before the rewrite, a branch forked before it,
and a restore to a point before it all read the old versions, which are complete.

## Downgrade: the one-way door

**A binary that predates this feature cannot read a row that is compressed or has an out-of-line
value.** Such a row carries a flag in its first four bytes that an older binary does not know.

If you must return to an older version, convert every table back first:

```sql
ALTER TABLE docs REWRITE STORAGE INLINE;
```

`INLINE` stores every value inside its row, uncompressed, and removes the out-of-line keys. The rows it
writes are byte-identical to rows an older binary writes. Before you downgrade:

1. Set `large_value_threshold_bytes` to `0` and `large_value_compression_enabled` to `false`, so that
   new writes stay inline.
2. Run `ALTER TABLE <table> REWRITE STORAGE INLINE` on every table.
3. Confirm that each run logs `0 deferred`. Run it again for a table that did not.

During a rolling upgrade, do not store large values until every node runs the new version. A node on
the old version cannot read a new-form row, including a row another node sends it for a distributed
query.

## Interaction with other features

- **Branches.** A branch reads the out-of-line values of an inherited row from its ancestor, at the
  fork timestamp, like the row itself. A write on the branch stores its values in the branch only; a
  delete writes tombstones. No branch write reaches the parent.
- **Time-travel reads.** A value is read at the same timestamp as its row.
- **Backups.** A backup captures the whole store, so out-of-line values are included.
- **`TRUNCATE TABLE` and `DROP TABLE`.** The retired or dropped contents include the out-of-line values,
  and the purge that reclaims the contents removes them. A table recovered with `RELINK` reads its
  values back.
- **Indexes.** An index entry holds the column value itself, not a pointer. Out-of-line storage changes
  only the row.
- **Distributed queries.** A node that scans a span for another node resolves the values that the query
  reads before it sends the rows.

## Error codes

| Code | Name | Meaning |
| --- | --- | --- |
| `CADB0414` | `ColumnStorageNotApplicable` | A storage strategy was given for a column type with no variable-length value. |
| `CADB0538` | `StatementNotAllowedInTransaction` | `ALTER TABLE ... REWRITE STORAGE` was issued inside an explicit transaction. |
| `CADB0540` | `LargeValueCorrupt` | A stored value failed to decompress, or did not match the checksum its row recorded at a fixed snapshot. |
| `CADB0541` | `LargeValueNotResolved` | An engine defect: a read path decoded a cell it did not resolve. Report it. |

A read without a snapshot can see a row and its value at two moments while a concurrent update commits.
CamusDB detects this with a checksum in the pointer and reads the row again. If the row keeps changing,
the read fails with the retryable `CADB0504` (`TransactionMustRetry`).

## Storage format reference

This section is for contributors. The row layout is owned by `CompiledRowCodec` and `RowStorageForms`
in `CamusDB.Core/Storage/Kv/`.

- **Flag.** Bit 31 of the leading schema-version word marks a row with a storage-form trailer. The other
  31 bits are the stored schema version. A row without compressed or out-of-line cells keeps the bit
  clear and has no trailer, so it is byte-identical to a row written before this feature.
- **Trailer.** After the variable area: an out-of-line bitmap and a compressed bitmap, one bit per
  variable column, then the variable column count and the offset of the variable offset directory, as
  two `u32` values. The trailer is at the end so that no fixed or variable offset moves, and it is
  self-describing so that the store can resolve cells without the schema.
- **Compressed inline cell.** A `u32` uncompressed length, then an LZ4 block.
- **Pointer cell.** 16 bytes: `u32` uncompressed length, `u32` stored length, `u64` XxHash64 of the
  stored bytes.
- **Out-of-line key.** `{dbId}:{tableId}|v/{rowIdHex24}{ordinalHex4}`, where the ordinal is the cell's
  variable-column ordinal in the row's stored layout. The key holds one `/`, so all large values of a
  table share the key space `{dbId}:{tableId}|v` and the table's placement group.
- **Resolution.** Store reads take a `LargeValueFetch`. The default resolves every marked cell, so any
  caller gets decodable bytes. A caller that decodes a known column set passes the same set, and the
  store resolves only those cells. A decoder that reads an unresolved cell raises `CADB0541`.
