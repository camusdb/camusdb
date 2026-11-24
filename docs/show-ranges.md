# `SHOW RANGES` / `SHOW RANGE`

CamusDB shards a table's row space and its eligible index spaces across Raft partitions, and Kahuna
divides those spaces further as they grow. The query planner reads that layout for each plan and
prices the fraction of spans whose leader is on another node. The distributed executor dispatches
fragments by it. This statement shows that layout over SQL.

```sql
SHOW RANGES FROM TABLE users;
SHOW RANGES FROM INDEX users@users_pkey;
SHOW RANGE  FROM TABLE users FOR ROW (1500);
SHOW RANGE  FROM INDEX users@by_email FOR ROW ('a@example.com');
```

It answers questions that no other statement answers.

- Whether this table has ever split, or is one range that spans its key space.
- Where its ranges' leaders are, and whether this node is one of them.
- Which range contains a given row, when one key is hot.
- Whether this index is key-range routed or hash-routed.
- Whether a plan's network cost is high because the data is remote, or because this node's range map
  is stale.

This statement shows placement. For the cost that placement produced, see
[`EXPLAIN`](explain.md); for Kahuna/Kommander runtime meters, see
[`SHOW ENGINE STATS`](engine-stats.md); for each Raft partition with its leader, replicas and load,
see [`SHOW PARTITIONS`](show-partitions.md); for the sharding mechanism itself, see
[key-range sharding](key-range-sharding.md).

`RANGES`, `RANGE` and `ROW` are not reserved words. All three can be used as table names, column
names and aliases.

## Result columns

One row per span, in ascending ordinal key order. Kahuna's router binary-searches in this order,
which means that the rows appear in the order routing uses.

| Column | Type | Meaning |
|--------|------|---------|
| `relation` | string | `users` for a table, `users@by_email` for an index. |
| `key_space` | string | The Kahuna bucket prefix, e.g. `3f2a…:7|r` or `3f2a…:7|i:2`. |
| `routing` | string | `key_range` or `hash` — **as this node routes it**. |
| `span` | int64 | 1-based position within this key space. Not a stable range identity. |
| `start_key` | string | Decoded lower bound. NULL = unbounded. |
| `end_key` | string | Decoded upper bound. NULL = unbounded. |
| `raw_start_key` | string | The encoded KV bound verbatim. NULL = unbounded. |
| `raw_end_key` | string | The encoded KV bound verbatim. NULL = unbounded. |
| `partition_id` | int64 | The Raft partition currently serving the span. |
| `generation` | int64 | Routing generation, the split/merge fence. 0 for hash. |
| `leader` | string | Leader endpoint hint. NULL means unknown. **It does not mean "no leader".** |
| `leader_is_local` | bool | This node believes it leads the span. |
| `hosted_locally` | bool | This node hosts the partition at all. |
| `replicas` | string | Comma-joined endpoints. Empty means legacy full replication. **It does not mean "no replicas".** |
| `probe_key` | string | The exact KV key `FOR ROW` located. NULL in the plural forms. |

Use `partition_id`, and not `span`, to identify the same range in two executions of the statement. A
split renumbers all spans after it, while the partition's identity does not change.

### Decoded bounds and raw bounds

`start_key` and `end_key` are rendered in column terms so the output is readable: an index bound
decodes to a comma-joined tuple of its key columns, and a row bound is the row's 24-character
hexadecimal id. `raw_start_key` and `raw_end_key` are the encoded KV keys themselves.

Both exist because decoding does not always produce a tuple of all key columns. A non-unique index
appends the row id to its key and uses no separator. A split point is also chosen from sampled keys.
This means that a bound can land at a boundary the decoder does not accept. In that case, the decoded
column shows the raw text, and no error is raised. **This statement never fails because a bound will
not decode.**

## `FOR ROW`

The two `FOR ROW` forms find their span in different ways.

**On an index**, the probe key is computed from the values alone. It needs no read and answers for a
key that does not exist. You may pass **fewer** values than the index has key columns; a prefix lands
in exactly one span, the range that the prefix starts in.
More values than key columns, or a value that will not convert to its key column's type, is an
error — both would otherwise locate some plausible-looking wrong span.

**On a table** the probe key cannot be computed from the values, which differs from CockroachDB. A
CamusDB row key is `{dbId}:{tableId}|r/{rowIdHex24}`. It is ordered by the row id that the engine
minted for the row. **The primary key does not determine its position.** This means that the range
holding the row with primary key `1500` cannot be derived from `1500`. The statement therefore encodes the values against the primary index, point-reads
that entry to obtain the row id, and locates the span holding that row's key.

The lookup through the primary index has two consequences.

- A primary key no row carries raises an error naming that fact. It does not return zero rows.
  An empty result would look the same as a filter that matched no rows, and inventing a key
  would be worse than either. Ask the index form when the question is about a key that need not exist.
- CockroachDB can compute a table row key from its primary key and will resolve a nonexistent one to
  a range. CamusDB cannot.

The primary-index probe is **lock-free and non-tracking**: it acquires no lock and does not add to
the surrounding transaction's read set. Running `SHOW RANGE … FOR ROW` in a serializable
transaction cannot change whether that transaction commits.

## Three properties that are not bugs

**The answer is node-local.** The range map is *this node's applied view*. A lagging follower
shows an older `generation` and possibly a pre-split shape. Another node may give a different
answer. There is no cluster-wide form. To compare two nodes, run the statement on each.

**`routing` is unreplicated node-local state.** A key space is opted into key-range routing when a
node opens the relation. The same space can therefore read `key_range` here and `hash` on a node that
has not opened the table. That reflects the other node's registration. It is not a fault and does
not need to be reconciled.

**Leadership is a hint.** `leader` is gossip and local belief. NULL means unknown. The hint is not a
correctness gate. Execution always re-resolves through the Kahuna locator. This means that a stale
hint costs a network hop and cannot produce a wrong answer.

## Standalone and sharding-off behavior

A hash-routed key space has exactly one span: both bounds NULL, `generation` 0, `routing` shown as
`hash`. That is usually the diagnostic you need. An index whose key columns are not order-safe does
not get a range to split, and neither does any space when key-range sharding is off.

A **standalone** node also splits. Key-range sharding works on a single node too. A single-node
engine with it enabled shows as many spans as it has divided its space into, with
`leader_is_local` and `hosted_locally` true and `replicas` empty.

`FOR ROW` performs its lookup in all configurations. The statement means the same thing everywhere,
split or not: *the span that contains this row*.

## Targets

- `FROM TABLE t` returns the spans of `t`'s row space.
- `FROM INDEX t@i` returns the spans of index `i`'s space on table `t`.
- The primary index is stored internally as `~pk`. Three spellings resolve to it: `t@~pk`,
  `t@t_pkey` and `t@primary`. If an index is literally named `primary` or `t_pkey`, that name refers
  to the index. The aliases apply when no index matches the name exactly.
- Asking for an index that no query can read yet (one that is backfilling or not yet public) returns
  an error. `SHOW INDEXES` hides such an index for the same reason.
- A **view** that is not materialized stores no rows and does not have a key space; ask for the
  ranges of the tables its body reads. A **materialized view** is a real relation and returns its
  ranges normally.
- Quoted or escaped identifiers are not accepted in the `table@index` token. `` `my_table`@`my_index` ``
  fails to parse. This means that it cannot be silently misread.

The target's key space is always taken from the relation's live store. This matters for a
materialized view. A refresh moves the contents to a fresh key space, and the relation's name does
not change. This means that `key_space` can change between two executions of the statement while
`relation` does not.

## Privileges

`SELECT` on the target table. Not superuser-gated.

`SELECT` is required because a decoded bound **is a real column value** out of the
table — a split point is data. Only a user who may read the rows may see where they divide.
The raw bound columns carry the same values in encoded form and are gated identically.

## What it does not report

- **No per-range size or row count.** Kahuna's range descriptors carry only a start key, an end key,
  a partition and a generation. The auto-splitter's size sampling is internal to the partition
  leader. A number here would have to come from scanning each span, which is an unbounded scan behind
  an introspection statement. It could also come from table statistics. That would present an
  estimate of the *table* as a measurement of a *range*, and the estimate is most wrong exactly when
  ranges are skewed and someone is looking. Reporting it needs a new Kahuna API. For this reason, the
  columns are deliberately absent.
- **No database-wide form.** One relation per statement.
- **No cluster-wide reconciliation.** All columns describe this node.
- **No split or merge administration.** This statement shows placement and does not move anything.

## Reading it alongside other statements

`EXPLAIN` shows a plan's network factor, which is derived from the fraction of spans whose leader
is remote. When that number looks wrong, `SHOW RANGES` is how you tell the two explanations apart:
count the rows with `leader_is_local = false` and see whether the data really is remote, or whether
this node's `generation` is behind a peer's.

`SHOW ENGINE STATS` shows what this process is doing. `SHOW RANGES` shows where the data is.
Neither substitutes for the other.

`SHOW RANGES` gives the `partition_id` of each span. [`SHOW PARTITIONS`](show-partitions.md) gives the
state of that partition: its leader, its replica set, its commit frontier, its write load and the lag
of its followers.
