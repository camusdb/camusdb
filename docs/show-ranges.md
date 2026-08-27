# `SHOW RANGES` / `SHOW RANGE`

CamusDB shards a table's row space and its eligible index spaces across Raft partitions, and Kahuna
divides those spaces further as they grow. The query planner reads that layout on every plan and
prices the fraction of spans whose leader is on another node; the distributed executor dispatches
fragments by it. None of it was visible over SQL. This statement shows it.

```sql
SHOW RANGES FROM TABLE users;
SHOW RANGES FROM INDEX users@users_pkey;
SHOW RANGE  FROM TABLE users FOR ROW (1500);
SHOW RANGE  FROM INDEX users@by_email FOR ROW ('a@example.com');
```

It answers questions nothing else does:

- Has this table ever split, or is it still one whole-space range?
- Where are its ranges' leaders, and is this node one of them?
- Which range holds a given row, when one key is hot?
- Is this index key-range routed at all, or still hash-routed?
- Is a plan's network cost high because the data really is remote, or because this node's range map
  is stale?

This is placement, not data and not process metrics. For the cost that placement produced, see
[`EXPLAIN`](explain.md); for Kahuna/Kommander runtime meters, see
[`SHOW ENGINE STATS`](engine-stats.md); for the sharding mechanism itself, see
[key-range sharding](key-range-sharding.md).

`RANGES`, `RANGE` and `ROW` are not reserved words. All three stay usable as table names, column
names and aliases.

## Result columns

One row per span, in ascending ordinal key order — the order Kahuna's router binary-searches, so
what you read is the order routing uses.

| Column | Type | Meaning |
|--------|------|---------|
| `relation` | string | `users` for a table, `users@by_email` for an index. |
| `key_space` | string | The Kahuna bucket prefix, e.g. `3f2a…:7:r` or `3f2a…:7:i:2`. |
| `routing` | string | `key_range` or `hash` — **as this node routes it**. |
| `span` | int64 | 1-based position within this key space. Not a stable range identity. |
| `start_key` | string | Decoded lower bound. NULL = unbounded. |
| `end_key` | string | Decoded upper bound. NULL = unbounded. |
| `raw_start_key` | string | The encoded KV bound verbatim. NULL = unbounded. |
| `raw_end_key` | string | The encoded KV bound verbatim. NULL = unbounded. |
| `partition_id` | int64 | The Raft partition currently serving the span. |
| `generation` | int64 | Routing generation, the split/merge fence. 0 for hash. |
| `leader` | string | Leader endpoint hint. **NULL means unknown, not "no leader".** |
| `leader_is_local` | bool | This node believes it leads the span. |
| `hosted_locally` | bool | This node hosts the partition at all. |
| `replicas` | string | Comma-joined endpoints. **Empty means legacy full replication, not "no replicas".** |
| `probe_key` | string | The exact KV key `FOR ROW` located. NULL in the plural forms. |

Read `partition_id`, not `span`, when you want to talk about the same range across two runs. A split
renumbers every span after it, while the partition keeps its identity.

### Decoded bounds and raw bounds

`start_key` and `end_key` are rendered in column terms so the output is readable: an index bound
decodes to a comma-joined tuple of its key columns, and a row bound is the row's 24-character
hexadecimal id. `raw_start_key` and `raw_end_key` are the encoded KV keys themselves.

Both exist because decoding can legitimately fail to produce a whole tuple. A non-unique index
appends the row id to its key with no separator, and a split point is chosen from sampled keys, so a
bound can land at a boundary the decoder does not accept. When that happens the decoded column falls
back to the raw text rather than raising. **This statement never fails because a bound will not
decode.**

## `FOR ROW`

The two `FOR ROW` forms differ in kind, not only in target.

**On an index** the probe key is computed from the values alone, so it needs no read and answers for
a key that does not exist. You may pass **fewer** values than the index has key columns; a prefix
still lands in exactly one span, and "which range would this prefix start in?" is a useful question.
More values than key columns, or a value that will not convert to its key column's type, is an
error — both would otherwise locate some plausible-looking wrong span.

**On a table** it is not computable, and this is where CamusDB genuinely differs from CockroachDB. A
CamusDB row key is `{dbId}:{tableId}:r/{rowIdHex24}` — ordered by the row id the engine minted for
the row, **not** by its primary key. So the range holding the row with primary key `1500` cannot be
derived from `1500`. The statement therefore encodes the values against the primary index, point-reads
that entry to obtain the row id, and locates the span holding that row's key.

Two consequences follow:

- A primary key no row carries raises an error naming that fact, rather than returning zero rows.
  An empty result would be indistinguishable from a filter that matched nothing, and inventing a key
  would be worse than either. Ask the index form when the question is about a key that need not exist.
- CockroachDB can compute a table row key from its primary key and will resolve a nonexistent one to
  a range. CamusDB cannot, and does not pretend to.

The primary-index probe is **lock-free and non-tracking**: it acquires no lock and adds nothing to
the surrounding transaction's read set. Running `SHOW RANGE … FOR ROW` inside a serializable
transaction cannot change whether that transaction commits.

## Three properties that are not bugs

**The answer is node-local.** The range map is *this node's applied view*. A lagging follower
reports an older `generation` and possibly a pre-split shape. Asking another node may legitimately
give a different answer, and comparing two nodes is a reasonable thing to do — there is no
cluster-wide form, so run the statement on each.

**`routing` is unreplicated node-local state.** A key space is opted into key-range routing when a
node opens the relation. The same space can therefore read `key_range` here and `hash` on a node that
has never opened the table. That is a fact about the other node's registration, not a fault to
reconcile away.

**Leadership is a hint.** `leader` is gossip and local belief. NULL means unknown. Nothing here is a
correctness gate: execution always re-resolves through the Kahuna locator, so a stale hint costs a
network hop, never a wrong answer.

## Standalone and sharding-off behavior

A hash-routed key space has exactly one span: both bounds NULL, `generation` 0, `routing` reported as
`hash`. That is the honest answer and usually the diagnostic you came for — an index whose key
columns are not order-safe never gets a range to split, and neither does any space when key-range
sharding is off.

A **standalone** node still splits. Key-range sharding is not a cluster-only mechanism, so a
single-node engine with it enabled reports as many spans as it has divided its space into, with
`leader_is_local` and `hosted_locally` true and `replicas` empty.

`FOR ROW` performs its lookup in every configuration, so the statement means the same thing
everywhere: *the span that holds this row*, not *the span, if we happen to have split*.

## Targets

- `FROM TABLE t` reports `t`'s row space.
- `FROM INDEX t@i` reports index `i`'s space on table `t`.
- The primary index is stored internally as `~pk`. Three spellings resolve to it: `t@~pk`,
  `t@t_pkey` and `t@primary`. An index literally named `primary` or `t_pkey` keeps its own identity —
  the aliases apply only when no index matches the name exactly.
- An index that no query can read yet — still backfilling, or not yet public — is rejected rather
  than shown, for the same reason `SHOW INDEXES` hides it.
- A plain **view** stores no rows and has no key space; ask for the ranges of the tables its body
  reads. A **materialized view** is a real relation and reports its own ranges normally.
- Quoted or escaped identifiers are not accepted in the `table@index` token. `` `my_table`@`my_index` ``
  fails to parse rather than being silently misread.

The target's key space is always taken from the relation's live store. For a materialized view that
matters: a refresh moves the contents to a fresh key space while the relation keeps its name, so
`key_space` can change between two runs while `relation` does not.

## Privileges

`SELECT` on the target table. Not superuser-gated.

`SELECT` is the right bar precisely because a decoded bound **is a real column value** out of the
table — a split point is data. Whoever may read the rows may see where they divide, and nobody else.
The raw bound columns carry the same values in encoded form and are gated identically.

## What it does not report

- **No per-range size or row count.** Kahuna's range descriptors carry a start key, an end key, a
  partition and a generation, and nothing else; the auto-splitter's size sampling stays internal to
  the partition leader. A number here would have to come from scanning each span — an unbounded scan
  behind an introspection statement — or from table statistics, which would present an estimate of
  the *table* as a measurement of a *range*, most wrong exactly when ranges are skewed and someone is
  looking. Reporting it needs a new Kahuna API, so the columns are deliberately absent.
- **No database-wide form.** One relation per statement.
- **No cluster-wide reconciliation.** Every column describes this node.
- **No split or merge administration.** This statement reports; it does not move anything.

## Reading it alongside other statements

`EXPLAIN` reports a plan's network factor, which is derived from the fraction of spans whose leader
is remote. When that number looks wrong, `SHOW RANGES` is how you tell the two explanations apart:
count the rows with `leader_is_local = false` and see whether the data really is remote, or whether
this node's `generation` is simply behind a peer's.

`SHOW ENGINE STATS` reports what this process is doing. `SHOW RANGES` reports where the data is.
Neither substitutes for the other.
