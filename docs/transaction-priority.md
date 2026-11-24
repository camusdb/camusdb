# Transaction priority

CamusDB can tag each transaction with a relative importance. When a node is saturated, the work you
care about starts before the work you don't.

**It is off by default and has no effect until you configure a concurrency ceiling.** Priority is
recorded and observable from the first release. However, no transaction is ever deferred until an
operator sets `kahuna.max_concurrent_sessions`. Read §4 before you do.

---

## 1. What it does, and what it does not

Priority decides **which queued transaction starts next** when the node is at its configured ceiling.
This is its one effect, and it has three consequences.

- **It orders starts only.** Once a transaction is admitted, it competes for CPU, I/O, and locks exactly
  like any other. A running transaction is not preempted, throttled, or descheduled. For a long-running
  statement, priority decides the start time and nothing after it.
- **It does not affect lock conflicts.** A `Background` transaction holding an exclusive lock blocks a
  `Critical` one exactly as a `Normal` one would. Isolation and commit semantics, including 2PC, do not
  change.
- **It is per node.** Each node orders only the work it receives. There is no cluster-wide fairness.

Priority does not stop background work from *consuming* resources. Use rate limiting for that;
see §5.

## 2. The scale

| Priority | Use for |
|---|---|
| `Background` | Bulk, deferrable work. Yields to everything. |
| `Low` | Below ordinary traffic, but latency-relevant. |
| `Normal` | **Default.** Everything that does not say otherwise. |
| `High` | Latency-critical work that should start ahead of ordinary traffic. |
| `Critical` | Must not be deferred. |

Don't tag ordinary application traffic `Critical`. If everything is critical, the ordering carries no
information. Also, `Critical` can claim reserved capacity. If you overuse it, it will use up the reserve
that makes the setting useful.

## 3. Setting a priority

**SQL** — `SET TRANSACTION PRIORITY` must be the first statement of the transaction, before any data
statement.

```sql
BEGIN;
SET TRANSACTION PRIORITY BACKGROUND;
DELETE FROM events WHERE created_at < '2026-01-01';
COMMIT;
```

The values are `BACKGROUND`, `LOW`, `NORMAL`, `HIGH` and `CRITICAL`, and they are case-insensitive. It may be
combined with `SET TRANSACTION LOCKING` and `SET TRANSACTION ISOLATION LEVEL`, in any order, as long
as all of them precede the first data statement.

Priority is consumed when the coordinator session opens. This means that it cannot be changed after the
transaction's first statement. `SET TRANSACTION PRIORITY` after that point returns an error.

**HTTP** — `/start-transaction` and the `execute-sql-*` endpoints take a `priority` field.

```json
{ "databaseName": "mydb", "priority": "background" }
```

An unrecognised value returns `InvalidInput`, like `locking` and `isolationLevel`.

**gRPC** — `SqlRequest` and `StartTxnRequest` have a `priority` field. The row-level CRUD requests have
one too. If you leave it unset, the server default is used. An unset field does not mean `Background`.

**Server default** — set `default_transaction_priority` in `config.yml`.

```yml
default_transaction_priority: normal   # background | low | normal | high | critical
```

Precedence: `SET TRANSACTION PRIORITY` → per-request field → `default_transaction_priority` → `Normal`.

## 4. Turning the gate on

Configure the gate with care. The settings are in the `kahuna:` section of `config.yml`.

```yml
kahuna:
  max_concurrent_sessions: 64             # 0 (default) = no gate at all
  transaction_priority_reserved_slots: 2  # slots only High/Critical may use
  transaction_priority_aging_threshold: 1000
  transaction_priority_max_queued: 4096
```

### `max_concurrent_sessions` is a ceiling on *work*, not on connections

CamusDB opens a coordinator session for **every** transaction. This includes its internal catalog writes,
schema checkpoints, and index backfill. This means that the setting bounds total concurrent engine work on
the node. A
ceiling below your normal concurrency turns a healthy node into a queueing one and adds latency for
no benefit. Size it at or above observed healthy concurrency; it is for shedding and ordering
*surplus* load.

(If you have read Kahuna's own admission guide: it describes this knob for clients that hold one
session per user session, where it bounds connections. That advice does not transfer to CamusDB.)

### A reserve is effectively mandatory

Because engine-internal work shares the same ceiling, set
`transaction_priority_reserved_slots` to at least 1 whenever you set a ceiling. Otherwise a flood of
user traffic can queue a schema checkpoint or a registry lookup behind it. A reserve of 1–2 is
usually enough. The reserve is subtracted from what ordinary traffic may use. This means that a large
reserve throttles your common case. A reserve at or above the ceiling is an error at startup.

### Aging bounds starvation — and erodes separation faster than you expect

A waiter's effective priority rises one level per `transaction_priority_aging_threshold` of waiting.
This means that low-priority work cannot starve forever. At the 1 000 ms default, a `Background` transaction
reaches `High` after roughly **three seconds** of waiting. `Background` is therefore a *soft* yield
measured in seconds. It does not wait for the node to be idle.

Raise the threshold (tens of seconds) to make background work wait longer. It is a single
global rate. Raising it also lengthens the worst-case wait for a starved ordinary
transaction. Setting it to `0` disables aging and permits indefinite starvation.

### How long a transaction waits at the door

A transaction that cannot be admitted waits in the queue for the **admission wait budget**. After that,
it returns the retryable error `CADB0504`. The transaction was not started, and a retry is always safe.
However, the node is shedding load, and you should retry after a back-off.

```yml
transaction_admission_wait_ms: 0          # 0 = leave the node's own budget in force

kahuna:
  default_admission_wait_ms: 5000         # node-side default when a caller asks for nothing
  max_admission_wait_ms: 30000            # hard clamp on any caller-supplied budget
```

This budget is separate from the transaction lifetime. `max_serializable_transaction_lifetime_ms`
(one hour by default) bounds how long an *admitted* transaction may live and doubles as the
abandoned-session reaper window; the budget above bounds how long an *unadmitted* one waits to begin.
A transaction meant to run for an hour is not meant to wait an hour to start. With a long door-wait,
a saturated node leaves requests open when it should shed them.

Keep the budget short, in seconds. Lengthening it does not increase throughput. It turns a prompt,
retryable refusal into a slow one. Also, each waiting transaction occupies a queue slot that
`transaction_priority_max_queued` would otherwise give to someone else.

## 5. What actually makes background work cheap

Priority decides who starts first. It does not reduce what running work consumes. For that, CamusDB
uses rate limiting and load-reactive backoff. These work when the gate is off.

- `auto_analyze_max_rows_per_second` — caps the background statistics scan's row rate.
- `auto_analyze_load_pause_threshold` — pauses or cancels a background analyze when in-flight
  foreground transactions exceed a threshold, re-checked during the scan as well as at start.

The auto-analyze **scan** is not admission-gated. It reads from a zero-identity snapshot and opens no
coordinator session. This means that no ceiling applies to it. Only its brief statistics-publish write
passes through the gate. The same is true of any read-committed autocommit `SELECT`. These statements
open no session and are never deferred.

## 6. What CamusDB tags internally

| Work | Priority | Reason |
|---|---|---|
| User statements | `Normal` | The default. |
| Index backfill | `Background` | Bulk, batched, deferrable — the best fit for the gate, since it re-enters admission per batch. |
| Statistics flush / analyze publish | `Background` | Maintenance; a deferred flush costs only optimizer freshness. |
| Schema checkpoint persist | `High` | A stalled checkpoint blocks DDL cluster-wide, and its commit is already deadline-bounded. |
| Database-registry cache-miss lookup | `High` | It is part of an already-admitted user request. Queueing it would stall work the gate has already let in. |

No internal work is tagged `Critical`.

## 7. Observability

The Kahuna node exposes admission gauges under `kahuna.tx_admission.*`, tagged by priority.

- `in_flight`
- `queued`
- `max_queue_depth`
- `admitted`
- `aged_promotions`
- `abandoned_while_waiting`
- `rejected_queue_full`

Watch `queued` first: zero means the gate is transparent and your ceiling is not binding.
Sustained non-zero at `High`/`Critical` means the ceiling is too low for the offered load, or that a
reserve is warranted.
