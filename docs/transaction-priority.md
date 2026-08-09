# Transaction priority

CamusDB can tag each transaction with a relative importance, so that when a node is saturated the
work you care about starts before the work you don't.

**It is off by default and does nothing until you configure a concurrency ceiling.** Priority is
recorded and observable from the first release, but no transaction is ever deferred until an operator
sets `kahuna.max_concurrent_sessions`. Read §4 before you do.

---

## 1. What it does, and what it does not

Priority decides **which queued transaction starts next** when the node is at its configured ceiling.
That is the whole feature. Three consequences are worth stating plainly, because each of them
surprises people:

- **It orders starts, not execution.** Once a transaction is admitted it competes for CPU, I/O, and
  locks exactly like any other. Nothing is preempted, throttled, or descheduled. For a long-running
  statement, priority decides one instant and nothing after it.
- **It does not affect lock conflicts.** A `Background` transaction holding an exclusive lock blocks a
  `Critical` one just as hard as a `Normal` one would. Isolation, 2PC, and commit semantics are
  completely untouched.
- **It is per node.** Each node orders only the work it receives. There is no cluster-wide fairness.

If your goal is to stop background work from *consuming* resources — rather than to decide who starts
first under contention — priority is the wrong lever. Rate limiting is the right one; see §5.

## 2. The scale

| Priority | Use for |
|---|---|
| `Background` | Bulk, deferrable work. Yields to everything. |
| `Low` | Below ordinary traffic but still latency-relevant. |
| `Normal` | **Default.** Everything that does not say otherwise. |
| `High` | Latency-critical work that should start ahead of ordinary traffic. |
| `Critical` | Must not be deferred. |

Don't tag ordinary application traffic `Critical`. If everything is critical, the ordering carries no
information — and `Critical` can claim reserved capacity, so over-using it destroys the reserve that
makes the setting useful in the first place.

## 3. Setting a priority

**SQL** — must be the first statement of the transaction, before any data statement:

```sql
BEGIN;
SET TRANSACTION PRIORITY BACKGROUND;
DELETE FROM events WHERE created_at < '2026-01-01';
COMMIT;
```

Accepted values: `BACKGROUND`, `LOW`, `NORMAL`, `HIGH`, `CRITICAL` (case-insensitive). It may be
combined with `SET TRANSACTION LOCKING` and `SET TRANSACTION ISOLATION LEVEL`, in any order, as long
as all of them precede the first data statement.

Priority is consumed when the coordinator session opens, so it cannot be changed after the
transaction's first statement — `SET TRANSACTION PRIORITY` after that point is rejected rather than
silently ignored.

**HTTP** — a `priority` field on `/start-transaction` and on the `execute-sql-*` endpoints:

```json
{ "databaseName": "mydb", "priority": "background" }
```

An unrecognised value is rejected with `InvalidInput`, like `locking` and `isolationLevel`.

**gRPC** — a `priority` field on `SqlRequest`, `StartTxnRequest`, and the row-level CRUD requests.
Leaving it unset means "use the server default"; it never means `Background`.

**Server default** — `default_transaction_priority` in `config.yml`:

```yml
default_transaction_priority: normal   # background | low | normal | high | critical
```

Precedence: `SET TRANSACTION PRIORITY` → per-request field → `default_transaction_priority` → `Normal`.

## 4. Turning the gate on

This is the part to be careful with. All the settings live under `kahuna:` in `config.yml`:

```yml
kahuna:
  max_concurrent_sessions: 64             # 0 (default) = no gate at all
  transaction_priority_reserved_slots: 2  # slots only High/Critical may use
  transaction_priority_aging_threshold: 1000
  transaction_priority_max_queued: 4096
```

### `max_concurrent_sessions` is a ceiling on *work*, not on connections

CamusDB opens a coordinator session for **every** transaction — including its own catalog writes,
schema checkpoints, and index backfill — so this bounds total concurrent engine work on the node. A
ceiling below your normal concurrency turns a healthy node into a queueing one and adds latency for
no benefit. Size it at or above observed healthy concurrency; it is for shedding and ordering
*surplus* load.

(If you have read Kahuna's own admission guide: it describes this knob for clients that hold one
session per user session, where it bounds connections. That advice does not transfer to CamusDB.)

### A reserve is effectively mandatory

Because engine-internal work shares the same ceiling, set
`transaction_priority_reserved_slots` to at least 1 whenever you set a ceiling. Otherwise a flood of
user traffic can queue a schema checkpoint or a registry lookup behind it. A reserve of 1–2 is
usually enough; it is subtracted from what ordinary traffic may use, so a large one throttles your
common case. A reserve at or above the ceiling is rejected at startup.

### Aging bounds starvation — and erodes separation faster than you expect

A waiter's effective priority rises one level per `transaction_priority_aging_threshold` of waiting,
so low-priority work cannot starve forever. At the 1 000 ms default, a `Background` transaction
reaches `High` after roughly **three seconds** of waiting. `Background` is therefore a *soft* yield
measured in seconds, not a "runs only when the node is idle" class.

Raise the threshold (tens of seconds) to make background work genuinely patient. It is a single
global rate, so raising it also lengthens the worst-case wait for a genuinely starved ordinary
transaction. Setting it to `0` disables aging and permits indefinite starvation.

### How long a transaction waits at the door

A transaction that cannot be admitted queues for the **admission wait budget**, then fails with the
retryable `CADB0504` — nothing was started, so retrying is always safe, but the node is shedding load
and the retry should follow a back-off.

```yml
transaction_admission_wait_ms: 0          # 0 = leave the node's own budget in force

kahuna:
  default_admission_wait_ms: 5000         # node-side default when a caller asks for nothing
  max_admission_wait_ms: 30000            # hard clamp on any caller-supplied budget
```

This is deliberately **not** the transaction lifetime. `max_serializable_transaction_lifetime_ms`
(one hour by default) bounds how long an *admitted* transaction may live and doubles as the
abandoned-session reaper window; the budget above bounds how long an *unadmitted* one waits to begin.
A transaction meant to run for an hour is not thereby willing to wait an hour to start, and a long
door-wait makes a saturated node hold requests open instead of shedding them.

Keep the budget short — seconds. Lengthening it does not increase throughput; it only converts a
prompt, retryable refusal into a slow one, and every waiting transaction occupies a queue slot that
`transaction_priority_max_queued` would otherwise give to someone else.

## 5. What actually makes background work cheap

Priority decides who starts first. It does not reduce what running work consumes. For that, CamusDB
uses rate limiting and load-reactive backoff, which work with the gate off:

- `auto_analyze_max_rows_per_second` — caps the background statistics scan's row rate.
- `auto_analyze_load_pause_threshold` — pauses or cancels a background analyze when in-flight
  foreground transactions exceed a threshold, re-checked mid-scan rather than only at start.

Note that the auto-analyze **scan** is not admission-gated at all: it runs on a zero-identity snapshot
with no coordinator session, so no ceiling applies to it. Only its brief statistics-publish write
passes through the gate. The same is true of any read-committed autocommit `SELECT` — those hold no
session and are never deferred.

## 6. What CamusDB tags internally

| Work | Priority | Reason |
|---|---|---|
| User statements | `Normal` | The default. |
| Index backfill | `Background` | Bulk, batched, deferrable — the best fit for the gate, since it re-enters admission per batch. |
| Statistics flush / analyze publish | `Background` | Maintenance; a deferred flush costs only optimizer freshness. |
| Schema checkpoint persist | `High` | A stalled checkpoint blocks DDL cluster-wide, and its commit is already deadline-bounded. |
| Database-registry cache-miss lookup | `High` | Runs underneath an already-admitted user request; queueing it would stall work the gate has already let in. |

Nothing is tagged `Critical`.

## 7. Observability

The Kahuna node exposes admission gauges under `kahuna.tx_admission.*`, tagged by priority:
`in_flight`, `queued`, `max_queue_depth`, `admitted`, `aged_promotions`, `abandoned_while_waiting`,
and `rejected_queue_full`.

`queued` is the headline: zero means the gate is transparent and your ceiling is not binding.
Sustained non-zero at `High`/`Critical` means the ceiling is too low for the offered load, or that a
reserve is warranted.
