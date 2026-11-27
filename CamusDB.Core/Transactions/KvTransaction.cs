
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.Transactions;

/// <summary>Lifecycle state of a <see cref="KvTransaction"/>.</summary>
public enum KvTransactionStatus
{
    /// <summary>Open and accepting data operations — the only state in which a read/write may register.</summary>
    Active,

    /// <summary>
    /// A commit or rollback has begun (the coordinator finalize fence is installed) but no terminal
    /// Kahuna result has been observed yet. This state is <b>non-terminal</b>: the transaction stays
    /// tracked and its handle stays valid so the <em>same</em> commit/rollback can be retried after a
    /// non-terminal <c>MustRetry</c>. New data operations are rejected once finalizing. It exists so a
    /// commit that returns <c>MustRetry</c> — meaning "outcome not known yet, retry the same handle" —
    /// is never mistaken for a completed rollback (which would let the caller re-run a business
    /// operation that may already have committed, double-applying it).
    /// </summary>
    Finalizing,

    /// <summary>Terminal: the coordinator returned <c>Committed</c>. The session is complete.</summary>
    Committed,

    /// <summary>Terminal: the coordinator returned <c>RolledBack</c> (or an equivalent definite
    /// non-committing outcome). The session is complete.</summary>
    RolledBack
}

/// <summary>
/// The bounds of a Kahuna key-range lock held by a transaction, retained purely as
/// <b>in-transaction coverage state</b>: the local index that lets a later read skip re-acquiring a
/// point lock already covered by a whole-bucket lock (<see cref="KvTransaction.HasWholeBucketLock"/>)
/// or by the same point acquired earlier (<see cref="KvTransaction.HasPointLock"/>), and lets the
/// escalation heuristic count per-bucket point locks
/// (<see cref="KvTransaction.CountPointLocksForBucket"/>). It is <b>not</b> finalization state — the
/// coordinator owns and releases the folded range lock on commit/rollback (and renews its TTL while the
/// session is live), so this list is never replayed at finalize.
/// A whole-bucket lock is <c>(prefix, null, true, null, true)</c>.
/// </summary>
public readonly record struct RangeLockBounds(
    string Prefix, string? StartKey, bool StartInclusive, string? EndKey, bool EndInclusive,
    KeyValueDurability Durability, RangeLockMode Mode = RangeLockMode.Shared);

/// <summary>
/// Holds the runtime state of a single CamusDB transaction backed by Kahuna.
///
/// Each instance owns a Kahuna <see cref="HLCTimestamp"/> obtained from
/// <c>LocateAndStartTransaction</c>. The coordinator owns the authoritative working set (staged
/// writes, point/range locks, read observations) and finalizes from it given only the
/// <see cref="Handle"/> — the client forwards no key list at commit/rollback. The only local
/// bookkeeping kept here is <b>in-transaction</b> state that the client itself consults while the
/// transaction runs: the range-lock coverage index (for lock-coverage/escalation decisions) and the
/// modified-key set (used to invalidate the query result cache when the frozen server working set is
/// unavailable — see <see cref="KvTransactionsManager.CloseServerWorkingSetAsync"/>). Neither is
/// replayed at finalize.
///
/// This object is NOT thread-safe — one transaction per logical thread/task.
/// </summary>
public sealed class KvTransaction
{
    /// <summary>
    /// Stable client-visible identity for this transaction. Set to a locally-minted HLC
    /// timestamp at construction time and never changes, even before the Kahuna coordinator
    /// session starts. <see cref="HttpTransactionCoordinator"/> keys its tracking dictionary
    /// on this value so the client can locate an explicit transaction by the ID returned in
    /// <c>/start-transaction</c>, regardless of whether the session has started yet.
    ///
    /// <para>Deliberately distinct from <see cref="TransactionId"/>: the wire handle carries only
    /// the identity's (physical, counter) pair, without its node component, and the tracking maps
    /// key on that pair. A Kahuna session id is minted by the session's coordinator partition
    /// leader — another node's clock for most sessions in a cluster — so using it as the tracking
    /// identity let two live transactions collide on the same pair. Every tracked identity is
    /// therefore minted by the local clock (see <c>KvTransactionsManager.MintTrackingId</c>),
    /// which never repeats a pair. For zero-snapshot read-only transactions both identities are
    /// <see cref="HLCTimestamp.Zero"/> and the transaction is not tracked.</para>
    /// </summary>
    public HLCTimestamp ClientId { get; }

    /// <summary>
    /// The Kahuna transaction timestamp. Passed as the <c>transactionId</c> argument
    /// to every transactional KV operation. Zero until <see cref="EnsureSessionStartedAsync"/>
    /// fires for a deferred-start transaction; set to the Kahuna handle once the coordinator
    /// session starts.
    /// </summary>
    public HLCTimestamp TransactionId { get; private set; }

    /// <summary>
    /// The Kahuna commit timestamp: the HLC timestamp that every write of this transaction carries.
    /// <see cref="KvTransactionsManager.CommitAsync"/> sets it from the commit answer. It is
    /// <see cref="HLCTimestamp.Zero"/> before a commit, after a rollback, and after a commit of a
    /// transaction that wrote nothing.
    ///
    /// <para>Two transactions that write the same key commit in this order. A transaction that read a
    /// committed value commits above that value's commit timestamp, on any partition, only when the
    /// read was registered with the coordinator (see <see cref="ReadRegistrationKey(string)"/>): a
    /// read of an optimistic transaction or of one with read validation, or a read of a key this
    /// transaction wrote. Other pessimistic reads give no such bound. A read-write anti-dependency has
    /// no order: a reader can commit above a writer that later overwrote what it read. See
    /// <c>docs/transactions-locking-and-isolation.md</c> §9.5.</para>
    ///
    /// <para>The causal token that <see cref="KvTransactionsManager.CommitAsync"/> returns is a
    /// different value. It is a reading of the local clock after the commit, at or above this value,
    /// and it gives a floor for later reads. It does not give the position of the commit among other
    /// commits.</para>
    /// </summary>
    public HLCTimestamp CommitTimestamp { get; internal set; }

    /// <summary>
    /// The transaction's own unique identifier: a GUID minted at <see cref="KvTransactionsManager.BeginAsync"/>,
    /// used in logs and conflict messages and as the unique tail of <see cref="CoordinatorKey"/>.
    /// It is <b>not</b> the coordinator key on its own once a deferred session anchors itself.
    /// </summary>
    public string UniqueId { get; }

    /// <summary>
    /// The coordinator key that pins this transaction's server-side session to a partition leader.
    /// Supplied as <c>KeyValueTransactionOptions.CoordinatorKey</c> when the Kahuna session starts,
    /// passed on every registered operation so the confirmed effect folds into the coordinator-owned
    /// working set, and carried by <see cref="Handle"/> at commit/rollback.
    ///
    /// <para>Kahuna places the session by hashing this key with the data placement rule, so its
    /// shape decides which node owns the session. An eager-start transaction uses the bare
    /// <see cref="UniqueId"/>, which hashes to an arbitrary partition. A deferred-start transaction
    /// replaces it at session open with the anchored form
    /// <c>{placementGroup}|tx/{UniqueId}</c> (see <c>KvKeyBuilder.SessionAnchorKeyOf</c>) when the
    /// first data operation names the table it touches, so the session lands on that table's
    /// partition and the leader of the data also owns the registrations and the commit. For a
    /// key-range routed table the anchored key is pinned to the partition of the table's one row
    /// range, and the key then has Kahuna's pinned form <c>"\u0001p/{partition}/{key}"</c>
    /// (<c>CoordinatorKeys</c>), which routes every later call to that partition without hashing.
    /// Treat the key as opaque. Read it only after <see cref="EnsureSessionStartedAsync"/> completed;
    /// before that it still equals <see cref="UniqueId"/>.</para>
    /// </summary>
    public string CoordinatorKey { get; private set; }

    /// <summary>
    /// The coordinator-assigned record anchor: the first confirmed persistent modified key, folded onto
    /// this transaction the moment the coordinator returns it — including alongside a non-terminal
    /// <c>MustRetry</c>. It names the data partition that owns a Durable transaction record, so a
    /// commit/rollback retried after the live coordinator session is lost can still be routed to the
    /// durable decision; a lost <em>unanchored</em> handle can only return <c>Errored</c>. Null on the
    /// best-effort path and until the first persistent write is confirmed.
    /// </summary>
    public string? RecordAnchorKey { get; private set; }

    /// <summary>
    /// The routing identity handed to <c>LocateAndCommitTransaction</c> / <c>LocateAndRollbackTransaction</c>.
    /// The Kahuna coordinator finalizes from its own working set, so the handle (transaction id +
    /// coordinator key + any record anchor) is the only thing the client supplies at finalize — no
    /// client-built key list. <see cref="RecordAnchorKey"/> is included so a finalize retried after the
    /// coordinator session is lost still reaches the durable decision instead of an unknown
    /// <c>Errored</c>. The coordinator key is <see cref="CoordinatorKey"/> (the value supplied as
    /// <c>KeyValueTransactionOptions.CoordinatorKey</c> when the session started).
    /// </summary>
    public TransactionHandle Handle => new(TransactionId, CoordinatorKey, RecordAnchorKey);

    /// <summary>
    /// Folds the coordinator-assigned record anchor onto this transaction so every subsequent
    /// commit/rollback attempt carries it via <see cref="Handle"/>. Called from the commit path as soon
    /// as the coordinator returns an anchor — <b>including alongside a non-terminal <c>MustRetry</c></b>,
    /// which is precisely when a later retry (after the coordinator session or its node is lost) needs
    /// the anchor to reach the durable decision. A null/empty anchor is a no-op, so the best-effort path
    /// never gains one.
    /// </summary>
    public void CaptureRecordAnchor(string? anchor)
    {
        if (string.IsNullOrEmpty(anchor))
            return;

        lock (trackSync)
            RecordAnchorKey = anchor;
    }

    /// <summary>Current lifecycle state. Set by <see cref="KvTransactionsManager"/>.</summary>
    public KvTransactionStatus Status { get; internal set; } = KvTransactionStatus.Active;

    private readonly Lock trackSync = new();
    private HashSet<RangeLockBounds>? acquiredRangeLocks;

    /// <summary>
    /// The prefixes on which <see cref="acquiredRangeLocks"/> holds a whole-bucket lock, kept beside it
    /// so <see cref="HasWholeBucketLock"/> is one lookup. Point locks never escalate on the foreign-key
    /// path, so a long transaction can hold thousands of them, and that check runs once per constraint
    /// per statement: a walk of the whole set there made N small statements cost O(N²). Only grows,
    /// because a whole-bucket lock is never released before the transaction ends, and an Exclusive
    /// acquire that replaces a Shared one keeps the same bounds.
    /// </summary>
    private HashSet<string>? wholeBucketPrefixes;

    /// <summary>
    /// The bucket prefixes in which this transaction holds at least one Exclusive range or point lock.
    /// Kahuna places a write intent of the locking transaction on every resident key an Exclusive lock
    /// covers, so a scan of one of these buckets meets this transaction's own intents and must carry
    /// its identity (<see cref="RangeScanIdentityFor"/>). Only grows: no lock is released before the
    /// transaction ends.
    /// </summary>
    private HashSet<string>? exclusiveLockPrefixes;

    /// <summary>
    /// For each bucket prefix, how many entries of <see cref="acquiredRangeLocks"/> it has: the
    /// singleton point locks, and all locks. Kept beside the set so
    /// <see cref="CountPointLocksForBucket"/> and <see cref="CountRangeLocksForBucket"/> are one
    /// lookup. Each point-lock acquire asks for the count to decide escalation, so a walk of the
    /// whole set there made a read of N rows cost O(N²). <see cref="TrackRangeLock"/> is the only
    /// writer, and it keeps the counts equal to the set.
    /// </summary>
    private Dictionary<string, (int Points, int Total)>? rangeLockCounts;
    private HashSet<(string key, KeyValueDurability durability)>? modifiedKeys;

    /// <summary>
    /// The stored bytes of rows this transaction read by key and has not written since, by row key.
    /// Guarded by <see cref="trackSync"/>. See <see cref="RememberPointRead"/>.
    /// </summary>
    private Dictionary<string, ReadOnlyMemory<byte>>? rememberedPointReads;

    /// <summary>
    /// How many rows <see cref="rememberedPointReads"/> holds at most. A transaction that reads more
    /// rows by key keeps the first ones; the others are read again by a write, as before.
    /// </summary>
    private const int MaxRememberedPointReads = 64;

    /// <summary>
    /// True once Kahuna granted this transaction an exclusive key lock. Written under
    /// <c>trackSync</c>, read under it by <see cref="HoldsNoLocksOrWrites"/>. See <see cref="MarkHoldsKeyLocks"/>.
    /// </summary>
    private bool holdsKeyLocks;

    private Dictionary<string, SchemaVersionPin>? schemaPins;

    // Write-shape fence state, guarded by trackSync. See PinWriteShape and TryEnterCommit. The first
    // pin is held inline, because most transactions write one table and then need no list at all.
    private WriteShapePin firstWriteShapePin;
    private List<WriteShapePin>? moreWriteShapePins;
    private bool commitInFlight;

    /// <summary>
    /// True when this transaction was built as a read-only snapshot: the synthetic
    /// <see cref="CreateReadOnly"/> and <see cref="CreateSnapshotReadOnly"/> transactions (identity
    /// <see cref="HLCTimestamp.Zero"/>, so commit and rollback are no-ops), and the Serializable
    /// read-only transaction of <see cref="KvTransactionsManager.BeginAsync"/> (a real identity, kept
    /// only to track the transaction, and finalized by a rollback).
    ///
    /// <para>Fixed at construction. The active-transaction gauge tags the begin and the end with it,
    /// so it must not change in between. It does not cover every read-only transaction: a transaction
    /// begun with <see cref="CamusTransactionMode.ReadOnly"/> at Read Committed, or made read-only by
    /// <c>SET TRANSACTION</c>, has it false. Use <see cref="RefusesWrites"/> to decide whether a
    /// write is allowed.</para>
    /// </summary>
    public bool IsReadOnly { get; }

    /// <summary>
    /// True when this transaction must not write: it is a read-only snapshot
    /// (<see cref="IsReadOnly"/>), or its <see cref="TransactionMode"/> is
    /// <see cref="CamusTransactionMode.ReadOnly"/>. Every write path checks this before it takes a
    /// lock or stages a write (<see cref="RequireWritable"/>, <see cref="ReserveMutations"/>), and the
    /// commit refuses a read-only transaction that staged one.
    ///
    /// <para>A Serializable read-only commit is a rollback of the Kahuna transaction, so a write that
    /// got through would be discarded while the client is told it committed. That is why a write is
    /// refused here and not left to the commit.</para>
    ///
    /// <para>This covers the snapshot that the server picks for an autocommit <c>SELECT</c> too,
    /// because that snapshot cannot write. A statement that changes state in transactions of its own
    /// is refused only when the client declared the transaction read-only
    /// (<see cref="IsDeclaredReadOnly"/>).</para>
    /// </summary>
    public bool RefusesWrites => IsReadOnly || TransactionMode == CamusTransactionMode.ReadOnly;

    /// <summary>
    /// True for a read-only snapshot that the server picked itself, where the client asked for no
    /// access mode: the snapshot of an autocommit <c>SELECT</c>, and the internal readers built with
    /// <see cref="CreateReadOnly"/> or <see cref="CreateSnapshotReadOnly"/>. Fixed at construction.
    /// </summary>
    private readonly bool serverChosenSnapshot;

    /// <summary>
    /// True when the client declared this transaction read-only: it began with
    /// <see cref="CamusTransactionMode.ReadOnly"/>, a <c>SET TRANSACTION … READ ONLY</c> made it
    /// read-only, or an autocommit request asked for <c>ReadOnly</c>. False for a snapshot that the
    /// server picked for an autocommit <c>SELECT</c> on its own.
    ///
    /// <para>A declared read-only transaction also refuses the statements that change state outside
    /// it (<see cref="RequireReadWriteMode"/>): schema DDL, <c>COMMENT ON</c>,
    /// <c>REFRESH MATERIALIZED VIEW</c>, <c>nextval()</c> and <c>setval()</c>. They commit in
    /// transactions of their own, or are not transactional, so a rollback cannot undo them. A
    /// server-picked snapshot allows them: an autocommit <c>SELECT nextval('s')</c> runs in one, and
    /// the client asked for no read-only transaction.</para>
    /// </summary>
    public bool IsDeclaredReadOnly => TransactionMode == CamusTransactionMode.ReadOnly && !serverChosenSnapshot;

    /// <summary>
    /// Refuses <paramref name="statement"/> with <see cref="CamusDBErrorCodes.ReadOnlyTransaction"/>
    /// when this transaction is read-only (<see cref="RefusesWrites"/>). A write statement calls it
    /// before it takes a lock or stages a write. <paramref name="statement"/> is the SQL verb the client
    /// wrote, such as <c>UPDATE</c>; the message uses PostgreSQL's wording.
    /// </summary>
    public void RequireWritable(string statement)
    {
        if (RefusesWrites)
            throw new CamusDBException(
                CamusDBErrorCodes.ReadOnlyTransaction,
                $"cannot execute {statement} in a read-only transaction");
    }

    /// <summary>
    /// Refuses <paramref name="statement"/> with <see cref="CamusDBErrorCodes.ReadOnlyTransaction"/>
    /// when the client declared this transaction read-only (<see cref="IsDeclaredReadOnly"/>). A
    /// statement that changes state outside this transaction calls it before it changes anything:
    /// schema DDL, <c>COMMENT ON</c>, <c>REFRESH MATERIALIZED VIEW</c>, <c>nextval()</c> and
    /// <c>setval()</c>. The message uses PostgreSQL's wording.
    /// </summary>
    public void RequireReadWriteMode(string statement)
    {
        if (IsDeclaredReadOnly)
            throw new CamusDBException(
                CamusDBErrorCodes.ReadOnlyTransaction,
                $"cannot execute {statement} in a read-only transaction");
    }

    /// <summary>
    /// True when a client explicitly opened this transaction and owns its commit or rollback —
    /// <c>BEGIN</c> / <c>START TRANSACTION</c> — as opposed to a transaction the server wrapped
    /// around one autocommit statement.
    /// </summary>
    /// <remarks>
    /// The engine cannot tell the two apart from the transaction itself: both are ordinary read-write
    /// transactions with a real identity. Only the transport that created it knows, so the transport
    /// marks it with <see cref="MarkSessionOwned"/>. A statement whose effects the caller's
    /// <c>ROLLBACK</c> could not undo reads this and refuses rather than promising otherwise.
    /// </remarks>
    public bool IsSessionOwned { get; private set; }

    /// <summary>
    /// Marks this transaction as explicitly opened by a client. Called once by the transport that
    /// handled the <c>BEGIN</c>, before any statement runs against it.
    /// </summary>
    public void MarkSessionOwned() => IsSessionOwned = true;

    /// <summary>
    /// The schema changes this transaction's DDL statements made, or null. Set by the first DDL
    /// statement of a session-owned transaction when transactional DDL is on, and never replaced; the
    /// commit and the rollback call its hooks. See <see cref="ITransactionSchemaChanges"/>.
    /// </summary>
    public ITransactionSchemaChanges? SchemaChanges { get; internal set; }

    /// <summary>
    /// Removes the schema pins of one table resource: <paramref name="tableResource"/> itself and every
    /// pin named <c>{tableResource}#…</c>, such as its contents pin. Called when this transaction's own
    /// DDL changes a table it pinned: the pin described the layout before the change, and keeping it
    /// would refuse the transaction's next statement on that table, and its commit, because of its own
    /// change. A change by another transaction is caught by the base-version check of the change set
    /// instead.
    /// </summary>
    internal void RemoveSchemaPins(string tableResource)
    {
        if (schemaPins is null)
            return;

        List<string>? stale = null;

        foreach (string resource in schemaPins.Keys)
        {
            if (string.Equals(resource, tableResource, StringComparison.Ordinal)
                || (resource.Length > tableResource.Length
                    && resource[tableResource.Length] == '#'
                    && resource.StartsWith(tableResource, StringComparison.Ordinal)))
                (stale ??= []).Add(resource);
        }

        if (stale is null)
            return;

        foreach (string resource in stale)
            schemaPins.Remove(resource);
    }

    // The values this transaction drew from user sequences, keyed by the sequence's immutable id,
    // plus the id of the sequence drawn from most recently. This is what currval() and lastval()
    // read. It is per-transaction rather than per-connection because CamusDB has no connection: the
    // REST path is stateless, and defining "session" as the auth token would make two unrelated
    // concurrent requests from one user share a lastval — wrong in a way that is very hard to
    // debug. A transaction is a real, bounded scope that no two callers share.
    private Dictionary<string, long>? sequenceValues;

    private string? lastSequenceId;

    /// <summary>
    /// The ceiling on how many distinct sequences one transaction remembers a value for. A
    /// transaction that touches more than this loses the oldest entries, and <c>currval</c> on an
    /// evicted one reports "not yet defined" rather than a stale number — a wrong answer being far
    /// worse here than a refusal. The cap exists because the map is otherwise unbounded in the
    /// number of sequences a long-running transaction names.
    /// </summary>
    internal const int MaxRememberedSequenceValues = 64;

    /// <summary>
    /// Records that this transaction drew <paramref name="value"/> from the sequence with
    /// <paramref name="sequenceId"/>. Called once per statement with the last value the statement
    /// actually used, not with the bounds of the run it reserved — a statement may reserve more
    /// than it uses, and <c>currval</c> must report a value that was issued.
    /// </summary>
    public void RecordSequenceValue(string sequenceId, long value)
    {
        sequenceValues ??= new Dictionary<string, long>(StringComparer.Ordinal);

        // Evicts an arbitrary entry rather than the least recently used: tracking recency would
        // cost a second structure on every draw to improve a case — more than 64 sequences in one
        // transaction — that has no realistic shape. What matters is that the map is bounded and
        // that a missing entry reports itself.
        if (sequenceValues.Count >= MaxRememberedSequenceValues && !sequenceValues.ContainsKey(sequenceId))
        {
            foreach (string evicted in sequenceValues.Keys)
            {
                sequenceValues.Remove(evicted);
                break;
            }
        }

        sequenceValues[sequenceId] = value;
        lastSequenceId = sequenceId;
    }

    // Distinguishes one sequence reservation from the next within a transaction, so each carries
    // its own idempotency key. It has to live here rather than on the statement: a per-statement
    // counter restarts at one for every statement, and two statements of the same transaction
    // would then reserve under the same key — which Kahuna correctly answers by replaying the
    // first allocation, so the second statement receives values it was already given.
    private long sequenceAllocationOrdinal;

    /// <summary>
    /// The next ordinal for a sequence reservation made by this transaction. Monotonic and never
    /// reused, so a reservation's idempotency key is stable across the retries of that one call
    /// and distinct from every other reservation's.
    /// </summary>
    public long NextSequenceAllocationOrdinal() => ++sequenceAllocationOrdinal;

    /// <summary>The value this transaction last drew from one sequence, if it drew one.</summary>
    public bool TryGetSequenceValue(string sequenceId, out long value)
    {
        value = 0;
        return sequenceValues is not null && sequenceValues.TryGetValue(sequenceId, out value);
    }

    /// <summary>The value this transaction last drew from any sequence, and which sequence it was.</summary>
    public bool TryGetLastSequenceValue(out string sequenceId, out long value)
    {
        sequenceId = "";
        value = 0;

        if (lastSequenceId is null || sequenceValues is null)
            return false;

        sequenceId = lastSequenceId;
        return sequenceValues.TryGetValue(lastSequenceId, out value);
    }

    /// <summary>
    /// Isolation level for this transaction. Defaults to <see cref="CamusIsolationLevel.ReadCommitted"/>.
    /// May be upgraded via <c>SET TRANSACTION ISOLATION LEVEL</c> before any locks are acquired —
    /// the SQL handler calls <see cref="ApplyIsolationLevel"/> which guards against mid-flight changes.
    /// </summary>
    public CamusIsolationLevel IsolationLevel { get; private set; }

    /// <summary>
    /// Mode (read-write or read-only) for this transaction. Defaults to <see cref="CamusTransactionMode.ReadWrite"/>.
    /// May be changed by <c>SET TRANSACTION … READ ONLY | READ WRITE</c> before the first statement
    /// (<see cref="ApplyIsolationLevel"/>). A read-only mode refuses every write
    /// (<see cref="RefusesWrites"/>).
    /// </summary>
    public CamusTransactionMode TransactionMode { get; private set; }

    /// <summary>
    /// The HLC timestamp that defines the MVCC snapshot of a
    /// <see cref="CamusIsolationLevel.Serializable"/> + <see cref="CamusTransactionMode.ReadOnly"/>
    /// transaction. Every read in the transaction uses this single <c>T</c> so the whole transaction
    /// observes one consistent point in the version history.
    ///
    /// <para>Set at construction for a transaction begun read-only. Set once more by
    /// <see cref="ApplyIsolationLevel"/> when <c>SET TRANSACTION</c> makes a read-write transaction
    /// Serializable and read-only before its first statement. It never changes after the first
    /// statement, and never goes back to Zero.</para>
    ///
    /// <para><see cref="HLCTimestamp.Zero"/> on all other transaction types — the default
    /// (Read Committed) path is unchanged.</para>
    /// </summary>
    public HLCTimestamp ReadTimestamp { get; private set; }

    /// <summary>
    /// Concurrency strategy for this transaction. Pessimistic (the default) acquires explicit
    /// locks before each write; optimistic defers conflict detection to commit-time read-set
    /// validation via the coordinator. May be changed via <c>SET TRANSACTION LOCKING</c> before
    /// the Kahuna coordinator session starts — see <see cref="ApplyLocking"/>.
    /// </summary>
    public KeyValueTransactionLocking Locking { get; private set; }

    /// <summary>
    /// Relative importance of this transaction when the Kahuna node has to choose which of several
    /// waiting transactions to start next. Governs <b>admission order only</b>: it never preempts a
    /// running transaction, never affects lock conflict resolution, and never changes isolation or
    /// commit semantics.
    ///
    /// <para>Pinned when the coordinator session opens, because the admission gate is entered inside
    /// the server's start call — so, exactly like <see cref="Locking"/>, it may be changed via
    /// <c>SET TRANSACTION PRIORITY</c> only before that point. See <see cref="ApplyPriority"/>.</para>
    ///
    /// <para>Note the gate is inert unless an operator has configured a concurrency ceiling on the
    /// node; with the default ceiling of zero every transaction is admitted immediately and this
    /// value is recorded but never consulted.</para>
    /// </summary>
    public TransactionPriority Priority { get; private set; }

    /// <summary>
    /// Whether reads issued by this transaction are tracked by the coordinator for a commit-time
    /// read-set conflict check. <see cref="ReadValidation.None"/> (the default) skips tracking;
    /// <see cref="ReadValidation.TrackAndValidate"/> requests the check even for a pessimistic
    /// transaction. It cannot be combined with a fixed <see cref="ReadTimestamp"/> (a historical
    /// snapshot has no live read dependency), so <see cref="BeginAsync"/> never sets both.
    /// </summary>
    public ReadValidation ReadValidation { get; }

    /// <summary>
    /// Whether each read this transaction performs should be registered with the coordinator so its
    /// existence/base-revision observation folds into the server-owned working set and is validated
    /// at commit. True only when the transaction actually wants read-set validation — optimistic
    /// locking (which validates its read set by definition) or an explicit
    /// <see cref="ReadValidation.TrackAndValidate"/> request — and only for a real registered
    /// transaction reading the latest version (a <see cref="HLCTimestamp.Zero"/> identity has no
    /// coordinator session to fold into, and a fixed <see cref="ReadTimestamp"/> snapshot carries no
    /// live read dependency).
    ///
    /// <para><b>Consumed by every read path</b> — point reads, batch reads, and both scan families
    /// pass the coordinator key when this is set, so an optimistic transaction detects read-write
    /// and write-skew conflicts as well as write-write ones.</para>
    ///
    /// <para><b>What folding does not cover.</b> Folding records the keys a read observed, a missing
    /// key included, but not a range: it cannot detect a phantom inserted into a scanned range, so a
    /// Serializable scan keeps its range lock whatever this flag says. A key that is read and then
    /// written leaves the read set and becomes the validated base of that write, which the commit
    /// checks, so a single-key read needs no lock to be proved (see
    /// <see cref="CamusDBOptions.SerializableOptimisticPointLocks"/>).</para>
    /// </summary>
    public bool FoldReads =>
        TransactionId != HLCTimestamp.Zero &&
        ReadTimestamp.IsNull() &&
        (Locking == KeyValueTransactionLocking.Optimistic ||
         ReadValidation == ReadValidation.TrackAndValidate);

    /// <summary>
    /// The Kahuna identity a <b>range scan</b> of <paramref name="bucketPrefix"/> issued by this
    /// transaction carries: the <see cref="TransactionId"/> when the scan has something to observe
    /// through it, otherwise <see cref="HLCTimestamp.Zero"/>, which makes the scan an untracked read
    /// of committed state.
    ///
    /// <para>Kahuna treats any read that carries a transaction id as transactional MVCC: for every
    /// key the scan visits it pins the revision first observed by the transaction, and any later
    /// evaluation of the same key under the same id answers <c>Aborted</c> once the committed head
    /// has moved past that pin. A paged scan re-evaluates its keys whenever a page is retried after a
    /// transient (<c>WaitingForReplication</c> / <c>MustRetry</c> — replication lag, a settling
    /// intent), so on a hot table a read-only <c>COUNT(*)</c> that carried the id failed with a
    /// write-conflict code as soon as one of the page's keys was overwritten between the attempts —
    /// 365 of 399 in-window probes on a lagging cluster. It also charged the server one MVCC entry
    /// per scanned key for the life of the session.</para>
    ///
    /// <para>The id is only meaningful to a scan in three cases, and it is carried exactly then:
    /// <list type="bullet">
    ///   <item><see cref="FoldReads"/> — the transaction wants its observations folded into the
    ///   commit-time read set (optimistic locking, <see cref="ReadValidation.TrackAndValidate"/>); the
    ///   pin is then the conflict detection the transaction asked for.</item>
    ///   <item><see cref="HasPendingWrites"/> — the transaction owns live write intents and must read
    ///   its own uncommitted rows (and not the rows it deleted) through the scan.</item>
    ///   <item>The transaction holds an Exclusive lock in <paramref name="bucketPrefix"/>. Kahuna
    ///   places a write intent of the locking transaction on every resident key the lock covers, before
    ///   anything is written. A scan pages through the range: the first page reads the latest state, and
    ///   every later page reads at the snapshot the first page took, where a live intent of another
    ///   transaction makes the page wait. An untracked scan is "another transaction" to its own
    ///   transaction's intents, so a write-driving scan (the source of <c>INSERT … SELECT</c>, the locate
    ///   scan of <c>UPDATE</c> and <c>DELETE</c>, which take that lock) waited on intents that only it
    ///   could resolve, and failed after Kahuna's page retry budget once it passed one page. The pin cost
    ///   is safe here: no other transaction can write under the Exclusive lock, so a page retry cannot
    ///   find a key that moved.</item>
    /// </list>
    /// A read-committed statement that has written nothing yet — every autocommit <c>SELECT</c>, every
    /// aggregate, the harness's probes and reconciliation — matches none, so its scan carries no
    /// id and can never be answered with a transaction abort; it serves each row's committed head
    /// exactly as a point read without a timestamp does. The third case is per bucket on purpose: a
    /// scan of a range this transaction does not hold exclusively meets none of its intents and stays
    /// untracked, so it keeps that protection.</para>
    ///
    /// <para><b>What this does not change.</b> Point reads keep the transaction id: they are the
    /// read-modify-write shape, and the pin they record is what makes a later write to the same key
    /// under pessimistic locking abort instead of overwriting a value another transaction committed in
    /// between (a guard that is not reliable across nodes, so no correctness path depends on it). Update
    /// and delete statements lock the rows they modify and then read them again
    /// (<c>KvTableStore.LockAndReadRowsForMutationAsync</c>) before writing, so the rows their locating scan yielded
    /// are validated by that locked read, never by the scan. Under Read Committed a row a scan
    /// returned is therefore not pinned for the rest of the transaction, which is precisely the
    /// non-repeatable read that level permits (see §9.2 of the isolation guide).</para>
    ///
    /// <para>Evaluate after <see cref="EnsureSessionStartedAsync"/>: a deferred transaction that folds
    /// reads has a Zero <see cref="TransactionId"/> — hence <see cref="FoldReads"/> false — until its
    /// session opens.</para>
    /// </summary>
    public HLCTimestamp RangeScanIdentityFor(string bucketPrefix) =>
        FoldReads || HasPendingWrites || HoldsExclusiveLockIn(bucketPrefix) ? TransactionId : HLCTimestamp.Zero;

    /// <summary>
    /// True when this transaction holds an Exclusive range or point lock in <paramref name="bucketPrefix"/>.
    /// See <see cref="RangeScanIdentityFor"/> for why a scan needs to know.
    /// </summary>
    public bool HoldsExclusiveLockIn(string bucketPrefix)
    {
        lock (trackSync)
            return exclusiveLockPrefixes?.Contains(bucketPrefix) == true;
    }

    /// <summary>
    /// The read timestamp of the statement this transaction runs now (a SELECT, or the read phase of a
    /// write statement), held in one object so a reader gets the timestamp and the decision to use it
    /// in one read. Null when no statement snapshot is open.
    /// </summary>
    private sealed class StatementSnapshot(HLCTimestamp timestamp)
    {
        public HLCTimestamp Timestamp { get; } = timestamp;
    }

    private StatementSnapshot? statementSnapshot;

    /// <summary>
    /// True for a transaction begun with <see cref="KvTransactionsManager.BeginAsync"/>. Only such a
    /// transaction reads each Read Committed SELECT at one statement timestamp. The zero-identity
    /// transaction of an autocommit SELECT (<see cref="CreateReadOnly"/>) keeps reading the latest
    /// committed value of each key.
    /// </summary>
    public bool UsesStatementSnapshots { get; private set; }

    /// <summary>
    /// The causal token the client passed when it began the transaction, or Zero. Every statement
    /// timestamp of the transaction is at least this value.
    ///
    /// <para>A statement timestamp comes from the local HLC, and Kahuna does not show a value committed
    /// at a later HLC time to a read at an earlier one. A client that commits through one node and then
    /// begins a transaction on a node whose clock is behind would therefore not see its own commit. The
    /// commit's causal token, passed at begin, closes that gap.</para>
    /// </summary>
    public HLCTimestamp CausalFloor { get; private set; }

    /// <summary>
    /// Marks the transaction as one whose Read Committed statements read at a statement
    /// timestamp, with <paramref name="causalFloor"/> as the lowest timestamp. Called once, by
    /// <see cref="KvTransactionsManager.BeginAsync"/>, before the transaction is returned.
    /// </summary>
    internal void EnableStatementSnapshots(HLCTimestamp causalFloor)
    {
        UsesStatementSnapshots = true;
        CausalFloor = causalFloor;
    }

    /// <summary>
    /// True when the next statement of this transaction must read at a statement timestamp: a
    /// transaction begun with <see cref="KvTransactionsManager.BeginAsync"/>, at Read Committed, with no
    /// fixed <see cref="ReadTimestamp"/>, that does not validate its reads at commit. Read at the start
    /// of each statement, because <c>SET TRANSACTION</c> can change the level or the locking before the
    /// first one.
    ///
    /// <para><b>Why a validating transaction is left out.</b> An optimistic or
    /// <see cref="ReadValidation.TrackAndValidate"/> transaction folds every read into its read set, so
    /// its commit is refused when a value it read changed; it cannot commit on a torn read. Its reads keep
    /// the transaction's read pins, and the pin is what lets Kahuna abort a lost update at the statement
    /// that writes the changed row, early, instead of at commit.</para>
    /// </summary>
    public bool TakesStatementSnapshot =>
        UsesStatementSnapshots &&
        IsolationLevel == CamusIsolationLevel.ReadCommitted &&
        ReadTimestamp.IsNull() &&
        Locking != KeyValueTransactionLocking.Optimistic &&
        ReadValidation != ReadValidation.TrackAndValidate;

    /// <summary>
    /// Opens the statement snapshot of one SELECT statement at <paramref name="timestamp"/>, and returns
    /// the token that <see cref="EndStatementSnapshot"/> takes to close it. Replaces a snapshot an earlier
    /// statement left open.
    /// </summary>
    internal object BeginStatementSnapshot(HLCTimestamp timestamp)
    {
        StatementSnapshot snapshot = new(timestamp);
        Volatile.Write(ref statementSnapshot, snapshot);
        return snapshot;
    }

    /// <summary>
    /// Closes the statement snapshot that <paramref name="token"/> opened. A no-op when a later statement
    /// already replaced or cleared it, so the end of one cursor never closes the snapshot of the
    /// statement after it.
    /// </summary>
    internal void EndStatementSnapshot(object token) =>
        Interlocked.CompareExchange(ref statementSnapshot, null, token as StatementSnapshot);

    /// <summary>
    /// The timestamp and read mode a <b>query</b> read of this transaction uses: a point read, a batched
    /// probe, a scan or a large-value fetch issued by a SELECT.
    ///
    /// <list type="bullet">
    ///   <item>A fixed <see cref="ReadTimestamp"/> (a Serializable read-only snapshot, AS OF SYSTEM TIME)
    ///   wins, in the default mode.</item>
    ///   <item>Otherwise, while a statement snapshot is open, the statement timestamp in
    ///   <see cref="KeyValueReadMode.StatementSnapshot"/>: Kahuna shows the transaction's own staged writes,
    ///   serves every other key as committed at the timestamp, and neither serves nor leaves a read pin.
    ///   Each statement then reads one consistent view, and a later statement sees later commits.</item>
    ///   <item>Otherwise Zero in the default mode: the latest committed value.</item>
    /// </list>
    ///
    /// <para>The pair comes from one read of the snapshot, so a concurrent close cannot pair a statement
    /// timestamp with the default mode, which would hide the transaction's own writes.</para>
    ///
    /// <para><b>Not for a write path.</b> A lock-then-read, a uniqueness check or a foreign-key check must
    /// read the latest value, or it can miss a commit that landed after the statement timestamp. Such a
    /// path uses <see cref="LatestReadView"/>. The statement snapshot is open only while a SELECT runs,
    /// and while a write statement runs a read phase: the subquery rewrite and the locate scan of an
    /// <c>UPDATE</c> or <c>DELETE</c>, and the source query of an <c>INSERT … SELECT</c>
    /// (<see cref="StatementSnapshotScope"/>). It is closed before the write phase begins, and
    /// <see cref="MarkStatementExecuted"/> closes it when any other statement starts.</para>
    /// </summary>
    public KvReadView QueryReadView
    {
        get
        {
            if (!ReadTimestamp.IsNull())
                return new KvReadView(ReadTimestamp, KeyValueReadMode.Default);

            StatementSnapshot? snapshot = Volatile.Read(ref statementSnapshot);
            return snapshot is null
                ? new KvReadView(HLCTimestamp.Zero, KeyValueReadMode.Default)
                : new KvReadView(snapshot.Timestamp, KeyValueReadMode.StatementSnapshot);
        }
    }

    /// <summary>
    /// The timestamp and read mode of a read that must see the latest committed value whatever statement
    /// runs: <see cref="ReadTimestamp"/> (Zero for every transaction that can write) in the default mode.
    /// See <see cref="QueryReadView"/>.
    /// </summary>
    public KvReadView LatestReadView => new(ReadTimestamp, KeyValueReadMode.Default);

    /// <summary>
    /// Monotonic elapsed-time counter started when the Kahuna coordinator session begins. Used to
    /// enforce <see cref="CamusDBOptions.MaxSerializableTransactionLifetimeMs"/> — immune to NTP
    /// wall-clock jumps, consistent with the monotonic lock-wait deadline used elsewhere in the
    /// store.
    ///
    /// <para><c>null</c> for zero-snapshot (read-only) transactions and for deferred-start
    /// transactions whose session has not yet started. In the deferred case the watch is started
    /// by <see cref="SetSessionId"/> when the session actually opens.</para>
    /// </summary>
    private Stopwatch? lifetimeWatch;

    /// <summary>
    /// Session-start delegate set by <see cref="KvTransactionsManager.BeginAsync"/> for
    /// deferred-start transactions. When non-null, the Kahuna coordinator session has not been
    /// opened yet; the first call to <see cref="EnsureSessionStartedAsync"/> invokes it exactly
    /// once, handing it the placement group of the table that first operation touches (or null
    /// when the caller has no table) so the starter can anchor the session there. Null for
    /// eager-start and zero-snapshot transactions — those have no pending start.
    /// </summary>
    internal Func<string?, CancellationToken, Task>? SessionStarter;

    /// <summary>Guards <see cref="sessionStartTask"/> so the deferred Kahuna session is opened exactly
    /// once even when several of a transaction's operations race to be the first.</summary>
    private readonly object sessionStartSync = new();

    /// <summary>The single in-flight/completed session-start task, memoized under
    /// <see cref="sessionStartSync"/>. Concurrent first-operations all await this same task.</summary>
    private Task? sessionStartTask;

    // Mutation budget, guarded by trackSync.
    private int mutationCount;
    private readonly int mutationLimit; // 0 or negative = unlimited (DDL, read-only, disabled)

    /// <summary>
    /// Returns <c>true</c> if <paramref name="maxLifetimeMs"/> is positive and the transaction
    /// has been open longer than that duration. Always <c>false</c> for zero-snapshot (RO) transactions.
    /// </summary>
    public bool IsExpired(int maxLifetimeMs) =>
        maxLifetimeMs > 0 &&
        lifetimeWatch is not null &&
        lifetimeWatch.ElapsedMilliseconds > maxLifetimeMs;

    /// <summary>
    /// Milliseconds elapsed since the Kahuna session for this transaction opened, or <c>null</c>
    /// when there is no session to measure (zero-snapshot read-only, or a deferred-start
    /// transaction whose session has not opened yet). Intended for diagnostics — a long age on a
    /// conflicting operation is the signal that the transaction, not the contending writer, is the
    /// one holding locks for too long.
    /// </summary>
    public long? AgeMs => lifetimeWatch?.ElapsedMilliseconds;

    /// <summary>
    /// Disk-space admission gate installed by <see cref="KvTransactionsManager"/>: throws
    /// <see cref="CamusDBErrorCodes.InsufficientDiskSpace"/> when the data volume's free space is
    /// below the configured watermark. Invoked from <see cref="ReserveMutations"/> only for
    /// transactions with a positive mutation budget, so DDL and internal system transactions
    /// (budget disabled) are never refused — an operator must be able to drop objects to free
    /// space. Null when no manager installed one (direct test constructions).
    /// </summary>
    public Action? WriteAdmissionGate { get; set; }

    /// <summary>
    /// Reserves <paramref name="count"/> mutations against this transaction's budget before the
    /// corresponding writes are issued. Throws <see cref="CamusDBErrorCodes.TransactionMutationLimitExceeded"/>
    /// if the budget would be exceeded; otherwise adds to the running total monotonically.
    /// Also runs <see cref="WriteAdmissionGate"/> first, which throws
    /// <see cref="CamusDBErrorCodes.InsufficientDiskSpace"/> while the node's data volume is below
    /// its free-space watermark — before the mutation is counted or any write is issued.
    /// No-op when the limit is disabled (<c>&lt;= 0</c>) or <paramref name="count"/> is zero.
    ///
    /// <para>Throws <see cref="CamusDBErrorCodes.ReadOnlyTransaction"/> for a read-only transaction
    /// (<see cref="RefusesWrites"/>). Every storage write path calls this before it takes the write
    /// locks, so this is the last check before a write reaches Kahuna. The write statements refuse
    /// earlier, with the statement's name (<see cref="RequireWritable"/>); this check catches a write
    /// path that does not.</para>
    /// </summary>
    public void ReserveMutations(int count)
    {
        if (count == 0)
            return;

        // Checked before the write mark moves, so a refused write leaves WriteCount unchanged.
        if (RefusesWrites)
            throw new CamusDBException(
                CamusDBErrorCodes.ReadOnlyTransaction,
                $"cannot write in a read-only transaction (transaction {UniqueId})");

        // A killed transaction's running write stops here, before its next write reaches Kahuna, so the
        // statement ends with a definite error and the kill's rollback can run. One field read per batch.
        if (Volatile.Read(ref killReason) is not null)
            throw KilledError();

        // Counted whatever the limit: the schema-fence retry reads it to tell an attempt that wrote
        // nothing from one that did. See WriteCount.
        Interlocked.Increment(ref writeBatches);

        if (mutationLimit <= 0)
            return;

        WriteAdmissionGate?.Invoke();

        lock (trackSync)
        {
            if ((long)mutationCount + count > mutationLimit)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionMutationLimitExceeded,
                    $"Transaction {UniqueId} would exceed the maximum of {mutationLimit} mutations " +
                    $"(already {mutationCount}, requested {count}); split the work into smaller transactions");
            mutationCount += count;
        }
    }

    /// <summary>
    /// The mutation budget of this transaction; zero or negative means unlimited. A write path reads it
    /// to refuse one row that alone needs more writes than the budget, with a message that names the
    /// cause, before <see cref="ReserveMutations"/> refuses it with the generic one.
    /// </summary>
    public int MutationLimit => mutationLimit;

    /// <summary>Running total of mutations reserved so far in this transaction.</summary>
    public int MutationCount { get { lock (trackSync) return mutationCount; } }

    /// <summary>
    /// How many distinct keys the transaction has written so far, as its client-side mirror records them.
    /// For diagnostics (<c>SHOW TRANSACTIONS</c>); it takes the tracking lock.
    /// </summary>
    public int ModifiedKeyCount { get { lock (trackSync) return modifiedKeys?.Count ?? 0; } }

    /// <summary>
    /// Origin, owner and last statement of an explicit transaction, for <c>SHOW TRANSACTIONS</c>. The host
    /// sets it when a client begins an explicit transaction, before the handle is published; it is null
    /// for every other transaction.
    /// </summary>
    public Diagnostics.TransactionActivity? Activity { get; set; }

    private long writeBatches;

    /// <summary>
    /// A monotonic mark of this transaction's write activity: it moves when a write path reserves a
    /// batch of mutations (<see cref="ReserveMutations"/>, before the Kahuna call) and when a key is
    /// recorded as modified (<see cref="TrackModified"/>, after one), whatever the mutation limit. It
    /// is a tell-tale, not a count of rows. A statement that meets a schema fence may retry in the
    /// <b>same</b> transaction only when its failed attempt wrote nothing, because nothing undoes a
    /// failed attempt's writes short of a rollback; that reader compares the mark before and after the
    /// attempt. The mark is conservative: a reservation whose Kahuna write never happened also moves
    /// it. Writes made by triggers and procedures count too, since their SQL reserves on this
    /// transaction.
    /// </summary>
    public long WriteCount => Interlocked.Read(ref writeBatches);

    private string? rollbackOnlyReason;

    /// <summary>
    /// Why this transaction may only roll back, or null. Set by <see cref="MarkRollbackOnly"/>; a
    /// commit of a marked transaction is refused with <see cref="CamusDBErrorCodes.TransactionRollbackOnly"/>.
    /// </summary>
    public string? RollbackOnlyReason => Volatile.Read(ref rollbackOnlyReason);

    /// <summary>
    /// Marks the transaction rollback-only, which is PostgreSQL's aborted state: every later statement
    /// is refused (<see cref="ThrowIfRollbackOnly"/>), a commit rolls the transaction back and reports
    /// the refusal (<see cref="CamusDBErrorCodes.TransactionRollbackOnly"/>), and only a rollback ends
    /// it cleanly. There are no savepoints, so a failed statement may have left write intents in the
    /// Kahuna transaction with no way to undo them alone. The rule applies to every failed statement,
    /// whether or not it wrote: a transactional Kahuna batch stages every key or none, so a duplicate
    /// key in a multi-row INSERT leaves nothing behind, while a foreign-key check refused after the row
    /// writes does, and a client cannot tell the two apart from the outside, so one rule is the one it
    /// can rely on. The first reason is kept, so the refusals name the statement that aborted the
    /// transaction, not a later one.
    /// </summary>
    public void MarkRollbackOnly(string reason) =>
        Interlocked.CompareExchange(ref rollbackOnlyReason, reason, null);

    /// <summary>
    /// The statement-boundary form of <see cref="MarkRollbackOnly(string)"/>: records the failure that
    /// ended a statement run on this transaction, by its error code and message.
    /// </summary>
    public void MarkRollbackOnly(Exception failure)
    {
        string code = failure is CamusDBException camus ? camus.Code : failure.GetType().Name;
        MarkRollbackOnly($"a statement failed ({code}: {failure.Message})");
    }

    /// <summary>
    /// Refuses a statement on an aborted transaction. Every statement entry point calls this before it
    /// validates or parses anything, so a client that ignores an error and goes on sees the refusal at
    /// once, as in PostgreSQL, rather than at commit.
    /// </summary>
    public void ThrowIfRollbackOnly()
    {
        if (Volatile.Read(ref killReason) is not null)
            throw KilledError();

        if (Volatile.Read(ref rollbackOnlyReason) is { } reason)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionRollbackOnly,
                $"Transaction {UniqueId} is aborted, commands are ignored until the end of the transaction block: {reason}");
    }

    private string? killReason;

    /// <summary>Why <c>KILL TRANSACTION</c> ended this transaction, or null. Set by <see cref="MarkKilled"/>.</summary>
    public string? KillReason => Volatile.Read(ref killReason);

    /// <summary>
    /// Marks the transaction killed by <c>KILL TRANSACTION</c>. From here a statement is refused at its
    /// entry (<see cref="ThrowIfRollbackOnly"/>), a running write stops at its next write
    /// (<see cref="ReserveMutations"/>), and a commit is refused, all with
    /// <see cref="CamusDBErrorCodes.TransactionKilled"/>. It also marks the transaction rollback-only,
    /// so a commit path that does not look at the kill still refuses.
    ///
    /// <para>It does not roll the transaction back. The host that tracks the transaction owns that, and
    /// runs it once no statement runs on the transaction any more, because a rollback beside a running
    /// write would race the write's own Kahuna calls. The first reason is kept.</para>
    /// </summary>
    public void MarkKilled(string reason)
    {
        Interlocked.CompareExchange(ref killReason, reason, null);
        MarkRollbackOnly(reason);
    }

    /// <summary>The error a statement or a commit of a killed transaction reports.</summary>
    public CamusDBException KilledError() => new(
        CamusDBErrorCodes.TransactionKilled,
        $"Transaction {UniqueId} was {Volatile.Read(ref killReason) ?? "killed by KILL TRANSACTION"}. " +
        "It is rolled back and accepts no further statement; begin a new transaction");

    /// <summary>
    /// Wraps a statement's lazy cursor so a failure while the rows stream aborts the transaction exactly
    /// as a failure while the statement planned: the rows the client already read stand, the error
    /// reaches the client as before, and the transaction is rollback-only from then on. A cancellation
    /// counts as a failure, as PostgreSQL treats a cancelled statement.
    ///
    /// <para>This is a hand-written wrapper, not a compiler-built iterator, on purpose. An iterator opens
    /// the inner cursor only on its first <c>MoveNextAsync</c>, so a caller that takes the enumerator and
    /// disposes it unread would never reach the inner cursor, and the clean-up that cursor does on
    /// dispose (ending the statement's entry on the running-statement list) would never run. This
    /// wrapper opens and disposes the inner enumerator together with its own.</para>
    /// </summary>
    public IAsyncEnumerable<T> AbortOnFailure<T>(IAsyncEnumerable<T> cursor) => new AbortingCursor<T>(this, cursor);

    /// <summary>The enumerable half of <see cref="AbortOnFailure{T}"/>.</summary>
    private sealed class AbortingCursor<T>(KvTransaction tx, IAsyncEnumerable<T> rows) : IAsyncEnumerable<T>
    {
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            try
            {
                return new AbortingEnumerator<T>(tx, rows.GetAsyncEnumerator(cancellationToken));
            }
            catch (Exception ex)
            {
                tx.MarkRollbackOnly(ex);
                throw;
            }
        }
    }

    /// <summary>
    /// The enumerator half of <see cref="AbortOnFailure{T}"/>: a step that fails, synchronously or after
    /// an await, aborts the transaction and rethrows. A step that completes at once is returned as is,
    /// without a state machine, because most rows of a buffered result arrive that way.
    /// </summary>
    private sealed class AbortingEnumerator<T>(KvTransaction tx, IAsyncEnumerator<T> inner) : IAsyncEnumerator<T>
    {
        public T Current => inner.Current;

        public ValueTask<bool> MoveNextAsync()
        {
            ValueTask<bool> step;

            try
            {
                step = inner.MoveNextAsync();
            }
            catch (Exception ex)
            {
                tx.MarkRollbackOnly(ex);
                throw;
            }

            return step.IsCompletedSuccessfully ? step : AwaitStep(step);
        }

        private async ValueTask<bool> AwaitStep(ValueTask<bool> step)
        {
            try
            {
                return await step.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                tx.MarkRollbackOnly(ex);
                throw;
            }
        }

        public ValueTask DisposeAsync() => inner.DisposeAsync();
    }

    public KvTransaction(
        HLCTimestamp transactionId,
        string uniqueId,
        bool isReadOnly = false,
        CamusIsolationLevel isolationLevel = CamusIsolationLevel.ReadCommitted,
        CamusTransactionMode transactionMode = CamusTransactionMode.ReadWrite,
        HLCTimestamp readTimestamp = default,
        int mutationLimit = 0,
        KeyValueTransactionLocking locking = KeyValueTransactionLocking.Pessimistic,
        ReadValidation readValidation = ReadValidation.None,
        HLCTimestamp clientId = default,
        TransactionPriority priority = TransactionPriority.Normal,
        bool serverChosenSnapshot = false)
    {
        TransactionId = transactionId;
        // ClientId is the stable tracking identity for HttpTransactionCoordinator; the manager
        // mints it from the local clock for every tracked transaction (see MintTrackingId — a
        // session id minted by another node's clock can collide with local identities on the wire
        // handle's node-less (physical, counter) pair). The transactionId fallback serves only
        // direct constructions that never pass a clientId and are not tracked across requests.
        ClientId = clientId.IsNull() ? transactionId : clientId;
        UniqueId = uniqueId;
        CoordinatorKey = uniqueId;
        IsReadOnly = isReadOnly;
        this.serverChosenSnapshot = serverChosenSnapshot;
        IsolationLevel = isolationLevel;
        TransactionMode = transactionMode;
        ReadTimestamp = readTimestamp;
        this.mutationLimit = mutationLimit;
        Locking = locking;
        ReadValidation = readValidation;
        Priority = priority;
        StartTime = ComputeStartTime(transactionId, ClientId, readTimestamp);
        if (transactionId != HLCTimestamp.Zero)
            lifetimeWatch = Stopwatch.StartNew();
    }

    /// <summary>
    /// The instant this transaction started, in UTC. It is what <c>now()</c>,
    /// <c>current_timestamp()</c>, <c>transaction_timestamp()</c>, <c>current_date()</c> and a
    /// no-argument <c>unix_timestamp()</c> report for every statement of the transaction, so that
    /// every row a transaction writes or reads in one statement, and every statement of an explicit
    /// <c>BEGIN … COMMIT</c>, sees one time.
    ///
    /// <para><b>Why it is the later of the wall clock and the transaction's HLCs.</b> The physical
    /// part of a hybrid logical clock is Unix milliseconds, so an HLC alone would drop the
    /// sub-millisecond part of every <c>now()</c>. The wall clock alone would lose the causal
    /// order: a node whose clock is behind could stamp a transaction that runs after another
    /// node's transaction with an earlier time. The HLC of a session minted by another node, or a
    /// local HLC advanced by replication, is never behind the clocks it heard from. Taking the later
    /// value keeps the full tick precision when the local clock is ahead, and keeps the causal
    /// order to the millisecond when it is not.</para>
    ///
    /// <para>Fixed at construction and never changed. A retried transaction is a new
    /// <see cref="KvTransaction"/>, so its retry reports a new time, as PostgreSQL does. A
    /// zero-identity read-only transaction has no HLC and takes the wall clock.</para>
    /// </summary>
    public DateTime StartTime { get; }

    /// <summary>The number of Unix milliseconds that still fit in a <see cref="DateTime"/>.</summary>
    private static readonly long MaxUnixMilliseconds =
        (DateTime.MaxValue.Ticks - DateTime.UnixEpoch.Ticks) / TimeSpan.TicksPerMillisecond;

    private static DateTime ComputeStartTime(HLCTimestamp transactionId, HLCTimestamp clientId, HLCTimestamp readTimestamp)
    {
        long ticks = DateTime.UtcNow.Ticks;
        long hlcMilliseconds = Math.Max(transactionId.L, Math.Max(clientId.L, readTimestamp.L));

        // A physical part out of the DateTime range is not a time (a hand-built test identity, for
        // instance); the wall clock is the only value left to report then.
        if (hlcMilliseconds > 0 && hlcMilliseconds <= MaxUnixMilliseconds)
            ticks = Math.Max(ticks, DateTime.UnixEpoch.Ticks + hlcMilliseconds * TimeSpan.TicksPerMillisecond);

        return new DateTime(ticks, DateTimeKind.Utc);
    }

    /// <summary>
    /// Creates a read-only snapshot transaction. Uses <see cref="HLCTimestamp.Zero"/> so Kahuna
    /// performs non-transactional reads of the latest committed value — no Kahuna round-trips.
    /// <paramref name="declaredReadOnly"/> is true when the client asked for a read-only transaction
    /// (<see cref="IsDeclaredReadOnly"/>); an internal reader leaves it false.
    /// </summary>
    public static KvTransaction CreateReadOnly(bool declaredReadOnly = false) =>
        new(HLCTimestamp.Zero, string.Empty, isReadOnly: true,
            transactionMode: CamusTransactionMode.ReadOnly,
            serverChosenSnapshot: !declaredReadOnly);

    /// <summary>
    /// Creates a cheap serializable read-only snapshot transaction pinned to
    /// <paramref name="snapshotT"/>. The transaction identity is still
    /// <see cref="HLCTimestamp.Zero"/> (no Kahuna transaction object, no begin/commit round-trip),
    /// but every read is issued with <c>readTimestamp = snapshotT</c> so all reads in the query
    /// see one consistent point in the version history — exactly as a full Serializable+ReadOnly
    /// transaction would, but without the session-open cost.
    ///
    /// <para><paramref name="isolationLevel"/> is a label only: it is what diagnostics report for the
    /// transaction (<c>SHOW QUERIES</c>). The reads are the same at every level, because a read-only
    /// transaction with a fixed <see cref="ReadTimestamp"/> takes no lock, folds no read and takes no
    /// statement snapshot. The autocommit SELECT passes the server's default level, so a Read Committed
    /// deployment does not see its reads reported as Serializable.</para>
    ///
    /// <para><paramref name="declaredReadOnly"/> is true when the client asked for a read-only
    /// transaction (<see cref="IsDeclaredReadOnly"/>). The autocommit SELECT passes it only when the
    /// request said <c>ReadOnly</c>; the server picks this snapshot for every other one too.</para>
    /// </summary>
    public static KvTransaction CreateSnapshotReadOnly(
        HLCTimestamp snapshotT,
        CamusIsolationLevel isolationLevel = CamusIsolationLevel.Serializable,
        bool declaredReadOnly = false) =>
        new(HLCTimestamp.Zero, string.Empty, isReadOnly: true,
            isolationLevel, CamusTransactionMode.ReadOnly,
            readTimestamp: snapshotT,
            serverChosenSnapshot: !declaredReadOnly);

    /// <summary>
    /// Whether any statement (SELECT, INSERT, UPDATE, DELETE, SHOW, EXPLAIN …) has executed
    /// in this transaction. Set to <c>true</c> by <see cref="MarkStatementExecuted"/> before
    /// the first non-<c>SET TRANSACTION</c> statement runs. Used by <see cref="ApplyIsolationLevel"/>
    /// to reject a level change after the transaction has already read or written data.
    /// </summary>
    private bool statementExecuted;

    /// <summary>
    /// Marks the transaction as having executed at least one statement. Must be called by the
    /// executor for every statement type <b>except</b> <c>SET TRANSACTION</c> — the standard SQL
    /// rule is that <c>SET TRANSACTION</c> must precede all other statements in the transaction.
    /// </summary>
    public void MarkStatementExecuted()
    {
        // No lock needed: a bool write is atomic on all .NET-supported architectures, and the
        // flag only transitions false→true (never back), so a torn read is harmless.
        statementExecuted = true;

        // A new statement starts: the statement snapshot of an earlier SELECT no longer applies. A
        // SELECT opens its own after this call.
        Volatile.Write(ref statementSnapshot, null);
    }

    /// <summary>
    /// Called by <see cref="KvTransactionsManager"/> when a deferred session actually starts.
    /// Seats the Kahuna transaction handle and the coordinator key the session was opened with
    /// (see <see cref="CoordinatorKey"/>), so subsequent operations have a valid identity and
    /// register against the right session owner, and starts the lifetime watch. The key is seated
    /// before the id: an operation that observes a non-zero <see cref="TransactionId"/> must never
    /// read the pre-anchor key. Never called for eager-start or zero-snapshot transactions.
    /// </summary>
    internal void SetSession(HLCTimestamp kahunaId, string coordinatorKey)
    {
        CoordinatorKey = coordinatorKey;
        TransactionId = kahunaId;
        lifetimeWatch = Stopwatch.StartNew();
    }

    /// <summary>
    /// Ensures the Kahuna coordinator session has been opened. No-op for zero-snapshot
    /// transactions (<see cref="SessionStarter"/> is null) and for transactions whose session is
    /// already started (<see cref="TransactionId"/> is non-zero). Called by write and lock paths
    /// in <see cref="CamusDB.Core.Storage.Kv.KvTableStore"/> before any KV operation that
    /// requires a valid transaction handle.
    ///
    /// <para><paramref name="placementGroup"/> is the placement group (<c>{dbId}:{tableId}</c>) of
    /// the table the calling operation touches, or null when the caller has none. Only the call that
    /// actually opens the session uses it: the session is anchored to that group's partition so
    /// the table's data leader owns the session. Later calls, and calls on an already-open
    /// session, ignore it — a session anchors once and never moves.</para>
    /// </summary>
    public Task EnsureSessionStartedAsync(CancellationToken ct, string? placementGroup = null)
    {
        // Reject data operations once the transaction has begun finalizing (or has finalized):
        // the coordinator installs a permanent fence at the first commit/rollback and rejects new
        // work, so registering a write/lock here would strand a pending operation that blocks the
        // finalize drain. This is the single client-side choke point for every write/lock path in
        // KvTableStore, kept cheap (one enum compare) so it can guard them all.
        if (Status != KvTransactionStatus.Active)
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionAlreadyCompleted,
                $"Transaction {UniqueId} is {Status} and can no longer accept data operations");

        // Fast path: not a deferred transaction, or the session is already open.
        if (SessionStarter is null || TransactionId != HLCTimestamp.Zero)
            return Task.CompletedTask;

        lock (sessionStartSync)
        {
            // Re-check under the lock: another operation may have already opened the session.
            if (TransactionId != HLCTimestamp.Zero)
                return Task.CompletedTask;

            // Invoke SessionStarter exactly once; every concurrent first-operation awaits the same
            // task, so only one Kahuna session is ever opened. The starter returns its Task
            // synchronously up to the first await, so nothing blocks under the lock.
            return sessionStartTask ??= SessionStarter(placementGroup, ct);
        }
    }

    /// <summary>
    /// Applies a new locking strategy to this transaction. Called by the
    /// <c>SET TRANSACTION LOCKING</c> SQL handler. Throws <see cref="CamusDBErrorCodes.InvalidInput"/>
    /// if any prior statement has executed (standard SQL requires <c>SET TRANSACTION</c> to precede
    /// all data statements) or if the Kahuna coordinator session has already started (locking is
    /// pinned at session start and cannot change after the session exists).
    /// </summary>
    public void ApplyLocking(KeyValueTransactionLocking locking)
    {
        lock (trackSync)
        {
            if (statementExecuted)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "SET TRANSACTION LOCKING must be the first statement of a transaction — " +
                    $"transaction {UniqueId} has already executed a statement");

            if (IsReadOnly)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "SET TRANSACTION LOCKING cannot be applied to a read-only transaction");

            if (TransactionId != HLCTimestamp.Zero)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "SET TRANSACTION LOCKING cannot be applied after the coordinator session has started — " +
                    $"transaction {UniqueId} already has an active session");

            Locking = locking;
        }
    }

    /// <summary>
    /// Applies an admission priority to this transaction. Called by the
    /// <c>SET TRANSACTION PRIORITY</c> SQL handler.
    ///
    /// <para>Rejects the same three cases as <see cref="ApplyLocking"/>, and for the same reasons: a
    /// prior statement means <c>SET TRANSACTION</c> is no longer first (standard SQL), a read-only
    /// transaction has no coordinator session to admit, and once the session has started the priority
    /// has already been consumed by the admission gate — accepting the change then would record a
    /// value that silently governs nothing.</para>
    /// </summary>
    public void ApplyPriority(TransactionPriority priority)
    {
        lock (trackSync)
        {
            if (statementExecuted)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "SET TRANSACTION PRIORITY must be the first statement of a transaction — " +
                    $"transaction {UniqueId} has already executed a statement");

            if (IsReadOnly)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "SET TRANSACTION PRIORITY cannot be applied to a read-only transaction");

            if (TransactionId != HLCTimestamp.Zero)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "SET TRANSACTION PRIORITY cannot be applied after the coordinator session has started — " +
                    $"transaction {UniqueId} already has an active session");

            Priority = priority;
        }
    }

    /// <summary>
    /// Applies a new isolation level and/or transaction mode to this transaction. Called by the
    /// <c>SET TRANSACTION ISOLATION LEVEL</c> SQL handler.
    ///
    /// <para>Throws <see cref="CamusDBErrorCodes.InvalidInput"/> if any prior statement has
    /// executed in this transaction. Standard SQL requires <c>SET TRANSACTION</c> to be the first
    /// statement. A ReadCommitted read acquires no locks but still "touches" the transaction —
    /// upgrading to Serializable after that read would silently omit the shared lock the new level
    /// requires for the data already read.</para>
    ///
    /// <para>A null <paramref name="mode"/> keeps the current mode, as in PostgreSQL: a
    /// <c>SET TRANSACTION ISOLATION LEVEL …</c> that names no access mode does not make a read-only
    /// transaction writable.</para>
    ///
    /// <para><b>READ WRITE on a snapshot.</b> A transaction that reads at a fixed
    /// <see cref="ReadTimestamp"/>, or that was built read-only (<see cref="IsReadOnly"/>), has no
    /// write session and cannot get one, so <c>READ WRITE</c> is refused with
    /// <see cref="CamusDBErrorCodes.InvalidInput"/>. Accepting it would report a change that does not
    /// happen, and every later write would fail.</para>
    ///
    /// <para><b>Becoming Serializable read-only.</b> A read-only transaction without a fixed snapshot
    /// takes no read lock, so at Serializable each read would see the latest committed value. When the
    /// change leaves the transaction Serializable and read-only, it therefore pins one snapshot, as a
    /// transaction begun that way has. A transaction whose Kahuna session is open reads at its own
    /// identity, which is how <see cref="KvTransactionsManager.BeginAsync"/> builds a Serializable
    /// read-only transaction. A deferred transaction reads at <paramref name="mintSnapshot"/>'s value,
    /// and never opens a session: it is then the same as the snapshot of an autocommit SELECT, and its
    /// commit is the zero-identity no-op. With no <paramref name="mintSnapshot"/>, or when it returns
    /// Zero (a manager with no local clock), the transaction reads the latest committed value, as
    /// <see cref="KvTransactionsManager.BeginReadOnly"/> does then.</para>
    /// </summary>
    /// <param name="level">The isolation level to apply.</param>
    /// <param name="mode">The access mode to apply, or null to keep the current one.</param>
    /// <param name="mintSnapshot">Returns the read timestamp of a deferred transaction that becomes
    /// Serializable and read-only; see above.</param>
    public void ApplyIsolationLevel(
        CamusIsolationLevel level, CamusTransactionMode? mode, Func<HLCTimestamp>? mintSnapshot = null)
    {
        lock (trackSync)
        {
            if (statementExecuted)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "SET TRANSACTION must be the first statement of a transaction — " +
                    $"transaction {UniqueId} has already executed a statement");

            if (mode == CamusTransactionMode.ReadWrite && (IsReadOnly || !ReadTimestamp.IsNull()))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "SET TRANSACTION READ WRITE cannot be applied to a read-only snapshot transaction — " +
                    $"transaction {UniqueId} reads at a fixed snapshot and has no write session; " +
                    "start a read-write transaction instead");

            CamusTransactionMode nextMode = mode ?? TransactionMode;

            if (level == CamusIsolationLevel.Serializable &&
                nextMode == CamusTransactionMode.ReadOnly &&
                !IsReadOnly &&
                ReadTimestamp.IsNull())
            {
                if (TransactionId != HLCTimestamp.Zero)
                {
                    ReadTimestamp = TransactionId;
                }
                else if (mintSnapshot?.Invoke() is { } snapshot && !snapshot.IsNull())
                {
                    ReadTimestamp = snapshot;
                    SessionStarter = null;
                }
            }

            IsolationLevel  = level;
            TransactionMode = nextMode;
        }
    }

    /// <summary>
    /// Records that a Kahuna key-range lock was acquired for this transaction (key-range routed
    /// spaces). Tracked only as in-transaction coverage state for
    /// <see cref="HasWholeBucketLock"/> / <see cref="HasPointLock"/> /
    /// <see cref="CountPointLocksForBucket"/>; the coordinator owns
    /// the folded lock and releases it at finalize, so this is never replayed at commit/rollback.
    /// Idempotent.
    ///
    /// <para>An Exclusive acquire over bounds already held as Shared <b>replaces</b> the Shared
    /// entry rather than joining it. Kahuna promotes such a lock in place, so the server holds one
    /// lock, not two; keeping both would misreport coverage — the write path would see a Shared
    /// entry still present and re-issue the upgrade on every subsequent write to that key, and the
    /// escalation heuristic would count one key twice.</para>
    /// </summary>
    public void TrackRangeLock(
        string prefix, 
        string? startKey, 
        bool startInclusive, 
        string? endKey, 
        bool endInclusive,
        KeyValueDurability durability, 
        RangeLockMode mode = RangeLockMode.Shared
    )
    {
        lock (trackSync)
        {
            acquiredRangeLocks ??= [];

            int delta = 0;

            if (mode == RangeLockMode.Exclusive)
            {
                if (acquiredRangeLocks.Remove(
                        new RangeLockBounds(prefix, startKey, startInclusive, endKey, endInclusive, durability, RangeLockMode.Shared)))
                    delta--;

                (exclusiveLockPrefixes ??= new(StringComparer.Ordinal)).Add(prefix);
            }

            if (acquiredRangeLocks.Add(new RangeLockBounds(prefix, startKey, startInclusive, endKey, endInclusive, durability, mode)))
                delta++;

            if (delta != 0)
            {
                rangeLockCounts ??= new(StringComparer.Ordinal);
                rangeLockCounts.TryGetValue(prefix, out (int Points, int Total) counts);
                bool isPoint = startKey is not null && startKey == endKey;
                rangeLockCounts[prefix] = (counts.Points + (isPoint ? delta : 0), counts.Total + delta);
            }

            if (startKey is null && endKey is null)
                (wholeBucketPrefixes ??= new(StringComparer.Ordinal)).Add(prefix);
        }
    }

    /// <summary>
    /// Returns <c>true</c> if the transaction holds a whole-bucket range lock (start and end
    /// both <c>null</c>) on <paramref name="prefix"/>, regardless of Shared/Exclusive mode.
    /// A whole-bucket lock already covers every point within the bucket, so callers can skip
    /// per-point lock acquisition. O(1): it reads the prefix set that <see cref="TrackRangeLock"/>
    /// keeps, not the tracked bounds.
    /// </summary>
    public bool HasWholeBucketLock(string prefix)
    {
        lock (trackSync)
            return wholeBucketPrefixes?.Contains(prefix) == true;
    }

    /// <summary>
    /// Returns <c>true</c> if the transaction already holds a singleton point lock <c>[key,key]</c>
    /// on <paramref name="prefix"/>. With <paramref name="mode"/> null any mode matches, which is
    /// what a read path wants: an Exclusive singleton covers a shared read of the same key, so
    /// re-acquiring would only spend a round trip to re-assert a lock the transaction already owns.
    /// Pass a specific mode when the caller cares which one it holds — the write path asks for
    /// Shared so it upgrades a read lock exactly once and skips the upgrade when it has already
    /// promoted the key to Exclusive.
    ///
    /// <para>Both lookups are O(1) against the tracking set and allocate nothing, because this runs
    /// on every point read. Skipping a re-acquire never shortens a lease: the coordinator renews
    /// every range lock it holds for the life of the session, so the lock's TTL does not depend on
    /// the client re-asserting it.</para>
    /// </summary>
    public bool HasPointLock(string prefix, string key, RangeLockMode? mode = null)
    {
        lock (trackSync)
        {
            if (acquiredRangeLocks is null)
                return false;

            if (mode is not null)
                return acquiredRangeLocks.Contains(
                    new RangeLockBounds(prefix, key, true, key, true, KeyValueDurability.Persistent, mode.Value));

            return acquiredRangeLocks.Contains(
                       new RangeLockBounds(prefix, key, true, key, true, KeyValueDurability.Persistent, RangeLockMode.Shared))
                || acquiredRangeLocks.Contains(
                       new RangeLockBounds(prefix, key, true, key, true, KeyValueDurability.Persistent, RangeLockMode.Exclusive));
        }
    }

    /// <summary>
    /// Counts singleton point locks (<c>[key,key]</c> entries) held for <paramref name="prefix"/>.
    /// Used to decide when to escalate to a whole-bucket lock. O(1): it reads the counts that
    /// <see cref="TrackRangeLock"/> keeps.
    /// </summary>
    public int CountPointLocksForBucket(string prefix)
    {
        lock (trackSync)
        {
            return rangeLockCounts is not null && rangeLockCounts.TryGetValue(prefix, out (int Points, int Total) counts)
                ? counts.Points
                : 0;
        }
    }

    /// <summary>
    /// Counts every tracked range lock held for <paramref name="prefix"/>, singleton point locks and
    /// bounded ranges alike. A read path that takes one bounded lock per probed key (the equality probe
    /// of an index nested-loop join) uses it to decide when to escalate to a whole-bucket lock, the way
    /// <see cref="CountPointLocksForBucket"/> drives the escalation of per-key point locks.
    /// </summary>
    public int CountRangeLocksForBucket(string prefix)
    {
        lock (trackSync)
        {
            return rangeLockCounts is not null && rangeLockCounts.TryGetValue(prefix, out (int Points, int Total) counts)
                ? counts.Total
                : 0;
        }
    }

    /// <summary>
    /// Returns <c>true</c> if the transaction already holds a range lock with exactly these bounds on
    /// <paramref name="prefix"/>, in either mode. A read path asks before it re-acquires a shared lock:
    /// an Exclusive lock over the same bounds covers a shared read of them, and a Shared one is held to
    /// commit under strict two-phase locking, so a second acquire would only spend a round trip.
    /// </summary>
    public bool HasRangeLock(string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive)
    {
        lock (trackSync)
        {
            if (acquiredRangeLocks is null)
                return false;

            return acquiredRangeLocks.Contains(
                       new RangeLockBounds(prefix, startKey, startInclusive, endKey, endInclusive, KeyValueDurability.Persistent, RangeLockMode.Shared))
                || acquiredRangeLocks.Contains(
                       new RangeLockBounds(prefix, startKey, startInclusive, endKey, endInclusive, KeyValueDurability.Persistent, RangeLockMode.Exclusive));
        }
    }

    /// <summary>Snapshot of the key-range locks acquired by this transaction.</summary>
    public IReadOnlyList<RangeLockBounds> GetAcquiredRangeLocks()
    {
        lock (trackSync)
        {
            if (acquiredRangeLocks is null || acquiredRangeLocks.Count == 0)
                return [];

            return [.. acquiredRangeLocks];
        }
    }

    /// <summary>
    /// True while this transaction holds no range lock, no exclusive key lock and has staged no write,
    /// so nothing of it can be waited on by another transaction. Wait-die aborts a younger lock
    /// requester because a wait from it could close a cycle through what it already holds; a
    /// requester that holds nothing cannot be on any cycle, so it may wait for its deadline instead of
    /// aborting at once. Range locks and key locks both wait under this rule, so a key lock counts as
    /// a holding whether or not a write followed it: <c>SELECT … FOR UPDATE</c> holds row locks with no
    /// write, and an <c>UPDATE</c> holds its row locks before it writes (<see cref="MarkHoldsKeyLocks"/>).
    /// </summary>
    public bool HoldsNoLocksOrWrites
    {
        get
        {
            lock (trackSync)
                return !holdsKeyLocks
                    && (acquiredRangeLocks is null || acquiredRangeLocks.Count == 0)
                    && (modifiedKeys is null || modifiedKeys.Count == 0);
        }
    }

    /// <summary>
    /// Records that Kahuna granted this transaction at least one exclusive key lock. Call it as soon as
    /// a grant is seen, before the same acquire decides how to treat a key another transaction holds:
    /// a batched acquire can be granted some keys and refused others in one answer, and from then on
    /// the transaction is a holder for the wait-die rule (<see cref="HoldsNoLocksOrWrites"/>). The
    /// coordinator owns the locks and releases them when the transaction ends, so only the fact is
    /// kept here, not the keys. It never goes back to false.
    /// </summary>
    public void MarkHoldsKeyLocks()
    {
        if (Volatile.Read(ref holdsKeyLocks))
            return;

        lock (trackSync)
            holdsKeyLocks = true;
    }

    /// <summary>True while this transaction holds at least one range lock, of any mode and any bounds.</summary>
    public bool HoldsRangeLocks
    {
        get
        {
            lock (trackSync)
                return acquiredRangeLocks is { Count: > 0 };
        }
    }

    /// <summary>True when this transaction already wrote or deleted <paramref name="key"/>.</summary>
    public bool HasModified(string key)
    {
        lock (trackSync)
            return modifiedKeys is not null
                && (modifiedKeys.Contains((key, KeyValueDurability.Persistent)) || modifiedKeys.Contains((key, KeyValueDurability.Ephemeral)));
    }

    /// <summary>
    /// Keeps the stored bytes a read by key returned for <paramref name="key"/>, so that a write phase
    /// of this transaction can use them and not read the row again
    /// (<see cref="TryGetRememberedPointReads"/>).
    ///
    /// <para>Only the caller knows that the read can stand in for a later one: it must be a read of the
    /// latest committed row by a transaction that proves its reads at commit (see
    /// <c>KvRangeLockManager.ReusesPointReadsForWrites</c>). A key this transaction already wrote is not
    /// kept, and every later write of a key drops it (<see cref="TrackModified"/>): after a write the
    /// row of the transaction is the written one.</para>
    /// </summary>
    internal void RememberPointRead(string key, ReadOnlyMemory<byte> payload)
    {
        lock (trackSync)
        {
            if (modifiedKeys is not null
                && (modifiedKeys.Contains((key, KeyValueDurability.Persistent)) || modifiedKeys.Contains((key, KeyValueDurability.Ephemeral))))
                return;

            rememberedPointReads ??= new Dictionary<string, ReadOnlyMemory<byte>>(StringComparer.Ordinal);

            if (rememberedPointReads.Count >= MaxRememberedPointReads && !rememberedPointReads.ContainsKey(key))
                return;

            rememberedPointReads[key] = payload;
        }
    }

    /// <summary>True when this transaction holds at least one row from a read by key.</summary>
    internal bool HasRememberedPointReads
    {
        get
        {
            lock (trackSync)
                return rememberedPointReads is { Count: > 0 };
        }
    }

    /// <summary>
    /// Fills <paramref name="rows"/> with the bytes this transaction read for every key of
    /// <paramref name="keys"/>, in order, and returns true. Returns false, and leaves
    /// <paramref name="rows"/> unspecified, when the transaction holds no read for one of them: the
    /// caller then reads the whole batch.
    /// </summary>
    internal bool TryGetRememberedPointReads(IReadOnlyList<string> keys, ReadOnlyMemory<byte>?[] rows)
    {
        lock (trackSync)
        {
            if (rememberedPointReads is null)
                return false;

            for (int i = 0; i < keys.Count; i++)
            {
                if (!rememberedPointReads.TryGetValue(keys[i], out ReadOnlyMemory<byte> payload))
                    return false;

                rows[i] = payload;
            }
        }

        return true;
    }

    /// <summary>
    /// The coordinator key a point read of <paramref name="key"/> registers under, or empty for an
    /// unregistered read.
    ///
    /// <para>A read registers when the transaction folds every read (<see cref="FoldReads"/>), and also
    /// when the transaction already wrote the key. Kahuna keeps a transaction's staged write in the
    /// partition leader's memory only; a leader change drops it, and the next read of the key answers the
    /// committed value with no sign that the transaction's own write is gone. A registered read carries
    /// its answer to the coordinator, which knows the staged revision and refuses the commit when the
    /// answer is not it. The cost is one coordinator hop per read of a key this transaction wrote, which
    /// is the read-after-write shape and not the common read.</para>
    /// </summary>
    public string ReadRegistrationKey(string key) => FoldReads || HasModified(key) ? CoordinatorKey : "";

    /// <summary>
    /// Batch form of <see cref="ReadRegistrationKey(string)"/>: the batch registers when any of its keys
    /// was written by this transaction, so that key's answer reaches the coordinator.
    /// </summary>
    public string ReadRegistrationKey(IReadOnlyList<string> keys)
    {
        if (FoldReads)
            return CoordinatorKey;

        lock (trackSync)
        {
            if (modifiedKeys is null)
                return "";

            for (int i = 0; i < keys.Count; i++)
                if (modifiedKeys.Contains((keys[i], KeyValueDurability.Persistent)) || modifiedKeys.Contains((keys[i], KeyValueDurability.Ephemeral)))
                    return CoordinatorKey;
        }

        return "";
    }

    /// <summary>
    /// Records that <paramref name="key"/> was written or deleted within this transaction.
    /// </summary>
    public void TrackModified(string key, KeyValueDurability durability)
    {
        // Moves the write mark too: a path that writes without a reservation (catalog, registry and
        // statistics writers) must still count as a write for the schema-fence retry. See WriteCount.
        Interlocked.Increment(ref writeBatches);

        lock (trackSync)
        {
            modifiedKeys ??= [];
            modifiedKeys.Add((key, durability));
            rememberedPointReads?.Remove(key);
        }
    }

    /// <summary>
    /// Bulk form of <see cref="TrackModified"/> for batch write paths: records every key of the
    /// batch under a single <c>trackSync</c> acquisition and one capacity reservation, instead of
    /// one lock round-trip per key — a large batch insert otherwise takes hundreds of thousands of
    /// uncontended-but-real monitor acquisitions. All keys are recorded with the same durability.
    /// </summary>
    public void TrackModifiedRange(IReadOnlyList<(string key, int expiresMs, KeyValueDurability durability)> keys, KeyValueDurability durability)
    {
        Interlocked.Increment(ref writeBatches);

        lock (trackSync)
        {
            modifiedKeys ??= [];
            modifiedKeys.EnsureCapacity(modifiedKeys.Count + keys.Count);
            for (int i = 0; i < keys.Count; i++)
                modifiedKeys.Add((keys[i].key, durability));

            if (rememberedPointReads is { Count: > 0 })
            {
                for (int i = 0; i < keys.Count; i++)
                    rememberedPointReads.Remove(keys[i].key);
            }
        }
    }

    /// <summary>
    /// Bulk form of <see cref="TrackModified"/> over plain key strings — see the tuple overload for
    /// why this exists. All keys are recorded with the same durability.
    /// </summary>
    public void TrackModifiedRange(IReadOnlyList<string> keys, KeyValueDurability durability)
    {
        Interlocked.Increment(ref writeBatches);

        lock (trackSync)
        {
            modifiedKeys ??= [];
            modifiedKeys.EnsureCapacity(modifiedKeys.Count + keys.Count);
            for (int i = 0; i < keys.Count; i++)
                modifiedKeys.Add((keys[i], durability));

            if (rememberedPointReads is { Count: > 0 })
            {
                for (int i = 0; i < keys.Count; i++)
                    rememberedPointReads.Remove(keys[i]);
            }
        }
    }

    /// <summary>
    /// Pins the schema version a statement planned against for <paramref name="resource"/> (one table
    /// of one database), so the commit can refuse the transaction if a schema change moved the table's
    /// layout in between (<see cref="TryValidateSchemaPins"/>). The first pin of a resource decides. A
    /// later statement of the same transaction that finds the table at another version is refused here,
    /// at the statement, with the retryable <see cref="CamusDBErrorCodes.TransactionConflict"/>: the
    /// transaction cannot see one consistent schema any more, nothing it staged is lost by a rollback,
    /// and a replay from <c>BEGIN</c> plans every statement against the new version. The generic
    /// internal code was used before, and no client retried it.
    /// </summary>
    public void PinSchemaVersion(
        string resource,
        long schemaVersion,
        Func<long> currentVersion,
        Func<bool>? isStillValid = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resource);
        ArgumentNullException.ThrowIfNull(currentVersion);

        schemaPins ??= new(StringComparer.Ordinal);

        if (schemaPins.TryGetValue(resource, out SchemaVersionPin pin))
        {
            if (pin.SchemaVersion != schemaVersion)
                throw new CamusDBException(
                    CamusDBErrorCodes.TransactionConflict,
                    $"Transaction {UniqueId} already pinned schema resource '{resource}' at version {pin.SchemaVersion}, not {schemaVersion}: " +
                    "a schema change landed between two statements of the transaction; retry it from BeginAsync"
                );

            return;
        }

        schemaPins[resource] = new(schemaVersion, currentVersion, isStillValid);
    }

    /// <summary>
    /// Checks every pin taken by <see cref="PinSchemaVersion"/> against the live schema, at commit. Returns
    /// false, with <paramref name="refusal"/> naming the first pin that no longer holds, when the table was
    /// dropped or recreated (its id changed) or its layout version moved: a column was added or dropped,
    /// or a column element changed state. The caller refuses the commit with the retryable
    /// <see cref="CamusDBErrorCodes.TransactionConflict"/> and rolls the transaction back, as the
    /// write-shape fence does for an index or a constraint, so a client replays from <c>BEGIN</c> against
    /// the new schema. Nothing the transaction staged can have landed, because this runs before the
    /// commit request leaves the node.
    /// </summary>
    public bool TryValidateSchemaPins([NotNullWhen(false)] out string? refusal)
    {
        refusal = null;

        if (schemaPins is null)
            return true;

        foreach ((string resource, SchemaVersionPin pin) in schemaPins)
        {
            if (pin.IsStillValid is not null && !pin.IsStillValid())
            {
                refusal = $"it pinned schema resource '{resource}', but it is no longer present";
                return false;
            }

            long current = pin.CurrentVersion();
            if (current == pin.SchemaVersion)
                continue;

            refusal = $"it pinned schema resource '{resource}' at version {pin.SchemaVersion}, but current version is {current}";
            return false;
        }

        return true;
    }

    public long? GetPinnedSchemaVersion(string resource)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(resource);

        return schemaPins is not null && schemaPins.TryGetValue(resource, out SchemaVersionPin pin)
            ? pin.SchemaVersion
            : null;
    }

    /// <summary>
    /// Records that a statement of this transaction writes <paramref name="table"/>, and that it
    /// captured <paramref name="epoch"/> from the database's write-shape clock <b>before</b> it
    /// resolved the table. The commit is refused when the table gained a write obligation after that
    /// epoch (see <see cref="TryEnterCommit"/>).
    ///
    /// <para><b>The first pin of a table wins.</b> A later statement can plan against a newer shape,
    /// but the writes of the first statement are still staged, and they are the ones that lack the
    /// new index entry or the new constraint check. So the oldest epoch decides.</para>
    ///
    /// <para>Guarded by <c>trackSync</c>, although one transaction runs on one logical thread: the
    /// schema-change drain reads the pins from another thread
    /// (<see cref="IsCommitInFlightPlannedBefore"/>).</para>
    /// </summary>
    public void PinWriteShape(IWriteShapeSource table, long epoch)
    {
        ArgumentNullException.ThrowIfNull(table);

        lock (trackSync)
        {
            if (firstWriteShapePin.Table is null)
            {
                firstWriteShapePin = new(table, epoch);
                return;
            }

            if (ReferenceEquals(firstWriteShapePin.Table, table))
                return;

            if (moreWriteShapePins is not null)
            {
                for (int i = 0; i < moreWriteShapePins.Count; i++)
                {
                    if (ReferenceEquals(moreWriteShapePins[i].Table, table))
                        return;
                }
            }

            (moreWriteShapePins ??= []).Add(new(table, epoch));
        }
    }

    /// <summary>
    /// The commit gate of the write-shape fence. In one critical section it checks every write-shape
    /// pin and, when all hold, moves the transaction to <see cref="KvTransactionStatus.Finalizing"/>
    /// and marks its commit as in flight. Returns false, and changes nothing, when a pinned table
    /// gained a write obligation after the pin; <paramref name="changed"/> is that table.
    ///
    /// <para><b>The check and the status change must be atomic.</b> A schema change first stamps the
    /// table and then looks for commits in flight (<see cref="IsCommitInFlightPlannedBefore"/>, under
    /// this same lock). With one critical section, every commit falls on one side: it entered before
    /// the look, so the schema change sees it and waits for its outcome; or it enters after the
    /// stamp, so it sees the stamp and is refused. With a check and a separate status write, a commit
    /// could pass the check before the stamp and become visible after the look. Then the backfill or
    /// the validation pass reads the table without the row, and the commit lands afterwards.</para>
    /// </summary>
    internal bool TryEnterCommit(out IWriteShapeSource? changed)
    {
        lock (trackSync)
        {
            if (firstWriteShapePin.Table is not null)
            {
                if (firstWriteShapePin.Table.WriteShapeChangedAt > firstWriteShapePin.Epoch)
                {
                    changed = firstWriteShapePin.Table;
                    return false;
                }

                if (moreWriteShapePins is not null)
                {
                    for (int i = 0; i < moreWriteShapePins.Count; i++)
                    {
                        WriteShapePin pin = moreWriteShapePins[i];

                        if (pin.Table.WriteShapeChangedAt > pin.Epoch)
                        {
                            changed = pin.Table;
                            return false;
                        }
                    }
                }
            }

            commitInFlight = true;
            Status = KvTransactionStatus.Finalizing;
        }

        changed = null;
        return true;
    }

    /// <summary>
    /// The check of <see cref="TryEnterCommit"/> without its state change: false, with
    /// <paramref name="changed"/> set, when a table this transaction wrote gained a write obligation after
    /// the write was planned. Used before a transaction with schema changes announces its change set.
    /// </summary>
    internal bool WriteShapePinsHold(out IWriteShapeSource? changed)
    {
        lock (trackSync)
        {
            if (firstWriteShapePin.Table is not null)
            {
                if (firstWriteShapePin.Table.WriteShapeChangedAt > firstWriteShapePin.Epoch)
                {
                    changed = firstWriteShapePin.Table;
                    return false;
                }

                if (moreWriteShapePins is not null)
                {
                    for (int i = 0; i < moreWriteShapePins.Count; i++)
                    {
                        if (moreWriteShapePins[i].Table.WriteShapeChangedAt > moreWriteShapePins[i].Epoch)
                        {
                            changed = moreWriteShapePins[i].Table;
                            return false;
                        }
                    }
                }
            }
        }

        changed = null;
        return true;
    }

    /// <summary>
    /// Drops every write-shape pin. Called once the transaction's change set is announced in the schema
    /// log: the pins held when it was announced, and the announcement fences the tables the change set
    /// touches, which stamps them. Kept, the pins would refuse this transaction's own commit because of
    /// its own change set. A change by another transaction after the announcement is refused by the
    /// schema log while the change set is pending.
    /// </summary>
    internal void ClearWriteShapePins()
    {
        lock (trackSync)
        {
            firstWriteShapePin = default;
            moreWriteShapePins = null;
        }
    }

    /// <summary>
    /// True while a commit of this transaction passed <see cref="TryEnterCommit"/> and has no terminal
    /// outcome yet. A commit that returned the non-terminal <c>MustRetry</c> stays in flight: the
    /// client can resume it at any time, and it can still land.
    /// </summary>
    internal bool IsCommitInFlight
    {
        get { lock (trackSync) return commitInFlight && Status == KvTransactionStatus.Finalizing; }
    }

    /// <summary>
    /// True when a commit of this transaction is in flight and one of its statements wrote one of
    /// <paramref name="tables"/> under a write-shape epoch earlier than <paramref name="epoch"/>.
    /// Such a commit passed the gate before the table was stamped, so only its outcome tells whether
    /// its rows exist. Called by the schema-change drain.
    /// </summary>
    internal bool IsCommitInFlightPlannedBefore(IReadOnlyList<IWriteShapeSource> tables, long epoch)
    {
        lock (trackSync)
        {
            if (!commitInFlight || Status != KvTransactionStatus.Finalizing || firstWriteShapePin.Table is null)
                return false;

            if (WasPlannedBefore(firstWriteShapePin, tables, epoch))
                return true;

            if (moreWriteShapePins is not null)
            {
                for (int i = 0; i < moreWriteShapePins.Count; i++)
                {
                    if (WasPlannedBefore(moreWriteShapePins[i], tables, epoch))
                        return true;
                }
            }

            return false;
        }
    }

    private static bool WasPlannedBefore(WriteShapePin pin, IReadOnlyList<IWriteShapeSource> tables, long epoch)
    {
        if (pin.Epoch >= epoch)
            return false;

        for (int i = 0; i < tables.Count; i++)
        {
            if (ReferenceEquals(pin.Table, tables[i]))
                return true;
        }

        return false;
    }

    /// <summary>
    /// True once this transaction has written or deleted at least one key that is still unresolved,
    /// i.e. it owns live write intents in the store. A statement that must read committed state under
    /// its own snapshot (rather than under this transaction) has to refuse to run in that situation:
    /// a snapshot read blocks on a foreign live intent whose commit timestamp is undetermined, and
    /// these intents can only be resolved by the very caller that is waiting for the read — a
    /// self-deadlock that resolves only when the session reaper eventually aborts the transaction.
    /// </summary>
    public bool HasPendingWrites
    {
        get { lock (trackSync) return modifiedKeys is { Count: > 0 }; }
    }

    /// <summary>
    /// Returns the modified-keys set as raw (key, durability) pairs. Used by the cache
    /// invalidation path, which needs the keys in tuple form to call
    /// <see cref="IQueryResultCache.InvalidateByModifiedKeys"/> without an extra allocation.
    /// </summary>
    public List<(string key, KeyValueDurability durability)> GetModifiedKeyPairs()
    {
        if (modifiedKeys is null)
            return [];

        return [.. modifiedKeys];
    }

    private readonly record struct SchemaVersionPin(
        long SchemaVersion,
        Func<long> CurrentVersion,
        Func<bool>? IsStillValid
    );
}
