
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Transactions;

/// <summary>
/// Isolation level for a <see cref="KvTransaction"/>. The level a transaction is begun with decides
/// which locks its reads take, and it is the level — never <see cref="KvTransaction.Locking"/> —
/// that gates them: an optimistic transaction skips only the explicit exclusive lock its writes
/// would take, so Serializable combined with optimistic locking is a hybrid, not a weaker level.
///
/// <para>Selection precedence is explicit argument → <see cref="CamusDBOptions.DefaultIsolationLevel"/>
/// (<see cref="Serializable"/> as shipped, settable to <c>read_committed</c> in <c>config.yml</c>) →
/// whatever <c>SET TRANSACTION ISOLATION LEVEL</c> changed it to before the first statement ran.</para>
///
/// <para>See <c>docs/transactions-locking-and-isolation.md</c> for the narrative version, and
/// <see cref="CamusTransactionMode"/> — the level alone does not determine behaviour, the
/// level/mode pair does.</para>
/// </summary>
public enum CamusIsolationLevel
{
    /// <summary>
    /// Reads observe the latest committed value and take no predicate locks, so they neither block
    /// concurrent writers nor repeat: the same key re-read within one transaction may return a newer
    /// value. Writes still take their exclusive locks under pessimistic locking. A transaction that
    /// needs its reads to carry a commit dependency at this level must ask for it explicitly through
    /// <see cref="Kahuna.Shared.KeyValue.ReadValidation.TrackAndValidate"/>.
    /// </summary>
    ReadCommitted,

    /// <summary>
    /// The default. Paired with <see cref="CamusTransactionMode.ReadWrite"/> it is strict two-phase
    /// locking: reads and scans take shared point/range predicate locks held to commit, a subsequent
    /// write on a read key upgrades its shared lock to exclusive, and the retained range locks fence
    /// phantom inserts into a scanned range. Paired with <see cref="CamusTransactionMode.ReadOnly"/>
    /// it is a lock-free MVCC snapshot pinned to one timestamp instead.
    ///
    /// <para>Because its locks are held for the whole transaction, a read-write transaction at this
    /// level is bounded by <see cref="CamusDBOptions.MaxSerializableTransactionLifetimeMs"/> and its
    /// conflicts are reported as retryable serialization failures — callers replay from
    /// <c>BeginAsync</c> (see <see cref="SerializableRetryHelper"/>).</para>
    /// </summary>
    Serializable
}

/// <summary>
/// Mode of a <see cref="KvTransaction"/>: read-write or read-only. Together with
/// <see cref="CamusIsolationLevel"/> it selects the transaction's behaviour — the pair is what
/// matters, which is why the store tests both before taking a predicate lock rather than testing the
/// level alone.
/// </summary>
public enum CamusTransactionMode
{
    /// <summary>
    /// The default: the transaction may write. Under <see cref="CamusIsolationLevel.Serializable"/>
    /// this is the combination that acquires shared predicate locks on read and upgrades them on
    /// write, and the only one subject to
    /// <see cref="CamusDBOptions.MaxSerializableTransactionLifetimeMs"/>.
    /// </summary>
    ReadWrite,

    /// <summary>
    /// The transaction never writes, so it takes no write locks and cannot block a writer.
    ///
    /// <para>Under <see cref="CamusIsolationLevel.Serializable"/> this is a true MVCC snapshot: one
    /// server timestamp is minted at begin and held in <see cref="KvTransaction.ReadTimestamp"/>, and
    /// every read is issued as-of that instant — so the transaction sees neither values written nor
    /// rows inserted after it began, and concurrent writers run unimpeded. It holds no server-side
    /// transaction state, which is why its commit is a lightweight rollback of an empty transaction
    /// rather than a 2PC round trip.</para>
    ///
    /// <para>Read-only is not by itself a promise of lock freedom: a read-committed scan promoted to
    /// a real read-only transaction can still hold shared range locks that finalize must release, so
    /// it takes the normal commit path. Only the serializable snapshot above is stateless.</para>
    /// </summary>
    ReadOnly
}
