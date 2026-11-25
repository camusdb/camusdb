
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// What an exclusive key-lock acquire does when another transaction holds a key it wants. A row-locking
/// read (<c>SELECT … FOR UPDATE</c>) picks the policy from its clause; every write path uses
/// <see cref="Wait"/>. In every policy a <em>transient</em> holder — an intent that waits for
/// replication or settlement — is waited out first, because it is not a held lock and failing on it
/// would report a conflict that does not exist.
/// </summary>
internal enum RowLockWaitPolicy
{
    /// <summary>
    /// No option written, and every UPDATE, DELETE and INSERT: wait for the holder under wait-die. A
    /// transaction that is older than the holder, or that holds no lock and no write yet, polls until
    /// <see cref="CamusDBOptions.LockWaitDeadlineMs"/>; a younger one that holds something fails at once.
    /// Both failures are the retryable <see cref="CamusDBErrorCodes.TransactionConflict"/>.
    /// </summary>
    Wait,

    /// <summary>
    /// <c>NOWAIT</c>: the statement fails at once with <see cref="CamusDBErrorCodes.LockNotAvailable"/>,
    /// which is not retried automatically.
    /// </summary>
    NoWait,

    /// <summary>
    /// <c>SKIP LOCKED</c>: a held row is left out of the result and does not count toward LIMIT. Only at
    /// Read Committed; at Serializable the workers' shared scan locks would still conflict at write.
    /// </summary>
    SkipLocked,
}
