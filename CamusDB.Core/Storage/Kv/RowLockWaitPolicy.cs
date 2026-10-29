
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// What a row-locking read (<c>SELECT … FOR UPDATE</c>) does when another transaction holds the
/// exclusive lock of a row it wants. In every policy a <em>transient</em> holder — an intent that waits
/// for replication or settlement — is waited out first, because it is not a held lock and failing on it
/// would report a conflict that does not exist.
/// </summary>
internal enum RowLockWaitPolicy
{
    /// <summary>
    /// No option written: the statement fails as an UPDATE of the same row fails, with the retryable
    /// <see cref="CamusDBErrorCodes.TransactionConflict"/>. A held key lock is refused at once today,
    /// so this is a no-wait behavior too, with a code the automatic retry handles.
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
