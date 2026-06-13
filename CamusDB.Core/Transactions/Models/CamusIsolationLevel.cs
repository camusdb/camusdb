
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Transactions;

/// <summary>
/// Isolation level for a <see cref="KvTransaction"/>.
///
/// Default is <see cref="ReadCommitted"/> — existing behaviour unchanged.
/// <see cref="Serializable"/> is intended to activate strict two-phase locking (read-write) or
/// a lock-free MVCC snapshot (read-only); it is currently carried as metadata only and does not
/// yet change locking or read behaviour.
/// </summary>
public enum CamusIsolationLevel
{
    ReadCommitted,
    Serializable
}

/// <summary>
/// Mode of a <see cref="KvTransaction"/>: read-write or read-only.
///
/// <see cref="ReadOnly"/> transactions never acquire write locks and are intended, under
/// <see cref="CamusIsolationLevel.Serializable"/>, to read a consistent MVCC snapshot as-of a
/// single timestamp. That snapshot behaviour is not yet wired; the mode is metadata only for now.
/// </summary>
public enum CamusTransactionMode
{
    ReadWrite,
    ReadOnly
}
