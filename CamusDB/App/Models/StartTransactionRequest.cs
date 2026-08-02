
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.App.Models;

public sealed class StartTransactionRequest
{
    public string? DatabaseName { get; set; }

    /// <summary>
    /// Optional isolation level for this transaction.
    /// Accepted values: <c>"ReadCommitted"</c> (default), <c>"Serializable"</c>.
    /// When absent, the server applies <see cref="CamusDB.Core.CamusDBOptions.DefaultIsolationLevel"/>.
    /// </summary>
    public string? IsolationLevel { get; set; }

    /// <summary>
    /// Optional transaction mode.
    /// Accepted values: <c>"ReadWrite"</c> (default), <c>"ReadOnly"</c>.
    /// </summary>
    public string? TransactionMode { get; set; }

    /// <summary>
    /// Optional locking mode for this transaction.
    /// Accepted values: <c>"Pessimistic"</c> (default), <c>"Optimistic"</c>.
    /// When absent, the server applies <see cref="CamusDB.Core.CamusDBOptions.DefaultTransactionLocking"/>.
    /// </summary>
    public string? Locking { get; set; }
}
