
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using Kommander.Time;

namespace CamusDB.App.Models;

public sealed class ExecuteSQLRequest
{
    public long TxnIdPT { get; set; }

    public uint TxnIdCounter { get; set; }

    public string? DatabaseName { get; set; }

    public string? Sql { get; set; }

    public Dictionary<string, ColumnValue>? Parameters { get; set; }

    /// <summary>
    /// Optional isolation level for the autocommit transaction begun by this request.
    /// Accepted values (case-insensitive): <c>"ReadCommitted"</c>, <c>"Serializable"</c>.
    /// Ignored when <c>TxnIdPT</c> resumes an existing transaction.
    /// </summary>
    public string? IsolationLevel { get; set; }

    /// <summary>
    /// Optional transaction mode for the autocommit transaction begun by this request.
    /// Accepted values (case-insensitive): <c>"ReadWrite"</c>, <c>"ReadOnly"</c>.
    /// Ignored when <c>TxnIdPT</c> resumes an existing transaction.
    /// </summary>
    public string? TransactionMode { get; set; }

    /// <summary>
    /// Optional locking mode for the autocommit transaction begun by this request.
    /// Accepted values (case-insensitive): <c>"Pessimistic"</c>, <c>"Optimistic"</c>.
    /// Ignored when <c>TxnIdPT</c> resumes an existing transaction, and ignored on the read-only
    /// <c>/execute-sql-query</c> path (an autocommit <c>SELECT</c> runs as a read-only snapshot,
    /// which has no locking mode). Applies to the writable autocommit paths
    /// (<c>/execute-sql-non-query</c>, <c>/execute-sql-ddl</c>).
    /// </summary>
    public string? Locking { get; set; }

    public HLCTimestamp? CausalToken { get; set; }
}
