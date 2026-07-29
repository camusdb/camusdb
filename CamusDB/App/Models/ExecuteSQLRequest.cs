
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
    /// Optional prepared-statement handle from <c>/prepare-sql-statement</c>.
    ///
    /// <para>When set, <see cref="Sql"/>, <see cref="DatabaseName"/> and <see cref="Parameters"/>
    /// must be absent — the handle already names all three, and sending both is refused rather than
    /// resolved by a precedence rule — and <see cref="PositionalParameters"/> supplies the values.
    /// Everything else on this request (transaction, isolation, locking, causal token) behaves
    /// exactly as it does for an inline statement.</para>
    ///
    /// <para>A handle that is unknown, expired, prepared on another node, or owned by another
    /// principal fails with <c>CADB0520</c> (HTTP 404). That is a routine outcome, not a client bug:
    /// prepare the statement again and replay the execution once.</para>
    /// </summary>
    public string? StatementId { get; set; }

    /// <summary>
    /// Values for a prepared execution, in the order the prepare reply published in
    /// <c>parameterNames</c>. The element encoding is identical to a value of the
    /// <see cref="Parameters"/> map, so a client reuses its existing value serialization unchanged.
    /// The count must equal the declared parameter count exactly.
    /// </summary>
    public List<ColumnValue>? PositionalParameters { get; set; }

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
