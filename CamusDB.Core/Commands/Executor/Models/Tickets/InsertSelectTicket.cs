
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// A ticket for <c>INSERT INTO t [(c1, …)] SELECT …</c>.
///
/// <para>Unlike <see cref="InsertTicket"/> it carries no rows: the values do not exist until the
/// source query runs, so the ticket holds the source SELECT's AST and the rows are produced by the
/// ordinary query pipeline at execution time. The source's output columns are mapped onto
/// <see cref="TargetColumns"/> <b>positionally</b>, so the two must have the same arity — a check
/// that can only be made once the source query has been bound.</para>
/// </summary>
public readonly struct InsertSelectTicket
{
    public KvTransaction TxnState { get; }

    public string DatabaseName { get; }

    public string TableName { get; }

    /// <summary>
    /// The explicit target column list, or null when the statement named none — in which case every
    /// writable column of the table, in schema order, is the target (matching <c>INSERT … VALUES</c>).
    /// </summary>
    public IReadOnlyList<string>? TargetColumns { get; }

    /// <summary>The source <see cref="NodeType.Select"/> statement, unexecuted.</summary>
    public NodeAst SourceSelect { get; }

    public Dictionary<string, ColumnValue>? Parameters { get; }

    public InsertSelectTicket(
        KvTransaction txnState,
        string databaseName,
        string tableName,
        IReadOnlyList<string>? targetColumns,
        NodeAst sourceSelect,
        Dictionary<string, ColumnValue>? parameters
    )
    {
        TxnState = txnState;
        DatabaseName = databaseName;
        TableName = tableName;
        TargetColumns = targetColumns;
        SourceSelect = sourceSelect;
        Parameters = parameters;
    }
}
