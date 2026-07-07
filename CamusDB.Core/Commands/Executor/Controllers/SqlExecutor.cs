
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// The methods in this class receive AST (Abstract Syntax Tree) from the SQL parser and transform them into tickets,
/// which are the representations and attributes of the different types of requests accepted by the command executor.
/// </summary>
internal sealed class SqlExecutor()
{
    private readonly SQLExecutorQueryCreator sqlExecutorQueryCreator = new();

    private readonly SQLExecutorInsertCreator sqlExecutorInsertCreator = new();

    private readonly SQLExecutorUpdateCreator sqlExecutorUpdateCreator = new();

    private readonly SQLExecutorDeletereator sqlExecutorDeleteCreator = new();

    private readonly SQLExecutorCreateTableCreator sqlExecutorCreateTableCreator = new();

    private readonly SQLExecutorDropTableCreator sqlExecutorDropTableCreator = new();

    private readonly SQLExecutorAlterTableCreator sqlExecutorAlterTableCreator = new();

    private readonly SQLExecutorAlterIndexCreator sqlExecutorAlterIndexCreator = new();

    public QueryTicket CreateQueryTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        return sqlExecutorQueryCreator.CreateQueryTicket(ticket, ast);
    }

    internal async Task<InsertTicket> CreateInsertTicket(CommandExecutor executor, DatabaseDescriptor database, ExecuteSQLTicket ticket, NodeAst ast)
    {
        return await sqlExecutorInsertCreator.CreateInsertTicket(executor, database, ticket, ast).ConfigureAwait(false);
    }

    internal UpdateTicket CreateUpdateTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        return sqlExecutorUpdateCreator.CreateUpdateTicket(ticket, ast);
    }

    internal DeleteTicket CreateDeleteTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        return sqlExecutorDeleteCreator.CreateDeleteTicket(ticket, ast);
    }

    /// <summary>
    /// Creates a ticket to create a table from the AST representation of a SQL statement.
    /// </summary>
    /// <param name="ticket"></param>
    /// <param name="ast"></param>
    /// <returns></returns>
    internal CreateTableTicket CreateCreateTableTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        return sqlExecutorCreateTableCreator.CreateCreateTableTicket(ticket, ast);
    }

    /// <summary>
    /// Creates a ticket to drop a table from the AST representation of a SQL statement.
    /// </summary>
    /// <param name="ticket"></param>
    /// <param name="ast"></param>
    /// <returns></returns>
    internal DropTableTicket CreateDropTableTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        return sqlExecutorDropTableCreator.CreateDropTableTicket(ticket, ast);
    }

    /// <summary>
    /// Creates a ticket to alter a table from the AST representation of a SQL statement.
    /// </summary>
    /// <param name="ticket"></param>
    /// <param name="ast"></param>
    /// <returns></returns>
    internal AlterTableTicket CreateAlterTableTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        return sqlExecutorAlterTableCreator.CreateAlterTableTicket(ticket, ast);
    }

    /// <summary>
    /// Creates a ticket to alter an index from the AST representation of a SQL statement.
    /// </summary>
    /// <param name="ticket"></param>
    /// <param name="ast"></param>
    /// <returns></returns>
    internal AlterIndexTicket CreateAlterIndexTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        return sqlExecutorAlterIndexCreator.CreateAlterIndexTicket(ticket, ast);
    }

    /// <summary>
    /// Evaluates an AST (Abstract Syntax Tree) representation of a SQL statement and returns a ColumnValue result.
    /// </summary>
    /// <param name="expr"></param>
    /// <param name="row"></param>
    /// <param name="parameters"></param>
    /// <returns></returns>
    public static ColumnValue EvalExpr(
        NodeAst expr,
        IReadOnlyDictionary<string, ColumnValue> row,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver = null)
    {
        return SQLExecutorBaseCreator.EvalExpr(expr, row, parameters, rowNameResolver);
    }

    /// <summary>
    /// Evaluates an expression AST with direct ordinal access for column identifier lookups.
    /// Reads <see cref="QueryRow.Values"/> via <see cref="RowLayout.IndexOf"/> instead of going
    /// through the <see cref="IReadOnlyDictionary{TKey,TValue}"/> adapter, eliminating virtual
    /// dispatch and the FrozenDictionary hash lookup for the common identifier case.
    /// Falls back to the dictionary path for identifiers not found in the row's layout
    /// (e.g. column aliases or schema-history gaps).
    /// </summary>
    public static ColumnValue EvalExpr(
        NodeAst expr,
        QueryRow queryRow,
        Dictionary<string, ColumnValue>? parameters,
        QueryRowNameResolver? rowNameResolver = null)
    {
        return SQLExecutorBaseCreator.EvalExpr(expr, queryRow, parameters, rowNameResolver, queryRow);
    }
}
