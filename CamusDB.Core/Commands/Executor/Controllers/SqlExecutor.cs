
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

    private readonly SQLExecutorAlterConstraintCreator sqlExecutorAlterConstraintCreator = new();

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
    /// Creates a ticket to add or drop a CHECK (or named NOT NULL) constraint from the AST
    /// representation of a SQL statement. Requires the current table schema so column references
    /// can be validated at parse time.
    /// </summary>
    internal AlterConstraintTicket CreateAlterConstraintTicket(ExecuteSQLTicket ticket, NodeAst ast, Catalogs.Models.TableSchema tableSchema)
    {
        return sqlExecutorAlterConstraintCreator.CreateAlterConstraintTicket(ticket, ast, tableSchema);
    }

    /// <summary>
    /// Builds a ticket for <c>ALTER TABLE t SET (key = value, ...)</c>. Validates at parse time that
    /// every key is a recognized table setting and every value is a boolean literal, so an unknown or
    /// malformed storage parameter is rejected before any schema mutation.
    /// </summary>
    internal AlterTableSettingsTicket CreateAlterTableSettingsTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        string tableName = ast.leftAst!.yytext!;

        // Collect raw pairs (preserving duplicates for detection), then run the single canonical
        // validator shared with the direct-ticket path.
        var raw = new List<KeyValuePair<string, string>>();
        CollectSettings(ast.rightAst!, raw);

        Dictionary<string, string> settings = Catalogs.Models.TableSettings.Canonicalize(raw);
        return new AlterTableSettingsTicket(ticket.DatabaseName, tableName, settings);
    }

    // Walks the SET (...) list (reusing the UpdateList/UpdateItem AST shape) into raw key→value pairs.
    // Value nodes must be boolean literals; all other validation/canonicalization is centralized in
    // TableSettings.Canonicalize.
    private static void CollectSettings(NodeAst node, List<KeyValuePair<string, string>> raw)
    {
        if (node.nodeType == NodeType.UpdateList)
        {
            if (node.leftAst is not null) CollectSettings(node.leftAst, raw);
            if (node.rightAst is not null) CollectSettings(node.rightAst, raw);
            return;
        }

        if (node.nodeType != NodeType.UpdateItem)
            return;

        string key = node.leftAst!.yytext ?? "";
        NodeAst valueNode = node.rightAst!;
        string value = valueNode.nodeType == NodeType.Bool ? valueNode.yytext ?? "" : "";
        raw.Add(new KeyValuePair<string, string>(key, value));
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
