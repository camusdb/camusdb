
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// The methods in this class receive AST (Abstract Syntax Tree) from the SQL parser and transform them into tickets, 
/// which are the representations and attributes of the different types of requests accepted by the command executor.
/// </summary>
internal sealed class SqlExecutor
{
    private readonly SQLExecutorQueryCreator sqlExecutorQueryCreator = new();

    private readonly SQLExecutorInsertCreator sqlExecutorInsertCreator = new();

    private readonly SQLExecutorUpdateCreator sqlExecutorUpdateCreator = new();

    private readonly SQLExecutorCreateTableCreator sqlExecutorCreateTableCreator = new();

    public SqlExecutor()
    {

    }

    public QueryTicket CreateQueryTicket(ExecuteSQLTicket ticket)
    {
        return sqlExecutorQueryCreator.CreateQueryTicket(ticket);
    }

    internal InsertTicket CreateInsertTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        return sqlExecutorInsertCreator.CreateInsertTicket(ticket, ast);
    }

    internal UpdateTicket CreateUpdateTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        return sqlExecutorUpdateCreator.CreateUpdateTicket(ticket, ast);
    }

    internal DeleteTicket CreateDeleteTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        string tableName = ast.leftAst!.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing delete conditions");

        return new(ticket.DatabaseName, tableName, ast.rightAst, null);
    }

    internal CreateTableTicket CreateCreateTableTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        return sqlExecutorCreateTableCreator.CreateCreateTableTicket(ticket, ast);
    }

    public static ColumnValue EvalExpr(NodeAst expr, Dictionary<string, ColumnValue> row)
    {
        return SQLExecutorBaseCreator.EvalExpr(expr, row);
    }
}
