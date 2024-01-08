
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
using CamusDB.Core.Util.Time;

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

    private readonly SQLExecutorAlterTableCreator sqlExecutorAlterTableCreator = new();

    public SqlExecutor()
    {

    }

    public async Task<QueryTicket> CreateQueryTicket(CommandExecutor executor, ExecuteSQLTicket ticket)
    {
        return await sqlExecutorQueryCreator.CreateQueryTicket(executor, ticket);
    }

    internal async Task<InsertTicket> CreateInsertTicket(CommandExecutor executor, ExecuteSQLTicket ticket, NodeAst ast)
    {
        return await sqlExecutorInsertCreator.CreateInsertTicket(executor, ticket, ast);
    }

    internal async Task<UpdateTicket> CreateUpdateTicket(CommandExecutor executor, ExecuteSQLTicket ticket, NodeAst ast)
    {
        return await sqlExecutorUpdateCreator.CreateUpdateTicket(executor, ticket, ast);
    }

    internal async Task<DeleteTicket> CreateDeleteTicket(CommandExecutor executor, ExecuteSQLTicket ticket, NodeAst ast)
    {
        string tableName = ast.leftAst!.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing delete conditions");

        return new(
            txnId: await executor.NextTxnId(),
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            where: ast.rightAst,
            filters: null
        );
    }

    internal CreateTableTicket CreateCreateTableTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        return sqlExecutorCreateTableCreator.CreateCreateTableTicket(ticket, ast);
    }

    internal AlterTableTicket CreateAlterTableTicket(HLCTimestamp hlcTimestamp, ExecuteSQLTicket ticket, NodeAst ast)
    {
        return sqlExecutorAlterTableCreator.CreateAlterTableTicket(hlcTimestamp, ticket, ast);
    }

    public static ColumnValue EvalExpr(NodeAst expr, Dictionary<string, ColumnValue> row, Dictionary<string, ColumnValue>? parameters)
    {
        return SQLExecutorBaseCreator.EvalExpr(expr, row, parameters);
    }
}
