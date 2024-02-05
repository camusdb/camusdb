
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.DML;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Creates a ticket to drop a table from the AST representation of a SQL statement.
/// 
/// @todo #1 Validate empty or null table name/fields here
/// </summary>
internal sealed class SQLExecutorDropTableCreator : SQLExecutorBaseCreator
{
    internal DropTableTicket CreateDropTableTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        string tableName = ast.leftAst!.yytext!;
        
        return new(txnState: ticket.TxnState, ticket.DatabaseName, tableName);
    }
}