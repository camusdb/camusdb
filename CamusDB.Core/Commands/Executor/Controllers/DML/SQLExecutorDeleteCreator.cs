
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

internal sealed class SQLExecutorDeletereator : SQLExecutorBaseCreator
{
    internal DeleteTicket CreateDeleteTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        string tableName = ast.leftAst!.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing delete conditions");

        return new(
            txnState: ticket.TxnState,
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            where: ast.rightAst,
            filters: null
        );
    }    
}