
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.Time;
using CamusDB.Core.CommandsExecutor.Controllers.DML;

namespace CamusDB.Core.Commands.Executor.Controllers.DDL;

/// <summary>
///
/// </summary>
internal sealed class SQLExecutorAlterIndexCreator : SQLExecutorBaseCreator
{
    internal AlterIndexTicket CreateAlterIndexTicket(HLCTimestamp hlcTimestamp, ExecuteSQLTicket ticket, NodeAst ast)
    {
        string tableName = ast.leftAst!.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing create table fields list");

        return new(
            hlcTimestamp,
            ticket.DatabaseName,
            tableName,
            ast.rightAst.yytext!,
            ast.extendedOne!.yytext!,
            AlterIndexOperation.AddIndex            
        );
    }
}