
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.Time;
using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Controllers.DML;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
///
/// </summary>
internal sealed class SQLExecutorAlterIndexCreator : SQLExecutorBaseCreator
{
    internal AlterIndexTicket CreateAlterIndexTicket(HLCTimestamp hlcTimestamp, ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing alter table name");

        string tableName = ast.leftAst.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing create table fields list");

        if (ast.nodeType == NodeType.AlterTableAddIndex)
            return new(
                hlcTimestamp,
                ticket.DatabaseName,
                tableName,
                ast.rightAst.yytext!,
                ast.extendedOne!.yytext!,
                AlterIndexOperation.AddIndex
            );

        if (ast.nodeType == NodeType.AlterTableAddUniqueIndex)
            return new(
                hlcTimestamp,
                ticket.DatabaseName,
                tableName,
                ast.rightAst.yytext!,
                ast.extendedOne!.yytext!,
                AlterIndexOperation.AddUniqueIndex
            );

        if (ast.nodeType == NodeType.AlterTableAddPrimaryKey)
            return new(
                hlcTimestamp,
                ticket.DatabaseName,
                tableName,
                CamusDBConfig.PrimaryKeyInternalName,
                ast.rightAst.yytext!,                
                AlterIndexOperation.AddPrimaryKey
            );

        if (ast.nodeType == NodeType.AlterTableDropIndex)
            return new(
                hlcTimestamp,
                ticket.DatabaseName,
                tableName,
                ast.rightAst.yytext!,
                "",
                AlterIndexOperation.DropIndex
            );

        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid alter index operation: {ast.nodeType}");
    }
}