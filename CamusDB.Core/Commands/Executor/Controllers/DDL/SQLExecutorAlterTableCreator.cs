
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Util.Time;
using CamusDB.Core.CommandsExecutor.Controllers.DML;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
///
/// </summary>
internal sealed class SQLExecutorAlterTableCreator : SQLExecutorBaseCreator
{
    internal AlterTableTicket CreateAlterTableTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        string tableName = ast.leftAst!.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing column name");

        if (ast.nodeType == NodeType.AlterTableAddColumn)
            return new(
                ticket.TxnState,
                ticket.DatabaseName,
                tableName,
                AlterTableOperation.AddColumn,
                new ColumnInfo(ast.rightAst!.yytext!, GetColumnType(ast.extendedOne!))
            );

        return new(
            ticket.TxnState,
            ticket.DatabaseName,
            tableName,
            AlterTableOperation.DropColumn,
            new ColumnInfo(ast.rightAst!.yytext!, ColumnType.Null)
        );
    }

    private static ColumnType GetColumnType(NodeAst nodeAst)
    {
        if (nodeAst.nodeType == NodeType.TypeInteger64)
            return ColumnType.Integer64;

        if (nodeAst.nodeType == NodeType.TypeString)
            return ColumnType.String;

        if (nodeAst.nodeType == NodeType.TypeObjectId)
            return ColumnType.Id;

        if (nodeAst.nodeType == NodeType.TypeBool)
            return ColumnType.Bool;

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown field type: " + nodeAst.nodeType);
    }
}