
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
/// @todo #1 Make constraint types work without O(n) lookup
/// @todo #1 Validate empty or null table name/fields here
/// </summary>
internal sealed class SQLExecutorAlterTableCreator : SQLExecutorBaseCreator
{
    internal AlterTableTicket CreateAlterTableTicket(HLCTimestamp hlcTimestamp, ExecuteSQLTicket ticket, NodeAst ast)
    {
        string tableName = ast.leftAst!.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing create table fields list");

        return new(
            hlcTimestamp,
            ticket.DatabaseName,
            tableName,
            AlterTableOperation.AddColumn,
            new ColumnInfo("x", GetColumnType(ast.extendedOne!))
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

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown field type: " + nodeAst.nodeType);
    }
}