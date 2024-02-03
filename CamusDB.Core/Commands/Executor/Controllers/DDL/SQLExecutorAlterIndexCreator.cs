
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

        if (ast.nodeType != NodeType.AlterTableDropPrimaryKey && ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing index name");

        if (ast.nodeType == NodeType.AlterTableAddIndex)
        {
            List<ColumnIndexInfo> indexColumns = new();
            GetColumns(ast.extendedOne, indexColumns);

            return new(
                hlcTimestamp,
                ticket.DatabaseName,
                tableName,
                ast.rightAst!.yytext!,
                indexColumns.ToArray(),
                AlterIndexOperation.AddIndex
            );
        }

        if (ast.nodeType == NodeType.AlterTableAddUniqueIndex)
        {
            List<ColumnIndexInfo> indexColumns = new();
            GetColumns(ast.extendedOne, indexColumns);

            return new(
                hlcTimestamp,
                ticket.DatabaseName,
                tableName,
                ast.rightAst!.yytext!,
                indexColumns.ToArray(),
                AlterIndexOperation.AddUniqueIndex
            );
        }

        if (ast.nodeType == NodeType.AlterTableAddPrimaryKey)
        {
            List<ColumnIndexInfo> indexColumns = new();
            GetColumns(ast.extendedOne, indexColumns);

            return new(
                hlcTimestamp,
                ticket.DatabaseName,
                tableName,
                CamusDBConfig.PrimaryKeyInternalName,
                indexColumns.ToArray(),
                AlterIndexOperation.AddPrimaryKey
            );
        }

        if (ast.nodeType == NodeType.AlterTableDropIndex)
            return new(
                hlcTimestamp,
                ticket.DatabaseName,
                tableName,
                ast.rightAst!.yytext!,
                Array.Empty<ColumnIndexInfo>(),
                AlterIndexOperation.DropIndex
            );

        if (ast.nodeType == NodeType.AlterTableDropPrimaryKey)
            return new(
                hlcTimestamp,
                ticket.DatabaseName,
                tableName,
                CamusDBConfig.PrimaryKeyInternalName,
                Array.Empty<ColumnIndexInfo>(),
                AlterIndexOperation.DropIndex
            );

        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid alter index operation: {ast.nodeType}");
    }

    private static void GetColumns(NodeAst? nodeAst, List<ColumnIndexInfo> indexColumns)
    {
        if (nodeAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid alter index operation: No columns");

        if (nodeAst.nodeType == NodeType.IndexIdentifierList)
        {
            if (nodeAst.leftAst != null)
                GetColumns(nodeAst.leftAst, indexColumns);

            if (nodeAst.rightAst != null)
                GetColumns(nodeAst.rightAst, indexColumns);
            
            return;
        }

        if (nodeAst.nodeType == NodeType.Identifier)
        {
            indexColumns.Add(new ColumnIndexInfo(nodeAst.yytext!, OrderType.Ascending));
            return;
        }

        if (nodeAst.nodeType == NodeType.IndexIdentifierAsc)
        {
            indexColumns.Add(new ColumnIndexInfo(nodeAst.leftAst!.yytext!, OrderType.Ascending));
            return;
        }

        if (nodeAst.nodeType == NodeType.IndexIdentifierDesc)
        {
            indexColumns.Add(new ColumnIndexInfo(nodeAst.leftAst!.yytext!, OrderType.Descending));
            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid alter index operation: {nodeAst.nodeType}");
    }
}