
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
    internal AlterIndexTicket CreateAlterIndexTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing alter table name");

        string tableName = ast.leftAst.yytext!;

        if (ast.nodeType != NodeType.AlterTableDropPrimaryKey && ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing index name");

        if (ast.nodeType is NodeType.AlterTableAddIndex or NodeType.AlterTableAddIndexIfNotExists)
        {
            List<ColumnIndexInfo> indexColumns = new();
            GetColumns(ast.extendedOne, indexColumns);

            return new(
                ticket.DatabaseName,
                tableName,
                ast.rightAst!.yytext!,
                indexColumns.ToArray(),
                AlterIndexOperation.AddIndex,
                ast.nodeType == NodeType.AlterTableAddIndexIfNotExists
            );
        }

        if (ast.nodeType is NodeType.AlterTableAddUniqueIndex or NodeType.AlterTableAddUniqueIndexIfNotExists)
        {
            List<ColumnIndexInfo> indexColumns = new();
            GetColumns(ast.extendedOne, indexColumns);

            return new(
                ticket.DatabaseName,
                tableName,
                ast.rightAst!.yytext!,
                indexColumns.ToArray(),
                AlterIndexOperation.AddUniqueIndex,
                ast.nodeType == NodeType.AlterTableAddUniqueIndexIfNotExists
            );
        }

        if (ast.nodeType == NodeType.AlterTableAddPrimaryKey)
        {
            // ADD PRIMARY KEY has no index-name token, so the grammar puts the column list in
            // rightAst (not extendedOne like the named ADD INDEX / ADD UNIQUE forms).
            List<ColumnIndexInfo> indexColumns = new();
            GetColumns(ast.rightAst, indexColumns);

            return new(
                ticket.DatabaseName,
                tableName,
                CamusDBConfig.PrimaryKeyInternalName,
                indexColumns.ToArray(),
                AlterIndexOperation.AddPrimaryKey
            );
        }

        if (ast.nodeType == NodeType.AlterTableDropIndex)
            return new(
                ticket.DatabaseName,
                tableName,
                ast.rightAst!.yytext!,
                Array.Empty<ColumnIndexInfo>(),
                AlterIndexOperation.DropIndex
            );

        if (ast.nodeType == NodeType.AlterTableDropPrimaryKey)
            return new(
                ticket.DatabaseName,
                tableName,
                CamusDBConfig.PrimaryKeyInternalName,
                Array.Empty<ColumnIndexInfo>(),
                AlterIndexOperation.DropIndex
            );

        if (ast.nodeType == NodeType.AlterTableRenameIndex)
        {
            string oldIndexName = ast.rightAst!.yytext!;
            string newIndexName = ast.extendedOne!.yytext!;

            return new(
                ticket.DatabaseName,
                tableName,
                oldIndexName,
                Array.Empty<ColumnIndexInfo>(),
                AlterIndexOperation.RenameIndex,
                newName: newIndexName
            );
        }

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
