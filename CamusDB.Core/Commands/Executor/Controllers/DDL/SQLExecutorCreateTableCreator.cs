
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
using CamusDB.Core.CommandsExecutor.Controllers.DML;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// @todo #1 Make constraint types work without O(n) lookup
/// @todo #1 Validate empty or null table name/fields here
/// </summary>
internal sealed class SQLExecutorCreateTableCreator : SQLExecutorBaseCreator
{
    internal CreateTableTicket CreateCreateTableTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        string tableName = ast.leftAst!.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing create table fields list");

        LinkedList<ColumnInfo> columnInfos = new();

        GetCreateTableFieldList(ast.rightAst, columnInfos);

        return new(ticket.DatabaseName, tableName, columnInfos.ToArray());
    }

    private static void GetCreateTableFieldList(NodeAst fieldList, LinkedList<ColumnInfo> allFieldLists)
    {
        if (fieldList.nodeType == NodeType.CreateTableItem)
        {
            if (fieldList.extendedOne != null)
            {
                LinkedList<ColumnConstraintType> constraintTypes = new();

                GetCreateTableItemConstraintList(fieldList.extendedOne, constraintTypes);

                allFieldLists.AddLast(
                    new ColumnInfo(
                        fieldList.leftAst!.yytext! ?? "",
                        GetColumnType(fieldList.rightAst!),
                        constraintTypes.Contains(ColumnConstraintType.PrimaryKey),
                        constraintTypes.Contains(ColumnConstraintType.NotNull),
                        IndexType.None,
                        null
                    )
                );

                return;
            }

            allFieldLists.AddLast(new ColumnInfo(fieldList.leftAst!.yytext! ?? "", GetColumnType(fieldList.rightAst!)));
            return;
        }

        if (fieldList.nodeType == NodeType.CreateTableItemList)
        {
            if (fieldList.leftAst != null)
                GetCreateTableFieldList(fieldList.leftAst, allFieldLists);

            if (fieldList.rightAst != null)
                GetCreateTableFieldList(fieldList.rightAst, allFieldLists);

            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid create table field list");
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

    private static void GetCreateTableItemConstraintList(NodeAst constraintsList, LinkedList<ColumnConstraintType> constraintTypes)
    {
        if (constraintsList.nodeType == NodeType.ConstraintNotNull)
        {
            constraintTypes.AddLast(ColumnConstraintType.NotNull);
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintNull)
        {
            constraintTypes.AddLast(ColumnConstraintType.Null);
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintPrimaryKey)
        {
            constraintTypes.AddLast(ColumnConstraintType.PrimaryKey);
            return;
        }

        if (constraintsList.nodeType == NodeType.CreateTableConstraintList)
        {
            if (constraintsList.leftAst != null)
                GetCreateTableItemConstraintList(constraintsList.leftAst, constraintTypes);

            if (constraintsList.rightAst != null)
                GetCreateTableItemConstraintList(constraintsList.rightAst, constraintTypes);

            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid constraint type found: " + constraintsList.nodeType);
    }
}