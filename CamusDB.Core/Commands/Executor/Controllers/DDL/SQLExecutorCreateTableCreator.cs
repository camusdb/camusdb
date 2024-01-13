
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
/// Creates a ticket to create a table from the AST representation of a SQL statement.
/// </summary>
internal sealed class SQLExecutorCreateTableCreator : SQLExecutorBaseCreator
{
    internal CreateTableTicket CreateCreateTableTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        if (ast.leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing table name");

        string tableName = ast.leftAst.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing create table fields list");

        LinkedList<ColumnInfo> columnInfos = new();

        GetCreateTableFieldList(ast.rightAst, columnInfos);

        return new(ticket.DatabaseName, tableName, columnInfos.ToArray(), ifNotExists: ast.nodeType == NodeType.CreateTableIfNotExists);
    }

    private static void GetCreateTableFieldList(NodeAst fieldList, LinkedList<ColumnInfo> allFieldLists)
    {
        if (fieldList.nodeType == NodeType.CreateTableItem)
        {
            if (fieldList.leftAst is null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing field name");

            if (fieldList.rightAst is null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing field type");

            if (fieldList.extendedOne != null)
            {                
                LinkedList<(ColumnConstraintType type, ColumnValue? value)> constraintTypes = new();

                GetCreateTableItemConstraintList(fieldList.extendedOne, constraintTypes);

                allFieldLists.AddLast(
                    new ColumnInfo(
                        name: fieldList.leftAst.yytext! ?? "",
                        type: GetColumnType(fieldList.rightAst),
                        primary: constraintTypes.Any(x => x.type == ColumnConstraintType.PrimaryKey),
                        notNull: constraintTypes.Any(x => x.type == ColumnConstraintType.NotNull),
                        index: IndexType.None,
                        defaultValue: GetDefaultValue(constraintTypes)
                    )
                );

                return;
            }

            allFieldLists.AddLast(new ColumnInfo(fieldList.leftAst.yytext! ?? "", GetColumnType(fieldList.rightAst)));
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

    private static ColumnValue? GetDefaultValue(LinkedList<(ColumnConstraintType type, ColumnValue? value)> constraintTypes)
    {
        foreach ((ColumnConstraintType type, ColumnValue? value) in constraintTypes)
        {
            if (type == ColumnConstraintType.Default)
                return value;
        }

        return null;
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

    private static void GetCreateTableItemConstraintList(NodeAst constraintsList, LinkedList<(ColumnConstraintType, ColumnValue?)> constraintTypes)
    {
        if (constraintsList.nodeType == NodeType.ConstraintNotNull)
        {
            constraintTypes.AddLast((ColumnConstraintType.NotNull, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintNull)
        {
            constraintTypes.AddLast((ColumnConstraintType.Null, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintPrimaryKey)
        {
            constraintTypes.AddLast((ColumnConstraintType.PrimaryKey, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintDefault)
        {
            constraintTypes.AddLast((ColumnConstraintType.Default, SqlExecutor.EvalExpr(constraintsList.leftAst!, new(), null)));
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