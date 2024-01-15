
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
    internal async Task<CreateTableTicket> CreateCreateTableTicket(
        CommandExecutor commandExecutor,
        ExecuteSQLTicket ticket,
        NodeAst ast
    )
    {
        if (ast.leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing table name");

        string tableName = ast.leftAst.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing create table fields list");

        List<ColumnInfo> columnInfos = new();
        List<ConstraintInfo> constraintInfos = new();

        GetCreateTableFieldList(ast.rightAst, columnInfos);

        GetCreateTableConstraintList(ast.extendedOne, constraintInfos);

        GetCreateTableConstraintFromFieldList(ast.rightAst, constraintInfos);

        return new(
            txnId: await commandExecutor.NextTxnId(),
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            columns: columnInfos.ToArray(),
            constraints: constraintInfos.ToArray(),
            ifNotExists: ast.nodeType == NodeType.CreateTableIfNotExists
        );
    }

    private static void GetCreateTableConstraintList(NodeAst? constraintList, List<ConstraintInfo> constraintInfos)
    {
        if (constraintList is null)
            return;

        if (constraintList.nodeType == NodeType.CreateTableConstraintPrimaryKey)
        {
            List<ColumnIndexInfo> columnIndexInfos = new();
            GetIndexColumnList(constraintList.leftAst, columnIndexInfos);
            ConstraintInfo constraintInfo = new(ConstraintType.PrimaryKey, CamusDBConfig.PrimaryKeyInternalName, columnIndexInfos.ToArray());
            constraintInfos.Add(constraintInfo);
            return;
        }

        if (constraintList.nodeType == NodeType.CreateTableConstraintPrimaryKey)
        {
            List<ColumnIndexInfo> columnIndexInfos = new();
            GetIndexColumnList(constraintList.leftAst, columnIndexInfos);
            ConstraintInfo constraintInfo = new(ConstraintType.PrimaryKey, CamusDBConfig.PrimaryKeyInternalName, columnIndexInfos.ToArray());
            constraintInfos.Add(constraintInfo);
            return;
        }

        if (constraintList.nodeType == NodeType.CreateTableConstraintList)
        {
            if (constraintList.leftAst != null)
                GetCreateTableConstraintList(constraintList.leftAst, constraintInfos);

            if (constraintList.rightAst != null)
                GetCreateTableConstraintList(constraintList.rightAst, constraintInfos);

            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid create table field list");
    }

    /// <summary>
    /// Returns the list of 
    /// </summary>
    /// <param name="leftAst"></param>
    /// <returns></returns>
    private static void GetIndexColumnList(NodeAst? leftAst, List<ColumnIndexInfo> indexColumns)
    {
        if (leftAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid field list in constraint");

        if (leftAst.nodeType == NodeType.Identifier)
        {
            indexColumns.Add(new ColumnIndexInfo(leftAst.yytext!, OrderType.Ascending));
            return;
        }

        if (leftAst.nodeType == NodeType.IndexIdentifierAsc)
        {
            indexColumns.Add(new ColumnIndexInfo(leftAst.yytext!, OrderType.Ascending));
            return;
        }

        if (leftAst.nodeType == NodeType.IndexIdentifierDesc)
        {
            indexColumns.Add(new ColumnIndexInfo(leftAst.yytext!, OrderType.Descending));
            return;
        }

        if (leftAst.nodeType == NodeType.IndexIdentifierList)
        {
            if (leftAst.leftAst != null)
                GetIndexColumnList(leftAst.leftAst, indexColumns);

            if (leftAst.rightAst != null)
                GetIndexColumnList(leftAst.rightAst, indexColumns);
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid index column field list");
    }

    /// <summary>
    /// Returns the list of fields that make up the table
    /// </summary>
    /// <param name="fieldList"></param>
    /// <param name="allFieldLists"></param>
    /// <exception cref="CamusDBException"></exception>
    private static void GetCreateTableFieldList(NodeAst fieldList, List<ColumnInfo> allFieldLists)
    {
        if (fieldList.nodeType == NodeType.CreateTableItem)
        {
            if (fieldList.leftAst is null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing field name");

            if (fieldList.rightAst is null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing field type");

            if (fieldList.extendedOne != null)
            {
                List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes = new();

                GetCreateTableItemConstraintList(fieldList.extendedOne, constraintTypes);

                allFieldLists.Add(
                    new ColumnInfo(
                        name: fieldList.leftAst.yytext! ?? "",
                        type: GetColumnType(fieldList.rightAst),
                        notNull: constraintTypes.Any(x => x.type == ColumnConstraintType.NotNull),
                        defaultValue: GetDefaultValue(constraintTypes)
                    )
                );

                return;
            }

            allFieldLists.Add(new ColumnInfo(fieldList.leftAst.yytext! ?? "", GetColumnType(fieldList.rightAst)));
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

    /// <summary>
    /// Returns the default value as ColumnValue
    /// </summary>
    /// <param name="constraintTypes"></param>
    /// <returns></returns>
    private static ColumnValue? GetDefaultValue(List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes)
    {
        foreach ((ColumnConstraintType type, ColumnValue? value) in constraintTypes)
        {
            if (type == ColumnConstraintType.Default)
                return value;
        }

        return null;
    }

    /// <summary>
    /// Get the column type as a ColumnType enum value
    /// </summary>
    /// <param name="nodeAst"></param>
    /// <returns></returns>
    /// <exception cref="CamusDBException"></exception>
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

    /// <summary>
    /// Creates a list of table field constraints
    /// </summary>
    /// <param name="constraintsList"></param>
    /// <param name="constraintTypes"></param>
    /// <exception cref="CamusDBException"></exception>
    private static void GetCreateTableItemConstraintList(NodeAst constraintsList, List<(ColumnConstraintType, ColumnValue?)> constraintTypes)
    {
        if (constraintsList.nodeType == NodeType.ConstraintNotNull)
        {
            constraintTypes.Add((ColumnConstraintType.NotNull, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintNull)
        {
            constraintTypes.Add((ColumnConstraintType.Null, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintPrimaryKey)
        {
            constraintTypes.Add((ColumnConstraintType.PrimaryKey, null));
            return;
        }

        if (constraintsList.nodeType == NodeType.ConstraintDefault)
        {
            constraintTypes.Add((ColumnConstraintType.Default, SqlExecutor.EvalExpr(constraintsList.leftAst!, new(), null)));
            return;
        }

        if (constraintsList.nodeType == NodeType.CreateTableFieldConstraintList)
        {
            if (constraintsList.leftAst != null)
                GetCreateTableItemConstraintList(constraintsList.leftAst, constraintTypes);

            if (constraintsList.rightAst != null)
                GetCreateTableItemConstraintList(constraintsList.rightAst, constraintTypes);

            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid constraint type found: " + constraintsList.nodeType);
    }

    private static void GetCreateTableConstraintFromFieldList(NodeAst fieldList, List<ConstraintInfo> constraintInfos)
    {
        if (fieldList.nodeType == NodeType.CreateTableItem)
        {
            if (fieldList.leftAst is null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing field name");

            if (fieldList.rightAst is null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing field type");

            if (fieldList.extendedOne != null)
            {
                List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes = new();

                GetCreateTableItemConstraintList(fieldList.extendedOne, constraintTypes);

                foreach ((ColumnConstraintType type, ColumnValue? _) in constraintTypes)
                {
                    if (type == ColumnConstraintType.PrimaryKey)
                    {
                        ConstraintInfo constraintInfo = new(
                            ConstraintType.PrimaryKey,
                            CamusDBConfig.PrimaryKeyInternalName,
                            new ColumnIndexInfo[] { new(name: fieldList.leftAst.yytext! ?? "", OrderType.Ascending) }
                        );

                        constraintInfos.Add(constraintInfo);
                    }
                }
            }

            return;
        }

        if (fieldList.nodeType == NodeType.CreateTableItemList)
        {
            if (fieldList.leftAst != null)
                GetCreateTableConstraintFromFieldList(fieldList.leftAst, constraintInfos);

            if (fieldList.rightAst != null)
                GetCreateTableConstraintFromFieldList(fieldList.rightAst, constraintInfos);

            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid create table field list");
    }
}