
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

        List<ColumnInfo> columnInfos = [];
        List<ConstraintInfo> constraintInfos = [];

        GetCreateTableFieldList(ast.rightAst, columnInfos);

        GetCreateTableConstraintList(ast.extendedOne, constraintInfos);

        GetCreateTableConstraintFromFieldList(ast.rightAst, constraintInfos);

        return new(
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            columns: [.. columnInfos],
            constraints: [.. constraintInfos],
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

        if (constraintList.nodeType == NodeType.CreateTableConstraintMultiIndex)
        {
            string indexName = constraintList.leftAst?.yytext
                ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing index name");
            List<ColumnIndexInfo> columnIndexInfos = [];
            GetIndexColumnList(constraintList.rightAst, columnIndexInfos);
            constraintInfos.Add(new(ConstraintType.IndexMulti, indexName, [.. columnIndexInfos]));
            return;
        }

        if (constraintList.nodeType == NodeType.CreateTableConstraintUniqueIndex)
        {
            string indexName = constraintList.leftAst?.yytext
                ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing index name");
            List<ColumnIndexInfo> columnIndexInfos = [];
            GetIndexColumnList(constraintList.rightAst, columnIndexInfos);
            constraintInfos.Add(new(ConstraintType.IndexUnique, indexName, [.. columnIndexInfos]));
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
    /// <param name="ast"></param>
    /// <returns></returns>
    private static void GetIndexColumnList(NodeAst? ast, List<ColumnIndexInfo> indexColumns)
    {
        if (ast is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid field list in constraint");

        if (ast.nodeType == NodeType.Identifier)
        {
            indexColumns.Add(new ColumnIndexInfo(ast.yytext!, OrderType.Ascending));
            return;
        }

        if (ast.nodeType == NodeType.IndexIdentifierAsc)
        {
            indexColumns.Add(new ColumnIndexInfo(ast.yytext!, OrderType.Ascending));
            return;
        }

        if (ast.nodeType == NodeType.IndexIdentifierDesc)
        {
            indexColumns.Add(new ColumnIndexInfo(ast.yytext!, OrderType.Descending));
            return;
        }

        if (ast.nodeType == NodeType.IndexIdentifierList)
        {
            if (ast.leftAst != null)
                GetIndexColumnList(ast.leftAst, indexColumns);

            if (ast.rightAst != null)
                GetIndexColumnList(ast.rightAst, indexColumns);

            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid index column field list: " + ast.nodeType);
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

                GetColumnConstraintList(fieldList.extendedOne, constraintTypes);

                allFieldLists.Add(
                    new ColumnInfo(
                        name: fieldList.leftAst.yytext! ?? "",
                        type: GetColumnType(fieldList.rightAst),
                        notNull: constraintTypes.Any(x => x.type == ColumnConstraintType.NotNull),
                        defaultValue: GetDefaultFromConstraints(constraintTypes)
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

        if (fieldList.nodeType == NodeType.CreateTableConstraintPrimaryKey
            || fieldList.nodeType == NodeType.CreateTableConstraintMultiIndex
            || fieldList.nodeType == NodeType.CreateTableConstraintUniqueIndex)
            return;

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid create table field list");
    }

    /// <summary>
    /// Returns the default value as ColumnValue
    /// </summary>
    /// <param name="constraintTypes"></param>
    /// <returns></returns>
    private static ColumnType GetColumnType(NodeAst nodeAst)
    {
        if (nodeAst.nodeType == NodeType.TypeInteger64)
            return ColumnType.Integer64;

        if (nodeAst.nodeType == NodeType.TypeFloat64)
            return ColumnType.Float64;

        if (nodeAst.nodeType == NodeType.TypeString)
            return ColumnType.String;

        if (nodeAst.nodeType == NodeType.TypeObjectId)
            return ColumnType.Id;

        if (nodeAst.nodeType == NodeType.TypeBool)
            return ColumnType.Bool;

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown field type: " + nodeAst.nodeType);
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

                GetColumnConstraintList(fieldList.extendedOne, constraintTypes);

                foreach ((ColumnConstraintType type, ColumnValue? _) in constraintTypes)
                {
                    if (type == ColumnConstraintType.PrimaryKey)
                    {
                        ConstraintInfo constraintInfo = new(
                            ConstraintType.PrimaryKey,
                            CamusDBConfig.PrimaryKeyInternalName,
                            [new(name: fieldList.leftAst.yytext! ?? "", OrderType.Ascending)]
                        );

                        constraintInfos.Add(constraintInfo);
                    }

                    if (type == ColumnConstraintType.Unique)
                    {
                        ConstraintInfo constraintInfo = new(
                            ConstraintType.IndexUnique,
                            fieldList.leftAst.yytext! ?? "",
                            [new(name: fieldList.leftAst.yytext! ?? "", OrderType.Ascending)]
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

        if (fieldList.nodeType == NodeType.CreateTableConstraintPrimaryKey)
        {
            List<ColumnIndexInfo> columnIndexInfos = new();
            GetIndexColumnList(fieldList.leftAst, columnIndexInfos);
            constraintInfos.Add(new(ConstraintType.PrimaryKey, CamusDBConfig.PrimaryKeyInternalName, columnIndexInfos.ToArray()));
            return;
        }

        if (fieldList.nodeType == NodeType.CreateTableConstraintMultiIndex)
        {
            string indexName = fieldList.leftAst?.yytext
                ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing index name");
            List<ColumnIndexInfo> columnIndexInfos = [];
            GetIndexColumnList(fieldList.rightAst, columnIndexInfos);
            constraintInfos.Add(new(ConstraintType.IndexMulti, indexName, [.. columnIndexInfos]));
            return;
        }

        if (fieldList.nodeType == NodeType.CreateTableConstraintUniqueIndex)
        {
            string indexName = fieldList.leftAst?.yytext
                ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing index name");
            List<ColumnIndexInfo> columnIndexInfos = [];
            GetIndexColumnList(fieldList.rightAst, columnIndexInfos);
            constraintInfos.Add(new(ConstraintType.IndexUnique, indexName, [.. columnIndexInfos]));
            return;
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid create table field list");
    }
}
