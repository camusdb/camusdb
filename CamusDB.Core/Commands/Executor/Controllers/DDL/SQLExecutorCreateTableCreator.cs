
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
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;

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
        List<CheckConstraintInfo> checkConstraintInfos = [];

        GetCreateTableFieldList(ast.rightAst, columnInfos);

        GetCreateTableConstraintList(ast.extendedOne, constraintInfos);

        GetCreateTableConstraintFromFieldList(ast.rightAst, constraintInfos);

        CollectCheckConstraints(ast.rightAst, tableName, columnInfos, checkConstraintInfos);

        // Constraint names must be unique per table so DROP CONSTRAINT resolves unambiguously.
        HashSet<string> seenCheckNames = new(StringComparer.OrdinalIgnoreCase);
        foreach (CheckConstraintInfo check in checkConstraintInfos)
        {
            if (!seenCheckNames.Add(check.Name))
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    $"Duplicate constraint name '{check.Name}'");
        }

        // Reject array columns in any PK or index — arrays are not indexable.
        Dictionary<string, ColumnType> colTypeByName = columnInfos.ToDictionary(c => c.Name, c => c.Type, StringComparer.Ordinal);
        foreach (ConstraintInfo constraint in constraintInfos)
        {
            foreach (ColumnIndexInfo col in constraint.Columns)
            {
                if (colTypeByName.TryGetValue(col.Name, out ColumnType colType) && colType == ColumnType.Array)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        $"Column '{col.Name}' has type Array which cannot be used in an index or primary key");
            }
        }

        return new(
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            columns: [.. columnInfos],
            constraints: [.. constraintInfos],
            ifNotExists: ast.nodeType == NodeType.CreateTableIfNotExists,
            checkConstraints: [.. checkConstraintInfos]
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
            constraintInfos.Add(new(ConstraintType.IndexMulti, indexName, [.. columnIndexInfos], GetIncludeColumnList(constraintList.extendedOne)));
            return;
        }

        if (constraintList.nodeType == NodeType.CreateTableConstraintUniqueIndex)
        {
            string indexName = constraintList.leftAst?.yytext
                ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing index name");
            List<ColumnIndexInfo> columnIndexInfos = [];
            GetIndexColumnList(constraintList.rightAst, columnIndexInfos);
            constraintInfos.Add(new(ConstraintType.IndexUnique, indexName, [.. columnIndexInfos], GetIncludeColumnList(constraintList.extendedOne)));
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

        // CHECK constraints are collected separately in CollectCheckConstraints.
        if (constraintList.nodeType == NodeType.CreateTableConstraintCheck)
            return;

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid create table field list");
    }

    /// <summary>
    /// Returns the list of
    /// </summary>
    /// <param name="ast"></param>
    /// <returns></returns>
    /// <summary>
    /// Walks the optional INCLUDE (...) list of an inline covering-index constraint into a bare name
    /// array. Payload columns are unordered, so a direction on an included column is a parse-time error.
    /// Returns an empty array when the clause is absent.
    /// </summary>
    private static string[] GetIncludeColumnList(NodeAst? ast)
    {
        if (ast is null)
            return [];

        List<string> names = [];
        CollectIncludeNames(ast, names);
        return [.. names];
    }

    private static void CollectIncludeNames(NodeAst ast, List<string> names)
    {
        if (ast.nodeType == NodeType.IndexIdentifierList)
        {
            if (ast.leftAst != null)
                CollectIncludeNames(ast.leftAst, names);
            if (ast.rightAst != null)
                CollectIncludeNames(ast.rightAst, names);
            return;
        }

        if (ast.nodeType == NodeType.Identifier)
        {
            names.Add(ast.yytext!);
            return;
        }

        if (ast.nodeType is NodeType.IndexIdentifierAsc or NodeType.IndexIdentifierDesc)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "INCLUDE columns cannot specify ASC/DESC (payload columns are unordered)");

        throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid INCLUDE column list: {ast.nodeType}");
    }

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
            // The column identifier is the child node (`any_identifier TASC`); this node itself
            // carries no text.
            indexColumns.Add(new ColumnIndexInfo(ast.leftAst!.yytext!, OrderType.Ascending));
            return;
        }

        if (ast.nodeType == NodeType.IndexIdentifierDesc)
        {
            indexColumns.Add(new ColumnIndexInfo(ast.leftAst!.yytext!, OrderType.Descending));
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

            (ColumnType colType, int? maxLen, ColumnType? elemType) = GetColumnMeta(fieldList.rightAst);

            if (fieldList.extendedOne != null)
            {
                List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes = new();
                GetColumnConstraintList(fieldList.extendedOne, constraintTypes);

                ColumnValue? defaultValue = GetDefaultFromConstraints(constraintTypes);
                if (defaultValue is not null)
                    defaultValue = CastScalarFunctions.CoerceToColumnType(defaultValue, colType);

                string columnName = fieldList.leftAst.yytext! ?? "";
                string? defaultFunction = GetDefaultFunctionFromConstraints(constraintTypes);
                if (defaultFunction is not null)
                    ValidateDefaultFunctionType(defaultFunction, colType, columnName);

                string? notNullConstraintName = GetNotNullConstraintNameFromConstraints(constraintTypes);

                allFieldLists.Add(
                    new ColumnInfo(
                        name: columnName,
                        type: colType,
                        notNull: constraintTypes.Any(x => x.type == ColumnConstraintType.NotNull),
                        defaultValue: defaultValue,
                        maxLength: maxLen,
                        arrayElementType: elemType,
                        defaultFunction: defaultFunction,
                        notNullConstraintName: notNullConstraintName
                    )
                );
                return;
            }

            allFieldLists.Add(new ColumnInfo(
                name: fieldList.leftAst.yytext! ?? "",
                type: colType,
                maxLength: maxLen,
                arrayElementType: elemType));
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
            || fieldList.nodeType == NodeType.CreateTableConstraintUniqueIndex
            || fieldList.nodeType == NodeType.CreateTableConstraintCheck)
            return;

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid create table field list");
    }

    // Returns (ColumnType, MaxLength, ArrayElementType) from a field_type AST node.
    private static (ColumnType type, int? maxLength, ColumnType? arrayElementType) GetColumnMeta(NodeAst nodeAst)
    {
        switch (nodeAst.nodeType)
        {
            case NodeType.TypeInteger64: return (ColumnType.Integer64, null, null);
            case NodeType.TypeFloat64:   return (ColumnType.Float64,   null, null);
            case NodeType.TypeFloat32:   return (ColumnType.Float32,   null, null);
            case NodeType.TypeObjectId:  return (ColumnType.Id,        null, null);
            case NodeType.TypeBool:      return (ColumnType.Bool,      null, null);
            case NodeType.TypeDate:      return (ColumnType.Date,      null, null);
            case NodeType.TypeDateTime:  return (ColumnType.DateTime,  null, null);
            case NodeType.TypeBytes:     return (ColumnType.Bytes,     null, null);
            case NodeType.TypeUuid:      return (ColumnType.Uuid,      null, null);
            case NodeType.TypeString:    return (ColumnType.String,    null, null);

            case NodeType.TypeStringSized:
                if (!int.TryParse(nodeAst.yytext, out int n) || n <= 0)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        $"Invalid string size '{nodeAst.yytext}': must be a positive integer");
                return (ColumnType.String, n, null);

            case NodeType.TypeArray:
            {
                NodeAst elemNode = nodeAst.leftAst
                    ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Array type node missing element type");
                if (elemNode.nodeType == NodeType.TypeArray)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        "Nested arrays are not supported: array(array(...)) is invalid");
                (ColumnType elemType, _, _) = GetColumnMeta(elemNode);
                return (ColumnType.Array, null, elemType);
            }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown field type: " + nodeAst.nodeType);
        }
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
            constraintInfos.Add(new(ConstraintType.IndexMulti, indexName, [.. columnIndexInfos], GetIncludeColumnList(fieldList.extendedOne)));
            return;
        }

        if (fieldList.nodeType == NodeType.CreateTableConstraintUniqueIndex)
        {
            string indexName = fieldList.leftAst?.yytext
                ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "Missing index name");
            List<ColumnIndexInfo> columnIndexInfos = [];
            GetIndexColumnList(fieldList.rightAst, columnIndexInfos);
            constraintInfos.Add(new(ConstraintType.IndexUnique, indexName, [.. columnIndexInfos], GetIncludeColumnList(fieldList.extendedOne)));
            return;
        }

        // CHECK constraints are collected separately in CollectCheckConstraints.
        if (fieldList.nodeType == NodeType.CreateTableConstraintCheck)
            return;

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Invalid create table field list");
    }

    /// <summary>
    /// Collects all CHECK constraints from the CREATE TABLE item list — both column-level inline
    /// checks (desugared to named constraints) and explicit table-level CHECK clauses — validates
    /// each condition (no subqueries, no aggregates, no volatile functions, all referenced column
    /// names exist on the table), and appends them to <paramref name="checkConstraints"/>.
    /// </summary>
    private static void CollectCheckConstraints(
        NodeAst fieldList,
        string tableName,
        List<ColumnInfo> columns,
        List<CheckConstraintInfo> checkConstraints)
    {
        HashSet<string> columnNames = columns.Select(c => c.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
        int autoIndex = 0;
        CollectCheckConstraintsRecursive(fieldList, tableName, columnNames, checkConstraints, ref autoIndex);
    }

    private static void CollectCheckConstraintsRecursive(
        NodeAst fieldList,
        string tableName,
        HashSet<string> columnNames,
        List<CheckConstraintInfo> checkConstraints,
        ref int autoIndex)
    {
        if (fieldList.nodeType == NodeType.CreateTableItem)
        {
            string columnName = fieldList.leftAst?.yytext ?? "";

            if (fieldList.extendedOne != null)
            {
                // A column-level ConstraintCheck may be buried in a CreateTableFieldConstraintList
                // or be the single constraint node directly.
                CollectColumnLevelCheck(fieldList.extendedOne, tableName, columnName, columnNames, checkConstraints, ref autoIndex);
            }
            return;
        }

        if (fieldList.nodeType == NodeType.CreateTableItemList)
        {
            if (fieldList.leftAst != null)
                CollectCheckConstraintsRecursive(fieldList.leftAst, tableName, columnNames, checkConstraints, ref autoIndex);

            if (fieldList.rightAst != null)
                CollectCheckConstraintsRecursive(fieldList.rightAst, tableName, columnNames, checkConstraints, ref autoIndex);

            return;
        }

        if (fieldList.nodeType == NodeType.CreateTableConstraintCheck)
        {
            // Table-level CHECK: CONSTRAINT name CHECK (cond) or bare CHECK (cond).
            NodeAst condition = fieldList.leftAst!;
            string name = fieldList.yytext is { Length: > 0 } n
                ? n
                : GenerateAutoCheckName(tableName, checkConstraints, ref autoIndex);

            AddValidatedCheck(name, condition, columnNames, checkConstraints);
            return;
        }

        // Inline PK/UNIQUE/INDEX constraint nodes — skip.
    }

    private static void CollectColumnLevelCheck(
        NodeAst constraintNode,
        string tableName,
        string columnName,
        HashSet<string> columnNames,
        List<CheckConstraintInfo> checkConstraints,
        ref int autoIndex)
    {
        if (constraintNode.nodeType == NodeType.ConstraintCheck)
        {
            string name = $"{tableName}_{columnName}_check";
            // If that name is already taken (two columns with same-named checks), make it unique.
            if (checkConstraints.Any(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase)))
                name = GenerateAutoCheckName(tableName, checkConstraints, ref autoIndex);

            AddValidatedCheck(name, constraintNode.leftAst!, columnNames, checkConstraints);
            return;
        }

        if (constraintNode.nodeType == NodeType.CreateTableFieldConstraintList)
        {
            if (constraintNode.leftAst != null)
                CollectColumnLevelCheck(constraintNode.leftAst, tableName, columnName, columnNames, checkConstraints, ref autoIndex);

            if (constraintNode.rightAst != null)
                CollectColumnLevelCheck(constraintNode.rightAst, tableName, columnName, columnNames, checkConstraints, ref autoIndex);
        }
    }

    private static void AddValidatedCheck(
        string name,
        NodeAst condition,
        HashSet<string> columnNames,
        List<CheckConstraintInfo> checkConstraints)
    {
        ValidateCheckConditionAst(condition, columnNames);

        string expression = CheckConditionRenderer.Render(condition);
        HashSet<string> referenced = [];
        ExtractReferencedColumns(condition, referenced);

        checkConstraints.Add(new CheckConstraintInfo(name, expression, [.. referenced]));
    }

    private static string GenerateAutoCheckName(string tableName, List<CheckConstraintInfo> existing, ref int index)
    {
        // Skip any auto-name that a user-supplied name already claimed, so an explicit
        // `CONSTRAINT products_check1 CHECK (...)` can't collide with a generated one.
        while (true)
        {
            index++;
            string candidate = $"{tableName}_check{index}";
            if (!existing.Any(c => string.Equals(c.Name, candidate, StringComparison.OrdinalIgnoreCase)))
                return candidate;
        }
    }

    /// <summary>
    /// Validates a CHECK condition AST at DDL time. Rejects subqueries, aggregate functions,
    /// and volatile/non-deterministic functions, all of which would make the constraint
    /// non-deterministic per row or dependent on external state.
    /// </summary>
    /// <summary>Exposes CHECK condition validation for ALTER TABLE ADD CONSTRAINT CHECK.</summary>
    internal static void ValidateCheckConditionForAlter(NodeAst condition, HashSet<string> columnNames)
        => ValidateCheckConditionAst(condition, columnNames);

    /// <summary>Exposes column-reference extraction for ALTER TABLE ADD CONSTRAINT CHECK.</summary>
    internal static void ExtractReferencedColumnsPublic(NodeAst condition, HashSet<string> identifiers)
        => ExtractReferencedColumns(condition, identifiers);

    private static void ValidateCheckConditionAst(NodeAst condition, HashSet<string> columnNames)
    {
        switch (condition.nodeType)
        {
            case NodeType.ExprScalarSubquery:
            case NodeType.ExprInSubquery:
            case NodeType.ExprNotInSubquery:
            case NodeType.ExprExistsSubquery:
            case NodeType.ExprExistsCorrelated:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                    "CHECK constraint conditions cannot contain subqueries");

            case NodeType.ExprFuncCall:
            {
                string? fnName = condition.leftAst?.yytext?.ToLowerInvariant();
                if (fnName is not null && IsAggregateFunction(fnName))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        $"CHECK constraint conditions cannot contain aggregate functions ('{fnName}')");

                if (ScalarFunctionEvaluator.ContainsVolatileFunction(condition))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        "CHECK constraint conditions cannot contain volatile/non-deterministic functions");

                // Validate arguments recursively (skip leftAst which is the function name identifier).
                if (condition.rightAst != null)
                    ValidateCheckConditionAst(condition.rightAst, columnNames);
                return;
            }

            case NodeType.Identifier:
            {
                string colName = condition.yytext ?? "";
                if (!columnNames.Contains(colName))
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        $"CHECK constraint references unknown column '{colName}'");
                return;
            }

            case NodeType.ExprCast:
                // rightAst is the target type (an Identifier for named types), not a column
                // reference — validate only the value being cast.
                if (condition.leftAst != null)
                    ValidateCheckConditionAst(condition.leftAst, columnNames);
                return;

            case NodeType.ExprRegexMatch:
            case NodeType.ExprRegexMatchCi:
            case NodeType.ExprRegexNotMatch:
            case NodeType.ExprRegexNotMatchCi:
            {
                // When the pattern operand is a string literal, compile it now so a malformed
                // regex fails at CREATE/ALTER instead of at the first INSERT. A non-literal
                // pattern (a column or expression) can only be validated at row-evaluation time.
                if (condition.rightAst?.nodeType == NodeType.String)
                {
                    bool ignoreCase = condition.nodeType is NodeType.ExprRegexMatchCi or NodeType.ExprRegexNotMatchCi;
                    RegexMatcher.ValidatePattern(UnquoteStringLiteral(condition.rightAst.yytext!), ignoreCase);
                }
                // Validate the subject operand (column reference, etc.).
                if (condition.leftAst != null)
                    ValidateCheckConditionAst(condition.leftAst, columnNames);
                return;
            }

            // Literals and NULL — always valid.
            case NodeType.Integer:
            case NodeType.Float:
            case NodeType.String:
            case NodeType.Bool:
            case NodeType.Null:
            case NodeType.ObjectIdLiteral:
            case NodeType.Placeholder:
                return;

            default:
                // For all compound nodes, validate children recursively.
                if (condition.leftAst != null)
                    ValidateCheckConditionAst(condition.leftAst, columnNames);
                if (condition.rightAst != null)
                    ValidateCheckConditionAst(condition.rightAst, columnNames);
                if (condition.extendedOne != null)
                    ValidateCheckConditionAst(condition.extendedOne, columnNames);
                if (condition.extendedTwo != null)
                    ValidateCheckConditionAst(condition.extendedTwo, columnNames);
                if (condition.extendedThree != null)
                    ValidateCheckConditionAst(condition.extendedThree, columnNames);
                if (condition.extendedFour != null)
                    ValidateCheckConditionAst(condition.extendedFour, columnNames);
                if (condition.extendedFive != null)
                    ValidateCheckConditionAst(condition.extendedFive, columnNames);
                return;
        }
    }

    private static bool IsAggregateFunction(string name) => name switch
    {
        "count" or "sum" or "avg" or "min" or "max" or "count_distinct"
            or "group_concat" or "string_agg" or "array_agg" => true,
        _ => false,
    };

    /// <summary>
    /// Walks the condition AST and collects all column-reference identifiers into
    /// <paramref name="identifiers"/>. Function-name identifiers (the <c>leftAst</c> of an
    /// <c>ExprFuncCall</c> node) are excluded because they are function names, not column refs.
    /// </summary>
    private static void ExtractReferencedColumns(NodeAst condition, HashSet<string> identifiers)
    {
        if (condition.nodeType == NodeType.Identifier)
        {
            if (!string.IsNullOrEmpty(condition.yytext))
                identifiers.Add(condition.yytext);
            return;
        }

        if (condition.nodeType == NodeType.ExprFuncCall)
        {
            // leftAst is the function name — skip it; recurse only into the argument list.
            if (condition.rightAst != null)
                ExtractReferencedColumns(condition.rightAst, identifiers);
            return;
        }

        if (condition.nodeType == NodeType.ExprCast)
        {
            // rightAst is the target type (an Identifier for named types) — it is not a column
            // reference. Recurse only into the value being cast.
            if (condition.leftAst != null)
                ExtractReferencedColumns(condition.leftAst, identifiers);
            return;
        }

        if (condition.leftAst != null)   ExtractReferencedColumns(condition.leftAst, identifiers);
        if (condition.rightAst != null)  ExtractReferencedColumns(condition.rightAst, identifiers);
        if (condition.extendedOne != null)  ExtractReferencedColumns(condition.extendedOne, identifiers);
        if (condition.extendedTwo != null)  ExtractReferencedColumns(condition.extendedTwo, identifiers);
        if (condition.extendedThree != null) ExtractReferencedColumns(condition.extendedThree, identifiers);
        if (condition.extendedFour != null)  ExtractReferencedColumns(condition.extendedFour, identifiers);
        if (condition.extendedFive != null)  ExtractReferencedColumns(condition.extendedFive, identifiers);
    }
}
