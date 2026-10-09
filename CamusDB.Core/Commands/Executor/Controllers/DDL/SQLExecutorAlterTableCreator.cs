
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

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

internal sealed class SQLExecutorAlterTableCreator : SQLExecutorBaseCreator
{
    internal AlterTableTicket CreateAlterTableTicket(ExecuteSQLTicket ticket, NodeAst ast)
    {
        string tableName = ast.leftAst!.yytext!;

        if (ast.rightAst is null)
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Missing column name");

        if (ast.nodeType == NodeType.AlterTableAddColumn)
            return CreateAddColumnTicket(ticket, tableName, ast);

        if (ast.nodeType == NodeType.AlterTableRenameColumn)
        {
            string oldColumnName = ast.rightAst!.yytext!;
            string newColumnName = ast.extendedOne!.yytext!;

            return new(
                ticket.DatabaseName,
                tableName,
                AlterTableOperation.RenameColumn,
                new ColumnInfo(oldColumnName, ColumnType.Null),
                newName: newColumnName
            );
        }

        return new(
            ticket.DatabaseName,
            tableName,
            AlterTableOperation.DropColumn,
            new ColumnInfo(ast.rightAst!.yytext!, ColumnType.Null)
        );
    }

    /// <summary>
    /// Builds the ticket for <c>ALTER TABLE … ADD COLUMN</c>. Every column constraint that
    /// <c>CREATE TABLE</c> accepts is either carried onto the new <see cref="ColumnInfo"/> or refused
    /// here. None is dropped without an error.
    /// </summary>
    /// <remarks>
    /// <para><b>Carried:</b> a constant default, a function default, <c>DEFAULT nextval('…')</c>,
    /// <c>GENERATED … AS IDENTITY</c>, <c>NOT NULL</c> (with its constraint name),
    /// <c>COMMENT</c> and <c>STORAGE</c>. The DDL path fills each existing row from the column's
    /// default, as <c>CREATE TABLE</c> fills each inserted row.</para>
    ///
    /// <para><b>Refused:</b> <c>PRIMARY KEY</c>, <c>UNIQUE</c>, <c>CHECK</c> and <c>REFERENCES</c>.
    /// Each one is a second schema change with its own rollout over the existing rows: an index build,
    /// or a validation pass. The statement that adds the column cannot also publish that change
    /// atomically, so the user runs it as its own statement, and the error names that statement.</para>
    /// </remarks>
    private static AlterTableTicket CreateAddColumnTicket(ExecuteSQLTicket ticket, string tableName, NodeAst ast)
    {
        string columnName = ast.rightAst!.yytext!;
        (ColumnType colType, int? maxLen, ColumnType? elemType) = GetColumnMeta(ast.extendedOne!);

        if (ast.extendedTwo is null)
            return new(
                ticket.DatabaseName,
                tableName,
                AlterTableOperation.AddColumn,
                new ColumnInfo(columnName, colType, maxLength: maxLen, arrayElementType: elemType));

        List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes = new();
        GetColumnConstraintList(ast.extendedTwo, constraintTypes);

        RefuseConstraintsAddColumnCannotApply(constraintTypes, tableName, columnName);

        ColumnValue? defaultValue = GetDefaultFromConstraints(constraintTypes);
        if (defaultValue is not null)
            defaultValue = CastScalarFunctions.CoerceToColumnType(defaultValue, colType);

        string? defaultFunction = GetDefaultFunctionFromConstraints(constraintTypes);
        if (defaultFunction is not null)
            ValidateDefaultFunctionType(defaultFunction, colType, columnName);

        ColumnIdentityKind? identity = GetIdentityFromConstraints(constraintTypes);
        string? defaultSequenceName = GetDefaultSequenceNameFromConstraints(constraintTypes);

        if (identity is not null || defaultSequenceName is not null)
            RequireIdentityColumnIsInteger(colType, columnName);

        return new(
            ticket.DatabaseName,
            tableName,
            AlterTableOperation.AddColumn,
            new ColumnInfo(
                name: columnName,
                type: colType,
                // An identity column is NOT NULL, as it is in CREATE TABLE: its counter never
                // produces a NULL.
                notNull: identity is not null || constraintTypes.Any(x => x.type == ColumnConstraintType.NotNull),
                defaultValue: defaultValue,
                maxLength: maxLen,
                arrayElementType: elemType,
                defaultFunction: defaultFunction,
                notNullConstraintName: GetNotNullConstraintNameFromConstraints(constraintTypes),
                comment: GetCommentFromConstraints(constraintTypes),
                storage: GetStorageFromConstraints(constraintTypes, colType, columnName),
                identityAlways: identity == ColumnIdentityKind.Always,
                identity: identity,
                defaultSequenceName: defaultSequenceName));
    }

    /// <summary>
    /// Refuses a column constraint that needs its own rollout over the existing rows, with
    /// <see cref="CamusDBErrorCodes.FeatureNotSupported"/> and the statement to run instead.
    /// </summary>
    private static void RefuseConstraintsAddColumnCannotApply(
        List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes, string tableName, string columnName)
    {
        foreach ((ColumnConstraintType type, _) in constraintTypes)
        {
            string? instead = type switch
            {
                ColumnConstraintType.PrimaryKey =>
                    $"declare it as PRIMARY KEY. Add the column first, then change the key with ALTER TABLE {tableName} " +
                    $"DROP PRIMARY KEY and ALTER TABLE {tableName} ADD PRIMARY KEY ({columnName})",
                ColumnConstraintType.Unique =>
                    $"declare it UNIQUE. Add the column first, then use CREATE UNIQUE INDEX <name> ON {tableName} ({columnName})",
                ColumnConstraintType.Check =>
                    $"declare a CHECK constraint on it. Add the column first, then use ALTER TABLE {tableName} " +
                    "ADD CONSTRAINT <name> CHECK (...)",
                ColumnConstraintType.ForeignKey =>
                    $"declare a foreign key on it. Add the column first, then use ALTER TABLE {tableName} " +
                    $"ADD FOREIGN KEY ({columnName}) REFERENCES ...",
                _ => null
            };

            if (instead is not null)
                throw new CamusDBException(
                    CamusDBErrorCodes.FeatureNotSupported,
                    $"ALTER TABLE ... ADD COLUMN '{columnName}' cannot {instead}.");
        }
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
            case NodeType.TypeNumeric:   return (ColumnType.Numeric,   null, null);
            case NodeType.TypeString:    return (ColumnType.String,    null, null);

            case NodeType.TypeStringSized:
                if (!int.TryParse(nodeAst.yytext, out int n) || n <= 0)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        $"Invalid string size '{nodeAst.yytext}': must be a positive integer");
                return (ColumnType.String, n, null);

            case NodeType.TypeBytesSized:
                if (!int.TryParse(nodeAst.yytext, out int b) || b <= 0)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        $"Invalid bytes size '{nodeAst.yytext}': must be a positive integer");
                return (ColumnType.Bytes, b, null);

            case NodeType.TypeArray:
            {
                NodeAst elemNode = nodeAst.leftAst
                    ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Array type node missing element type");
                if (elemNode.nodeType == NodeType.TypeArray)
                    throw new CamusDBException(CamusDBErrorCodes.InvalidInput,
                        "Nested arrays are not supported: array(array(...)) is invalid");
                (ColumnType elemType, _, _) = GetColumnMeta(elemNode);
                // The array row and key codecs have no NUMERIC element form yet.
                if (elemType == ColumnType.Numeric)
                    throw new CamusDBException(CamusDBErrorCodes.FeatureNotSupported,
                        "array(numeric) is not supported");
                return (ColumnType.Array, null, elemType);
            }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, "Unknown field type: " + nodeAst.nodeType);
        }
    }
}
