
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
        {
            ColumnValue? defaultValue = null;
            bool notNull = false;

            if (ast.extendedTwo is not null)
            {
                List<(ColumnConstraintType type, ColumnValue? value)> constraintTypes = new();
                GetColumnConstraintList(ast.extendedTwo, constraintTypes);
                defaultValue = GetDefaultFromConstraints(constraintTypes);
                notNull = constraintTypes.Any(x => x.type == ColumnConstraintType.NotNull);
            }

            (ColumnType colType, int? maxLen, ColumnType? elemType) = GetColumnMeta(ast.extendedOne!);

            if (defaultValue is not null)
                defaultValue = CastScalarFunctions.CoerceToColumnType(defaultValue, colType);

            return new(
                ticket.DatabaseName,
                tableName,
                AlterTableOperation.AddColumn,
                new ColumnInfo(ast.rightAst!.yytext!, colType, notNull, defaultValue, maxLen, elemType)
            );
        }

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
}
