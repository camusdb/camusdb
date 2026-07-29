
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

internal static class ColumnValueAstBuilder
{
    public static NodeAst FromColumnValue(ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Integer64 => new NodeAst(
                NodeType.Integer,
                leftAst: null,
                rightAst: null,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: value.LongValue.ToString()),
            ColumnType.Float64 => new NodeAst(
                NodeType.Float,
                leftAst: null,
                rightAst: null,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: value.FloatValue.ToString()),
            ColumnType.Bool => value.BoolValue ? NodeAst.True : NodeAst.False,
            ColumnType.String => new NodeAst(
                NodeType.String,
                leftAst: null,
                rightAst: null,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                // Must go through the shared quoter: yytext is a source *token*, not a value, and a
                // hand-wrapped one silently corrupts anything the decoder treats as special (a
                // doubled delimiter collapses to one character on the way back).
                yytext: SqlStringLiteral.Quote(value.StrValue ?? "")),
            ColumnType.Id => new NodeAst(
                NodeType.ObjectIdLiteral,
                leftAst: null,
                rightAst: null,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: value.StrValue),
            // No dedicated Uuid literal token exists; re-express the value as CAST('<canonical>' AS
            // uuid). ExprCast is already handled everywhere in the expression pipeline, so this
            // substituted subtree evaluates back to the same Uuid without new node-type plumbing.
            ColumnType.Uuid => new NodeAst(
                NodeType.ExprCast,
                leftAst: new NodeAst(
                    NodeType.String,
                    leftAst: null,
                    rightAst: null,
                    extendedOne: null,
                    extendedTwo: null,
                    extendedThree: null,
                    extendedFour: null,
                    extendedFive: null,
                    yytext: SqlStringLiteral.Quote(value.ToGuid().ToString("D"))),
                rightAst: NodeAst.TypeUuid,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: null),
            ColumnType.Null => NodeAst.Null,
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Unsupported scalar subquery result type: " + value.Type),
        };
    }
}
