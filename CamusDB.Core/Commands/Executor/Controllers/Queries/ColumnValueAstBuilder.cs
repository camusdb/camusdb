
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
            // ±infinity and NaN have no literal: they are re-expressed as CAST('Infinity' AS float64),
            // which parses back to the same value. A finite value keeps its shortest round-trip literal.
            ColumnType.Float64 when !double.IsFinite(value.FloatValue) =>
                TypedCast(FloatMath.NonFiniteText(value.FloatValue), NodeAst.TypeFloat64),
            // A Float32 value is re-expressed as CAST('<text>' AS float32), so it keeps its type; the
            // single-precision round-trip text parses back to the same float.
            ColumnType.Float32 => TypedCast(
                double.IsFinite(value.FloatValue)
                    ? ((float)value.FloatValue).ToString("R", System.Globalization.CultureInfo.InvariantCulture)
                    : FloatMath.NonFiniteText(value.FloatValue),
                NodeAst.TypeFloat32),
            ColumnType.Float64 => new NodeAst(
                NodeType.Float,
                leftAst: null,
                rightAst: null,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: value.FloatValue.ToString("R", System.Globalization.CultureInfo.InvariantCulture)),
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
            // A NUMERIC is re-expressed as its typed literal, NUMERIC '<canonical>', which the parser
            // builds as CAST('<canonical>' AS numeric). The text is exact, so the value comes back
            // unchanged; a float literal would not, since it compares as a double.
            ColumnType.Numeric => new NodeAst(
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
                    yytext: SqlStringLiteral.Quote(NumericMath.Format(value.NumericUnscaled))),
                rightAst: NodeAst.TypeNumeric,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: null),
            // A jsonb value is re-expressed as CAST('<text>' AS jsonb), the node JSONB '<text>' parses
            // to. The text is the canonical document, so the value comes back unchanged.
            ColumnType.Jsonb => new NodeAst(
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
                    yytext: SqlStringLiteral.Quote(value.JsonValue!)),
                rightAst: NodeAst.TypeJsonb,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: null),
            // A bytes value has a literal form, X'<hex>', and the comparison of two bytes values
            // orders them byte by byte. Without this arm a subquery that returns a bytes column
            // fails here, although the same comparison of two bytes literals succeeds.
            ColumnType.Bytes => new NodeAst(
                NodeType.BytesLiteral,
                leftAst: null,
                rightAst: null,
                extendedOne: null,
                extendedTwo: null,
                extendedThree: null,
                extendedFour: null,
                extendedFive: null,
                yytext: SqlStringLiteral.QuoteBytes(value.BytesValue ?? [])),
            ColumnType.Null => NodeAst.Null,
            _ => throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "Unsupported scalar subquery result type: " + value.Type),
        };
    }

    /// <summary>The node <c>CAST('&lt;text&gt;' AS &lt;type&gt;)</c>, the form of a value that has no literal token.</summary>
    private static NodeAst TypedCast(string text, NodeAst type) => new(
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
            yytext: SqlStringLiteral.Quote(text)),
        rightAst: type,
        extendedOne: null,
        extendedTwo: null,
        extendedThree: null,
        extendedFour: null,
        extendedFive: null,
        yytext: null);
}
