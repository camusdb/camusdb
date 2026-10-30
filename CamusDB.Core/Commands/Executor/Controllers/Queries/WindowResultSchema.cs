/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The columns of a windowed SELECT's derived table: the inner query's columns, with the window
/// inputs marked <see cref="DerivedColumnSchema.Hidden"/>, followed by one hidden result column per
/// window call (<see cref="WindowFunctionCall.ResultColumn"/>).
///
/// <para><b>Result types</b> follow the function: the ranking functions are Integer64, except
/// <c>percent_rank</c> and <c>cume_dist</c>, which are Float64; the value functions take the type of
/// their value; the aggregates take the type the plain aggregate has (<c>count</c> Integer64,
/// <c>avg</c> NUMERIC over NUMERIC and Float64 otherwise, <c>sum</c>, <c>min</c> and <c>max</c> the
/// type of their argument). The type of a value comes from the inner column it reads, which the inner
/// query typed with the ordinary inference.</para>
///
/// <para>The default of <c>lag</c> and <c>lead</c> must have the type of the value, or be NULL, or both
/// must be numeric; anything else is refused here, before the statement runs, as PostgreSQL refuses a
/// default it cannot convert. Two numeric types give the result the type their sum would have
/// (<see cref="MixedNumericComparison.ArithmeticResultType"/>), so <c>lag(int_col, 1, 0.5)</c> is a
/// Float64 column, and the derived table's widening step then converts every value cell to that type.
/// A placeholder default has no type here; the window operator checks it with the same rule when the
/// statement runs (<see cref="DefaultResultType"/>).</para>
/// </summary>
internal static class WindowResultSchema
{
    public static IReadOnlyList<DerivedColumnSchema> Build(
        IReadOnlyList<DerivedColumnSchema> innerColumns,
        IReadOnlyList<WindowFunctionCall> windows)
    {
        List<DerivedColumnSchema> columns = new(innerColumns.Count + windows.Count);

        foreach (DerivedColumnSchema column in innerColumns)
            columns.Add(WindowQueryRewriter.IsHiddenColumn(column.Name) ? column with { Hidden = true } : column);

        foreach (WindowFunctionCall window in windows)
        {
            (ColumnType type, ColumnType? elementType) = ResultType(window, innerColumns);
            columns.Add(new DerivedColumnSchema(window.ResultColumn, type) { Hidden = true, ElementType = elementType });
        }

        return columns;
    }

    private static (ColumnType Type, ColumnType? ElementType) ResultType(
        WindowFunctionCall window,
        IReadOnlyList<DerivedColumnSchema> innerColumns)
    {
        switch (window.Kind)
        {
            case WindowFunctionKind.RowNumber:
            case WindowFunctionKind.Rank:
            case WindowFunctionKind.DenseRank:
            case WindowFunctionKind.Ntile:
            case WindowFunctionKind.Count:
                return (ColumnType.Integer64, null);

            case WindowFunctionKind.PercentRank:
            case WindowFunctionKind.CumeDist:
                return (ColumnType.Float64, null);

            case WindowFunctionKind.Avg:
                return (ArgumentType(window, 0, innerColumns).Type == ColumnType.Numeric ? ColumnType.Numeric : ColumnType.Float64, null);

            case WindowFunctionKind.Lag:
            case WindowFunctionKind.Lead:
            {
                (ColumnType Type, ColumnType? ElementType) value = ArgumentType(window, 0, innerColumns);

                if (window.Arguments.Count == 3)
                    return (DefaultResultType(window.Kind, value.Type, ArgumentType(window, 2, innerColumns).Type), value.ElementType);

                return value;
            }

            default:
                return ArgumentType(window, 0, innerColumns);
        }
    }

    private static (ColumnType Type, ColumnType? ElementType) ArgumentType(
        WindowFunctionCall window,
        int index,
        IReadOnlyList<DerivedColumnSchema> innerColumns)
    {
        NodeAst argument = window.Arguments[index];

        switch (argument.nodeType)
        {
            case NodeType.Identifier:
                foreach (DerivedColumnSchema column in innerColumns)
                {
                    if (string.Equals(column.Name, argument.yytext, StringComparison.Ordinal))
                        return (column.Type, column.ElementType);
                }

                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Window input column '{argument.yytext}' is missing from the inner query");

            case NodeType.Integer:
                return (ColumnType.Integer64, null);

            case NodeType.Float:
                return (ColumnType.Float64, null);

            case NodeType.String:
                return (ColumnType.String, null);

            case NodeType.Bool:
                return (ColumnType.Bool, null);

            case NodeType.BytesLiteral:
                return (ColumnType.Bytes, null);

            default:
                // NULL, a placeholder (its type is known only at execution) and count(*).
                return (ColumnType.Null, null);
        }
    }

    /// <summary>
    /// The type of a <c>lag</c>/<c>lead</c> result from the type of its value and of its default: the
    /// value's type when the default is NULL or has that type, the arithmetic result type when both are
    /// numeric, and a refusal otherwise. One rule for both moments it is applied: here at bind time for
    /// a typed default, and in the window operator for a placeholder default, whose type is known only
    /// when the statement runs.
    /// </summary>
    internal static ColumnType DefaultResultType(WindowFunctionKind kind, ColumnType valueType, ColumnType defaultType)
    {
        if (defaultType == ColumnType.Null || valueType == ColumnType.Null || defaultType == valueType)
            return valueType;

        if (MixedNumericComparison.ArithmeticResultType(valueType, defaultType) is { } common)
            return common;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"The default of {(kind == WindowFunctionKind.Lag ? "lag" : "lead")} has type {defaultType}, "
            + $"which does not match the type {valueType} of its value");
    }
}
