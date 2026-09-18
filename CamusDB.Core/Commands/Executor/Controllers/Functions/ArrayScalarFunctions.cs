/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// Functions that read an <c>array(T)</c> value as a whole: <c>cardinality</c> and
/// <c>array_length</c> (its length) and <c>array_contains</c> (whether it holds a value). They
/// exist mainly so a CHECK constraint can limit an array's length or contents; a subscript can
/// only test one position.
///
/// <para>Each gives NULL for a NULL array, so a CHECK built on one accepts a NULL array — CHECK
/// fails only on FALSE. None is volatile, so all are allowed in CHECK constraints and DEFAULT
/// expressions.</para>
/// </summary>
internal static class ArrayScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "cardinality",
            MinArity = 1,
            MaxArity = 1,
            Evaluator = EvaluateCardinality,
            InferReturnType = _ => ColumnType.Integer64,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "array_length",
            MinArity = 2,
            MaxArity = 2,
            Evaluator = EvaluateArrayLength,
            InferReturnType = _ => ColumnType.Integer64,
        });

        registry.Register(new ScalarFunctionDescriptor
        {
            Name = "array_contains",
            MinArity = 2,
            MaxArity = 2,
            Evaluator = EvaluateArrayContains,
            InferReturnType = _ => ColumnType.Bool,
        });
    }

    /// <summary>
    /// The number of elements, NULL elements included, as PostgreSQL's <c>cardinality</c>: an empty
    /// array gives 0, a NULL array gives NULL.
    /// </summary>
    private static ColumnValue EvaluateCardinality(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        ColumnValue array = arguments[0];

        if (array.Type == ColumnType.Null)
            return ColumnValue.Null;

        RequireArray(calledName, 0, array);

        return new ColumnValue(ColumnType.Integer64, array.ArrayValues!.Count);
    }

    /// <summary>
    /// The length of one dimension, as PostgreSQL's <c>array_length(array, dimension)</c>. A CamusDB
    /// array has exactly one dimension, so dimension 1 gives the element count and any other
    /// dimension gives NULL. It differs from <see cref="EvaluateCardinality"/> on purpose, to match
    /// PostgreSQL: an empty array gives NULL, not 0, because it has no dimensions at all. So a
    /// CHECK on <c>array_length(tags, 1) &gt;= 1</c> accepts an empty array through UNKNOWN;
    /// <c>cardinality</c> is the function to use when an empty array must count as 0. A NULL array
    /// or a NULL dimension gives NULL; a dimension that is not an integer is an error.
    /// </summary>
    private static ColumnValue EvaluateArrayLength(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        ColumnValue array = arguments[0];
        ColumnValue dimension = arguments[1];

        if (array.Type != ColumnType.Null)
            RequireArray(calledName, 0, array);

        if (dimension.Type != ColumnType.Null)
            ScalarFunctionArguments.RequireType(calledName, 1, dimension, ColumnType.Integer64);

        if (array.Type == ColumnType.Null || dimension.Type == ColumnType.Null)
            return ColumnValue.Null;

        int count = array.ArrayValues!.Count;

        return dimension.LongValue == 1 && count > 0
            ? new ColumnValue(ColumnType.Integer64, count)
            : ColumnValue.Null;
    }

    /// <summary>
    /// Whether <c>value</c> is an element of <c>array</c>, with the three-valued answer of
    /// <c>value IN (elements)</c> and of PostgreSQL's <c>value = ANY (array)</c>:
    /// <list type="bullet">
    ///   <item>an empty array gives FALSE, even for a NULL value — there is nothing to compare;</item>
    ///   <item>a NULL array or a NULL value gives NULL;</item>
    ///   <item>a match gives TRUE;</item>
    ///   <item>no match gives NULL when the array holds a NULL element (that element might have been
    ///     the value), and FALSE otherwise.</item>
    /// </list>
    /// Equality is <see cref="MixedNumericComparison.EqualsForMembership"/>, the rule <c>IN</c> uses,
    /// so the two spellings cannot disagree: mixed numerics compare by value and a cross-type pair is
    /// a non-match, not an error.
    /// </summary>
    private static ColumnValue EvaluateArrayContains(string calledName, IReadOnlyList<ColumnValue> arguments)
    {
        ColumnValue array = arguments[0];
        ColumnValue value = arguments[1];

        if (array.Type == ColumnType.Null)
            return ColumnValue.Null;

        RequireArray(calledName, 0, array);

        IReadOnlyList<ColumnValue> elements = array.ArrayValues!;

        if (elements.Count == 0)
            return ColumnValue.FromBool(false);

        if (value.Type == ColumnType.Null)
            return ColumnValue.Null;

        bool sawNull = false;

        for (int i = 0; i < elements.Count; i++)
        {
            ColumnValue element = elements[i];

            if (element.Type == ColumnType.Null)
            {
                sawNull = true;
                continue;
            }

            if (MixedNumericComparison.EqualsForMembership(element, value))
                return ColumnValue.FromBool(true);
        }

        return sawNull ? ColumnValue.Null : ColumnValue.FromBool(false);
    }

    private static void RequireArray(string calledName, int argumentIndex, ColumnValue argument)
    {
        if (argument.Type == ColumnType.Array)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Function '{calledName}' expects argument {argumentIndex + 1} to be an array but received {argument.Type}");
    }
}
