
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Evaluates the arguments of a function in <c>FROM</c> (see <see cref="TableFunctionSource"/>), for the
/// leaf scan that streams its rows and for the planner that needs its row count.
/// </summary>
internal static class TableFunctionEvaluation
{
    /// <summary>
    /// Evaluates every argument once, with the statement's parameters. The query creator already
    /// refused any argument that reads a column, so an empty row is the whole evaluation context.
    /// </summary>
    public static ColumnValue[] EvaluateArguments(TableFunctionSource call, Dictionary<string, ColumnValue>? parameters)
    {
        ColumnValue[] arguments = new ColumnValue[call.Arguments.Count];

        for (int i = 0; i < arguments.Length; i++)
            arguments[i] = SqlExecutor.EvalExpr(call.Arguments[i], QueryResultRow.EmptyRow, parameters);

        return arguments;
    }

    /// <summary>
    /// The exact row count of a function source when its arguments are constants, or null otherwise.
    /// <para>Only literal arguments are counted. A parameter would make a cached plan's estimate depend on
    /// the first execution's value, and a function call (<c>now()</c>, <c>nextval</c>) may read the clock
    /// or have an effect, which planning must not trigger. Such a leaf gets the default estimate.</para>
    /// </summary>
    public static long? TryCountRows(BoundDerivedTableSource source)
    {
        if (source.Source.Function is not { } call)
            return null;

        foreach (NodeAst argument in call.Arguments)
        {
            if (NodeAstWalk.Any(argument, static n => n.nodeType is NodeType.Placeholder or NodeType.ExprFuncCall))
                return null;
        }

        TableFunction function = TableFunctionRegistry.Resolve(call.FunctionName, call.Arguments.Count);

        try
        {
            return function.CountRows(EvaluateArguments(call, null));
        }
        catch (CamusDBException)
        {
            // An argument that cannot be evaluated (an overflow in 1 + 9223372036854775807) fails the
            // statement when the rows are generated; planning only goes without an estimate.
            return null;
        }
    }
}
