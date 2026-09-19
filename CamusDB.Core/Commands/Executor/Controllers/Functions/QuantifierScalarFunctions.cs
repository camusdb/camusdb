/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// Stubs named <c>any</c>, <c>some</c> and <c>all</c> that always fail with a clear message. The
/// parser turns <c>x = ANY (a)</c>, <c>x = SOME (a)</c> and <c>x &lt;&gt; ALL (a)</c> into a
/// membership test (see <c>SQLParser.QuantifiedComparison</c>), so a call to one of these names that
/// is still in the tree is a quantifier the rewrite did not see: <c>SELECT any(tags)</c>,
/// <c>ANY (tags) = x</c>, or <c>x = any(tags) + 1</c>, where <c>+</c> binds first. Without the stubs
/// such a call would fail as an unknown function. They also keep a future user-defined function
/// from taking these names. A column named <c>any</c>, <c>some</c> or <c>all</c> is not affected,
/// because a function name is looked up only in call syntax.
/// <para>Any arity is accepted, so every stray call gets this message and not an arity error.</para>
/// </summary>
internal static class QuantifierScalarFunctions
{
    public static void Register(ScalarFunctionRegistry registry)
    {
        foreach (string name in (ReadOnlySpan<string>)["any", "some", "all"])
        {
            registry.Register(new ScalarFunctionDescriptor
            {
                Name = name,
                MinArity = 0,
                MaxArity = int.MaxValue,
                Evaluator = (calledName, _) => throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"{calledName.ToUpperInvariant()} is not a function: ANY/SOME/ALL is valid only as the right operand of = or <>"),
                InferReturnType = _ => ColumnType.Bool,
                IsVolatile = false,
            });
        }
    }
}
