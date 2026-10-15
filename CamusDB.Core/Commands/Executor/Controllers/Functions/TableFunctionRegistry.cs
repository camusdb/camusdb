
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

/// <summary>
/// Resolves the name of a function written as a relation in <c>FROM</c>
/// (<c>FROM generate_series(1, 10)</c>) to its <see cref="TableFunction"/>.
///
/// <para>Kept apart from <see cref="ScalarFunctionRegistry"/> on purpose: a scalar function returns one
/// value per call and has no place in <c>FROM</c>, so a scalar name there is an error, never a one-row
/// relation. The reverse holds too — <c>SELECT generate_series(1, 3)</c> does not find this registry and
/// fails as an unknown scalar function.</para>
/// </summary>
internal static class TableFunctionRegistry
{
    private static readonly Dictionary<string, TableFunction> Functions = new(StringComparer.OrdinalIgnoreCase)
    {
        [GenerateSeriesTableFunction.Instance.Name] = GenerateSeriesTableFunction.Instance,
    };

    /// <summary>
    /// Set-returning functions PostgreSQL has but CamusDB does not support in <c>FROM</c> yet. Named so
    /// the error says "not supported" instead of "not a table function". The two regexp ones also exist
    /// as scalar stubs that refuse at evaluation time.
    /// </summary>
    private static readonly HashSet<string> NotYetSupported = new(StringComparer.OrdinalIgnoreCase)
    {
        "unnest",
        "generate_subscripts",
        "regexp_matches",
        "regexp_split_to_table",
    };

    /// <summary>True when <paramref name="name"/> is a table function this registry resolves.</summary>
    public static bool IsTableFunction(string name) => Functions.ContainsKey(name);

    /// <summary>
    /// Returns the function named <paramref name="name"/> after checking the argument count. Throws
    /// with the function's name when it is unknown, a scalar function, not supported yet, or called
    /// with the wrong number of arguments.
    /// </summary>
    public static TableFunction Resolve(string name, int argumentCount)
    {
        if (Functions.TryGetValue(name, out TableFunction? function))
        {
            if (argumentCount < function.MinArguments || argumentCount > function.MaxArguments)
            {
                string expected = function.MinArguments == function.MaxArguments
                    ? function.MinArguments.ToString()
                    : $"{function.MinArguments} or {function.MaxArguments}";

                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"{function.Name} expects {expected} arguments, got {argumentCount}");
            }

            return function;
        }

        if (NotYetSupported.Contains(name))
            throw new CamusDBException(
                CamusDBErrorCodes.FeatureNotSupported,
                $"The set-returning function '{name}' is not supported in FROM yet");

        if (ScalarFunctionEvaluator.IsRegisteredScalarFunction(name))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"'{name}' is a scalar function and cannot be used as a relation in FROM; call it in the select list instead");

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInput,
            $"Unknown table function '{name}' in FROM");
    }
}
