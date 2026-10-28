
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

    /// <summary>
    /// True when <paramref name="name"/> is a table function a statement in this flow can call: a
    /// built-in, or a table function of an extension installed in the statement's database.
    /// </summary>
    public static bool IsTableFunction(string name) =>
        Functions.ContainsKey(name) ||
        (Extensions.ExtensionFunctionScope.CurrentInstalled?.HasTableFunction(name) ?? false);

    /// <summary>True when <paramref name="name"/> is a built-in table function, whatever extension scope the flow carries.</summary>
    public static bool IsBuiltInTableFunction(string name) => Functions.ContainsKey(name);

    /// <summary>
    /// True when <paramref name="name"/> resolves to a volatile table function in this flow. A name
    /// that does not resolve answers false; it fails later, where an unknown function belongs.
    /// </summary>
    public static bool IsVolatileTableFunction(string name)
    {
        try
        {
            return TryResolve(name, out TableFunction? function) && function.IsVolatile;
        }
        catch (CamusDBException)
        {
            return false;
        }
    }

    /// <summary>
    /// The built-in table first, then the table functions of the extensions installed in the database
    /// whose statement this flow runs (<see cref="Extensions.ExtensionFunctionScope"/>). The loader
    /// refuses an extension name that collides with a built-in, so the order never changes what a name
    /// means. A member of an installed extension this node does not load throws
    /// <see cref="CamusDBErrorCodes.ExtensionNotLoadedOnNode"/>.
    /// </summary>
    private static bool TryResolve(string name, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TableFunction? function)
    {
        if (Functions.TryGetValue(name, out function))
            return true;

        // Kept out of the built-in map, so it stays out of every listing of the functions a user can
        // call; only the query creator writes this name (see SingleRowTableFunction).
        if (string.Equals(name, SingleRowTableFunction.InternalName, StringComparison.Ordinal))
        {
            function = SingleRowTableFunction.Instance;
            return true;
        }

        if (Extensions.ExtensionFunctionScope.CurrentInstalled is { } installed && installed.TryGetTableFunction(name, out Extensions.ExtensionTableFunction? extension))
        {
            function = extension;
            return true;
        }

        function = null;
        return false;
    }

    /// <summary>
    /// Returns the function named <paramref name="name"/> after checking the argument count. Throws
    /// with the function's name when it is unknown, a scalar function, not supported yet, or called
    /// with the wrong number of arguments.
    /// </summary>
    public static TableFunction Resolve(string name, int argumentCount)
    {
        if (TryResolve(name, out TableFunction? function))
        {
            if (argumentCount < function.MinArguments || argumentCount > function.MaxArguments)
            {
                string expected = function.MinArguments == function.MaxArguments
                    ? function.MinArguments.ToString()
                    : function.MaxArguments == function.MinArguments + 1
                        ? $"{function.MinArguments} or {function.MaxArguments}"
                        : $"{function.MinArguments} to {function.MaxArguments}";

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
