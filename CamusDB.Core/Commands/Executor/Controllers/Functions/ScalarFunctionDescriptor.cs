
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.Functions;

internal delegate ColumnValue ScalarFunctionEvaluatorDelegate(string calledName, IReadOnlyList<ColumnValue> arguments);

internal delegate ColumnType ScalarReturnTypeInferenceDelegate(IReadOnlyList<ColumnType> argumentTypes);

internal sealed class ScalarFunctionDescriptor
{
    public required string Name { get; init; }

    public IReadOnlyList<string> Aliases { get; init; } = [];

    public int MinArity { get; init; }

    /// <summary>
    /// Maximum accepted arity. Use <see cref="int.MaxValue"/> for variadic functions.
    /// </summary>
    public int MaxArity { get; init; }

    public required ScalarFunctionEvaluatorDelegate Evaluator { get; init; }

    public required ScalarReturnTypeInferenceDelegate InferReturnType { get; init; }

    public bool IsVolatile { get; init; }

    public IEnumerable<string> AllNames
    {
        get
        {
            yield return Name;

            foreach (string alias in Aliases)
                yield return alias;
        }
    }
}
