
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

/// <summary>
/// Evaluator for a function whose result comes from the session rather than from its arguments. It
/// receives the statement's parameter dictionary, which is where the SQL entry points snapshot the
/// session (see <see cref="SessionScalarFunctions"/>).
/// </summary>
internal delegate ColumnValue ScalarSessionEvaluatorDelegate(string calledName, IReadOnlyDictionary<string, ColumnValue>? parameters);

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

    /// <summary>
    /// Set instead of using <see cref="Evaluator"/> alone when the result is decided by the session
    /// (<c>current_user()</c> and friends). When present the evaluator dispatches here and hands over
    /// the statement's parameters; <see cref="Evaluator"/> is then only reached from paths that carry
    /// no session and exists to raise a clear error there.
    /// </summary>
    public ScalarSessionEvaluatorDelegate? SessionEvaluator { get; init; }

    public required ScalarReturnTypeInferenceDelegate InferReturnType { get; init; }

    public bool IsVolatile { get; init; }

    /// <summary>
    /// True for a function whose value is fixed within a session but varies between sessions. Such a
    /// function is also <see cref="IsVolatile"/> so that a query naming it bypasses the shared result
    /// cache, but unlike a per-row generator it must not be accepted as a column <c>DEFAULT</c>: the
    /// default is replayed by the insert path, which has no session to report.
    /// </summary>
    public bool IsSessionScoped { get; init; }

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
