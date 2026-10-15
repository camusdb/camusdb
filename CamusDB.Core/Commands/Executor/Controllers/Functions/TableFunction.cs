
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
/// A set-returning function that can stand as a relation in <c>FROM</c> (see
/// <see cref="TableFunctionRegistry"/>). It produces one output column.
///
/// <para><b>Contract.</b> <see cref="Generate"/> must be lazy: the caller pulls one value at a time and
/// may stop at any point (a <c>LIMIT</c>, a cancelled statement), so an implementation must never
/// build its whole output first. It must also be pure — a function of its arguments only — because the
/// leaf scanner calls it again, from the start, each time a join re-reads its input.</para>
/// </summary>
internal abstract class TableFunction
{
    /// <summary>The canonical lower-case name. With no alias, it names the relation and its column.</summary>
    public abstract string Name { get; }

    public abstract int MinArguments { get; }

    public abstract int MaxArguments { get; }

    /// <summary>The declared type of the output column, which the binder reports before any row exists.</summary>
    public abstract ColumnType OutputType { get; }

    /// <summary>
    /// Yields the output values for already-evaluated <paramref name="arguments"/>. Argument errors
    /// are raised when the first value is pulled, before any value is yielded.
    /// </summary>
    public abstract IEnumerable<ColumnValue> Generate(IReadOnlyList<ColumnValue> arguments);

    /// <summary>
    /// The exact number of values <see cref="Generate"/> would yield, or null when the arguments do
    /// not determine it without an error (a wrong type, an invalid step). The planner uses it as the
    /// row estimate of the leaf.
    /// </summary>
    public abstract long? CountRows(IReadOnlyList<ColumnValue> arguments);
}
