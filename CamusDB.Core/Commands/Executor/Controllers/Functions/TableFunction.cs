
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
/// leaf scanner calls it again, from the start, each time a join re-reads its input. A function that
/// cannot be pure (an extension function declared volatile) says so with <see cref="IsVolatile"/>.</para>
///
/// <para>Built-ins live in <see cref="TableFunctionRegistry"/>; a C# extension adds its own through the
/// extensions installed in the statement's database.</para>
/// </summary>
internal abstract class TableFunction
{
    /// <summary>The canonical lower-case name. With no alias, it names the relation and its column.</summary>
    public abstract string Name { get; }

    public abstract int MinArguments { get; }

    public abstract int MaxArguments { get; }

    /// <summary>The declared type of the output column, which the binder reports before any row exists.</summary>
    public abstract ColumnType OutputType { get; }

    /// <summary>The element type of an array output column, or null for a scalar output.</summary>
    public virtual ColumnType? OutputElementType => null;

    /// <summary>
    /// The output column's name when the call has no column alias, or null to name it after the
    /// relation (the function name, or its alias), as <c>generate_series</c> does.
    /// </summary>
    public virtual string? DefaultColumnName => null;

    /// <summary>
    /// True when <see cref="Generate"/> can yield other values for the same arguments, which breaks the
    /// purity rule above. The leaf scan then keeps the output for the statement after the first read,
    /// so every re-read sees the same rows, and the statement never uses the result cache.
    /// </summary>
    public virtual bool IsVolatile => false;

    /// <summary>How many values the leaf scan pulls between two cancellation checks.</summary>
    public virtual int CancellationCheckInterval => 1024;

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
