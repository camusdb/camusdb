/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// The result types of the CASE, COALESCE, IFNULL, IF, GREATEST and LEAST nodes, and the element types
/// of the <c>ARRAY[…]</c> nodes, of one UPDATE, DELETE or upsert DO UPDATE. The statement check types
/// them from the target table's columns (<see cref="ConditionalResultTypes.Check"/>), and the evaluator
/// reads them while the statement runs.
///
/// <para><b>A record outside the binding.</b> A SELECT keeps these types on its binding
/// (<see cref="QueryRowNameResolver"/>). These statements have no binding: they evaluate their WHERE,
/// their SET values and the re-check of a locked row with no resolver, and their tree comes from the
/// parse cache, which shares one tree between databases, so a type that depends on a column cannot be
/// kept on the node. Without the record a NULL hides the type of its column, and the type comes from
/// the values of each row: <c>ARRAY[int_col, float_col]</c> is an <c>ARRAY&lt;Integer64&gt;</c> in a row
/// where <c>float_col</c> is NULL and does not compare with an <c>ARRAY&lt;Float64&gt;</c>, and
/// <c>COALESCE(int_col, float_col)</c> is an INT64 in that row.</para>
///
/// <para><b>How the evaluator reaches it.</b> The statement makes its record current for the time it
/// runs (<see cref="Enter"/>), through an <see cref="AsyncLocal{T}"/>, so the evaluator needs no new
/// parameter. An evaluation with no resolver reads this record, and then the type the SQL text gives
/// (<see cref="ConditionalResultTypes.TryGetRecordedResultType"/>, <see cref="ArrayLiteralTypes.Build"/>).
/// An evaluation with a resolver belongs to a SELECT and reads only its binding, so a SELECT costs
/// nothing more.</para>
///
/// <para><b>Safety.</b> Types are keyed by node identity. A statement that a trigger runs has other
/// nodes, so it finds nothing here, and a nested UPDATE or DELETE makes its own record current until it
/// ends. Each execution builds its own record, so two executions of one cached tree on databases with
/// different column types do not share it. A record is written only before it is entered and only read
/// after, so the concurrent evaluations of one statement read it without a lock.</para>
/// </summary>
internal sealed class StatementExpressionTypes : IExpressionTypeRecorder
{
    private static readonly AsyncLocal<StatementExpressionTypes?> current = new();

    private Dictionary<NodeAst, ExpressionType>? conditionalResultTypes;

    private Dictionary<NodeAst, ExpressionType>? arrayElementTypes;

    private bool entered;

    /// <summary>The record of the UPDATE, DELETE or upsert that is running in this flow, or null.</summary>
    internal static StatementExpressionTypes? Current => current.Value;

    /// <summary>True when the check recorded no type, so the statement has nothing to make current.</summary>
    internal bool IsEmpty => conditionalResultTypes is null && arrayElementTypes is null;

    /// <inheritdoc/>
    public void RecordConditionalResultType(NodeAst node, ExpressionType resultType)
    {
        RequireNotEntered();
        (conditionalResultTypes ??= new(ReferenceEqualityComparer.Instance))[node] = resultType;
    }

    /// <inheritdoc/>
    public void RecordArrayElementType(NodeAst node, ExpressionType elementType)
    {
        RequireNotEntered();
        (arrayElementTypes ??= new(ReferenceEqualityComparer.Instance))[node] = elementType;
    }

    /// <summary>The result type recorded for the conditional node <paramref name="node"/>, if any.</summary>
    internal bool TryGetConditionalResultType(NodeAst node, out ExpressionType resultType)
    {
        if (conditionalResultTypes is { } recorded && recorded.TryGetValue(node, out resultType))
            return true;

        resultType = default;
        return false;
    }

    /// <summary>The element type recorded for the <c>ARRAY[…]</c> node <paramref name="node"/>, if any.</summary>
    internal bool TryGetArrayElementType(NodeAst node, out ExpressionType elementType)
    {
        if (arrayElementTypes is { } recorded && recorded.TryGetValue(node, out elementType))
            return true;

        elementType = default;
        return false;
    }

    // Concurrent evaluations read the dictionaries without a lock, which is safe only while nothing
    // writes them.
    private void RequireNotEntered()
    {
        if (entered)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "A statement's expression types cannot be recorded after the statement started running");
    }

    /// <summary>
    /// Makes <paramref name="types"/> the current record until the returned scope is disposed, which
    /// restores the record that was current before. Call it after the statement check and before the
    /// first row is read, and dispose the scope when the statement ends. A null or empty record makes no
    /// record current, so the record of an outer statement does not stay current in a nested one. When
    /// neither the outer nor this statement has a record, nothing changes and nothing is allocated.
    /// </summary>
    internal static Scope Enter(StatementExpressionTypes? types)
    {
        StatementExpressionTypes? next = types is { IsEmpty: false } ? types : null;
        StatementExpressionTypes? previous = current.Value;

        if (next is not null)
            next.entered = true;

        if (ReferenceEquals(previous, next))
            return default;

        current.Value = next;
        return new Scope(previous);
    }

    /// <summary>Restores the record that was current before <see cref="Enter"/>.</summary>
    internal readonly struct Scope : IDisposable
    {
        private readonly StatementExpressionTypes? previous;

        private readonly bool active;

        internal Scope(StatementExpressionTypes? previous)
        {
            this.previous = previous;
            active = true;
        }

        public void Dispose()
        {
            if (active)
                current.Value = previous;
        }
    }
}
