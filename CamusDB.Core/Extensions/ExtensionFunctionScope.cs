/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Extensions;

/// <summary>
/// Ambient (per-async-flow) carrier of the <see cref="ExtensionRegistry"/> of the engine whose work
/// this flow runs. It is how extension functions reach the expression evaluator, which is static and
/// takes no context: <c>ScalarFunctionEvaluator</c> reads the built-in table first and this scope on a
/// miss. The same pattern as <c>AuthorizationContext</c>.
///
/// <para><b>Why not a static registry.</b> Several engines run in one process (tests, the in-process
/// cluster), each built with its own <see cref="CamusDBOptions"/>. A static table would let one
/// engine's extensions answer another engine's statements, and the configuration rule forbids a
/// static setting.</para>
///
/// <para><b>Where it is set.</b> The <c>CommandExecutor</c> calls <see cref="Enter"/> at the start of
/// every entry point that can evaluate an expression, and at the start of each background job that
/// evaluates one (column backfills, constraint validation, materialized-view refresh). The call is
/// made in the synchronous part of the entry method, so the value also reaches the caller's flow:
/// a query returns a lazy cursor, and the caller later pulls rows from it in the caller's own flow.
/// An <see cref="AsyncLocal{T}"/> value set inside an awaited async method does not flow back out, so
/// a set placed after the first await would be lost for that cursor.</para>
///
/// <para><b>Cursors.</b> Entering the scope in the entry method is not enough for a cursor that the
/// caller pulls later: by then the caller may be in another flow, or inside another engine. The query
/// entry points therefore also wrap the cursor (<see cref="ExtensionScopedCursor"/>), so each row is
/// evaluated in the scope of the engine that created the cursor.</para>
///
/// <para><b>Failure is closed.</b> A flow that never entered a scope resolves built-ins only, and a
/// call to an extension function fails as "Function not found". It never resolves to another engine's
/// function.</para>
///
/// <para><b>Cost.</b> An engine without extensions enters <c>null</c>, which is a no-op when the flow
/// already holds <c>null</c>, so a server without extensions pays one comparison per entry and nothing
/// on the per-row path. Built-in function lookups never read the scope.</para>
/// </summary>
internal static class ExtensionFunctionScope
{
    private static readonly AsyncLocal<ExtensionRegistry?> current = new();

    /// <summary>The registry of the engine this flow runs for, or <c>null</c>.</summary>
    public static ExtensionRegistry? Current => current.Value;

    /// <summary>
    /// Makes <paramref name="registry"/> the scope of this flow. An empty registry is stored as
    /// <c>null</c>. Skips the write when the flow already holds the same value, so repeated entries
    /// from one request cost nothing.
    /// </summary>
    public static void Enter(ExtensionRegistry? registry)
    {
        ExtensionRegistry? value = registry is null || registry.IsEmpty ? null : registry;

        if (!ReferenceEquals(current.Value, value))
            current.Value = value;
    }
}
