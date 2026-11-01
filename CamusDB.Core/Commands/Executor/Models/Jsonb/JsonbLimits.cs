/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Jsonb;

/// <summary>
/// Fixed limits of the <c>jsonb</c> type. They are constants, not settings, for the same reason as the
/// statement nesting limit: a document is parsed from casts, typed literals, write coercion and the
/// HTTP and gRPC parameter decoders, and none of those static paths carries per-statement
/// configuration. A per-node setting would let one INSERT pass on one node and fail on another.
///
/// <para><see cref="MaxNestingDepth"/> is what makes the recursive walks over a document safe
/// (comparison, containment, the builders): every document that reaches a cell or an operator was
/// checked against it, so a recursion is bounded by it and cannot overflow the stack. The encoder, the
/// text writer, the skip and the hash are iterative and do not depend on it.</para>
///
/// <para>The number limits follow PostgreSQL <c>numeric</c> input: an exponent of at most ±1000, at
/// most 131072 digits before the point and 16383 after it.</para>
/// </summary>
internal static class JsonbLimits
{
    /// <summary>The deepest nesting of objects and arrays a document may have. A scalar has depth 0.</summary>
    public const int MaxNestingDepth = 512;

    /// <summary>The first byte of every <c>jsonb</c> cell. A reader refuses any other value.</summary>
    public const byte FormatVersion = 1;

    public const int MaxIntegerDigits = 131072;

    public const int MaxScale = 16383;
}
