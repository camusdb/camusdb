/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// Remembers the source text of each float literal value, so a literal that reaches a
/// <see cref="Catalogs.Models.ColumnType.Numeric"/> column, cast or parameter converts from what the
/// user wrote and not from the <see cref="double"/> the lexer made of it.
///
/// <para><b>Why a side table.</b> A literal such as <c>12345678901234567890.123456789</c> or
/// <c>2.0000000005</c> evaluates to a <see cref="Catalogs.Models.ColumnType.Float64"/> value, because a
/// bare decimal literal is FLOAT64 in a general expression (as in Spanner GoogleSQL). The double cannot
/// hold that value, and the conversion to NUMERIC happens later, in
/// <c>CastScalarFunctions.CoerceToColumnType</c>, where the AST is gone. A field on
/// <see cref="ColumnValue"/> would cost eight bytes on every value of every row; this table costs
/// nothing for values that are not float literals.</para>
///
/// <para><b>Why it is safe.</b> The key is the literal's own <see cref="ColumnValue"/> instance, which
/// the evaluator creates once per literal node and caches. A <see cref="ColumnValue"/> is immutable,
/// so the remembered text always describes the value it is attached to. Any value that is not that
/// instance — the result of arithmetic, a value read from a row, a copy through a
/// <see cref="ValueSlot"/> — has no entry, and the conversion falls back to the exact binary value of
/// the double (<see cref="NumericMath.TryFromDouble"/>), the Spanner rule for a FLOAT64 cast.</para>
/// </summary>
internal static class FloatLiteralText
{
    private static readonly ConditionalWeakTable<ColumnValue, string> Texts = new();

    /// <summary>Attaches <paramref name="text"/> to <paramref name="literal"/> and returns the literal.</summary>
    public static ColumnValue Remember(ColumnValue literal, string text)
    {
        Texts.AddOrUpdate(literal, text);
        return literal;
    }

    /// <summary>The source text of <paramref name="value"/>, when it is a float literal instance.</summary>
    public static bool TryGet(ColumnValue value, out string? text) => Texts.TryGetValue(value, out text);
}
