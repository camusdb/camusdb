/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Runtime.CompilerServices;
using CamusDB.Core.Catalogs.Models;

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// The declared width of an integer type: <c>SMALLINT</c> is 16 bits, <c>INT</c> and <c>INTEGER</c> are
/// 32 bits, and <c>INT64</c> and <c>BIGINT</c> are 64 bits. These are the PostgreSQL ranges.
///
/// <para><b>A width is a write rule, not a storage form.</b> Every width stores as
/// <see cref="ColumnType.Integer64"/>, so row bytes, index keys and comparisons do not change, and no
/// storage revision is involved. The declared width lives on
/// <see cref="TableColumnSchema.IntegerBits"/> and works like an implicit CHECK: a value outside the
/// range is <see cref="CamusDBErrorCodes.IntegerValueOutOfRange"/>. Arithmetic is still int64, so
/// <c>s + 1</c> on a <c>SMALLINT</c> holding 32767 gives 32768 in a query, and only a write of that
/// result to a narrow column, or a cast of it to a narrow type, is refused.</para>
///
/// <para><b>Which paths call it.</b> INSERT and the insert arm of an upsert, through
/// <c>RowInserter.Validate</c>. UPDATE and the update arm of an upsert, through
/// <c>RowUpdater.CheckForNotNulls</c>. The fill of an added column, because a sequence or function
/// default can give a value that no INSERT judged. A constant <c>DEFAULT</c>, at the ticket layer. And
/// <c>CAST</c>, which keeps the int64 result type and checks the value.</para>
/// </summary>
internal static class IntegerWidth
{
    /// <summary>The width of <c>SMALLINT</c>.</summary>
    public const int SmallIntBits = 16;

    /// <summary>The width of <c>INT</c> and <c>INTEGER</c>.</summary>
    public const int IntBits = 32;

    /// <summary>True for a width the schema can hold. Null, which means int64, is not a width.</summary>
    public static bool IsValid(int bits) => bits is SmallIntBits or IntBits;

    /// <summary>The name of a width in an error message.</summary>
    public static string TypeName(int bits) => bits == SmallIntBits ? "smallint" : "int";

    /// <summary>
    /// Refuses a value of a column write that does not fit <paramref name="bits"/>. A NULL fits, and a
    /// value of another type is not judged here, because the encoder refuses it. For an array the rule
    /// applies to each element.
    /// </summary>
    public static void EnforceOnColumn(int bits, ColumnValue value, string columnName)
    {
        if (value.Type == ColumnType.Integer64)
        {
            if (!Fits(value.LongValue, bits))
                throw ColumnOutOfRange(bits, value.LongValue, columnName);

            return;
        }

        if (value.Type != ColumnType.Array || value.ArrayValues is not { } elements)
            return;

        for (int i = 0; i < elements.Count; i++)
        {
            ColumnValue element = elements[i];

            if (element.Type == ColumnType.Integer64 && !Fits(element.LongValue, bits))
                throw ColumnOutOfRange(bits, element.LongValue, columnName);
        }
    }

    /// <summary>
    /// Returns the result of a cast to a narrow integer type, or refuses it when it does not fit. The
    /// value keeps its int64 type: the width belongs to a column, not to a value.
    /// </summary>
    public static ColumnValue EnforceOnCast(int bits, ColumnValue value)
    {
        if (value.Type == ColumnType.Integer64 && !Fits(value.LongValue, bits))
            throw Int64Math.OutOfRange(
                $"Value {Format(value.LongValue)} is out of range for {TypeName(bits)} ({Format(Min(bits))} to {Format(Max(bits))})");

        return value;
    }

    /// <summary>Compares through a shift, so a width needs no table of bounds and no branch for each type.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool Fits(long value, int bits) => value >= Min(bits) && value <= Max(bits);

    private static long Min(int bits) => -(1L << (bits - 1));

    private static long Max(int bits) => (1L << (bits - 1)) - 1;

    private static CamusDBException ColumnOutOfRange(int bits, long value, string columnName) =>
        Int64Math.OutOfRange(
            $"Value {Format(value)} is out of range for {TypeName(bits)} column '{columnName}' ({Format(Min(bits))} to {Format(Max(bits))})");

    private static string Format(long value) => value.ToString(CultureInfo.InvariantCulture);
}
