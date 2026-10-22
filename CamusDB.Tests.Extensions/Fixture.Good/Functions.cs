/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Text;
using System.Threading;
using CamusDB.Extensions;

namespace CamusDB.Fixtures.Good;

/// <summary>
/// Test-only scalar functions that cover the value mapping: one function per CLR type, NULL in
/// and out, an optional parameter, an alias, a volatile function and a function that throws.
/// </summary>
public static class Functions
{
    private static long counter;

    [CamusFunction("fx_slugify")]
    public static string Slugify(string value)
    {
        StringBuilder builder = new(value.Length);
        bool pendingDash = false;

        foreach (char c in value.Trim().ToLowerInvariant())
        {
            if (char.IsLetterOrDigit(c))
            {
                if (pendingDash && builder.Length > 0)
                    builder.Append('-');

                builder.Append(c);
                pendingDash = false;
            }
            else
                pendingDash = true;
        }

        return builder.ToString();
    }

    [CamusFunction("fx_add")]
    public static long Add(long a, long b) => a + b;

    [CamusFunction("fx_half")]
    public static double Half(double value) => value / 2;

    [CamusFunction("fx_float_twice")]
    public static float FloatTwice(float value) => value * 2;

    [CamusFunction("fx_not")]
    public static bool Not(bool value) => !value;

    [CamusFunction("fx_uuid_text")]
    public static string UuidText(Guid value) => value.ToString("D");

    [CamusFunction("fx_parse_uuid")]
    public static Guid ParseUuid(string value) => Guid.Parse(value);

    [CamusFunction("fx_bytes_len")]
    public static long BytesLength(byte[] value) => value.Length;

    [CamusFunction("fx_bytes_rev")]
    public static byte[] BytesReverse(byte[] value)
    {
        byte[] copy = (byte[])value.Clone();
        Array.Reverse(copy);
        return copy;
    }

    [CamusFunction("fx_next_day")]
    public static DateTime NextDay(DateTime value) => value.AddDays(1);

    [CamusFunction("fx_dec_double")]
    public static decimal DecimalDouble(decimal value) => value * 2;

    /// <summary>A nullable parameter receives NULL; the other one propagates it.</summary>
    [CamusFunction("fx_coalesce")]
    public static string Coalesce(string? first, string second) => first ?? second;

    [CamusFunction("fx_nullable_len")]
    public static long NullableLength(long? value) => value ?? -1;

    /// <summary>Returns SQL NULL for an empty string.</summary>
    [CamusFunction("fx_null_if_empty")]
    public static string? NullIfEmpty(string value) => value.Length == 0 ? null : value;

    [CamusFunction("fx_optional")]
    public static long Optional(long a, long b = 10) => a + b;

    [CamusFunction("fx_tag", Aliases = new[] { "fx_tag_alias" })]
    public static string Tag(string value) => "[" + value + "]";

    /// <summary>Volatile: a new value on each call.</summary>
    [CamusFunction("fx_counter", IsVolatile = true)]
    public static long Counter() => Interlocked.Increment(ref counter);

    [CamusFunction("fx_throw")]
    public static string Throw(string message) => throw new InvalidOperationException(message);

    /// <summary>The build that runs: "1.0.0" here, "2.0.0" in Fixture.GoodV2.</summary>
    [CamusFunction("fx_good_version")]
    public static string GoodVersion() => Build.Version;

#if !GOOD_V2
    /// <summary>Defined by version 1.0.0 only, so an update to 2.0.0 is refused while an object uses it.</summary>
    [CamusFunction("fx_v1_only")]
    public static long V1Only(long value) => value;
#endif

    /// <summary>How many times the counting trigger fired for a table name.</summary>
    [CamusFunction("fx_trigger_count", IsVolatile = true)]
    public static long TriggerCount(string table) => CountingTrigger.Count(table);
}

/// <summary>The fixture build's version, shared by the functions and the triggers.</summary>
internal static class Build
{
#if GOOD_V2
    public const string Version = "2.0.0";
#else
    public const string Version = "1.0.0";
#endif
}
