/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using CamusDB.Extensions;

namespace CamusDB.Fixtures.Rich;

/// <summary>
/// Test-only scalar functions over arrays: one echo per mapped element type, NULL arrays and NULL
/// elements, and a list the extension keeps and changes after it returned it.
/// </summary>
public static class Arrays
{
    private static readonly List<long> Shared = new() { 1, 2, 3 };

    [CamusFunction("rx_sum_list")]
    public static long SumList(IReadOnlyList<long> values) => values.Sum();

    /// <summary>A nullable element type receives NULL elements.</summary>
    [CamusFunction("rx_count_null_elements")]
    public static long CountNullElements(IReadOnlyList<long?> values) => values.Count(v => v is null);

    /// <summary>A nullable array parameter receives a NULL array as null.</summary>
    [CamusFunction("rx_list_length")]
    public static long ListLength(IReadOnlyList<string>? values) => values?.Count ?? -1;

    [CamusFunction("rx_join")]
    public static string Join(IReadOnlyList<string?> values, string separator) => string.Join(separator, values.Select(v => v ?? "<null>"));

    [CamusFunction("rx_range_list")]
    public static IReadOnlyList<long> RangeList(long count) => Enumerable.Range(1, (int)count).Select(i => (long)i).ToArray();

    [CamusFunction("rx_null_list")]
    public static IReadOnlyList<long>? NullList(long value) => null;

    [CamusFunction("rx_with_null_element")]
    public static IReadOnlyList<long?> WithNullElement(long value) => new long?[] { value, null };

    [CamusFunction("rx_echo_doubles")]
    public static IReadOnlyList<double> EchoDoubles(IReadOnlyList<double> values) => values.Select(v => v * 2).ToArray();

    [CamusFunction("rx_echo_floats")]
    public static IReadOnlyList<float> EchoFloats(IReadOnlyList<float> values) => values.ToArray();

    [CamusFunction("rx_echo_bools")]
    public static IReadOnlyList<bool> EchoBools(IReadOnlyList<bool> values) => values.Select(v => !v).ToArray();

    [CamusFunction("rx_echo_strings")]
    public static IReadOnlyList<string> EchoStrings(IReadOnlyList<string> values) => values.Select(v => v.ToUpperInvariant()).ToArray();

    [CamusFunction("rx_echo_uuids")]
    public static IReadOnlyList<Guid> EchoUuids(IReadOnlyList<Guid> values) => values.Reverse().ToArray();

    [CamusFunction("rx_echo_dates")]
    public static IReadOnlyList<DateTime> EchoDates(IReadOnlyList<DateTime> values) => values.Select(v => v.AddDays(1)).ToArray();

    [CamusFunction("rx_echo_decimals")]
    public static IReadOnlyList<decimal> EchoDecimals(IReadOnlyList<decimal> values) => values.Select(v => v * 10).ToArray();

    /// <summary>Formats every element with its CLR type, so a test sees what the extension received.</summary>
    [CamusFunction("rx_describe_doubles")]
    public static string DescribeDoubles(IReadOnlyList<double> values) =>
        string.Join(",", values.Select(v => v.ToString(CultureInfo.InvariantCulture)));

    /// <summary>Returns the list the extension keeps; <see cref="MutateShared"/> changes it afterwards.</summary>
    [CamusFunction("rx_shared_list", IsVolatile = true)]
    public static IReadOnlyList<long> SharedList() => Shared;

    [CamusFunction("rx_mutate_shared", IsVolatile = true)]
    public static long MutateShared(long value)
    {
        Shared[0] = value;
        Shared.Add(value);
        return Shared.Count;
    }

    /// <summary>Tries to change the list it received; the list is read-only.</summary>
    [CamusFunction("rx_try_mutate")]
    public static string TryMutate(IReadOnlyList<long> values)
    {
        if (values is IList<long> list)
        {
            try
            {
                list[0] = 999;
                return "changed";
            }
            catch (NotSupportedException)
            {
                return "read-only";
            }
        }

        return "read-only";
    }
}
