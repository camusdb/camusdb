/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using CamusDB.Extensions;

namespace CamusDB.Fixtures.Rich;

/// <summary>
/// Test-only table functions: a splitter with a named column, a counter with a row-count method, one
/// that records when its enumerator is disposed, one that fails, a volatile one, and one that yields
/// arrays.
/// </summary>
public static class TableFunctions
{
    private static long disposals;

    private static long volatileStart;

    [CamusTableFunction("rx_split", Column = "word")]
    public static IEnumerable<string> Split(string text)
    {
        foreach (string word in text.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries))
            yield return word;
    }

    [CamusTableFunction("rx_count_to", CountRowsMethod = nameof(CountToRows))]
    public static IEnumerable<long> CountTo(long n)
    {
        for (long i = 1; i <= n; i++)
            yield return i;
    }

    public static long? CountToRows(long n) => n < 0 ? 0 : n;

    /// <summary>Counts up without end; the finally block records each disposal.</summary>
    [CamusTableFunction("rx_tracked")]
    public static IEnumerable<long> Tracked(long start)
    {
        try
        {
            for (long i = start; ; i++)
                yield return i;
        }
        finally
        {
            Interlocked.Increment(ref disposals);
        }
    }

    /// <summary>Yields <paramref name="n"/> values, then throws; the finally block still runs.</summary>
    [CamusTableFunction("rx_fail_after")]
    public static IEnumerable<long> FailAfter(long n)
    {
        try
        {
            for (long i = 1; i <= n; i++)
                yield return i;

            throw new InvalidOperationException("rx_fail_after reached " + n);
        }
        finally
        {
            Interlocked.Increment(ref disposals);
        }
    }

    [CamusFunction("rx_disposals", IsVolatile = true)]
    public static long Disposals() => Interlocked.Read(ref disposals);

    /// <summary>Volatile: each call yields <paramref name="n"/> values that start where the last call stopped.</summary>
    [CamusTableFunction("rx_volatile_seq", IsVolatile = true)]
    public static IEnumerable<long> VolatileSeq(long n)
    {
        long start = Interlocked.Add(ref volatileStart, n) - n;

        for (long i = 1; i <= n; i++)
            yield return start + i;
    }

    [CamusTableFunction("rx_lists")]
    public static IEnumerable<IReadOnlyList<long>> Lists(long n)
    {
        for (long i = 1; i <= n; i++)
            yield return new[] { i, i * 10 };
    }

    /// <summary>A NULL element of the output is a NULL row value; a null enumerable is zero rows.</summary>
    [CamusTableFunction("rx_maybe")]
    public static IEnumerable<string?>? Maybe(string? mode) =>
        mode switch
        {
            null => null,
            "nulls" => new string?[] { "a", null, "b" },
            _ => new string?[] { mode },
        };
}
