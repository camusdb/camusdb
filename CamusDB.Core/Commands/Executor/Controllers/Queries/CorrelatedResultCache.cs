/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Keeps the result of a correlated subquery for each distinct set of outer values, for one execution of
/// one statement. A correlated subquery runs once for each outer row, and under a join the outer rows
/// repeat the values that the body reads: <c>a0 LEFT JOIN a1 CROSS JOIN a2 WHERE NOT EXISTS (… a0.c1 …)</c>
/// runs the body once for each joined row, but its result changes only with <c>a0.c1</c>. With the cache,
/// the body runs once for each distinct value.
///
/// <para><b>Why a result can be kept.</b> The body reads its outer values (the key, see
/// <see cref="CorrelatedOuterValues"/>), the statement parameters, and the data at the read timestamp of
/// the statement. The parameters and the timestamp do not change within one execution, and the statement
/// does not see its own writes while it reads: INSERT … SELECT reads every source row before it writes one,
/// and UPDATE and DELETE find their rows in a locate scan before they write. So two runs with the same key
/// give the same result. The prepared subquery that owns a cache belongs to the registry of one execution,
/// which the bound-query cache never keeps (see <see cref="ExistsSubqueryRegistry"/>), so a result never
/// reaches another execution.</para>
///
/// <para><b>When there is no cache.</b> The preparer makes no cache for a body with a volatile function
/// (<c>random()</c>, <c>nextval</c>, <c>clock_timestamp()</c>, see
/// <see cref="ScalarFunctionEvaluator.ContainsVolatileFunction"/>), because each run can give a new value. It
/// makes none for a body that reads a hidden value of an enclosing query level, because the key does not
/// hold that value. A row whose key has an array value gets no entry (<see cref="TryKey"/>).</para>
///
/// <para><b>The key is exact.</b> Two keys are equal only when each value has the same type and the same
/// stored fields: a float compares by its bits, so <c>0.0</c> and <c>-0.0</c> are two keys, and a string
/// compares by ordinal, so <c>'A'</c> and <c>'a'</c> are two keys. A key that SQL calls equal but that is
/// stored differently only costs a run, never a wrong result.</para>
///
/// <para><b>Bounds.</b> An error is not kept: the next row with the same key runs the body again and gets
/// the error again. The cache stops taking entries when its weight reaches <see cref="MaxWeight"/>; an entry
/// weighs 1 plus the number of values it holds (an IN list). Rows after that run the body as before.</para>
///
/// <para><b>Concurrency.</b> One stage evaluates a subquery for one outer row at a time, but a lock keeps the
/// dictionary safe if two runs meet. Two runs that miss the same key both run the body, and the second
/// <see cref="Add"/> is ignored.</para>
/// </summary>
internal sealed class CorrelatedResultCache<T>
{
    /// <summary>The largest total weight of the entries of one cache.</summary>
    internal const int MaxWeight = 16_384;

    private readonly Dictionary<ColumnValue[], T> entries = new(OuterValuesComparer.Instance);

    private readonly Lock sync = new();

    private int weight;

    /// <summary>The reader of the outer values that form the key.</summary>
    public CorrelatedOuterValues Outer { get; }

    public CorrelatedResultCache(CorrelatedOuterValues outer) => Outer = outer;

    /// <summary>
    /// True when <paramref name="values"/> can be a key. An array value has no exact key here, so such a row
    /// runs the body as before.
    /// </summary>
    public static bool TryKey(ColumnValue[] values)
    {
        foreach (ColumnValue value in values)
        {
            if (value.Type == ColumnType.Array)
                return false;
        }

        return true;
    }

    /// <summary>The kept result for <paramref name="key"/>, when there is one.</summary>
    public bool TryGet(ColumnValue[] key, out T value)
    {
        lock (sync)
            return entries.TryGetValue(key, out value!);
    }

    /// <summary>
    /// Keeps <paramref name="value"/> for <paramref name="key"/> when the weight budget allows it.
    /// <paramref name="size"/> is the number of values the result holds, 0 for one value.
    /// </summary>
    public void Add(ColumnValue[] key, T value, int size = 0)
    {
        int entryWeight = 1 + size;

        lock (sync)
        {
            if (weight + entryWeight > MaxWeight)
                return;

            if (entries.TryAdd(key, value))
                weight += entryWeight;
        }
    }

    /// <summary>
    /// True when a cache can keep the results of a body that reads <paramref name="body"/>: no volatile
    /// function, and no hidden value of an enclosing query level (a hidden placeholder that is not in
    /// <paramref name="ownPlaceholders"/>).
    /// </summary>
    internal static bool CanKeepResultsOf(NodeAst? body, IReadOnlySet<NodeAst> ownPlaceholders)
    {
        if (body is null)
            return true;

        if (ScalarFunctionEvaluator.ContainsVolatileFunction(body))
            return false;

        return !NodeAstWalk.Any(body, n =>
            n.nodeType == NodeType.Placeholder
            && n.yytext is { } name
            && name.StartsWith(CorrelatedSubqueryBody.ParameterPrefix, StringComparison.Ordinal)
            && !ownPlaceholders.Contains(n));
    }
}

/// <summary>
/// Compares two keys of a <see cref="CorrelatedResultCache{T}"/> field by field. See the cache summary for
/// why the comparison is exact and not the SQL equality.
/// </summary>
internal sealed class OuterValuesComparer : IEqualityComparer<ColumnValue[]>
{
    public static readonly OuterValuesComparer Instance = new();

    public bool Equals(ColumnValue[]? x, ColumnValue[]? y)
    {
        if (ReferenceEquals(x, y))
            return true;

        if (x is null || y is null || x.Length != y.Length)
            return false;

        for (int i = 0; i < x.Length; i++)
        {
            if (!SameValue(x[i], y[i]))
                return false;
        }

        return true;
    }

    public int GetHashCode(ColumnValue[] key)
    {
        HashCode hash = new();

        foreach (ColumnValue value in key)
        {
            hash.Add(value.Type);
            hash.Add(value.LongValue);
            hash.Add(value.UuidHigh);
            hash.Add(BitConverter.DoubleToInt64Bits(value.FloatValue));
            hash.Add(value.BoolValue);
            hash.Add(value.StrValue, StringComparer.Ordinal);

            if (value.BytesValue is { } bytes)
                hash.AddBytes(bytes);
        }

        return hash.ToHashCode();
    }

    private static bool SameValue(ColumnValue x, ColumnValue y) =>
        x.Type == y.Type
        && x.LongValue == y.LongValue
        && x.UuidHigh == y.UuidHigh
        && BitConverter.DoubleToInt64Bits(x.FloatValue) == BitConverter.DoubleToInt64Bits(y.FloatValue)
        && x.BoolValue == y.BoolValue
        && string.Equals(x.StrValue, y.StrValue, StringComparison.Ordinal)
        && SameBytes(x.BytesValue, y.BytesValue);

    private static bool SameBytes(byte[]? x, byte[]? y) =>
        x is null ? y is null : y is not null && x.AsSpan().SequenceEqual(y);
}

/// <summary>
/// Test-only counters of the runs of correlated subquery bodies and of the results that a
/// <see cref="CorrelatedResultCache{T}"/> gave instead of a run. A test opens a scope with
/// <see cref="Observe"/>, and only statements in its async flow count. Outside a scope the counters cost one
/// read of an empty async-local value. Nothing replaces it in production: it exists so that a test can show
/// how many times a body ran, which the result rows do not show.
/// </summary>
internal sealed class CorrelatedRunProbe
{
    private static readonly AsyncLocal<CorrelatedRunProbe?> Current = new();

    private int runs;

    private int hits;

    /// <summary>The number of times a correlated body ran.</summary>
    public int Runs => Volatile.Read(ref runs);

    /// <summary>The number of times a kept result was used instead of a run.</summary>
    public int Hits => Volatile.Read(ref hits);

    /// <summary>Starts counting in the current async flow, until the returned scope is disposed.</summary>
    public static IDisposable Observe(out CorrelatedRunProbe probe)
    {
        CorrelatedRunProbe? previous = Current.Value;
        probe = new CorrelatedRunProbe();
        Current.Value = probe;
        return new Scope(previous);
    }

    internal static void CountRun()
    {
        if (Current.Value is { } probe)
            Interlocked.Increment(ref probe.runs);
    }

    internal static void CountHit()
    {
        if (Current.Value is { } probe)
            Interlocked.Increment(ref probe.hits);
    }

    private sealed class Scope(CorrelatedRunProbe? previous) : IDisposable
    {
        public void Dispose() => Current.Value = previous;
    }
}
