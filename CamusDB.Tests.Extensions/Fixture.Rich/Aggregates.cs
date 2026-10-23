/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Text;
using CamusDB.Extensions;

namespace CamusDB.Fixtures.Rich;

/// <summary>Test-only: the geometric mean of the positive values; NULL rows are skipped.</summary>
[CamusAggregate("rx_geomean")]
public sealed class GeoMean : ICamusAggregate<GeoMean.State>
{
    public sealed class State
    {
        public double LogSum;
        public long Count;
    }

    public State Create() => new();

    public void Accumulate(State state, double value)
    {
        if (value > 0)
        {
            state.LogSum += Math.Log(value);
            state.Count++;
        }
    }

    public void Merge(State into, State other)
    {
        into.LogSum += other.LogSum;
        into.Count += other.Count;
    }

    public double? Finish(State state) => state.Count == 0 ? null : Math.Exp(state.LogSum / state.Count);
}

/// <summary>Test-only: counts NULL arguments, which a nullable parameter receives.</summary>
[CamusAggregate("rx_count_nulls")]
public sealed class CountNulls : ICamusAggregate<CountNulls.State>
{
    public sealed class State
    {
        public long Nulls;
        public long Rows;
    }

    public State Create() => new();

    public void Accumulate(State state, long? value)
    {
        state.Rows++;

        if (value is null)
            state.Nulls++;
    }

    public string Finish(State state) => state.Nulls + "/" + state.Rows;
}

/// <summary>Test-only: joins strings with a separator (two arguments), in input order.</summary>
[CamusAggregate("rx_concat", StateSizeHint = 256)]
public sealed class Concat : ICamusAggregate<StringBuilder>
{
    public StringBuilder Create() => new();

    public void Accumulate(StringBuilder state, string value, string separator)
    {
        if (state.Length > 0)
            state.Append(separator);

        state.Append(value);
    }

    public string Finish(StringBuilder state) => state.ToString();
}

/// <summary>Test-only: collects the values into an array result.</summary>
[CamusAggregate("rx_collect")]
public sealed class Collect : ICamusAggregate<List<long>>
{
    public List<long> Create() => new();

    public void Accumulate(List<long> state, long value) => state.Add(value);

    public IReadOnlyList<long> Finish(List<long> state)
    {
        long[] sorted = state.ToArray();
        Array.Sort(sorted);
        return sorted;
    }
}

/// <summary>Test-only: reports how many states it created and finished, so a test sees Create and Finish on zero rows.</summary>
[CamusAggregate("rx_rows")]
public sealed class Rows : ICamusAggregate<Rows.State>
{
    public sealed class State
    {
        public long Count;
    }

    public State Create() => new();

    public void Accumulate(State state, long value) => state.Count++;

    public string Finish(State state) => "rows:" + state.Count;
}

/// <summary>Test-only: throws on the value 13.</summary>
[CamusAggregate("rx_fail_on13")]
public sealed class FailOn13 : ICamusAggregate<FailOn13.State>
{
    public sealed class State
    {
        public long Sum;
    }

    public State Create() => new();

    public void Accumulate(State state, long value)
    {
        if (value == 13)
            throw new InvalidOperationException("unlucky 13");

        state.Sum += value;
    }

    public long Finish(State state) => state.Sum;
}
