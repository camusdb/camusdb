/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Extensions;

[assembly: CamusExtension("cross_kind", "1.0.0")]

namespace CamusDB.Fixtures.CrossKindDuplicate;

/// <summary>Test-only: a scalar function and an aggregate with one name, which must fail the boot.</summary>
public static class Functions
{
    [CamusFunction("ck_same")]
    public static long Same(long value) => value;
}

[CamusAggregate("ck_same")]
public sealed class SameAggregate : ICamusAggregate<SameAggregate.State>
{
    public sealed class State
    {
        public long Sum;
    }

    public State Create() => new();

    public void Accumulate(State state, long value) => state.Sum += value;

    public long Finish(State state) => state.Sum;
}
