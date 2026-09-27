/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core;

namespace CamusDB.Tests.Utils;

/// <summary>
/// Counts every <see cref="CamusDBException"/> thrown on one async flow, rethrows included, which is
/// what an exception trace of a server counts. Each await of a faulted task throws again, so a test
/// that proves a path carries an error as a value asserts a count of zero here.
///
/// <para>The count is scoped to the flow that called <see cref="CountAsync"/> and the tasks it starts,
/// so tests running concurrently in other flows do not add to it.</para>
/// </summary>
public static class EngineThrowCounter
{
    private static readonly AsyncLocal<StrongBox<int>?> Current = new();

    static EngineThrowCounter()
    {
        AppDomain.CurrentDomain.FirstChanceException += (_, e) =>
        {
            if (e.Exception is CamusDBException && Current.Value is { } counter)
                Interlocked.Increment(ref counter.Value);
        };
    }

    /// <summary>Runs <paramref name="body"/> and returns how many engine exceptions it threw.</summary>
    public static async Task<int> CountAsync(Func<Task> body)
    {
        StrongBox<int> counter = new();
        Current.Value = counter;
        try
        {
            await body();
        }
        finally
        {
            Current.Value = null;
        }

        return counter.Value;
    }
}
