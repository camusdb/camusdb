
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading.Tasks;

using Nito.AsyncEx;

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.Utils;

/// <summary>
/// Test-only: holds every statement that opens a table, until <see cref="Open"/>. It replaces the cached
/// descriptor of the table with one that waits, so the statement stops in the middle of its real entry
/// point and the engine itself is unchanged. A schema change of the table drops the gate.
/// </summary>
internal sealed class TableGate
{
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(20);

    private readonly TaskCompletionSource reached = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly TaskCompletionSource open = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private TableGate()
    {
    }

    /// <summary>Completes when a statement waits at the gate. Fails when none arrives in time.</summary>
    public Task Reached => reached.Task.WaitAsync(Deadline);

    /// <summary>Lets the statements at the gate, and every later one, continue.</summary>
    public void Open() => open.TrySetResult();

    /// <summary>Installs a gate on a table that this engine already opened once.</summary>
    public static async Task<TableGate> InstallAsync(DatabaseDescriptor database, string table)
    {
        TableDescriptor real = await database.TableDescriptors[table];
        TableGate gate = new();

        database.TableDescriptors[table] = new AsyncLazy<TableDescriptor>(async () =>
        {
            gate.reached.TrySetResult();
            await gate.open.Task.ConfigureAwait(false);
            return real;
        });

        return gate;
    }
}
