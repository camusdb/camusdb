
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Nito.AsyncEx;
using Microsoft.Extensions.Logging;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Closes database descriptors, stopping the associated Kahuna node and freeing memory.
/// </summary>
internal sealed class DatabaseCloser : IAsyncDisposable
{
    private readonly DatabaseDescriptors databaseDescriptors;

    private readonly ILogger<ICamusDB> logger;

    public DatabaseCloser(DatabaseDescriptors databaseDescriptors, ILogger<ICamusDB> logger)
    {
        this.databaseDescriptors = databaseDescriptors;
        this.logger = logger;
    }

    public async Task Close(string id)
    {
        if (!databaseDescriptors.Descriptors.TryRemove(id, out AsyncLazy<DatabaseDescriptor>? databaseDescriptorLazy))
            return;

        DatabaseDescriptor databaseDescriptor = await databaseDescriptorLazy;

        // Roll back transactions still active at close time while the node is alive, so the coordinator
        // releases their working set (including range locks) cleanly rather than leaving them to lapse
        // on the session timeout. Best-effort: a failure here must not block the close.
        try
        {
            await databaseDescriptor.Transactions.RollbackAllActiveAsync().ConfigureAwait(false);
        }
        catch
        {
            // The abandoned sessions are reclaimed by the coordinator reaper on their timeout.
        }

        databaseDescriptor.Dispose();

        Log.LogDatabaseClosed(logger, databaseDescriptor.Name);
    }

    public async ValueTask DisposeAsync()
    {
        // Keyed by database id, not name — the descriptor cache is id-keyed so a rename cannot orphan
        // an entry. Naming this variable "name" is how it reads as a by-name lookup that it is not.
        foreach (string id in databaseDescriptors.Descriptors.Keys.ToList())
            await Close(id).ConfigureAwait(false);
    }
}
