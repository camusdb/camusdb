
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// The refresh fences of every materialized view one drop statement removes, taken together before the
/// statement removes anything.
/// </summary>
/// <remarks>
/// <para>A <c>DROP … CASCADE</c> removes its dependents one replicated delta at a time, and each delta
/// commits at once. If a running refresh of the target, or of a materialized view further down the
/// cascade, refused the drop only after some dependents had gone, the statement would fail with part of
/// the cascade already removed. Taking every fence first moves that refusal before the first
/// deletion.</para>
///
/// <para>The fences are taken in relation-id order, so two statements that need overlapping sets ask
/// for them in the same order. When one fence is held by a running refresh, the fences already taken
/// are released and the statement is refused with <c>RefreshAlreadyInProgress</c>.
/// <see cref="DisposeAsync"/> releases every fence the set took, on every path.</para>
/// </remarks>
internal sealed class RefreshFenceSet : IAsyncDisposable
{
    private readonly DatabaseRegistry registry;

    private readonly List<string> held = [];

    private RefreshFenceSet(DatabaseRegistry registry)
    {
        this.registry = registry;
    }

    /// <summary>
    /// Takes the refresh fence of each listed materialized view. Raises
    /// <c>RefreshAlreadyInProgress</c>, naming the view, when a refresh holds one of them; nothing is
    /// held after a refusal.
    /// </summary>
    internal static async Task<RefreshFenceSet> AcquireAsync(
        DatabaseRegistry registry,
        DatabaseDescriptor database,
        IEnumerable<(string Name, string Id)> materializedViews)
    {
        List<(string Name, string Id)> ordered = materializedViews
            .DistinctBy(view => view.Id, StringComparer.Ordinal)
            .OrderBy(view => view.Id, StringComparer.Ordinal)
            .ToList();

        RefreshFenceSet fences = new(registry);

        try
        {
            foreach ((string name, string id) in ordered)
            {
                string fenceId = RegistryDropMarkers.TableFenceId(database.Id, id);

                if (!await MaterializedViewRefresher.AcquireRefreshFenceAsync(registry, fenceId).ConfigureAwait(false))
                    throw new CamusDBException(
                        CamusDBErrorCodes.RefreshAlreadyInProgress,
                        $"Cannot drop materialized view '{name}' while a REFRESH of it is running; nothing was dropped. " +
                        "Wait for the refresh to finish and retry");

                fences.held.Add(fenceId);
            }
        }
        catch
        {
            await fences.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return fences;
    }

    /// <summary>
    /// Releases every fence the set took, in reverse order. Each release is attempted even when an
    /// earlier one fails, and the first failure is raised after all of them ran. A fence whose release
    /// failed is a lease, so it expires on its own.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        Exception? first = null;

        for (int i = held.Count - 1; i >= 0; i--)
        {
            try
            {
                await registry.DropMarkers.ReleaseDropIntentAsync(held[i]).ConfigureAwait(false);
            }
            catch (Exception error)
            {
                first ??= error;
            }
        }

        held.Clear();

        if (first is not null)
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(first);
    }
}
