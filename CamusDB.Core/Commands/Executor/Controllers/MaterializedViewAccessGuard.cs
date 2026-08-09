
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// The two rules that separate a materialized view from the ordinary table it is stored as.
///
/// <para>They live together, and outside the storage layer, because both are about what a
/// materialized view <em>means</em> rather than about how it is stored: its rows are a query's output,
/// not user data, so nobody may write them directly, and rows it has never been given are absent
/// rather than empty. Everything else about it — indexes, statistics, scans, backup, branching —
/// deliberately goes through the unmodified table machinery.</para>
/// </summary>
internal static class MaterializedViewAccessGuard
{
    /// <summary>
    /// Refuses direct DML against a materialized view.
    /// </summary>
    /// <remarks>
    /// Its contents are defined by its query, so a row written by hand would be silently discarded by
    /// the next <c>REFRESH</c> — the write would appear to work and then evaporate, which is worse
    /// than being told no. <c>REFRESH MATERIALIZED VIEW</c> is the only way to change them.
    /// </remarks>
    internal static void RequireWritable(TableDescriptor table)
    {
        if (!table.Schema.IsMaterializedView)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.ViewNotUpdatable,
            $"'{table.Name}' is a materialized view and cannot be written to directly; its contents come " +
            "from its defining query. Use REFRESH MATERIALIZED VIEW to update them, or write to the " +
            "underlying table.");
    }

    /// <summary>
    /// Refuses a read of a materialized view that has never been populated.
    /// </summary>
    /// <remarks>
    /// Returning zero rows instead would make a forgotten <c>REFRESH</c> indistinguishable from a
    /// correct empty answer — the query would be believed. PostgreSQL raises the same error for the
    /// same reason.
    /// </remarks>
    internal static void RequireReadable(TableDescriptor table)
    {
        if (!table.Schema.IsMaterializedView || table.Schema.IsPopulated)
            return;

        throw new CamusDBException(
            CamusDBErrorCodes.MaterializedViewNotPopulated,
            $"Materialized view '{table.Name}' has not been populated. Run " +
            $"REFRESH MATERIALIZED VIEW {table.Name} before querying it.");
    }
}
