
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.Catalogs.Meta;

/// <summary>
/// Reads the per-version column layouts that let a row written under an older schema still be
/// decoded. The family is append-only: once a version is recorded it never changes, so a reader
/// may cache an entry and load it under its own read timestamp without re-validating.
///
/// <para><b>History is loaded lazily, and that is deliberate.</b> A table read from disk has
/// <c>SchemaHistory == null</c> plus the loader delegate that
/// <see cref="ConfigureSchemaHistoryLoader"/> installs; versions are fetched only when a decode
/// actually needs one. Never assume the in-memory list is complete, and never make the loader
/// eager — a database with many tables and long histories would pay the whole cost on open.</para>
///
/// <para><see cref="PreloadContentsRetirementHistoriesAsync"/> is the one exception, and it exists
/// for an ordering reason rather than a performance one: it must run <b>before</b> the checkpoint
/// transaction opens, never inside it. Reading history through the checkpoint's own transaction
/// would make that transaction wait on intents it is itself about to write.</para>
/// </summary>
internal static class SchemaHistoryStore
{
    internal static void ConfigureSchemaHistoryLoader(DatabaseDescriptor database, TableSchema table)
    {
        string tableId = table.Id ?? "";
        table.SchemaHistoryLoader = (txId, version) =>
            new ValueTask<TableSchemaHistory?>(LoadSchemaHistoryEntryAsync(database, tableId, txId, version));
    }

    private static async Task<TableSchemaHistory?> LoadSchemaHistoryEntryAsync(DatabaseDescriptor database, string tableId, HLCTimestamp txId, int version)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;

        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) =
            await kahuna.LocateAndTryGetValue(
                txId,
                MetaKeys.HistoryKey(database.Id, tableId, version),
                -1,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                CancellationToken.None
            ).ConfigureAwait(false);

        if (type != KeyValueResponseType.Get || entry?.Value is null)
            return null;

        return MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.TableSchemaHistory);
    }

    /// <summary>
    /// Loads the column layouts every pending retirement needs to carry, <b>before</b> the checkpoint
    /// transaction opens.
    /// </summary>
    /// <remarks>
    /// <para>The timing is the whole point, and getting it wrong hangs the node. These reads use the
    /// non-transactional snapshot, which cannot see a transaction's own unresolved write intents — it
    /// waits for them to resolve. Run inside the checkpoint transaction, after that transaction has
    /// already staged a meta write, the scan would wait on an intent only its own caller could
    /// resolve, and the caller is the thing waiting. Read first, write second.</para>
    ///
    /// <para>Idempotent and cheap to repeat: a retry after a failed checkpoint simply reloads the same
    /// append-only layouts.</para>
    /// </remarks>
    internal static async Task PreloadContentsRetirementHistoriesAsync(DatabaseDescriptor database)
    {
        foreach (ContentsRetirementIntent intent in database.PendingContentsRetirements())
        {
            // The retained rows were written under the source relation's schema versions, and the
            // history keys that describe those versions are append-only under the source relation's
            // id. A recovery publishes the contents under a *new* relation id, so it cannot reach
            // them there — they are copied into the record instead.
            intent.RetiredSchema.SchemaHistory =
                await LoadAllSchemaHistoryAsync(database, intent.SourceTableId).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Reads every persisted column layout recorded for <paramref name="tableId"/>, oldest version
    /// first. Used when a retired contents generation has to carry its own decoding history.
    /// </summary>
    private static async Task<List<TableSchemaHistory>> LoadAllSchemaHistoryAsync(DatabaseDescriptor database, string tableId)
    {
        string prefix = MetaKeys.HistoryKeyPrefix(database.Id, tableId);
        List<TableSchemaHistory> history = [];

        await foreach ((string key, ReadOnlyKeyValueEntry entry) in database.Kahuna.Kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, MetaKeys.MetaBucketPrefix(database.Id), null, true, null, true, 512,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false))
        {
            if (!key.StartsWith(prefix, StringComparison.Ordinal) || entry.Value is not { Length: > 0 })
                continue;

            history.Add(MetaJsonSerializer.Deserialize(entry.Value, MetaJsonContext.Default.TableSchemaHistory));
        }

        history.Sort(static (a, b) => a.Version.CompareTo(b.Version));
        return history;
    }
}
