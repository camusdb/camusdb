
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.Transactions;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.Catalogs.Meta;

/// <summary>
/// Makes a replaced key-space recoverable after a relation's <em>contents</em> are swapped while its
/// <em>identity</em> survives — a TRUNCATE, or a materialized-view refresh that adopts a freshly
/// built generation.
///
/// <para><b>The problem this solves:</b> after the swap, nothing in the schema names the old
/// generation any more. Its rows and index entries are still on disk, and without a record pointing
/// at them they are unreachable — not deleted, just invisible to every scan and to the collector.
/// The orphan record written here is the only thing that still names them.</para>
///
/// <para><b>Capture and persist are deliberately separate, and run in different places.</b>
/// <see cref="CaptureContentsRetirementIntent"/> is called from the apply pipeline and is pure: it
/// reads the payload and records an in-memory intent, touching no KV, because a write from inside
/// that pipeline re-enters the schema partition and deadlocks it.
/// <see cref="PersistContentsRetirementsAsync"/> then writes the intents from the proposer's
/// checkpoint transaction, where a KV write is safe.</para>
///
/// <para><b>Every write here is idempotent</b> — fixed key, fixed content — so a re-run after a
/// failed commit overwrites with the same bytes. That matters because the alternative to
/// idempotency is a replay that resurrects a reference to storage the collector has already
/// purged.</para>
/// </summary>
internal static class ContentsRetirementStore
{
    /// <summary>
    /// Writes the durable half of every contents generation this node has detached but not yet
    /// recorded, in the caller's transaction. Returns what it wrote so the caller can forget those
    /// intents once the transaction has actually committed.
    /// </summary>
    /// <remarks>
    /// <para>Both the live checkpoint and post-restore reconciliation call this, and they must produce
    /// the same metadata — a crash between the schema-log commit and the checkpoint has to cost a
    /// delay, not a lost key-space. Writing them all in one transaction also keeps a run of truncates
    /// that committed between two checkpoints atomic: either every intermediate generation becomes
    /// recoverable or none does.</para>
    ///
    /// <para>Idempotent. Every write is a fixed key with fixed content, so re-running after a failed
    /// commit overwrites with the same bytes.</para>
    /// </remarks>
    internal static async Task<IReadOnlyList<ContentsRetirementIntent>> PersistContentsRetirementsAsync(
        DatabaseDescriptor database, KvTransaction tx)
    {
        ContentsRetirementIntent[] pending = database.PendingContentsRetirements();
        if (pending.Length == 0)
            return pending;

        IKahuna kahuna = database.Kahuna.Kahuna;

        foreach (ContentsRetirementIntent intent in pending)
        {
            byte[] orphanBytes = MetaJsonSerializer.Serialize(new OrphanTableRecord
            {
                Kind = OrphanKind.RetiredContents,
                // The record is addressed by the key-space it protects. Storage ids are globally
                // allocated, so this cannot collide with another relation's record even though on a
                // first truncate it is also the still-live source relation's id.
                TableId = intent.RetiredStorageId,
                SourceTableId = intent.SourceTableId,
                RetiredStorageId = intent.RetiredStorageId,
                FormerName = intent.SourceTableName,
                DroppedAt = intent.RetiredAt,
                Schema = intent.RetiredSchema,
            }, MetaJsonContext.Default.OrphanTableRecord);

            await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.OrphanKey(database.Id, intent.RetiredStorageId), orphanBytes).ConfigureAwait(false);

            // Freeze what the retired generation owns. Recomputing it at reclaim time from the live
            // schema would miss every index dropped before the truncate, leaking those entries.
            byte[] retiredCatalog = MetaJsonSerializer.Serialize(intent.RetiredIndexIds, MetaJsonContext.Default.StringArray);
            await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.KeyspaceCatalogKey(database.Id, intent.RetiredStorageId), retiredCatalog).ConfigureAwait(false);

            // Seed the new generation's catalog in the same step, so a DROP DATABASE that runs before
            // the next schema persist still finds the new key-space.
            byte[] newCatalog = MetaJsonSerializer.Serialize(intent.NewIndexIds, MetaJsonContext.Default.StringArray);
            await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.KeyspaceCatalogKey(database.Id, intent.NewStorageId), newCatalog).ConfigureAwait(false);
        }

        return pending;
    }

    /// <summary>
    /// Reads the persisted grow-only index-id catalog for one storage generation, unioned with the
    /// indexes the relation currently has.
    /// </summary>
    /// <remarks>
    /// Called by the proposer, never from apply. The result is frozen into the schema-log payload so
    /// every node — including one replaying the entry long afterwards — retires exactly the same set
    /// of index key-spaces without reading anything.
    /// </remarks>
    internal static async Task<string[]> ReadStorageIndexCatalogAsync(DatabaseDescriptor database, TableSchema tableSchema)
    {
        HashSet<string> ids = [];

        string storageId = tableSchema.EffectiveStorageId;

        foreach (string candidateKey in string.Equals(storageId, tableSchema.Id, StringComparison.Ordinal)
                     ? new[] { MetaKeys.KeyspaceCatalogKey(database.Id, storageId) }
                     : [MetaKeys.KeyspaceCatalogKey(database.Id, storageId), MetaKeys.KeyspaceCatalogKey(database.Id, tableSchema.Id ?? "")])
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await database.Kahuna.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, candidateKey, -1, HLCTimestamp.Zero,
                KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false);

            if (type == KeyValueResponseType.Get && entry?.Value is { Length: > 0 } bytes)
            {
                foreach (string id in MetaJsonSerializer.Deserialize(bytes, MetaJsonContext.Default.StringArray))
                    ids.Add(id);
            }
        }

        if (tableSchema.Indexes is not null)
        {
            foreach (TableIndexSchema index in tableSchema.Indexes)
                if (!string.IsNullOrEmpty(index.Id))
                    ids.Add(index.Id);
        }

        return [.. ids];
    }

    /// <summary>
    /// Completes the durable half of a materialized-view refresh swap: retires the key-space the view
    /// has just stopped reading, and removes the meta key of the relation the rebuild was staged
    /// under.
    ///
    /// <para>Runs in the same checkpoint transaction that publishes the view's new schema, so the
    /// switch-over is one durable step — a crash cannot leave the view pointing at a key-space that
    /// has already been retired, or leave both key-spaces live with nothing recording which is which.</para>
    ///
    /// <para>The retirement is a <b>deferred</b> drop: the previous contents keep their orphan record,
    /// stay <c>RELINK</c>-able for the retention window, and are then reclaimed by the ordinary orphan
    /// collector. Without it every refresh would leak a complete copy of the materialized view.</para>
    /// </summary>
    /// <remarks>
    /// The replaced key-space has no meta key of its own — the view's single meta key described it —
    /// so its orphan record is built from the schema that key <em>still</em> holds at this point: the
    /// checkpoint has not overwritten it yet. Idempotent on replay: a record whose key-space no longer
    /// appears in the stored schema means this already ran, and writing it again would resurrect a
    /// reference to storage the collector may have purged.
    /// </remarks>
    internal static async Task RetireReplacedMaterializedViewStorageAsync(
        DatabaseDescriptor database,
        TableSchema view,
        string stagingTableId,
        SchemaChangeLogEntry entry,
        KvTransaction tx)
    {
        IKahuna kahuna = database.Kahuna.Kahuna;

        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? stored) = await kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, MetaKeys.TableKey(database.Id, view.Id!), -1, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, CancellationToken.None).ConfigureAwait(false);

        if (getType == KeyValueResponseType.Get && stored?.Value is { Length: > 0 })
        {
            TableSchema previous = MetaJsonSerializer.Deserialize(stored.Value, MetaJsonContext.Default.TableSchema);
            string retiredStorageId = previous.EffectiveStorageId;

            // Equal means the stored schema already names the new key-space: this entry is a replay.
            if (!string.Equals(retiredStorageId, view.EffectiveStorageId, StringComparison.Ordinal))
            {
                // The orphan record stands for the retired key-space, so it must present itself as a
                // relation whose id *is* that key-space — that is what relink and the collector act on.
                previous.Id = retiredStorageId;
                previous.StorageId = null;

                byte[] orphanBytes = MetaJsonSerializer.Serialize(new OrphanTableRecord
                {
                    TableId = retiredStorageId,
                    FormerName = view.Name ?? "",
                    DroppedAt = entry.Ts,
                    Schema = previous,
                }, MetaJsonContext.Default.OrphanTableRecord);

                await MetaKeyWriter.WriteMetaKey(kahuna, tx, MetaKeys.OrphanKey(database.Id, retiredStorageId), orphanBytes).ConfigureAwait(false);
            }
        }

        // The staging relation is gone from the schema; its meta key must go with it, or a reopen
        // would load a relation that no statement can reach and that owns the view's live key-space.
        await MetaKeyWriter.DeleteMetaKey(kahuna, tx, MetaKeys.TableKey(database.Id, stagingTableId)).ConfigureAwait(false);
    }

    /// <summary>
    /// Records, in memory, the contents generation a TruncateTable delta is about to detach — so the
    /// durable retirement can be written later, by whichever path gets there first.
    /// </summary>
    /// <remarks>
    /// <para><b>Call this under the schema lock, immediately before applying the delta.</b> It reads
    /// the pre-swap schema: that is where the retired generation's column layout, index list and
    /// schema version still are. After the swap the relation describes the new, empty key-space and
    /// nothing left in memory says what the old one contained.</para>
    ///
    /// <para>Guarded by the same compare-and-swap the apply uses, so it is silent on a replay and on a
    /// relation that has already moved on. That guard is also what keeps a replayed entry from
    /// resurrecting an orphan record that retention has already purged: after a checkpoint the stored
    /// schema names the new storage id, so the expectation no longer matches.</para>
    ///
    /// <para>Index ids come from the payload, not from the live schema, because the retired generation
    /// owns entries for every index it ever had — including ones dropped before the truncate, which
    /// the live schema no longer names.</para>
    /// </remarks>
    internal static void CaptureContentsRetirementIntent(DatabaseDescriptor database, SchemaChangeLogEntry entry)
    {
        if (entry.Op != SchemaOp.TruncateTable)
            return;

        SchemaTruncateTablePayload payload = SchemaDeltaApplier.DecodePayload<SchemaTruncateTablePayload>(entry);

        TableSchema? live = SchemaDeltaApplier.FindRelationById(database.Schema, payload.TableId);
        if (live is null)
            return;

        if (!string.Equals(live.EffectiveStorageId, payload.ExpectedStorageId, StringComparison.Ordinal) ||
            live.ContentsGeneration != payload.ExpectedContentsGeneration)
            return;

        // The record has to present itself as a relation whose id *is* the retired key-space: that is
        // what a recovery reads its rows through, and what the collector purges.
        TableSchema retired = SchemaReplicator.CloneTable(live);
        retired.Id = payload.ExpectedStorageId;
        retired.StorageId = null;
        retired.Name = payload.TableName;
        retired.ContentsValidFrom = null;
        // A loader bound to the live relation would go on serving the live table's history keys after
        // this snapshot is persisted; the checkpoint fills the layouts in explicitly instead.
        retired.SchemaHistoryLoader = null;

        database.AddContentsRetirement(new ContentsRetirementIntent
        {
            SourceTableId = payload.TableId,
            SourceTableName = payload.TableName,
            RetiredStorageId = payload.ExpectedStorageId,
            RetiredIndexIds = payload.RetiredIndexIds,
            NewStorageId = payload.NewStorageId,
            NewIndexIds = payload.NewIndexIds,
            RetiredAt = payload.ContentsValidFrom,
            RetiredSchema = retired,
        });
    }
}
