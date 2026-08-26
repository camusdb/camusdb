/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// One physical contents generation that a committed schema delta has just detached from a live
/// relation, together with everything needed to make it recoverable on disk.
///
/// <para><b>Why an intent and not a direct write.</b> The apply that detaches the generation runs
/// inside the schema partition's commit pipeline, where issuing a KV write would deadlock the
/// partition. Apply therefore records this object in memory and the durable work happens afterwards —
/// in the proposer's checkpoint transaction, or, when that checkpoint never ran, in the reconciliation
/// that follows a WAL restore. Both paths write the same record, so a crash between commit and
/// checkpoint costs nothing but a delay.</para>
///
/// <para><b>Why it is self-contained.</b> Several truncates can commit between two checkpoints. Only
/// the last one's storage id is live afterwards, so an intermediate generation cannot be described by
/// reading the schema later — the schema no longer mentions it. Everything the reclaimer and a
/// recovery need is copied in here at capture time instead.</para>
/// </summary>
internal sealed class ContentsRetirementIntent
{
    /// <summary>The still-live relation whose contents these were.</summary>
    public required string SourceTableId { get; init; }

    /// <summary>The relation's name at capture time, for the record's human-facing label.</summary>
    public required string SourceTableName { get; init; }

    /// <summary>The key-space the relation stopped reading.</summary>
    public required string RetiredStorageId { get; init; }

    /// <summary>
    /// Every index id the retired generation ever allocated. Frozen rather than recomputed: an index
    /// dropped before the truncate still owns entries in that key-space, and the live schema no longer
    /// names it.
    /// </summary>
    public required string[] RetiredIndexIds { get; init; }

    /// <summary>The key-space the relation adopted.</summary>
    public required string NewStorageId { get; init; }

    /// <summary>The index ids the new generation starts with — used to initialize its catalog.</summary>
    public required string[] NewIndexIds { get; init; }

    /// <summary>
    /// When the generation was retired. Drives the retention window, and is an HLC rather than a wall
    /// clock so every node agrees on when the record becomes reclaimable.
    /// </summary>
    public required HLCTimestamp RetiredAt { get; init; }

    /// <summary>
    /// The relation's definition as it was when it owned the retired key-space: the column layout the
    /// retained rows were encoded against, the indexes they were indexed by, and the schema version
    /// they carry. A recovery rebuilds a readable relation from this alone.
    /// </summary>
    public required TableSchema RetiredSchema { get; init; }
}
