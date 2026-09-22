/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// The catalog record of a user sequence: everything CamusDB knows about it that the counter
/// itself cannot hold.
///
/// <para><b>The counter is not here, and it is not in this database's keyspace either.</b> The
/// numbers come from Kahuna's sequencer, which stores its record under the reserved
/// <c>__kahuna:</c> namespace that the public key-value API refuses. So a sequence is not a KV key:
/// it cannot join a <c>KvTransaction</c>, a <c>{dbId}</c> prefix scan cannot see it, and every sweep
/// that works by scanning such a prefix — the whole-database purge, the branch metadata copy — must
/// name it from this catalog instead. This record exists because the Kahuna record has nowhere to
/// put a user-facing name, an owning column, or a comment, and because <see cref="MinValue"/> has no
/// Kahuna equivalent at all.</para>
///
/// <para><b><see cref="Id"/> is separate from <see cref="Name"/> on purpose.</b> The Kahuna counter
/// is named after the id, never after the user's name, so <c>ALTER SEQUENCE … RENAME TO</c> stays a
/// metadata-only change and a drop-plus-recreate of the same name cannot inherit the old counter's
/// reserved block. A column default binds to the id for the same reason.</para>
///
/// <para>This record holds no current value. What Kahuna reports is the reserved high-water mark,
/// not the last value issued, and copying it here would publish a number that is wrong by up to a
/// whole block — see <c>SequenceAllocator.ReadReservedCeilingAsync</c>.</para>
/// </summary>
public sealed class SequenceSchema
{
    /// <summary>
    /// Immutable identifier, allocated from the same per-store base-62 counter that mints table and
    /// view ids. Sharing that counter is what guarantees a sequence, a table and a view can never
    /// collide in the metadata keyspace.
    /// </summary>
    public string? Id { get; set; }

    /// <summary>The sequence's name. Changed by <c>ALTER SEQUENCE … RENAME TO</c>; the <see cref="Id"/> is not.</summary>
    public string? Name { get; set; }

    /// <summary>
    /// The value the sequence starts from, and the value <c>ALTER SEQUENCE … RESTART</c> with no
    /// argument returns to. Recorded here because Kahuna's record keeps the initial value only as a
    /// description and a restart has to know where "the start" is.
    /// </summary>
    public long StartValue { get; set; } = 1;

    /// <summary>The step between successive values. Always positive; a descending sequence is out of scope.</summary>
    public long Increment { get; set; } = 1;

    /// <summary>
    /// The lowest value the sequence may hold. Enforced by CamusDB alone — Kahuna's sequencer has no
    /// minimum — so it bounds <c>START WITH</c> and <c>setval</c> and nothing else. A counter that
    /// only climbs cannot fall below it on its own.
    /// </summary>
    public long MinValue { get; set; } = 1;

    /// <summary>
    /// The highest value the sequence may issue, or null for no ceiling. Passed to Kahuna, which
    /// refuses an allocation that would pass it.
    /// </summary>
    public long? MaxValue { get; set; }

    /// <summary>
    /// How many values the owning node reserves per durable commit, or null to follow the node-wide
    /// setting. <c>1</c> is the gap-free register: one Raft commit, with its fsync, per value.
    /// </summary>
    public int? CacheSize { get; set; }

    /// <summary>
    /// The relation id of the column that owns this sequence, or null for a free-standing sequence.
    /// An owned sequence is created by <c>serial</c> / <c>GENERATED AS IDENTITY</c>, is dropped with
    /// its owner, and is refused by a bare <c>DROP SEQUENCE</c>.
    /// </summary>
    /// <remarks>
    /// <para>The <b>relation</b> id, never the storage id. A truncate and a materialized-view
    /// refresh both replace a relation's storage while keeping its identity, and ownership follows
    /// the identity.</para>
    ///
    /// <para><b>Ownership records the relation, not the column, and that is deliberate.</b> A
    /// column's immutable id is minted while the <c>CREATE TABLE</c> delta is applied, which is
    /// after the proposer has already created this sequence — so a column id here could only ever
    /// be filled in by a second schema change, with its own crash window, for information nothing
    /// needs. The owning column is found instead by the link that does exist:
    /// <c>TableColumnSchema.DefaultSequenceId</c>.</para>
    /// </remarks>
    public string? OwnedByTableId { get; set; }

    /// <summary>
    /// Free-text description attached via <c>COMMENT ON SEQUENCE</c>. Null means no comment; an empty
    /// string is a present-but-empty comment, and the two stay distinguishable — the same rule
    /// <see cref="TableSchema.Comment"/> follows.
    /// </summary>
    public string? Comment { get; set; }

    /// <summary>A copy carrying the same values, used where a delta must not mutate the live record in place.</summary>
    public SequenceSchema Clone() => new()
    {
        Id = Id,
        Name = Name,
        StartValue = StartValue,
        Increment = Increment,
        MinValue = MinValue,
        MaxValue = MaxValue,
        CacheSize = CacheSize,
        OwnedByTableId = OwnedByTableId,
        Comment = Comment,
    };
}
