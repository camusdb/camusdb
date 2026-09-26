/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Persisted definition of one <c>FOREIGN KEY … REFERENCES</c> constraint, stored in the
/// <see cref="TableSchema.ForeignKeys"/> list of the <b>referencing (child)</b> table.
///
/// <para><b>Ids only.</b> Apart from the constraint's own <see cref="Name"/>, nothing here is a name:
/// columns, the parent table and both indexes are referenced by their immutable ids. A rename of either
/// table, of a column or of an index is therefore a metadata-only change that cannot leave a stale name
/// behind — the gap that CHECK constraints, which store column names, still have. Names are resolved
/// from the live schema when something needs them (see <see cref="ForeignKeyGraph"/>).</para>
///
/// <para><b>Child side only.</b> The parent table records nothing about who references it. A constraint
/// change therefore touches exactly one table, which the schema checkpoint writer requires (it persists
/// one table per log entry), and dropping a child never has to rewrite its parent. The reverse direction
/// is answered by <see cref="ForeignKeyGraph"/>, rebuilt in memory after every schema change.</para>
///
/// <para>Immutable: a state change replaces the instance (<see cref="WithState"/>), the same discipline
/// as <see cref="TableIndexSchema"/>, so a published schema can be read without a lock.</para>
/// </summary>
public sealed class ForeignKeySchema
{
    /// <summary>Immutable ObjectId of the constraint. Never reused, and survives a rename.</summary>
    public string Id { get; }

    /// <summary>
    /// User-visible constraint name. Unique on the table across CHECK, named NOT NULL and foreign-key
    /// constraints, because <c>DROP CONSTRAINT name</c> resolves across all three.
    /// </summary>
    public string Name { get; }

    /// <summary>Ids of the referencing (child) columns, in constraint order.</summary>
    public string[] ColumnIds { get; }

    /// <summary>
    /// Id of the referenced (parent) relation: its <see cref="TableSchema.Id"/>, never its storage id,
    /// because a TRUNCATE or a materialized-view refresh replaces the storage id while the relation
    /// keeps its identity.
    /// </summary>
    public string ReferencedTableId { get; }

    /// <summary>Ids of the referenced (parent) columns, paired by position with <see cref="ColumnIds"/>.</summary>
    public string[] ReferencedColumnIds { get; }

    /// <summary>
    /// <see cref="TableIndexSchema.KvId"/> of the parent's unique index (or <c>~pk</c>) whose key columns
    /// are exactly <see cref="ReferencedColumnIds"/>. Its entry key is the point a child writer locks and
    /// a parent writer deletes, so the index must outlive the constraint.
    /// </summary>
    public string ReferencedIndexId { get; }

    /// <summary>
    /// <see cref="TableIndexSchema.KvId"/> of the child index whose leading columns are exactly
    /// <see cref="ColumnIds"/>. The parent-side check probes it; without it that check would be a full
    /// scan. It is either an index the user already had, or one the engine created for this constraint
    /// (<see cref="TableIndexSchema.OwnerConstraintId"/> equals <see cref="Id"/>).
    /// </summary>
    public string BackingIndexId { get; }

    /// <summary>What a DELETE of a referenced parent row does. Only NoAction and Restrict are enforced.</summary>
    public ForeignKeyAction OnDelete { get; }

    /// <summary>What an UPDATE of a referenced parent key does. Only NoAction and Restrict are enforced.</summary>
    public ForeignKeyAction OnUpdate { get; }

    /// <summary>How a partly-NULL composite key is treated. Only Simple is enforced.</summary>
    public ForeignKeyMatch Match { get; }

    /// <summary>
    /// Online rollout state. <see cref="SchemaElementState.WriteOnly"/> means enforced on both sides but
    /// not yet validated against the rows that existed before; <see cref="SchemaElementState.Public"/>
    /// means validated. Absent constraints are not kept in the list.
    /// </summary>
    public SchemaElementState State { get; }

    /// <summary>True when DML must check this constraint: every state from WriteOnly up.</summary>
    [JsonIgnore]
    public bool IsEnforced => State is SchemaElementState.WriteOnly or SchemaElementState.Public;

    [JsonConstructor]
    public ForeignKeySchema(
        string id,
        string name,
        string[] columnIds,
        string referencedTableId,
        string[] referencedColumnIds,
        string referencedIndexId,
        string backingIndexId,
        ForeignKeyAction onDelete,
        ForeignKeyAction onUpdate,
        ForeignKeyMatch match,
        SchemaElementState state)
    {
        Id = id;
        Name = name;
        ColumnIds = columnIds;
        ReferencedTableId = referencedTableId;
        ReferencedColumnIds = referencedColumnIds;
        ReferencedIndexId = referencedIndexId;
        BackingIndexId = backingIndexId;
        OnDelete = onDelete;
        OnUpdate = onUpdate;
        Match = match;
        State = state;
    }

    /// <summary>Returns a copy in <paramref name="state"/>. Every other field is carried unchanged.</summary>
    public ForeignKeySchema WithState(SchemaElementState state) =>
        new(Id, Name, ColumnIds, ReferencedTableId, ReferencedColumnIds, ReferencedIndexId, BackingIndexId, OnDelete, OnUpdate, Match, state);
}
