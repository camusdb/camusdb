/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// One <see cref="ForeignKeySchema"/> resolved against the live schema: everything a DML statement
/// needs to check the constraint, computed once per schema change instead of once per row.
///
/// <para>Built only by <see cref="ForeignKeyGraph.Build"/> and published with the graph, so a plan is
/// never mutated after a reader can see it. A rename, drop or index change is a schema delta, and every
/// delta rebuilds the graph, so the resolved names here are never staler than the schema they came
/// from.</para>
///
/// <para><b>Unresolved plans.</b> If an id no longer resolves — the parent table, a column or an index
/// is gone — the plan is kept with <see cref="IsResolved"/> false and an
/// <see cref="UnresolvedReason"/>, and it is not enforced. A database must still open with a stale
/// constraint in it; the DDL guards make the state unreachable, so this is defense in depth. The plan is
/// still indexed under both tables, so a DDL guard that asks "is this table referenced?" keeps answering
/// yes.</para>
/// </summary>
public sealed class ForeignKeyPlan
{
    /// <summary>The persisted constraint this plan resolves.</summary>
    public ForeignKeySchema Constraint { get; }

    /// <summary>Id of the table that owns the constraint (the referencing, child table).</summary>
    public string ChildTableId { get; }

    /// <summary>Id of the referenced (parent) table.</summary>
    public string ParentTableId => Constraint.ReferencedTableId;

    /// <summary>True when the constraint references its own table.</summary>
    public bool IsSelfReference => string.Equals(ChildTableId, ParentTableId, StringComparison.Ordinal);

    /// <summary>False when an id did not resolve. An unresolved plan is never enforced.</summary>
    public bool IsResolved => UnresolvedReason is null;

    /// <summary>Why the plan did not resolve, or null when it did.</summary>
    public string? UnresolvedReason { get; }

    /// <summary>True when DML must check the constraint: resolved, and WriteOnly or Public.</summary>
    public bool IsEnforced => IsResolved && Constraint.IsEnforced;

    /// <summary>Current names of the child columns, in constraint order. Empty when unresolved.</summary>
    public string[] ChildColumnNames { get; }

    /// <summary>Current names of the parent columns, in constraint order. Empty when unresolved.</summary>
    public string[] ParentColumnNames { get; }

    /// <summary>
    /// For each key position of the parent index, the constraint position that supplies its value:
    /// the parent key of a child row is built from <c>ChildColumnNames[ParentKeyOrder[j]]</c> for
    /// j = 0..n-1. The constraint may list its columns in a different order from the index.
    /// </summary>
    public int[] ParentKeyOrder { get; }

    /// <summary>Per-position sort directions of the parent index, or null for all ascending.</summary>
    public OrderType[]? ParentKeyDirections { get; }

    /// <summary>
    /// For each leading key position of the backing index, the constraint position that supplies its
    /// value. Only the first <c>ColumnIds.Length</c> positions of the index are covered.
    /// </summary>
    public int[] BackingKeyOrder { get; }

    /// <summary>Per-position sort directions of the backing index, or null for all ascending.</summary>
    public OrderType[]? BackingKeyDirections { get; }

    internal ForeignKeyPlan(
        ForeignKeySchema constraint,
        string childTableId,
        string[] childColumnNames,
        string[] parentColumnNames,
        int[] parentKeyOrder,
        OrderType[]? parentKeyDirections,
        int[] backingKeyOrder,
        OrderType[]? backingKeyDirections)
    {
        Constraint = constraint;
        ChildTableId = childTableId;
        ChildColumnNames = childColumnNames;
        ParentColumnNames = parentColumnNames;
        ParentKeyOrder = parentKeyOrder;
        ParentKeyDirections = parentKeyDirections;
        BackingKeyOrder = backingKeyOrder;
        BackingKeyDirections = backingKeyDirections;
    }

    internal ForeignKeyPlan(ForeignKeySchema constraint, string childTableId, string unresolvedReason)
    {
        Constraint = constraint;
        ChildTableId = childTableId;
        UnresolvedReason = unresolvedReason;
        ChildColumnNames = [];
        ParentColumnNames = [];
        ParentKeyOrder = [];
        BackingKeyOrder = [];
    }
}
