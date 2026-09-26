/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Every foreign key of one database, indexed in both directions: by the table that owns the constraint
/// (<see cref="ChildPlansOf"/>) and by the table it references (<see cref="ParentPlansOf"/>).
///
/// <para><b>Why a published graph.</b> A parent DELETE has to find every table that references it, and
/// the constraint is stored only on the child. The graph answers that in O(1). It is built from
/// <c>Schema.Tables</c>, which holds every live table whether or not a descriptor for it is open on this
/// node — a lookup through the descriptor cache would miss a child that nobody has opened yet, and the
/// parent would delete rows that child still references.</para>
///
/// <para><b>Publication.</b> The graph is immutable. <c>Schema</c> rebuilds it at the same points as the
/// relation-name index — after every applied schema delta, before the version advances, and after every
/// wholesale schema load — and swaps the reference. A lock-free reader sees the graph before the delta or
/// the graph after it, never a torn one. A parent's DML reads the graph rather than its own descriptor,
/// so a constraint added on a child needs no eviction of the parent's descriptor.</para>
///
/// <para><b>Cost.</b> A database with no foreign key shares <see cref="Empty"/>; a DML statement then pays
/// one reference read and one <see cref="IsEmpty"/> test.</para>
/// </summary>
public sealed class ForeignKeyGraph
{
    private static readonly Dictionary<string, ForeignKeyPlan[]> NoPlans = new(0, StringComparer.Ordinal);

    /// <summary>The shared graph of a database that has no foreign key.</summary>
    public static ForeignKeyGraph Empty { get; } = new(NoPlans, NoPlans, 0, []);

    private readonly Dictionary<string, ForeignKeyPlan[]> byChild;

    private readonly Dictionary<string, ForeignKeyPlan[]> byParent;

    /// <summary>True when the database has no foreign key at all, in any state.</summary>
    public bool IsEmpty => Count == 0;

    /// <summary>Number of constraints in the graph, resolved or not.</summary>
    public int Count { get; }

    /// <summary>
    /// A description of each constraint that did not resolve. Such a constraint is not enforced; the
    /// schema loader logs each entry as a warning.
    /// </summary>
    public IReadOnlyList<string> Unresolved { get; }

    private ForeignKeyGraph(
        Dictionary<string, ForeignKeyPlan[]> byChild,
        Dictionary<string, ForeignKeyPlan[]> byParent,
        int count,
        IReadOnlyList<string> unresolved)
    {
        this.byChild = byChild;
        this.byParent = byParent;
        Count = count;
        Unresolved = unresolved;
    }

    /// <summary>
    /// The constraints that table <paramref name="tableId"/> owns, in any state and resolved or not.
    /// Empty when there are none. DML must check <see cref="ForeignKeyPlan.IsEnforced"/> on each.
    /// </summary>
    public ForeignKeyPlan[] ChildPlansOf(string tableId) =>
        byChild.TryGetValue(tableId, out ForeignKeyPlan[]? plans) ? plans : [];

    /// <summary>
    /// The constraints that reference table <paramref name="tableId"/>, including a self-reference, in
    /// any state and resolved or not. Empty when there are none. A DDL guard that protects the parent
    /// must consider every entry; DML must check <see cref="ForeignKeyPlan.IsEnforced"/> on each.
    /// </summary>
    public ForeignKeyPlan[] ParentPlansOf(string tableId) =>
        byParent.TryGetValue(tableId, out ForeignKeyPlan[]? plans) ? plans : [];

    /// <summary>
    /// Builds the graph of <paramref name="tables"/>. The caller must hold the schema lock, or own the
    /// collection outright, because the walk enumerates it.
    /// </summary>
    public static ForeignKeyGraph Build(ICollection<TableSchema> tables)
    {
        int total = 0;

        foreach (TableSchema table in tables)
            total += table.ForeignKeys?.Count ?? 0;

        if (total == 0)
            return Empty;

        Dictionary<string, TableSchema> tablesById = new(tables.Count, StringComparer.Ordinal);

        foreach (TableSchema table in tables)
        {
            if (table.Id is { Length: > 0 } id)
                tablesById[id] = table;
        }

        Dictionary<string, List<ForeignKeyPlan>> childLists = new(StringComparer.Ordinal);
        Dictionary<string, List<ForeignKeyPlan>> parentLists = new(StringComparer.Ordinal);
        List<string> unresolved = [];

        foreach (TableSchema child in tables)
        {
            if (child.ForeignKeys is not { Count: > 0 } || child.Id is not { Length: > 0 } childId)
                continue;

            foreach (ForeignKeySchema constraint in child.ForeignKeys)
            {
                ForeignKeyPlan plan = Resolve(child, childId, constraint, tablesById);

                if (plan.UnresolvedReason is not null)
                    unresolved.Add($"'{constraint.Name}' on table '{child.Name}': {plan.UnresolvedReason}");

                Append(childLists, childId, plan);
                Append(parentLists, constraint.ReferencedTableId, plan);
            }
        }

        return new(Freeze(childLists), Freeze(parentLists), total, unresolved);
    }

    private static ForeignKeyPlan Resolve(
        TableSchema child,
        string childId,
        ForeignKeySchema constraint,
        Dictionary<string, TableSchema> tablesById)
    {
        int width = constraint.ColumnIds.Length;

        if (width == 0 || constraint.ReferencedColumnIds.Length != width)
            return new(constraint, childId, "the referencing and referenced column lists do not have the same, non-zero length");

        if (!tablesById.TryGetValue(constraint.ReferencedTableId, out TableSchema? parent))
            return new(constraint, childId, $"referenced table id '{constraint.ReferencedTableId}' does not exist");

        string[] childNames = new string[width];
        string[] parentNames = new string[width];

        for (int i = 0; i < width; i++)
        {
            TableColumnSchema? childColumn = FindColumn(child, constraint.ColumnIds[i]);
            if (childColumn is null)
                return new(constraint, childId, $"column id '{constraint.ColumnIds[i]}' does not exist on the referencing table");

            TableColumnSchema? parentColumn = FindColumn(parent, constraint.ReferencedColumnIds[i]);
            if (parentColumn is null)
                return new(constraint, childId, $"column id '{constraint.ReferencedColumnIds[i]}' does not exist on referenced table '{parent.Name}'");

            childNames[i] = childColumn.Name;
            parentNames[i] = parentColumn.Name;
        }

        TableIndexSchema? parentIndex = FindIndex(parent, constraint.ReferencedIndexId);
        if (parentIndex is null)
            return new(constraint, childId, $"referenced index '{constraint.ReferencedIndexId}' does not exist on table '{parent.Name}'");

        if (parentIndex.Type != IndexType.Unique || parentIndex.ColumnIds is null || parentIndex.ColumnIds.Length != width)
            return new(constraint, childId, $"referenced index '{parentIndex.Name}' is not a unique index over exactly the referenced columns");

        int[]? parentOrder = MapPositions(parentIndex.ColumnIds, width, constraint.ReferencedColumnIds);
        if (parentOrder is null)
            return new(constraint, childId, $"referenced index '{parentIndex.Name}' is not a unique index over exactly the referenced columns");

        TableIndexSchema? backingIndex = FindIndex(child, constraint.BackingIndexId);
        if (backingIndex is null)
            return new(constraint, childId, $"backing index '{constraint.BackingIndexId}' does not exist on the referencing table");

        if (backingIndex.ColumnIds is null || backingIndex.ColumnIds.Length < width)
            return new(constraint, childId, $"backing index '{backingIndex.Name}' does not lead with the referencing columns");

        int[]? backingOrder = MapPositions(backingIndex.ColumnIds, width, constraint.ColumnIds);
        if (backingOrder is null)
            return new(constraint, childId, $"backing index '{backingIndex.Name}' does not lead with the referencing columns");

        return new(
            constraint,
            childId,
            childNames,
            parentNames,
            parentOrder,
            parentIndex.ColumnDirections,
            backingOrder,
            backingIndex.ColumnDirections
        );
    }

    /// <summary>
    /// For each of the first <paramref name="width"/> index positions, the position in
    /// <paramref name="constraintIds"/> of the same column id. Null unless those index positions are
    /// exactly the constraint's column set.
    /// </summary>
    private static int[]? MapPositions(string[] indexIds, int width, string[] constraintIds)
    {
        int[] order = new int[width];
        bool[] used = new bool[width];

        for (int j = 0; j < width; j++)
        {
            int position = Array.IndexOf(constraintIds, indexIds[j]);
            if (position < 0 || used[position])
                return null;

            used[position] = true;
            order[j] = position;
        }

        return order;
    }

    private static TableColumnSchema? FindColumn(TableSchema table, string columnId)
    {
        if (table.Columns is null)
            return null;

        foreach (TableColumnSchema column in table.Columns)
        {
            if (string.Equals(column.Id, columnId, StringComparison.Ordinal))
                return column;
        }

        return null;
    }

    private static TableIndexSchema? FindIndex(TableSchema table, string kvId)
    {
        if (table.Indexes is null)
            return null;

        foreach (TableIndexSchema index in table.Indexes)
        {
            if (string.Equals(index.KvId, kvId, StringComparison.Ordinal))
                return index;
        }

        return null;
    }

    private static void Append(Dictionary<string, List<ForeignKeyPlan>> lists, string key, ForeignKeyPlan plan)
    {
        if (!lists.TryGetValue(key, out List<ForeignKeyPlan>? list))
        {
            list = new(1);
            lists[key] = list;
        }

        list.Add(plan);
    }

    private static Dictionary<string, ForeignKeyPlan[]> Freeze(Dictionary<string, List<ForeignKeyPlan>> lists)
    {
        Dictionary<string, ForeignKeyPlan[]> frozen = new(lists.Count, StringComparer.Ordinal);

        foreach (KeyValuePair<string, List<ForeignKeyPlan>> entry in lists)
            frozen[entry.Key] = [.. entry.Value];

        return frozen;
    }
}
