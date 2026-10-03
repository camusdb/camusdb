/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Apply;

/// <summary>
/// The structural rules a <see cref="ForeignKeySchema"/> must satisfy against the schema it is applied
/// to. Every check here works on ids, never on names.
///
/// <para><b>Why the rules run at apply time.</b> The proposer resolves names to ids and checks the
/// statement first, for a good message. That check alone is not enough: between the proposer's check
/// and the commit, another node can drop the parent, drop the referenced index or enable row-level TTL
/// on the parent. A schema delta is only ever applied on top of the exact version it was proposed
/// against (<see cref="SchemaChangeLogEntry.FromVersion"/>), and the proposer dry-runs the apply under
/// the schema lock against that version. Running the rules inside the apply therefore orders them
/// against every other DDL in log order. Because the base version is the same on every node, a delta
/// that passes on the leader also passes on each follower, so a throw here never splits the cluster.</para>
///
/// <para>The rules mirror what <see cref="ForeignKeyGraph"/> needs to resolve a plan, plus the
/// conditions the graph does not check: the index states, the column types, the relation kind and the
/// TTL setting. A constraint that passes here always resolves in the graph that the same apply
/// publishes.</para>
/// </summary>
internal static class ForeignKeyDefinitionRules
{
    /// <summary>
    /// Checks every constraint of <paramref name="child"/>. <paramref name="child"/> may be a table that
    /// is not yet in <paramref name="schema"/> (a CREATE TABLE in the middle of its apply); a
    /// self-reference then resolves against <paramref name="child"/> itself.
    /// </summary>
    internal static void ValidateTable(Schema schema, TableSchema child)
    {
        if (child.ForeignKeys is not { Count: > 0 } foreignKeys)
        {
            RequireOwnedIndexesHaveOwners(child);
            return;
        }

        HashSet<string> ids = new(foreignKeys.Count, StringComparer.Ordinal);
        HashSet<string> names = new(StringComparer.OrdinalIgnoreCase);

        if (child.CheckConstraints is not null)
        {
            foreach (CheckConstraintSchema check in child.CheckConstraints)
                names.Add(check.Name);
        }

        if (child.Columns is not null)
        {
            foreach (TableColumnSchema column in child.Columns)
            {
                if (column.NotNullConstraintName is { Length: > 0 } notNullName)
                    names.Add(notNullName);
            }
        }

        foreach (ForeignKeySchema foreignKey in foreignKeys)
        {
            if (string.IsNullOrWhiteSpace(foreignKey.Id) || !ids.Add(foreignKey.Id))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Foreign key '{foreignKey.Name}' on table '{child.Name}' has a missing or repeated id");

            if (string.IsNullOrWhiteSpace(foreignKey.Name) || !names.Add(foreignKey.Name))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"Constraint '{foreignKey.Name}' already exists on table '{child.Name}'");

            Validate(schema, child, foreignKey);
        }

        RequireOwnedIndexesHaveOwners(child);
    }

    /// <summary>
    /// Checks one constraint of <paramref name="child"/> against <paramref name="schema"/>. Throws the
    /// code a user would expect for the statement that declared it.
    /// </summary>
    internal static void Validate(Schema schema, TableSchema child, ForeignKeySchema foreignKey)
    {
        string subject = $"Foreign key '{foreignKey.Name}' on table '{child.Name}'";

        if (foreignKey.State is not (SchemaElementState.WriteOnly or SchemaElementState.Public))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"{subject} cannot be stored in state {foreignKey.State}");

        if (foreignKey.OnDelete is not (ForeignKeyAction.NoAction or ForeignKeyAction.Restrict)
            || foreignKey.OnUpdate is not (ForeignKeyAction.NoAction or ForeignKeyAction.Restrict)
            || foreignKey.Match != ForeignKeyMatch.Simple)
            throw new CamusDBException(
                CamusDBErrorCodes.FeatureNotSupported,
                $"{subject}: only NO ACTION and RESTRICT with MATCH SIMPLE are supported");

        int width = foreignKey.ColumnIds.Length;
        if (width == 0 || foreignKey.ReferencedColumnIds.Length != width)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidForeignKeyDefinition,
                $"{subject} must name the same, non-zero number of referencing and referenced columns");

        TableSchema parent = ResolveParent(schema, child, foreignKey, subject);
        string parentName = parent.Name ?? foreignKey.ReferencedTableId;

        if (parent.Kind != RelationKind.Table)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidForeignKeyDefinition,
                $"{subject} cannot reference '{parentName}': it is a materialized view, and a refresh replaces its rows without any foreign-key check");

        if (child.Kind != RelationKind.Table)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidForeignKeyDefinition,
                $"{subject}: a materialized view cannot hold a foreign key");

        // The TTL sweep deletes expired rows through its own path, which has no parent-side check. A
        // referenced parent would lose rows that children still point at.
        if (HasRowLevelTtl(parent))
            throw new CamusDBException(
                CamusDBErrorCodes.FeatureNotSupported,
                $"{subject} cannot reference '{parentName}': it has row-level TTL, and expired rows are deleted without a foreign-key check");

        TableColumnSchema[] childColumns = ResolveColumns(child, foreignKey.ColumnIds, subject, child.Name ?? "");
        TableColumnSchema[] parentColumns = ResolveColumns(parent, foreignKey.ReferencedColumnIds, subject, parentName);

        for (int i = 0; i < width; i++)
        {
            TableColumnSchema childColumn = childColumns[i];
            TableColumnSchema parentColumn = parentColumns[i];

            // Array columns cannot be index keys, so neither side of the rendezvous could exist.
            if (childColumn.Type == ColumnType.Array || parentColumn.Type == ColumnType.Array)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject}: array column '{(childColumn.Type == ColumnType.Array ? childColumn.Name : parentColumn.Name)}' cannot take part in a foreign key");

            // The child's value is encoded as the parent's index key, so the two encodings must be the
            // same bytes. A string(N) length can differ; the type cannot.
            if (childColumn.Type != parentColumn.Type)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject}: column '{childColumn.Name}' is {childColumn.Type}, but referenced column '{parentName}.{parentColumn.Name}' is {parentColumn.Type}");
        }

        TableIndexSchema referencedIndex = FindIndex(parent, foreignKey.ReferencedIndexId)
            ?? throw new CamusDBException(
                CamusDBErrorCodes.InvalidForeignKeyDefinition,
                $"{subject}: no unique index or primary key on '{parentName}' covers exactly the referenced columns");

        if (referencedIndex.Type != IndexType.Unique
            || referencedIndex.State != SchemaElementState.Public
            || referencedIndex.ColumnIds is null
            || referencedIndex.ColumnIds.Length != width
            || !LeadsWithExactly(referencedIndex.ColumnIds, foreignKey.ReferencedColumnIds))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidForeignKeyDefinition,
                $"{subject}: index '{referencedIndex.Name}' on '{parentName}' is not a public unique index over exactly the referenced columns");

        TableIndexSchema backingIndex = FindIndex(child, foreignKey.BackingIndexId)
            ?? throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"{subject}: backing index '{foreignKey.BackingIndexId}' does not exist");

        if (backingIndex.State != SchemaElementState.Public
            || backingIndex.ColumnIds is null
            || backingIndex.ColumnIds.Length < width
            || !LeadsWithExactly(backingIndex.ColumnIds, foreignKey.ColumnIds))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"{subject}: index '{backingIndex.Name}' is not a public index that leads with the referencing columns");

        if (backingIndex.OwnerConstraintId is { } owner && !string.Equals(owner, foreignKey.Id, StringComparison.Ordinal))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"{subject}: index '{backingIndex.Name}' belongs to another constraint");
    }

    /// <summary>
    /// Refuses a constraint that would close a cycle of two or more tables. The walk starts at the
    /// parent and follows the parent's own constraints, and the constraints of every table it reaches.
    /// If it reaches <paramref name="child"/>, the new constraint closes a cycle. A self-reference is
    /// accepted, and a self-reference met on the walk is not followed.
    ///
    /// <para>Only <c>ALTER TABLE ... ADD CONSTRAINT</c> can close a cycle: a table that CREATE TABLE
    /// makes is referenced by nothing yet. The check runs at apply time, so two concurrent ADDs that
    /// would together close a cycle are ordered in the log, and the second one is refused.</para>
    /// </summary>
    internal static void RequireNoCycle(Schema schema, TableSchema child, ForeignKeySchema foreignKey)
    {
        if (child.Id is null || string.Equals(foreignKey.ReferencedTableId, child.Id, StringComparison.Ordinal))
            return;

        HashSet<string> visited = new(StringComparer.Ordinal) { foreignKey.ReferencedTableId };
        Stack<string> pending = new();
        pending.Push(foreignKey.ReferencedTableId);

        while (pending.Count > 0)
        {
            TableSchema? table = SchemaDeltaApplier.FindRelationById(schema, pending.Pop());
            if (table?.ForeignKeys is null)
                continue;

            foreach (ForeignKeySchema edge in table.ForeignKeys)
            {
                string next = edge.ReferencedTableId;

                if (string.Equals(next, table.Id, StringComparison.Ordinal))
                    continue;

                if (string.Equals(next, child.Id, StringComparison.Ordinal))
                {
                    string parentName = SchemaDeltaApplier.FindRelationById(schema, foreignKey.ReferencedTableId)?.Name ?? foreignKey.ReferencedTableId;

                    throw new CamusDBException(
                        CamusDBErrorCodes.ForeignKeyCycle,
                        $"Foreign key '{foreignKey.Name}' on table '{child.Name}' would close a cycle: '{parentName}' already references '{child.Name}', directly or through other tables (foreign key '{edge.Name}' on table '{table.Name}')");
                }

                if (visited.Add(next))
                    pending.Push(next);
            }
        }
    }

    /// <summary>
    /// True when <paramref name="table"/> has row-level TTL configured, paused or not: a paused sweep
    /// can be resumed without any DDL that this check would see.
    /// </summary>
    internal static bool HasRowLevelTtl(TableSchema table) =>
        table.Settings is not null
        && table.Settings.TryGetValue(TableSettings.TtlExpirationExpressionKey, out string? expression)
        && !string.IsNullOrWhiteSpace(expression);

    /// <summary>
    /// True when the first <c>ids.Length</c> entries of <paramref name="indexColumnIds"/> are exactly
    /// the set <paramref name="ids"/>, in any order.
    /// </summary>
    internal static bool LeadsWithExactly(string[] indexColumnIds, string[] ids)
    {
        if (indexColumnIds.Length < ids.Length)
            return false;

        for (int j = 0; j < ids.Length; j++)
        {
            if (Array.IndexOf(ids, indexColumnIds[j]) < 0)
                return false;

            // A repeated id in the index prefix would leave another constraint column uncovered.
            if (Array.IndexOf(indexColumnIds, indexColumnIds[j], 0, j) >= 0)
                return false;
        }

        return true;
    }

    private static TableSchema ResolveParent(Schema schema, TableSchema child, ForeignKeySchema foreignKey, string subject)
    {
        if (string.Equals(foreignKey.ReferencedTableId, child.Id, StringComparison.Ordinal))
            return child;

        return SchemaDeltaApplier.FindRelationById(schema, foreignKey.ReferencedTableId)
            ?? throw new CamusDBException(
                CamusDBErrorCodes.TableDoesntExist,
                $"{subject} references a table that does not exist (id '{foreignKey.ReferencedTableId}')");
    }

    private static TableColumnSchema[] ResolveColumns(TableSchema table, string[] columnIds, string subject, string tableName)
    {
        TableColumnSchema[] columns = new TableColumnSchema[columnIds.Length];

        for (int i = 0; i < columnIds.Length; i++)
        {
            if (Array.IndexOf(columnIds, columnIds[i], 0, i) >= 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject} names a column of '{tableName}' more than once");

            TableColumnSchema? column = null;

            if (table.Columns is not null)
            {
                foreach (TableColumnSchema candidate in table.Columns)
                {
                    if (string.Equals(candidate.Id, columnIds[i], StringComparison.Ordinal))
                    {
                        column = candidate;
                        break;
                    }
                }
            }

            if (column is null || column.State != SchemaElementState.Public)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject}: column id '{columnIds[i]}' is not a public column of '{tableName}'");

            columns[i] = column;
        }

        return columns;
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

    /// <summary>
    /// An index that a constraint owns must be that constraint's backing index. An owner id that names
    /// no constraint would make the index undroppable by name and unowned in practice.
    /// </summary>
    private static void RequireOwnedIndexesHaveOwners(TableSchema child)
    {
        if (child.Indexes is null)
            return;

        foreach (TableIndexSchema index in child.Indexes)
        {
            if (index.OwnerConstraintId is not { } owner)
                continue;

            bool found = false;

            if (child.ForeignKeys is not null)
            {
                foreach (ForeignKeySchema foreignKey in child.ForeignKeys)
                {
                    if (string.Equals(foreignKey.Id, owner, StringComparison.Ordinal)
                        && string.Equals(foreignKey.BackingIndexId, index.KvId, StringComparison.Ordinal))
                    {
                        found = true;
                        break;
                    }
                }
            }

            if (!found)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Index '{index.Name}' on table '{child.Name}' names owner constraint '{owner}', which does not use it as its backing index");
        }
    }
}
