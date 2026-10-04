/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Catalogs.Replication;

/// <summary>
/// Turns the foreign keys a CREATE TABLE declared by name (<see cref="ForeignKeyInfo"/>) into persisted
/// definitions that hold ids only (<see cref="ForeignKeySchema"/>), and chooses or creates the child
/// index each constraint needs for its parent-side probe.
///
/// <para><b>Proposer only.</b> This reads the live schema by name, under the schema lock, and freezes
/// the result into the <c>CreateTable</c> payload. A follower never runs it: it applies the ids the
/// payload carries, and <see cref="ForeignKeyDefinitionRules"/> checks them again at apply time on
/// every node. The checks here exist for the message — they name what the user wrote.</para>
///
/// <para><b>The backing index.</b> The parent-side check must find the children of a parent key
/// without a full scan, so it needs a child index whose leading columns are exactly the constraint's
/// columns. A matching index the statement already declares is reused, as MySQL InnoDB does; the
/// preferred one has exactly that many columns, then the shortest. Without one, a non-unique index
/// named <c>~fk_{constraint}</c> is added to the same delta, with
/// <see cref="TableIndexSchema.OwnerConstraintId"/> set, so it goes only with its constraint. An index
/// that another constraint owns is never reused: dropping that constraint drops its index, which would
/// leave this one without a probe path.</para>
/// </summary>
internal static class ForeignKeyDefinitionBuilder
{
    /// <summary>Prefix of an index the engine creates for a constraint that had no usable index.</summary>
    internal const string OwnedIndexPrefix = "~fk_";

    /// <summary>
    /// Resolves every foreign key of <paramref name="ticket"/>. Returns the constraints and the full
    /// index list for the new table: <paramref name="inlineIndexes"/> followed by any index created for
    /// a constraint. Both are null when the ticket declares no foreign key and no inline index.
    /// </summary>
    /// <param name="initialState">
    /// <c>Public</c> on a standalone node, where there is no second schema version to wait for and the
    /// new table holds no rows. <c>WriteOnly</c> in a cluster, where the validation pass runs after
    /// every node has applied the table.
    /// </param>
    internal static (ForeignKeySchema[]? ForeignKeys, TableIndexSchema[]? Indexes) Build(
        Schema schema,
        CreateTableTicket ticket,
        string tableId,
        SchemaColumnPayload[] columns,
        TableIndexSchema[]? inlineIndexes,
        SchemaElementState initialState,
        CamusDBOptions options)
    {
        if (ticket.ForeignKeys.Length == 0)
            return (null, inlineIndexes);

        if (ticket.Kind != RelationKind.Table)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidForeignKeyDefinition,
                $"Relation '{ticket.TableName}': a materialized view cannot hold a foreign key");

        List<TableIndexSchema> indexes = inlineIndexes is null ? new(ticket.ForeignKeys.Length) : new(inlineIndexes);
        ForeignKeySchema[] foreignKeys = new ForeignKeySchema[ticket.ForeignKeys.Length];
        bool addedIndex = false;

        for (int i = 0; i < ticket.ForeignKeys.Length; i++)
        {
            ForeignKeyInfo info = ticket.ForeignKeys[i];
            string constraintId = ObjectIdGenerator.Generate().ToString();
            string subject = $"Foreign key '{info.Name}' on table '{ticket.TableName}'";

            string[] childIds = ResolveChildColumns(info, columns, subject, out ColumnType[] childTypes);

            ParentView parent = ResolveParent(schema, ticket, tableId, columns, indexes, info, subject);
            string[] parentIds = ResolveParentColumns(parent, info, subject, out ColumnType[] parentTypes);

            CheckColumnPairs(info, subject, parent.Name, childTypes, parentTypes, childIds.Length, parentIds.Length);

            TableIndexSchema referencedIndex = FindReferencedIndex(parent.Indexes, parentIds)
                ?? throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject}: there is no unique index or primary key on '{parent.Name}' over exactly the referenced columns");

            TableIndexSchema? backingIndex = FindBackingIndex(indexes, childIds);

            if (backingIndex is null)
            {
                backingIndex = BuildOwnedIndex(indexes, info.Name, childIds, constraintId, subject, options);
                indexes.Add(backingIndex);
                addedIndex = true;
            }

            foreignKeys[i] = new ForeignKeySchema(
                constraintId,
                info.Name,
                childIds,
                parent.Id,
                parentIds,
                referencedIndex.KvId,
                backingIndex.KvId,
                info.OnDelete,
                info.OnUpdate,
                info.Match,
                initialState);
        }

        if (addedIndex && options.MaxIndexesPerTable > 0 && indexes.Count > options.MaxIndexesPerTable)
            throw new CamusDBException(
                CamusDBErrorCodes.SchemaLimitExceeded,
                $"Table '{ticket.TableName}' would have {indexes.Count} indexes, including those its foreign keys need, which exceeds the maximum of {options.MaxIndexesPerTable}");

        return (foreignKeys, [.. indexes]);
    }

    /// <summary>
    /// Resolves one constraint that <c>ALTER TABLE ... ADD CONSTRAINT</c> adds to the existing table
    /// <paramref name="child"/>. Reads both tables from the live schema; the caller holds the DDL
    /// semaphore, so no other DDL of this node changes them before the constraint is proposed, and the
    /// apply checks the result again by id in log order.
    ///
    /// <para>When no index can serve as the backing index, the plan names the index to build,
    /// <c>~fk_{constraint}</c>, and the caller builds it through the staged index build before the
    /// constraint exists. A table with rows needs a backfill, which a single schema delta cannot do.</para>
    /// </summary>
    internal static ForeignKeyAlterPlan ResolveForAlter(Schema schema, TableSchema child, ForeignKeyInfo info, CamusDBOptions options)
    {
        string childName = child.Name ?? "";
        string subject = $"Foreign key '{info.Name}' on table '{childName}'";

        if (child.Kind != RelationKind.Table)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidForeignKeyDefinition,
                $"Relation '{childName}': a materialized view cannot hold a foreign key");

        ForeignKeyDeltaApplier.RequireUnusedConstraintName(child, info.Name);

        string[] childIds = new string[info.Columns.Length];
        string[] childNames = new string[info.Columns.Length];
        ColumnType[] childTypes = new ColumnType[info.Columns.Length];

        for (int i = 0; i < info.Columns.Length; i++)
        {
            TableColumnSchema column = child.Columns?.Find(c =>
                c.State == SchemaElementState.Public && string.Equals(c.Name, info.Columns[i], StringComparison.OrdinalIgnoreCase))
                ?? throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject}: column '{info.Columns[i]}' does not exist");

            if (Array.IndexOf(childIds, column.Id, 0, i) >= 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject} names column '{column.Name}' more than once");

            childIds[i] = column.Id;
            childNames[i] = column.Name;
            childTypes[i] = column.Type;
        }

        TableSchema parentSchema = FindExistingParent(schema, info, subject);
        ParentView parent = ViewOf(parentSchema, info.ReferencedTable);
        string[] parentIds = ResolveParentColumns(parent, info, subject, out ColumnType[] parentTypes);

        CheckColumnPairs(info, subject, parent.Name, childTypes, parentTypes, childIds.Length, parentIds.Length);

        TableIndexSchema referencedIndex = FindReferencedIndex(parent.Indexes, parentIds)
            ?? throw new CamusDBException(
                CamusDBErrorCodes.InvalidForeignKeyDefinition,
                $"{subject}: there is no unique index or primary key on '{parent.Name}' over exactly the referenced columns");

        TableIndexSchema? backingIndex = FindBackingIndex((IReadOnlyList<TableIndexSchema>?)child.Indexes ?? [], childIds);
        string? ownedIndexName = null;

        if (backingIndex is null)
        {
            ownedIndexName = OwnedIndexPrefix + info.Name;

            if (child.Indexes?.Exists(ix => string.Equals(ix.Name, ownedIndexName, StringComparison.OrdinalIgnoreCase)) == true)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"{subject} needs an index named '{ownedIndexName}', and that name is already taken");

            if (options.MaxIndexColumns > 0 && childIds.Length > options.MaxIndexColumns)
                throw new CamusDBException(
                    CamusDBErrorCodes.SchemaLimitExceeded,
                    $"{subject} spans {childIds.Length} columns, exceeding the maximum of {options.MaxIndexColumns} for the index it needs");
        }

        return new ForeignKeyAlterPlan(
            ObjectIdGenerator.Generate().ToString(),
            info.Name,
            childIds,
            childNames,
            parent.Id,
            parentIds,
            referencedIndex.KvId,
            backingIndex?.KvId,
            ownedIndexName,
            info.OnDelete,
            info.OnUpdate,
            info.Match);
    }

    private static TableSchema FindExistingParent(Schema schema, ForeignKeyInfo info, string subject)
    {
        if (!schema.Tables.TryGetValue(info.ReferencedTable, out TableSchema? parent))
        {
            if (schema.Views.ContainsKey(info.ReferencedTable))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject} cannot reference '{info.ReferencedTable}': it is a view");

            throw new CamusDBException(
                CamusDBErrorCodes.TableDoesntExist,
                $"{subject} references table '{info.ReferencedTable}', which does not exist");
        }

        string parentName = parent.Name ?? info.ReferencedTable;

        if (parent.Kind != RelationKind.Table)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidForeignKeyDefinition,
                $"{subject} cannot reference '{parentName}': it is a materialized view, and a refresh replaces its rows without any foreign-key check");

        if (ForeignKeyDefinitionRules.HasRowLevelTtl(parent))
            throw RowLevelTtlRefusal(subject, parentName);

        return parent;
    }

    private static ParentView ViewOf(TableSchema parent, string fallbackName) => new(
        parent.Id!,
        parent.Name ?? fallbackName,
        name =>
        {
            TableColumnSchema? column = parent.Columns?.Find(c =>
                c.State == SchemaElementState.Public && string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase));
            return column is null ? null : (column.Id, column.Type);
        },
        id => parent.Columns?.Find(c => string.Equals(c.Id, id, StringComparison.Ordinal))?.Type,
        (IReadOnlyList<TableIndexSchema>?)parent.Indexes ?? []);

    private static void CheckColumnPairs(
        ForeignKeyInfo info,
        string subject,
        string parentName,
        ColumnType[] childTypes,
        ColumnType[] parentTypes,
        int childCount,
        int parentCount)
    {
        if (parentCount != childCount)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidForeignKeyDefinition,
                $"{subject} names {childCount} referencing column(s) but '{parentName}' has {parentCount} referenced column(s)");

        for (int c = 0; c < childCount; c++)
        {
            if (childTypes[c] == ColumnType.Array || parentTypes[c] == ColumnType.Array)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject}: an array column cannot take part in a foreign key");

            if (childTypes[c] != parentTypes[c])
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject}: column '{info.Columns[c]}' is {childTypes[c]}, but the referenced column of '{parentName}' is {parentTypes[c]}");
        }
    }

    /// <summary>What a constraint needs to know about its parent, whether it exists yet or not.</summary>
    private readonly record struct ParentView(
        string Id,
        string Name,
        Func<string, (string Id, ColumnType Type)?> FindColumnByName,
        Func<string, ColumnType?> FindTypeById,
        IReadOnlyList<TableIndexSchema> Indexes);

    private static string[] ResolveChildColumns(ForeignKeyInfo info, SchemaColumnPayload[] columns, string subject, out ColumnType[] types)
    {
        string[] ids = new string[info.Columns.Length];
        types = new ColumnType[info.Columns.Length];

        for (int i = 0; i < info.Columns.Length; i++)
        {
            SchemaColumnPayload? column = null;

            foreach (SchemaColumnPayload candidate in columns)
            {
                if (string.Equals(candidate.Name, info.Columns[i], StringComparison.OrdinalIgnoreCase))
                {
                    column = candidate;
                    break;
                }
            }

            if (column is null)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject}: column '{info.Columns[i]}' does not exist");

            if (Array.IndexOf(ids, column.Id, 0, i) >= 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject} names column '{column.Name}' more than once");

            ids[i] = column.Id!;
            types[i] = column.Type;
        }

        return ids;
    }

    private static ParentView ResolveParent(
        Schema schema,
        CreateTableTicket ticket,
        string tableId,
        SchemaColumnPayload[] columns,
        List<TableIndexSchema> indexes,
        ForeignKeyInfo info,
        string subject)
    {
        if (string.Equals(info.ReferencedTable, ticket.TableName, StringComparison.OrdinalIgnoreCase))
        {
            if (TtlConfigured(ticket.Settings))
                throw RowLevelTtlRefusal(subject, ticket.TableName);

            return new(
                tableId,
                ticket.TableName,
                name =>
                {
                    foreach (SchemaColumnPayload column in columns)
                    {
                        if (string.Equals(column.Name, name, StringComparison.OrdinalIgnoreCase))
                            return (column.Id!, column.Type);
                    }

                    return null;
                },
                id =>
                {
                    foreach (SchemaColumnPayload column in columns)
                    {
                        if (string.Equals(column.Id, id, StringComparison.Ordinal))
                            return column.Type;
                    }

                    return null;
                },
                indexes);
        }

        return ViewOf(FindExistingParent(schema, info, subject), info.ReferencedTable);
    }

    private static string[] ResolveParentColumns(ParentView parent, ForeignKeyInfo info, string subject, out ColumnType[] types)
    {
        // No column list means the parent's primary key, in its key order, as in PostgreSQL.
        if (info.ReferencedColumns.Length == 0)
        {
            TableIndexSchema? primaryKey = null;

            foreach (TableIndexSchema index in parent.Indexes)
            {
                if (string.Equals(index.Name, CamusDBConstants.PrimaryKeyInternalName, StringComparison.Ordinal))
                {
                    primaryKey = index;
                    break;
                }
            }

            if (primaryKey?.ColumnIds is not { Length: > 0 } keyIds)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject}: '{parent.Name}' has no primary key to reference");

            types = new ColumnType[keyIds.Length];
            for (int i = 0; i < keyIds.Length; i++)
                types[i] = parent.FindTypeById(keyIds[i]) ?? ColumnType.Null;

            return [.. keyIds];
        }

        string[] ids = new string[info.ReferencedColumns.Length];
        types = new ColumnType[info.ReferencedColumns.Length];

        for (int i = 0; i < info.ReferencedColumns.Length; i++)
        {
            (string Id, ColumnType Type)? column = parent.FindColumnByName(info.ReferencedColumns[i])
                ?? throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject}: referenced column '{parent.Name}.{info.ReferencedColumns[i]}' does not exist");

            if (Array.IndexOf(ids, column.Value.Id, 0, i) >= 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidForeignKeyDefinition,
                    $"{subject} names referenced column '{info.ReferencedColumns[i]}' more than once");

            ids[i] = column.Value.Id;
            types[i] = column.Value.Type;
        }

        return ids;
    }

    /// <summary>A public unique index over exactly the referenced columns; the primary key wins a tie.</summary>
    private static TableIndexSchema? FindReferencedIndex(IReadOnlyList<TableIndexSchema> indexes, string[] parentIds)
    {
        TableIndexSchema? found = null;

        foreach (TableIndexSchema index in indexes)
        {
            if (index.Type != IndexType.Unique
                || index.State != SchemaElementState.Public
                || index.ColumnIds is null
                || index.ColumnIds.Length != parentIds.Length
                || !ForeignKeyDefinitionRules.LeadsWithExactly(index.ColumnIds, parentIds))
                continue;

            if (string.Equals(index.Name, CamusDBConstants.PrimaryKeyInternalName, StringComparison.Ordinal))
                return index;

            found ??= index;
        }

        return found;
    }

    /// <summary>
    /// A public, unowned index whose leading columns are exactly <paramref name="childIds"/> and that
    /// holds an entry for every row the constraint checks
    /// (<see cref="ForeignKeyDefinitionRules.HoldsEveryReferencingRow"/>). Prefers an index of exactly
    /// that width, then the shortest; ties keep declaration order.
    /// </summary>
    internal static TableIndexSchema? FindBackingIndex(IReadOnlyList<TableIndexSchema> indexes, string[] childIds)
    {
        TableIndexSchema? best = null;

        foreach (TableIndexSchema index in indexes)
        {
            if (index.State != SchemaElementState.Public
                || index.OwnerConstraintId is not null
                || index.ColumnIds is null
                || !ForeignKeyDefinitionRules.LeadsWithExactly(index.ColumnIds, childIds)
                || !ForeignKeyDefinitionRules.HoldsEveryReferencingRow(index, childIds.Length))
                continue;

            if (best is null || index.ColumnIds.Length < best.ColumnIds!.Length)
                best = index;
        }

        return best;
    }

    private static TableIndexSchema BuildOwnedIndex(
        List<TableIndexSchema> indexes,
        string constraintName,
        string[] childIds,
        string constraintId,
        string subject,
        CamusDBOptions options)
    {
        string indexName = OwnedIndexPrefix + constraintName;

        foreach (TableIndexSchema index in indexes)
        {
            if (string.Equals(index.Name, indexName, StringComparison.OrdinalIgnoreCase))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"{subject} needs an index named '{indexName}', and that name is already taken");
        }

        if (options.MaxIndexColumns > 0 && childIds.Length > options.MaxIndexColumns)
            throw new CamusDBException(
                CamusDBErrorCodes.SchemaLimitExceeded,
                $"{subject} spans {childIds.Length} columns, exceeding the maximum of {options.MaxIndexColumns} for the index it needs");

        return new TableIndexSchema(
            ObjectIdGenerator.Generate().ToString(),
            indexName,
            [.. childIds],
            IndexType.Multi,
            SchemaElementState.Public,
            startOffset: null,
            columnDirections: null,
            includeColumnIds: null,
            comment: null,
            ownerConstraintId: constraintId);
    }

    private static bool TtlConfigured(IReadOnlyDictionary<string, string>? settings) =>
        settings is not null
        && settings.TryGetValue(TableSettings.TtlExpirationExpressionKey, out string? expression)
        && !string.IsNullOrWhiteSpace(expression);

    private static CamusDBException RowLevelTtlRefusal(string subject, string parentName) => new(
        CamusDBErrorCodes.FeatureNotSupported,
        $"{subject} cannot reference '{parentName}': it has row-level TTL, and expired rows are deleted without a foreign-key check");
}

/// <summary>
/// One constraint that <c>ALTER TABLE ... ADD CONSTRAINT</c> resolved against the live schema, before
/// its backing index exists. Exactly one of <see cref="BackingIndexId"/> (an index the table already
/// has) and <see cref="OwnedIndexName"/> (an index to build for this constraint) is set.
/// </summary>
internal sealed record ForeignKeyAlterPlan(
    string ConstraintId,
    string Name,
    string[] ChildColumnIds,
    string[] ChildColumnNames,
    string ParentTableId,
    string[] ParentColumnIds,
    string ReferencedIndexId,
    string? BackingIndexId,
    string? OwnedIndexName,
    ForeignKeyAction OnDelete,
    ForeignKeyAction OnUpdate,
    ForeignKeyMatch Match)
{
    /// <summary>The constraint in <c>WriteOnly</c>, backed by the index with <paramref name="backingIndexId"/>.</summary>
    internal ForeignKeySchema ToSchema(string backingIndexId) => new(
        ConstraintId,
        Name,
        ChildColumnIds,
        ParentTableId,
        ParentColumnIds,
        ReferencedIndexId,
        backingIndexId,
        OnDelete,
        OnUpdate,
        Match,
        SchemaElementState.WriteOnly);
}
