/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Proves that every row of a child table has its parent, before a foreign key goes from
/// <c>WriteOnly</c> to <c>Public</c>. CREATE TABLE uses it in a cluster, and ALTER TABLE ADD
/// CONSTRAINT uses it on every node.
///
/// <para><b>Why no lock is needed.</b> The pass runs only after every live node acked the constraint
/// in <c>WriteOnly</c>, so each child write and each parent write since then is checked by the DML
/// itself. An orphan can only come from the window before the ack, when a node that did not know the
/// constraint deleted a parent. Such an orphan is already committed, and a plain read finds it.</para>
///
/// <para><b>How it reads.</b> It reads the child's backing index in key order, in pages, each page in
/// its own Read Committed read-only transaction, so no transaction stays open for the whole table.
/// Rows that share a key are adjacent in that order and collapse to one distinct key; each next page
/// starts after the last key of the page before, so a key is never checked twice. A key with a NULL
/// column is skipped, because MATCH SIMPLE is satisfied by any NULL. The parent keys of a page are
/// checked in one batched read (<c>LookupUniqueManyAsync</c>), never one read per child row.</para>
///
/// <para><b>Before it reports an orphan, it reads both sides again</b> in a fresh transaction. A child
/// DELETE or a parent INSERT that committed after the page was read can have removed the orphan, and
/// the constraint must not fail for a row that no longer exists. The first orphan in key order is
/// reported, so the error is the same on every run.</para>
///
/// <para>On a branch, both reads merge the ancestry as of the fork, so inherited rows count.</para>
/// </summary>
internal static class ForeignKeyValidationPass
{
    /// <summary>
    /// Index entries read per page. Bounds the memory of one page and the size of the batched parent
    /// read that follows it.
    /// </summary>
    internal const int PageEntries = 1024;

    /// <summary>
    /// Validates the constraint <paramref name="constraintName"/> of <paramref name="childTableName"/>.
    /// Returns when every row has a parent, or when the constraint no longer exists. Throws
    /// <see cref="CamusDBErrorCodes.ForeignKeyViolation"/> with the first orphan key.
    /// </summary>
    internal static async Task ValidateAsync(
        DatabaseDescriptor database,
        TableOpener tableOpener,
        string childTableName,
        string constraintName,
        CancellationToken cancellationToken = default)
    {
        if (!database.Schema.Tables.TryGetValue(childTableName, out TableSchema? childSchema) || childSchema.Id is null)
            throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"Table '{childTableName}' does not exist");

        ForeignKeySchema? constraint = childSchema.ForeignKeys?.Find(fk => string.Equals(fk.Name, constraintName, StringComparison.OrdinalIgnoreCase));
        if (constraint is null)
            return;

        ForeignKeyPlan? plan = null;
        foreach (ForeignKeyPlan candidate in database.Schema.ForeignKeys.ChildPlansOf(childSchema.Id))
        {
            if (string.Equals(candidate.Constraint.Id, constraint.Id, StringComparison.Ordinal))
            {
                plan = candidate;
                break;
            }
        }

        if (plan is null || !plan.IsResolved)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Foreign key '{constraintName}' on table '{childTableName}' does not resolve: {plan?.UnresolvedReason ?? "it is not in the published graph"}");

        TableSchema parentSchema = SchemaDeltaApplier.FindRelationById(database.Schema, plan.ParentTableId)
            ?? throw new CamusDBException(CamusDBErrorCodes.TableDoesntExist, $"The table that foreign key '{constraintName}' references does not exist");

        TableDescriptor child;
        TableDescriptor parent;

        // Engine work on behalf of a statement whose authority was already checked: the tables are
        // opened without the caller's per-table privilege check.
        using (AuthorizationContext.SuspendTableCheck())
        {
            child = await OpenByIdAsync(database, tableOpener, childSchema).ConfigureAwait(false);
            parent = plan.IsSelfReference ? child : await OpenByIdAsync(database, tableOpener, parentSchema).ConfigureAwait(false);
        }

        TableIndexSchema backing = FindIndex(child.Schema, constraint.BackingIndexId, childTableName);
        ColumnType[] keyTypes = KeyTypesOf(child.Schema, backing);
        bool unique = backing.Type == IndexType.Unique;
        int width = constraint.ColumnIds.Length;

        CompositeColumnValue? after = null;

        while (true)
        {
            List<CompositeColumnValue> prefixes = [];
            List<CompositeColumnValue> parentKeys = [];
            CompositeColumnValue? lastEntry = null;
            int emitted = 0;
            bool[] found;

            KvTransaction tx = await database.Transactions.BeginAsync(
                CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly
            ).ConfigureAwait(false);
            try
            {
                await foreach ((CompositeColumnValue key, _, _) in child.Store.ScanIndex(
                    tx, backing.KvId, keyTypes, after, null, unique,
                    fromInclusive: false, toInclusive: true, maxRows: PageEntries, cancellationToken).ConfigureAwait(false))
                {
                    emitted++;

                    CompositeColumnValue prefix = Prefix(key, width);
                    lastEntry = prefix;

                    if (HasNull(prefix))
                        continue;

                    if (prefixes.Count > 0 && SameValues(prefixes[^1], prefix))
                        continue;

                    prefixes.Add(prefix);
                    parentKeys.Add(ParentKey(prefix, plan));
                }

                found = parentKeys.Count == 0
                    ? []
                    : await parent.Store.LookupUniqueManyAsync(tx, constraint.ReferencedIndexId, parentKeys, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
            }

            ServerDiagnostics.AddForeignKeyOperation(ServerDiagnostics.ForeignKeyOperation.ValidationKey, prefixes.Count);

            for (int i = 0; i < found.Length; i++)
            {
                if (found[i])
                    continue;

                if (await IsStillOrphanAsync(database, child, parent, backing, keyTypes, unique, constraint.ReferencedIndexId, prefixes[i], parentKeys[i], cancellationToken).ConfigureAwait(false))
                {
                    ServerDiagnostics.AddForeignKeyOperation(ServerDiagnostics.ForeignKeyOperation.Violation);
                    throw new CamusDBException(
                        CamusDBErrorCodes.ForeignKeyViolation,
                        $"Foreign key '{constraint.Name}' on table '{child.Name}' is violated by existing rows: " +
                        $"key {ForeignKeyKeyText.Format(plan.ChildColumnNames, InConstraintOrder(prefixes[i], plan))} is not present in table '{parent.Name}'");
                }
            }

            // Fewer entries than a full page means the scan reached the end of the index.
            if (emitted < PageEntries || lastEntry is null)
                return;

            after = lastEntry;
        }
    }

    /// <summary>
    /// Reads the child entry and the parent key again in a fresh transaction. True only when the child
    /// still has the key and the parent still does not.
    /// </summary>
    private static async Task<bool> IsStillOrphanAsync(
        DatabaseDescriptor database,
        TableDescriptor child,
        TableDescriptor parent,
        TableIndexSchema backing,
        ColumnType[] keyTypes,
        bool unique,
        string referencedIndexId,
        CompositeColumnValue prefix,
        CompositeColumnValue parentKey,
        CancellationToken cancellationToken)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadOnly
        ).ConfigureAwait(false);
        try
        {
            bool childPresent = false;

            await foreach ((CompositeColumnValue _, _, _) in child.Store.ScanIndex(
                tx, backing.KvId, keyTypes, prefix, prefix, unique,
                fromInclusive: true, toInclusive: true, maxRows: 1, cancellationToken).ConfigureAwait(false))
            {
                childPresent = true;
            }

            if (!childPresent)
                return false;

            bool[] parentPresent = await parent.Store.LookupUniqueManyAsync(tx, referencedIndexId, [parentKey], cancellationToken).ConfigureAwait(false);
            return !parentPresent[0];
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Opens a table by the name its id has now, and refuses a descriptor whose id differs: a concurrent
    /// rename must not swap one table for another under the pass.
    /// </summary>
    private static async Task<TableDescriptor> OpenByIdAsync(DatabaseDescriptor database, TableOpener tableOpener, TableSchema schema)
    {
        TableDescriptor table = await tableOpener.Open(database, schema.Name!).ConfigureAwait(false);

        if (!string.Equals(table.Schema.Id, schema.Id, StringComparison.Ordinal))
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionMustRetry,
                $"Table '{schema.Name}' changed while a foreign key was being validated; retry the statement");

        return table;
    }

    private static TableIndexSchema FindIndex(TableSchema table, string kvId, string tableName)
    {
        if (table.Indexes is not null)
        {
            foreach (TableIndexSchema index in table.Indexes)
            {
                if (string.Equals(index.KvId, kvId, StringComparison.Ordinal))
                    return index;
            }
        }

        throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Index '{kvId}' does not exist on table '{tableName}'");
    }

    /// <summary>The types of every key column of <paramref name="index"/>, which the index scan decodes.</summary>
    private static ColumnType[] KeyTypesOf(TableSchema table, TableIndexSchema index)
    {
        string[] columnIds = index.ColumnIds ?? [];
        ColumnType[] types = new ColumnType[columnIds.Length];

        for (int i = 0; i < columnIds.Length; i++)
        {
            TableColumnSchema column = table.Columns?.Find(c => string.Equals(c.Id, columnIds[i], StringComparison.Ordinal))
                ?? throw new CamusDBException(CamusDBErrorCodes.InvalidInternalOperation, $"Index '{index.Name}' names a column that does not exist");
            types[i] = column.Type;
        }

        return types;
    }

    private static CompositeColumnValue Prefix(CompositeColumnValue key, int width)
    {
        if (key.Values.Length == width)
            return key;

        ColumnValue[] values = new ColumnValue[width];
        Array.Copy(key.Values, values, width);
        return new CompositeColumnValue(values);
    }

    /// <summary>
    /// Builds the parent key of a backing-index prefix: backing position j holds constraint column
    /// <c>BackingKeyOrder[j]</c>, and parent position j takes constraint column <c>ParentKeyOrder[j]</c>.
    /// </summary>
    private static CompositeColumnValue ParentKey(CompositeColumnValue prefix, ForeignKeyPlan plan)
    {
        int width = prefix.Values.Length;
        ColumnValue[] inConstraintOrder = InConstraintOrder(prefix, plan);

        ColumnValue[] parentKey = new ColumnValue[width];
        for (int j = 0; j < width; j++)
            parentKey[j] = inConstraintOrder[plan.ParentKeyOrder[j]];

        return new CompositeColumnValue(parentKey);
    }

    private static bool HasNull(CompositeColumnValue key)
    {
        foreach (ColumnValue value in key.Values)
        {
            if (value.Type == ColumnType.Null)
                return true;
        }

        return false;
    }

    private static bool SameValues(CompositeColumnValue left, CompositeColumnValue right)
    {
        for (int i = 0; i < left.Values.Length; i++)
        {
            if (left.Values[i].CompareTo(right.Values[i]) != 0)
                return false;
        }

        return true;
    }

    /// <summary>The prefix values in constraint column order, for the error message.</summary>
    private static ColumnValue[] InConstraintOrder(CompositeColumnValue prefix, ForeignKeyPlan plan)
    {
        ColumnValue[] values = new ColumnValue[prefix.Values.Length];

        for (int j = 0; j < values.Length; j++)
            values[plan.BackingKeyOrder[j]] = prefix.Values[j];

        return values;
    }
}
