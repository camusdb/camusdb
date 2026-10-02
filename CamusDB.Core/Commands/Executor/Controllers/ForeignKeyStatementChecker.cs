/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Kahuna.Shared.KeyValue;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// Checks the foreign keys of one DML statement, after the statement's last write and inside the
/// statement's own transaction. One instance serves one statement: the writer adds each row it writes,
/// and <see cref="CompleteAsync"/> runs the checks once.
///
/// <para><b>Child side.</b> For each constraint the written table owns, the checker keeps the distinct
/// parent keys the new rows reference. <see cref="CompleteAsync"/> takes the rendezvous lock on each
/// key — a shared point lock on the parent's unique-index entry — and then reads the keys in one batch
/// per constraint (<see cref="KvTableStore.LockAndLookupUniqueManyAsync"/>). A parent writer that wants
/// to delete or re-key that entry is fenced by the lock until this transaction ends, so the read answers
/// for the life of the transaction. A key that is absent raises
/// <see cref="CamusDBErrorCodes.ForeignKeyViolation"/>.</para>
///
/// <para><b>Why after the writes.</b> The read sees the transaction's own writes, so one statement can
/// write a parent and its child in either order, and a row can reference itself. The lock is taken after
/// the writes too: an explicit transaction anchors its session on the first table it touches, and an
/// anchor on the parent's partition would cost the child's write its one-phase commit.</para>
///
/// <para><b>MATCH SIMPLE.</b> A row with a NULL in any referencing column satisfies the constraint and
/// adds no key.</para>
///
/// <para><b>Memory.</b> The key sets hold at most one entry per written row, and
/// <see cref="CamusDBOptions.MaxMutationsPerTransaction"/> bounds the rows a statement can write.</para>
///
/// <para><b>Cost when there is nothing to check.</b> A database or a table with no enforced constraint
/// gets <see cref="None"/>: one graph read at statement start, no allocation, and an empty loop per
/// row.</para>
///
/// <para><b>Parent side.</b> For each constraint that references the written table, the checker keeps
/// the distinct referenced keys that the statement removed (a DELETE; a key UPDATE later). After the
/// last write, <see cref="CompleteAsync"/> probes each child's backing index for each key
/// (<see cref="KvTableStore.IndexPrefixExistsAsync"/>). A live child row raises
/// <see cref="CamusDBErrorCodes.ForeignKeyRestrictDelete"/>. The probe runs after the write, never before
/// it: the write of the referenced key is what waits for a child transaction that holds the rendezvous
/// lock, so after it the probe sees every child there will be. The probe reads the transaction's own
/// writes, so children that the same statement deleted do not count.</para>
///
/// <para><b>Optimistic parent writers.</b> An optimistic delete takes no lock on the key; Kahuna only
/// stages it. Before the probe, an optimistic writer therefore takes an exclusive lock on each removed key
/// (<see cref="KvRangeLockManager.AcquireForeignKeyExclusiveLocksAsync"/>), which a pessimistic writer
/// already holds through its write.</para>
/// </summary>
internal sealed class ForeignKeyStatementChecker
{
    /// <summary>The checker of a statement that writes a table with no enforced foreign key.</summary>
    internal static ForeignKeyStatementChecker None { get; } = new([], []);

    private readonly ChildSide[] childSides;

    private readonly ParentSide[] parentSides;

    /// <summary>True when the statement has at least one constraint to check.</summary>
    internal bool HasWork => childSides.Length > 0 || parentSides.Length > 0;

    /// <summary>True when the statement removes rows that other rows can reference.</summary>
    internal bool HasParentWork => parentSides.Length > 0;

    private ForeignKeyStatementChecker(ChildSide[] childSides, ParentSide[] parentSides)
    {
        this.childSides = childSides;
        this.parentSides = parentSides;
    }

    /// <summary>
    /// Builds the checker for a statement that writes rows into <paramref name="child"/>. Call it at
    /// statement start, before the first write: it opens and pins each parent table, and an open can
    /// throw <see cref="CamusDBErrorCodes.SchemaCatchingUp"/>, which the dispatcher retries only while
    /// nothing has been written.
    ///
    /// <para>Each parent is opened by id, without the caller's per-table privilege check — the check is
    /// the engine's, not the user's — and pinned like a table a SELECT reads. The pin fails the commit if
    /// the parent's layout or contents generation changed under the statement: a TRUNCATE of the parent
    /// moves its rows to a new key-space, where the lock this statement took no longer stands.</para>
    /// </summary>
    internal static async ValueTask<ForeignKeyStatementChecker> ForChildWritesAsync(
        DatabaseDescriptor database,
        TableDescriptor child,
        TableOpener tableOpener,
        KvTransaction tx)
    {
        ForeignKeyGraph graph = database.Schema.ForeignKeys;
        if (graph.IsEmpty)
            return None;

        ForeignKeyPlan[] plans = graph.ChildPlansOf(child.Id);
        int enforced = 0;

        foreach (ForeignKeyPlan plan in plans)
        {
            if (plan.IsEnforced)
                enforced++;
        }

        if (enforced == 0)
            return None;

        ChildSide[] sides = new ChildSide[enforced];
        int next = 0;

        foreach (ForeignKeyPlan plan in plans)
        {
            if (!plan.IsEnforced)
                continue;

            TableDescriptor parent = plan.IsSelfReference
                ? child
                : await OpenParentAsync(database, tableOpener, plan, tx).ConfigureAwait(false);

            sides[next++] = new ChildSide(plan, parent, child.Name);
        }

        return new ForeignKeyStatementChecker(sides, []);
    }

    /// <summary>
    /// Builds the checker for a statement that deletes rows from <paramref name="parent"/>. Call it at
    /// statement start, before the first write, for the same reason as <see cref="ForChildWritesAsync"/>.
    /// Each child is opened by id without the caller's privilege check and pinned: a TRUNCATE of a child
    /// moves its rows to a new key-space, and a probe through a stale descriptor would miss a new child.
    /// </summary>
    internal static async ValueTask<ForeignKeyStatementChecker> ForParentDeletesAsync(
        DatabaseDescriptor database,
        TableDescriptor parent,
        TableOpener tableOpener,
        KvTransaction tx)
    {
        ForeignKeyGraph graph = database.Schema.ForeignKeys;
        if (graph.IsEmpty)
            return None;

        ForeignKeyPlan[] plans = graph.ParentPlansOf(parent.Id);
        int enforced = 0;

        foreach (ForeignKeyPlan plan in plans)
        {
            if (plan.IsEnforced)
                enforced++;
        }

        if (enforced == 0)
            return None;

        ParentSide[] sides = new ParentSide[enforced];
        int next = 0;

        foreach (ForeignKeyPlan plan in plans)
        {
            if (!plan.IsEnforced)
                continue;

            TableDescriptor child = plan.IsSelfReference
                ? parent
                : await OpenRelatedAsync(database, tableOpener, plan.ChildTableId, plan, tx).ConfigureAwait(false);

            sides[next++] = new ParentSide(plan, parent, child);
        }

        return new ForeignKeyStatementChecker([], sides);
    }

    /// <summary>
    /// True when any foreign key, in any state, references table <paramref name="tableId"/>. For
    /// assertions on paths that delete rows without a parent-side check.
    /// </summary>
    internal static bool IsReferenced(DatabaseDescriptor database, string tableId) =>
        database.Schema.ForeignKeys.ParentPlansOf(tableId).Length > 0;

    /// <summary>
    /// The parent columns the parent side reads from each removed row. A writer that decodes only some
    /// columns must decode these too.
    /// </summary>
    internal void CollectParentColumns(HashSet<string> columns)
    {
        foreach (ParentSide side in parentSides)
            side.CollectColumns(columns);
    }

    /// <summary>
    /// Records one row the statement removed from the parent table. <paramref name="values"/> must hold
    /// at least the columns <see cref="CollectParentColumns"/> names.
    /// </summary>
    internal void AddRemovedParentRow(Dictionary<string, ColumnValue> values)
    {
        foreach (ParentSide side in parentSides)
            side.Add(values);
    }

    /// <summary>
    /// Records one row the statement wrote into the child table. <paramref name="values"/> holds every
    /// column of the row, defaults included.
    /// </summary>
    internal void AddChildRow(Dictionary<string, ColumnValue> values)
    {
        foreach (ChildSide side in childSides)
            side.Add(values);
    }

    /// <summary>
    /// Runs every check. Call it once, after the statement's last write. Throws
    /// <see cref="CamusDBErrorCodes.ForeignKeyViolation"/> for the first missing parent key, in
    /// constraint order and then in the order the rows were written.
    /// </summary>
    internal async Task CompleteAsync(KvTransaction tx, CancellationToken cancellationToken = default)
    {
        foreach (ChildSide side in childSides)
            await side.CheckAsync(tx, cancellationToken).ConfigureAwait(false);

        foreach (ParentSide side in parentSides)
            await side.CheckAsync(tx, cancellationToken).ConfigureAwait(false);
    }

    private static Task<TableDescriptor> OpenParentAsync(
        DatabaseDescriptor database,
        TableOpener tableOpener,
        ForeignKeyPlan plan,
        KvTransaction tx) =>
        OpenRelatedAsync(database, tableOpener, plan.ParentTableId, plan, tx);

    /// <summary>
    /// Opens the table <paramref name="tableId"/> of <paramref name="plan"/> by id, without the caller's
    /// privilege check, and pins it.
    /// </summary>
    private static async Task<TableDescriptor> OpenRelatedAsync(
        DatabaseDescriptor database,
        TableOpener tableOpener,
        string tableId,
        ForeignKeyPlan plan,
        KvTransaction tx)
    {
        TableSchema schema = SchemaDeltaApplier.FindRelationById(database.Schema, tableId)
            ?? throw new CamusDBException(
                CamusDBErrorCodes.TableDoesntExist,
                $"A table of foreign key '{plan.Constraint.Name}' does not exist (id '{tableId}')");

        TableDescriptor table;

        using (AuthorizationContext.SuspendTableCheck())
            table = await tableOpener.Open(database, schema.Name!).ConfigureAwait(false);

        // A rename between the lookup and the open could hand back a different table under the name.
        if (!string.Equals(table.Id, tableId, StringComparison.Ordinal))
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionMustRetry,
                $"A table of foreign key '{plan.Constraint.Name}' changed during the statement; retry it");

        SelectStatementExecutor.PinSchemaVersion(database, table, tx);
        return table;
    }

    /// <summary>The distinct parent keys one constraint must find, and the table they must be in.</summary>
    private sealed class ChildSide
    {
        private readonly ForeignKeyPlan plan;

        private readonly TableDescriptor parent;

        private readonly string childTableName;

        private readonly string[] columnNames;

        private readonly Dictionary<string, int> seen = new(StringComparer.Ordinal);

        private readonly List<CompositeColumnValue> parentKeys = [];

        private readonly List<ColumnValue[]> childValues = [];

        public ChildSide(ForeignKeyPlan plan, TableDescriptor parent, string childTableName)
        {
            this.plan = plan;
            this.parent = parent;
            this.childTableName = childTableName;
            columnNames = plan.ChildColumnNames;
        }

        public void Add(Dictionary<string, ColumnValue> values)
        {
            int width = columnNames.Length;
            ColumnValue[]? inConstraintOrder = null;

            for (int i = 0; i < width; i++)
            {
                if (!values.TryGetValue(columnNames[i], out ColumnValue? value) || value.Type == ColumnType.Null)
                    return;

                (inConstraintOrder ??= new ColumnValue[width])[i] = value;
            }

            ColumnValue[] keyValues = new ColumnValue[width];
            for (int j = 0; j < width; j++)
                keyValues[j] = inConstraintOrder![plan.ParentKeyOrder[j]];

            CompositeColumnValue parentKey = new(keyValues);

            // The encoded key is the identity of the parent entry, so two rows whose values encode
            // alike share one lock and one read. A key that cannot be encoded cannot be in the parent's
            // unique index either; it is kept apart and reported as missing.
            string identity = KeyEncoder.TryEncode(parentKey, plan.ParentKeyDirections, out string encoded)
                ? encoded
                : "\u0001" + parentKeys.Count.ToString(System.Globalization.CultureInfo.InvariantCulture);

            if (!seen.TryAdd(identity, parentKeys.Count))
                return;

            parentKeys.Add(parentKey);
            childValues.Add(inConstraintOrder!);
        }

        public async Task CheckAsync(KvTransaction tx, CancellationToken cancellationToken)
        {
            if (parentKeys.Count == 0)
                return;

            bool[] found = await parent.Store.LockAndLookupUniqueManyAsync(
                tx, plan.Constraint.ReferencedIndexId, parentKeys, cancellationToken).ConfigureAwait(false);

            for (int i = 0; i < found.Length; i++)
            {
                if (found[i])
                    continue;

                ServerDiagnostics.AddForeignKeyOperation(ServerDiagnostics.ForeignKeyOperation.Violation);

                throw new CamusDBException(
                    CamusDBErrorCodes.ForeignKeyViolation,
                    $"Insert or update on table '{childTableName}' violates foreign key constraint '{plan.Constraint.Name}': " +
                    $"key {ForeignKeyKeyText.Format(columnNames, childValues[i])} is not present in table '{parent.Name}'");
            }
        }
    }
    /// <summary>
    /// The distinct referenced keys a statement removed for one constraint, and the child index that
    /// must hold no row with any of them.
    /// </summary>
    private sealed class ParentSide
    {
        private readonly ForeignKeyPlan plan;

        private readonly TableDescriptor parent;

        private readonly TableDescriptor child;

        private readonly string[] columnNames;

        private readonly string backingIndexId;

        private readonly ColumnType[] backingKeyTypes;

        private readonly bool backingUnique;

        private readonly Dictionary<string, int> seen = new(StringComparer.Ordinal);

        private readonly List<CompositeColumnValue> parentKeys = [];

        private readonly List<CompositeColumnValue> childPrefixes = [];

        private readonly List<ColumnValue[]> parentValues = [];

        public ParentSide(ForeignKeyPlan plan, TableDescriptor parent, TableDescriptor child)
        {
            this.plan = plan;
            this.parent = parent;
            this.child = child;
            columnNames = plan.ParentColumnNames;
            backingIndexId = plan.Constraint.BackingIndexId;

            TableIndexSchema backing = child.Schema.Indexes?.Find(i => string.Equals(i.KvId, backingIndexId, StringComparison.Ordinal))
                ?? throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation,
                    $"Foreign key '{plan.Constraint.Name}': backing index '{backingIndexId}' does not exist on table '{child.Name}'");

            string[] columnIds = backing.ColumnIds ?? [];
            backingKeyTypes = new ColumnType[columnIds.Length];

            for (int i = 0; i < columnIds.Length; i++)
            {
                TableColumnSchema column = child.Schema.Columns?.Find(c => string.Equals(c.Id, columnIds[i], StringComparison.Ordinal))
                    ?? throw new CamusDBException(
                        CamusDBErrorCodes.InvalidInternalOperation,
                        $"Foreign key '{plan.Constraint.Name}': backing index '{backing.Name}' names a column that does not exist");

                backingKeyTypes[i] = column.Type;
            }

            backingUnique = backing.Type == IndexType.Unique;
        }

        public void CollectColumns(HashSet<string> columns)
        {
            foreach (string name in columnNames)
                columns.Add(name);
        }

        public void Add(Dictionary<string, ColumnValue> values)
        {
            int width = columnNames.Length;
            ColumnValue[]? inConstraintOrder = null;

            // A row with a NULL in a referenced column has no unique-index entry, so no child can
            // reference it.
            for (int i = 0; i < width; i++)
            {
                if (!values.TryGetValue(columnNames[i], out ColumnValue? value) || value.Type == ColumnType.Null)
                    return;

                (inConstraintOrder ??= new ColumnValue[width])[i] = value;
            }

            ColumnValue[] keyValues = new ColumnValue[width];
            ColumnValue[] prefixValues = new ColumnValue[width];

            for (int j = 0; j < width; j++)
            {
                keyValues[j] = inConstraintOrder![plan.ParentKeyOrder[j]];
                prefixValues[j] = inConstraintOrder[plan.BackingKeyOrder[j]];
            }

            CompositeColumnValue parentKey = new(keyValues);

            string identity = KeyEncoder.TryEncode(parentKey, plan.ParentKeyDirections, out string encoded)
                ? encoded
                : "\u0001" + parentKeys.Count.ToString(System.Globalization.CultureInfo.InvariantCulture);

            if (!seen.TryAdd(identity, parentKeys.Count))
                return;

            parentKeys.Add(parentKey);
            childPrefixes.Add(new CompositeColumnValue(prefixValues));
            parentValues.Add(inConstraintOrder!);
        }

        public async Task CheckAsync(KvTransaction tx, CancellationToken cancellationToken)
        {
            if (parentKeys.Count == 0)
                return;

            if (tx.Locking == KeyValueTransactionLocking.Optimistic)
                await parent.Store.LockUniqueKeysExclusiveAsync(tx, plan.Constraint.ReferencedIndexId, parentKeys, cancellationToken).ConfigureAwait(false);

            for (int i = 0; i < childPrefixes.Count; i++)
            {
                if (!await child.Store.IndexPrefixExistsAsync(tx, backingIndexId, backingKeyTypes, childPrefixes[i], backingUnique, cancellationToken).ConfigureAwait(false))
                    continue;

                ServerDiagnostics.AddForeignKeyOperation(ServerDiagnostics.ForeignKeyOperation.Violation);

                throw new CamusDBException(
                    CamusDBErrorCodes.ForeignKeyRestrictDelete,
                    $"Delete on table '{parent.Name}' violates foreign key constraint '{plan.Constraint.Name}' on table '{child.Name}': " +
                    $"key {ForeignKeyKeyText.Format(columnNames, parentValues[i])} is still referenced from table '{child.Name}'");
            }
        }
    }
}
