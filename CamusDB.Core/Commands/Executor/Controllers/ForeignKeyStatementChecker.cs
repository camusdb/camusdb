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
using CamusDB.Core.SQLParser;
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
/// the distinct referenced keys that the statement removed: the key of a deleted row, or the old key of a
/// row whose referenced key an UPDATE changed. After the last write, <see cref="CompleteAsync"/> probes
/// each child's backing index for each key (<see cref="KvTableStore.IndexPrefixExistsAsync"/>). A live
/// child row raises <see cref="CamusDBErrorCodes.ForeignKeyRestrictDelete"/> for a DELETE and
/// <see cref="CamusDBErrorCodes.ForeignKeyRestrictUpdate"/> for an UPDATE. The probe runs after the write, never before
/// it: the write of the referenced key is what waits for a child transaction that holds the rendezvous
/// lock, so after it the probe sees every child there will be. The probe reads the transaction's own
/// writes, so children that the same statement deleted do not count.</para>
///
/// <para><b>NO ACTION and a key that came back.</b> An UPDATE can move a referenced key to another row
/// in the same statement (a swap). Under NO ACTION the constraint holds if the old key exists again when
/// the statement ends, as in PostgreSQL, so such a key is not probed. RESTRICT refuses the change
/// anyway. A DELETE cannot bring a key back, so it does not pay for the extra read.</para>
///
/// <para><b>UPDATE.</b> A statement that assigns no referencing and no referenced column does no
/// foreign-key work at all (<see cref="ForUpdatesAsync"/>). Otherwise only a row whose key really changed
/// adds a key: the new referencing key to the child side, the old referenced key to the parent side.</para>
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

    private static int CountEnforced(ForeignKeyPlan[] plans)
    {
        int enforced = 0;

        foreach (ForeignKeyPlan plan in plans)
        {
            if (plan.IsEnforced)
                enforced++;
        }

        return enforced;
    }

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
    internal static ValueTask<ForeignKeyStatementChecker> ForChildWritesAsync(
        DatabaseDescriptor database,
        TableDescriptor child,
        TableOpener tableOpener,
        KvTransaction tx)
    {
        // Not async on purpose: the common answer is None, and it must not allocate a state machine.
        ForeignKeyGraph graph = database.Schema.ForeignKeys;
        if (graph.IsEmpty)
            return new ValueTask<ForeignKeyStatementChecker>(None);

        ForeignKeyPlan[] plans = graph.ChildPlansOf(child.Id);
        int enforced = CountEnforced(plans);

        return enforced == 0
            ? new ValueTask<ForeignKeyStatementChecker>(None)
            : BuildChildWritesAsync(database, child, tableOpener, tx, plans, enforced);
    }

    private static async ValueTask<ForeignKeyStatementChecker> BuildChildWritesAsync(
        DatabaseDescriptor database,
        TableDescriptor child,
        TableOpener tableOpener,
        KvTransaction tx,
        ForeignKeyPlan[] plans,
        int enforced)
    {
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
    internal static ValueTask<ForeignKeyStatementChecker> ForParentDeletesAsync(
        DatabaseDescriptor database,
        TableDescriptor parent,
        TableOpener tableOpener,
        KvTransaction tx)
    {
        // Not async on purpose, as in ForChildWritesAsync.
        ForeignKeyGraph graph = database.Schema.ForeignKeys;
        if (graph.IsEmpty)
            return new ValueTask<ForeignKeyStatementChecker>(None);

        ForeignKeyPlan[] plans = graph.ParentPlansOf(parent.Id);
        int enforced = CountEnforced(plans);

        return enforced == 0
            ? new ValueTask<ForeignKeyStatementChecker>(None)
            : BuildParentDeletesAsync(database, parent, tableOpener, tx, plans, enforced);
    }

    private static async ValueTask<ForeignKeyStatementChecker> BuildParentDeletesAsync(
        DatabaseDescriptor database,
        TableDescriptor parent,
        TableOpener tableOpener,
        KvTransaction tx,
        ForeignKeyPlan[] plans,
        int enforced)
    {
        ParentSide[] sides = new ParentSide[enforced];
        int next = 0;

        foreach (ForeignKeyPlan plan in plans)
        {
            if (!plan.IsEnforced)
                continue;

            TableDescriptor child = plan.IsSelfReference
                ? parent
                : await OpenRelatedAsync(database, tableOpener, plan.ChildTableId, plan, tx).ConfigureAwait(false);

            sides[next++] = new ParentSide(plan, parent, child, forUpdate: false);
        }

        return new ForeignKeyStatementChecker([], sides);
    }

    /// <summary>
    /// Builds the checker for an UPDATE of <paramref name="table"/> that assigns the columns named by
    /// <paramref name="plainValues"/> and <paramref name="exprValues"/>. Decided before any row is read:
    /// a constraint takes part only when the statement assigns one of its referencing columns (child
    /// side) or one of its referenced columns (parent side). A statement that assigns neither gets
    /// <see cref="None"/> and does no foreign-key work — no table opens, no keys. Call it before the first
    /// write, for the same reason as <see cref="ForChildWritesAsync"/>.
    ///
    /// <para>The set of assigned columns is built only after the graph is known to hold a constraint, so
    /// an UPDATE in a database without one allocates nothing here.</para>
    /// </summary>
    internal static ValueTask<ForeignKeyStatementChecker> ForUpdatesAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        TableOpener tableOpener,
        KvTransaction tx,
        Dictionary<string, ColumnValue>? plainValues,
        Dictionary<string, NodeAst>? exprValues)
    {
        // Not async on purpose, as in ForChildWritesAsync. A table that no constraint touches needs no
        // look at the assigned columns either.
        ForeignKeyGraph graph = database.Schema.ForeignKeys;
        if (graph.IsEmpty || (graph.ChildPlansOf(table.Id).Length == 0 && graph.ParentPlansOf(table.Id).Length == 0))
            return new ValueTask<ForeignKeyStatementChecker>(None);

        return BuildUpdatesAsync(database, table, tableOpener, tx, graph, plainValues, exprValues);
    }

    private static async ValueTask<ForeignKeyStatementChecker> BuildUpdatesAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        TableOpener tableOpener,
        KvTransaction tx,
        ForeignKeyGraph graph,
        Dictionary<string, ColumnValue>? plainValues,
        Dictionary<string, NodeAst>? exprValues)
    {
        HashSet<string> assignedColumns = new(StringComparer.OrdinalIgnoreCase);

        if (plainValues is not null)
            assignedColumns.UnionWith(plainValues.Keys);

        if (exprValues is not null)
            assignedColumns.UnionWith(exprValues.Keys);

        if (assignedColumns.Count == 0)
            return None;

        List<ChildSide>? childSides = null;
        List<ParentSide>? parentSides = null;

        foreach (ForeignKeyPlan plan in graph.ChildPlansOf(table.Id))
        {
            if (!plan.IsEnforced || !AssignsAny(assignedColumns, plan.ChildColumnNames))
                continue;

            TableDescriptor parent = plan.IsSelfReference
                ? table
                : await OpenParentAsync(database, tableOpener, plan, tx).ConfigureAwait(false);

            (childSides ??= []).Add(new ChildSide(plan, parent, table.Name));
        }

        foreach (ForeignKeyPlan plan in graph.ParentPlansOf(table.Id))
        {
            if (!plan.IsEnforced || !AssignsAny(assignedColumns, plan.ParentColumnNames))
                continue;

            TableDescriptor child = plan.IsSelfReference
                ? table
                : await OpenRelatedAsync(database, tableOpener, plan.ChildTableId, plan, tx).ConfigureAwait(false);

            (parentSides ??= []).Add(new ParentSide(plan, table, child, forUpdate: true));
        }

        if (childSides is null && parentSides is null)
            return None;

        return new ForeignKeyStatementChecker(
            childSides is null ? [] : [.. childSides],
            parentSides is null ? [] : [.. parentSides]);
    }

    private static bool AssignsAny(IReadOnlySet<string> assignedColumns, string[] columnNames)
    {
        foreach (string name in columnNames)
        {
            if (assignedColumns.Contains(name))
                return true;
        }

        return false;
    }

    /// <summary>
    /// The columns the checker reads from each row, on either side. A writer that decodes only some
    /// columns must decode these too.
    /// </summary>
    internal void CollectCheckedColumns(HashSet<string> columns)
    {
        foreach (ChildSide side in childSides)
            side.CollectColumns(columns);

        foreach (ParentSide side in parentSides)
            side.CollectColumns(columns);
    }

    /// <summary>
    /// Records one row an UPDATE changed: <paramref name="oldRow"/> as the write phase read it under its
    /// lock, <paramref name="newRow"/> as it was written. Only a key that changed adds work.
    /// </summary>
    internal void AddUpdatedRow(Dictionary<string, ColumnValue> oldRow, Dictionary<string, ColumnValue> newRow)
    {
        foreach (ChildSide side in childSides)
            side.AddIfChanged(oldRow, newRow);

        foreach (ParentSide side in parentSides)
            side.AddIfChanged(oldRow, newRow);
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
    internal Task CompleteAsync(KvTransaction tx, CancellationToken cancellationToken = default) =>
        HasWork ? CompleteCoreAsync(tx, cancellationToken) : Task.CompletedTask;

    private async Task CompleteCoreAsync(KvTransaction tx, CancellationToken cancellationToken)
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

    /// <summary>
    /// The values of <paramref name="columnNames"/> in constraint order, or null when one is absent or
    /// NULL: under MATCH SIMPLE such a row neither needs a parent nor can be one.
    /// </summary>
    private static ColumnValue[]? ReadKey(Dictionary<string, ColumnValue> row, string[] columnNames)
    {
        ColumnValue[] values = new ColumnValue[columnNames.Length];

        for (int i = 0; i < columnNames.Length; i++)
        {
            if (!row.TryGetValue(columnNames[i], out ColumnValue? value) || value.Type == ColumnType.Null)
                return null;

            values[i] = value;
        }

        return values;
    }

    private static bool SameValues(ColumnValue[] left, ColumnValue[] right)
    {
        for (int i = 0; i < left.Length; i++)
        {
            if (left[i].CompareTo(right[i]) != 0)
                return false;
        }

        return true;
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

        public void CollectColumns(HashSet<string> columns)
        {
            foreach (string name in columnNames)
                columns.Add(name);
        }

        public void Add(Dictionary<string, ColumnValue> values)
        {
            if (ReadKey(values, columnNames) is { } inConstraintOrder)
                AddKey(inConstraintOrder);
        }

        /// <summary>The new referencing key, when it has no NULL and differs from the old one.</summary>
        public void AddIfChanged(Dictionary<string, ColumnValue> oldRow, Dictionary<string, ColumnValue> newRow)
        {
            if (ReadKey(newRow, columnNames) is not { } newKey)
                return;

            if (ReadKey(oldRow, columnNames) is { } oldKey && SameValues(oldKey, newKey))
                return;

            AddKey(newKey);
        }

        private void AddKey(ColumnValue[] inConstraintOrder)
        {
            int width = inConstraintOrder.Length;

            ColumnValue[] keyValues = new ColumnValue[width];
            for (int j = 0; j < width; j++)
                keyValues[j] = inConstraintOrder[plan.ParentKeyOrder[j]];

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
            childValues.Add(inConstraintOrder);
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

        /// <summary>True for an UPDATE that changed a referenced key; false for a DELETE.</summary>
        private readonly bool forUpdate;

        private readonly Dictionary<string, int> seen = new(StringComparer.Ordinal);

        private readonly List<CompositeColumnValue> parentKeys = [];

        private readonly List<CompositeColumnValue> childPrefixes = [];

        private readonly List<ColumnValue[]> parentValues = [];

        public ParentSide(ForeignKeyPlan plan, TableDescriptor parent, TableDescriptor child, bool forUpdate)
        {
            this.plan = plan;
            this.forUpdate = forUpdate;
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
            // A row with a NULL in a referenced column has no unique-index entry, so no child can
            // reference it.
            if (ReadKey(values, columnNames) is { } inConstraintOrder)
                AddKey(inConstraintOrder);
        }

        /// <summary>The old referenced key, when it had no NULL and the update changed it.</summary>
        public void AddIfChanged(Dictionary<string, ColumnValue> oldRow, Dictionary<string, ColumnValue> newRow)
        {
            if (ReadKey(oldRow, columnNames) is not { } oldKey)
                return;

            if (ReadKey(newRow, columnNames) is { } newKey && SameValues(oldKey, newKey))
                return;

            AddKey(oldKey);
        }

        private void AddKey(ColumnValue[] inConstraintOrder)
        {
            int width = inConstraintOrder.Length;
            ColumnValue[] keyValues = new ColumnValue[width];
            ColumnValue[] prefixValues = new ColumnValue[width];

            for (int j = 0; j < width; j++)
            {
                keyValues[j] = inConstraintOrder[plan.ParentKeyOrder[j]];
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
            parentValues.Add(inConstraintOrder);
        }

        public async Task CheckAsync(KvTransaction tx, CancellationToken cancellationToken)
        {
            if (parentKeys.Count == 0)
                return;

            if (tx.Locking == KeyValueTransactionLocking.Optimistic)
                await parent.Store.LockUniqueKeysExclusiveAsync(tx, plan.Constraint.ReferencedIndexId, parentKeys, cancellationToken).ConfigureAwait(false);

            // Under NO ACTION an UPDATE may have given the old key to another row. The read sees this
            // statement's own writes, so a key that exists again holds its children and is not probed.
            bool[]? backAgain = forUpdate && plan.Constraint.OnUpdate == ForeignKeyAction.NoAction
                ? await parent.Store.LookupUniqueManyAsync(tx, plan.Constraint.ReferencedIndexId, parentKeys, cancellationToken).ConfigureAwait(false)
                : null;

            bool[] referenced = new bool[childPrefixes.Count];

            if (childPrefixes.Count == 1)
            {
                if (backAgain is null || !backAgain[0])
                    referenced[0] = await child.Store.IndexPrefixExistsAsync(tx, backingIndexId, backingKeyTypes, childPrefixes[0], backingUnique, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                // The probes only read, and a scan reads the transaction's identity without changing
                // it, so they run side by side. All of them finish before a violation is reported, so
                // the error names the first referenced key in statement order, whatever finished first.
                await Parallel.ForEachAsync(
                    Enumerable.Range(0, childPrefixes.Count),
                    new ParallelOptions { MaxDegreeOfParallelism = KvStoreConstants.ForeignKeyProbeConcurrency, CancellationToken = cancellationToken },
                    async (i, ct) =>
                    {
                        if (backAgain is null || !backAgain[i])
                            referenced[i] = await child.Store.IndexPrefixExistsAsync(tx, backingIndexId, backingKeyTypes, childPrefixes[i], backingUnique, ct).ConfigureAwait(false);
                    }).ConfigureAwait(false);
            }

            for (int i = 0; i < referenced.Length; i++)
            {
                if (!referenced[i])
                    continue;

                ServerDiagnostics.AddForeignKeyOperation(ServerDiagnostics.ForeignKeyOperation.Violation);

                throw new CamusDBException(
                    forUpdate ? CamusDBErrorCodes.ForeignKeyRestrictUpdate : CamusDBErrorCodes.ForeignKeyRestrictDelete,
                    $"{(forUpdate ? "Update" : "Delete")} on table '{parent.Name}' violates foreign key constraint '{plan.Constraint.Name}' on table '{child.Name}': " +
                    $"key {ForeignKeyKeyText.Format(columnNames, parentValues[i])} is still referenced from table '{child.Name}'");
            }
        }
    }
}
