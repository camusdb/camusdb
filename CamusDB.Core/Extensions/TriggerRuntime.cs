/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Transactions;
using CamusDB.Extensions;

namespace CamusDB.Core.Extensions;

/// <summary>
/// The per-engine entry to row triggers. The DML controllers ask it, once per statement, for the
/// triggers that fire on their table and operation; a table without triggers costs one null check
/// and allocates nothing.
///
/// <para><b>Binding.</b> A <see cref="TriggerDefinition"/> names its type and its installed extension.
/// It is bound to this engine's loaded build of the installed version the first time a statement
/// fires it, and the result is cached on the record (<see cref="TriggerDefinition.Binding"/>) together
/// with the extension record it was resolved against. <c>ALTER EXTENSION ... UPDATE</c> publishes a new
/// extension record, so the next DML binds the trigger to the new build with no change to the trigger.
/// A node that does not load the installed build refuses the DML with
/// <see cref="CamusDBErrorCodes.ExtensionNotLoadedOnNode"/>; that failure is cached too, because the
/// loaded set is fixed for the life of the engine.</para>
///
/// <para><b>Where triggers fire.</b> On the node that runs the statement, inside its transaction,
/// once per affected row. No SQL-layer DML is forwarded between nodes, so a statement's triggers run
/// exactly once per execution. A statement can still execute more than once: a serializable conflict
/// in autocommit mode replays it from a new transaction, which rolls back the first attempt's
/// in-database effects but not effects outside the database.</para>
/// </summary>
internal sealed class TriggerRuntime
{
    private readonly CommandExecutor executor;

    public TriggerRuntime(CommandExecutor executor)
    {
        this.executor = executor;
    }

    /// <summary>
    /// The triggers that fire for <paramref name="operation"/> on <paramref name="table"/> in this
    /// statement, bound and ready, or null when there are none. Throws
    /// <see cref="CamusDBErrorCodes.ExtensionNotLoadedOnNode"/> (or
    /// <see cref="CamusDBErrorCodes.TriggerBindingMismatch"/>) before any row is written when a trigger
    /// cannot bind on this node.
    /// </summary>
    public StatementTriggers? ForStatement(DatabaseDescriptor database, TableDescriptor table, KvTransaction transaction, TriggerEventSet operation)
    {
        if (table.Schema.Triggers is not { Count: > 0 } definitions)
            return null;

        List<BoundTrigger>? before = null;
        List<BoundTrigger>? after = null;

        foreach (TriggerDefinition definition in definitions)
        {
            if (!definition.FiresOn(operation))
                continue;

            BoundTrigger bound = Bind(database, definition);

            if (definition.Timing == TriggerFireTiming.Before)
                (before ??= []).Add(bound);
            else
                (after ??= []).Add(bound);
        }

        if (before is null && after is null)
            return null;

        // The statement's own principal: nested SQL runs with the caller's rights.
        Principal? principal = AuthorizationContext.Current.Principal;

        // The queue's byte budget is the per-operator memory cap, read from the snapshot this
        // statement runs under; <= 0 removes it, as it does for the operators.
        long queueBudget = database.Options.SpillThresholdBytes > 0 ? database.Options.SpillThresholdBytes : long.MaxValue;

        return new StatementTriggers(
            new ExtensionSqlSession(executor, database.Name, transaction, principal),
            table,
            operation,
            before?.ToArray() ?? [],
            after?.ToArray() ?? [],
            queueBudget);
    }

    private BoundTrigger Bind(DatabaseDescriptor database, TriggerDefinition definition)
    {
        ExtensionRegistry registry = executor.Extensions;
        database.Schema.Extensions.TryGetValue(definition.ExtensionName, out ExtensionDefinition? record);

        // Cached per registry and per extension record: a record shared by two engines in one process
        // (never in production) still binds against each engine's own extensions, and an update of the
        // extension, which publishes a new record, makes the trigger bind again.
        if (definition.Binding is TriggerBinding cached && ReferenceEquals(cached.Registry, registry) && ReferenceEquals(cached.Record, record))
            return cached.Bound ?? throw cached.Error!;

        TriggerBinding binding = Resolve(executor.ExtensionScopes, database, definition, record, registry);
        definition.Binding = binding;

        return binding.Bound ?? throw binding.Error!;
    }

    private static TriggerBinding Resolve(
        ExtensionScopeFrames scopes, DatabaseDescriptor database, TriggerDefinition definition, ExtensionDefinition? record, ExtensionRegistry registry)
    {
        if (record is null)
            return TriggerBinding.Failed(registry, record, new CamusDBException(
                CamusDBErrorCodes.TriggerBindingMismatch,
                $"Trigger '{definition.Name}' needs extension '{definition.ExtensionName}', which is not installed in this database"));

        LoadedExtension? build;

        try
        {
            build = scopes.Resolve(database).GetBound(record.Name, "trigger '" + definition.Name + "'");
        }
        catch (CamusDBException ex)
        {
            return TriggerBinding.Failed(registry, record, ex);
        }

        if (build is null || build.FindTriggerType(definition.TypeName, out ExtensionTriggerType? type) != TriggerTypeLookup.Found)
            return TriggerBinding.Failed(registry, record, new CamusDBException(
                CamusDBErrorCodes.TriggerBindingMismatch,
                $"Trigger '{definition.Name}' needs type '{definition.TypeName}', which extension '{record.Name}' {record.Version} does not define"));

        return TriggerBinding.Succeeded(registry, record, new BoundTrigger(definition, type!));
    }

    /// <summary>The cached outcome of binding one record on one engine against one extension record.</summary>
    private sealed class TriggerBinding
    {
        public required ExtensionRegistry Registry { get; init; }

        public required ExtensionDefinition? Record { get; init; }

        public BoundTrigger? Bound { get; init; }

        public CamusDBException? Error { get; init; }

        public static TriggerBinding Succeeded(ExtensionRegistry registry, ExtensionDefinition record, BoundTrigger bound) =>
            new() { Registry = registry, Record = record, Bound = bound };

        public static TriggerBinding Failed(ExtensionRegistry registry, ExtensionDefinition? record, CamusDBException error) =>
            new() { Registry = registry, Record = record, Error = error };
    }
}

/// <summary>A trigger record and the loaded type it fires.</summary>
internal sealed record BoundTrigger(TriggerDefinition Definition, ExtensionTriggerType Type);

/// <summary>
/// The triggers of one statement on one table for one operation. The DML controller calls
/// <see cref="FireBeforeAsync"/> for each row before it validates and writes the row,
/// <see cref="QueueAfter"/> for each row it wrote, and <see cref="FireAfterAsync"/> once, after the
/// statement's last write and after its foreign-key checks.
///
/// <para><b>AFTER fires per row, after the statement.</b> The queue keeps the statement's rows (by
/// reference; the controllers do not reuse the dictionaries) until the tail write, so an AFTER trigger
/// never sees a half-written statement. A row count is no memory bound — the rows carry their large
/// values in full — so the queue charges each row's estimated bytes (<c>RowMemoryEstimator</c>) against
/// the per-operator budget, <c>spill_threshold_bytes</c>, and fails the statement with
/// <see cref="CamusDBErrorCodes.TriggerQueueTooLarge"/> past it. It does not spill.</para>
///
/// <para><b>BEFORE output is coerced and then validated.</b> The values a BEFORE trigger set are
/// converted to their column types here, exactly as statement values are; the caller then runs its
/// usual NOT NULL, length and CHECK validation, so a trigger cannot store a value the statement could
/// not.</para>
///
/// <para><b>Errors.</b> An exception from trigger code ends the statement as
/// <see cref="CamusDBErrorCodes.TriggerFailed"/>. A <see cref="CamusDBException"/> raised by the
/// trigger's own SQL keeps its code (a duplicate key stays a duplicate key), and cancellation passes
/// through unchanged.</para>
/// </summary>
internal sealed class StatementTriggers
{
    private readonly ExtensionSqlSession session;

    private readonly TableDescriptor table;

    private readonly TriggerOperation operation;

    private readonly BoundTrigger[] before;

    private readonly BoundTrigger[] after;

    private List<(IReadOnlyDictionary<string, ColumnValue>? Old, IReadOnlyDictionary<string, ColumnValue>? New)>? queue;

    private readonly long queueBudget;

    private long queuedBytes;

    internal StatementTriggers(ExtensionSqlSession session, TableDescriptor table, TriggerEventSet operation, BoundTrigger[] before, BoundTrigger[] after, long queueBudget)
    {
        this.queueBudget = queueBudget;
        this.session = session;
        this.table = table;
        this.operation = operation switch
        {
            TriggerEventSet.Insert => TriggerOperation.Insert,
            TriggerEventSet.Update => TriggerOperation.Update,
            _ => TriggerOperation.Delete,
        };
        this.before = before;
        this.after = after;
    }

    public bool HasBefore => before.Length > 0;

    public bool HasAfter => after.Length > 0;

    /// <summary>
    /// Fires the BEFORE triggers for one row. <paramref name="newRow"/> is the statement's own row
    /// (null for DELETE); a change a trigger makes lands in it and is coerced to the column type.
    /// Returns the columns the triggers set, or null when they set none, so an UPDATE can keep copying
    /// the stored cells of the columns nobody changed.
    /// </summary>
    public async Task<IReadOnlyList<TableColumnSchema>?> FireBeforeAsync(IReadOnlyDictionary<string, ColumnValue>? oldRow, Dictionary<string, ColumnValue>? newRow)
    {
        if (before.Length == 0)
            return null;

        List<TableColumnSchema>? changedColumns = null;

        TriggerRow? oldAccessor = oldRow is null ? null : new TriggerRow(oldRow);

        foreach (BoundTrigger trigger in before)
        {
            TriggerRow? newAccessor = newRow is null ? null : new TriggerRow(newRow, table.Schema.Columns!);

            await InvokeAsync(trigger, TriggerTiming.Before, oldAccessor, newAccessor).ConfigureAwait(false);

            if (newAccessor?.ChangedColumns is { } changed)
            {
                foreach (TableColumnSchema column in changed)
                {
                    string key = newRow!.ContainsKey(column.Name) ? column.Name : newRow.Keys.First(k => string.Equals(k, column.Name, StringComparison.OrdinalIgnoreCase));
                    newRow[key] = CastScalarFunctions.CoerceToColumnType(newRow[key], column);
                }

                (changedColumns ??= []).AddRange(changed);
            }
        }

        return changedColumns;
    }

    /// <summary>Queues one written row for the AFTER triggers. A no-op without AFTER triggers.</summary>
    public void QueueAfter(IReadOnlyDictionary<string, ColumnValue>? oldRow, IReadOnlyDictionary<string, ColumnValue>? newRow)
    {
        if (after.Length == 0)
            return;

        queuedBytes += (oldRow is null ? 0 : RowMemoryEstimator.EstimateRow(oldRow))
                       + (newRow is null || ReferenceEquals(newRow, oldRow) ? 0 : RowMemoryEstimator.EstimateRow(newRow));

        if (queuedBytes > queueBudget)
            throw new CamusDBException(
                CamusDBErrorCodes.TriggerQueueTooLarge,
                $"The rows kept for the AFTER triggers of table '{table.Name}' exceed {queueBudget} bytes " +
                "(spill_threshold_bytes); write fewer rows per statement or raise the budget");

        (queue ??= []).Add((oldRow, newRow));
    }

    /// <summary>Fires the AFTER triggers once per queued row, in write order, and empties the queue.</summary>
    public async Task FireAfterAsync()
    {
        if (queue is not { Count: > 0 } rows)
            return;

        queue = null;

        foreach ((IReadOnlyDictionary<string, ColumnValue>? oldRow, IReadOnlyDictionary<string, ColumnValue>? newRow) in rows)
        {
            TriggerRow? oldAccessor = oldRow is null ? null : new TriggerRow(oldRow);
            TriggerRow? newAccessor = newRow is null ? null : new TriggerRow(newRow);

            foreach (BoundTrigger trigger in after)
                await InvokeAsync(trigger, TriggerTiming.After, oldAccessor, newAccessor).ConfigureAwait(false);
        }
    }

    private async Task InvokeAsync(BoundTrigger trigger, TriggerTiming timing, TriggerRow? oldRow, TriggerRow? newRow)
    {
        ExtensionNesting.Enter("Trigger '" + trigger.Definition.Name + "'");

        TriggerContext context = new(trigger.Definition.Name, table.Name, timing, operation, oldRow, newRow, session);

        try
        {
            await trigger.Type.Instance.FireAsync(context).ConfigureAwait(false);

            // The trigger caught an error from its own SQL and returned: the failed statement may have
            // left writes staged, so this statement cannot continue.
            if (session.Failure is { } failure)
                throw new CamusDBException(
                    CamusDBErrorCodes.ExtensionCaughtSqlError,
                    $"Trigger '{trigger.Definition.Name}' on table '{table.Name}' returned after {failure}; " +
                    "without savepoints the statement cannot continue");
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (CamusDBException ex) when (ex.Code != CamusDBErrorCodes.SchemaCatchingUp)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.TriggerFailed,
                $"Trigger '{trigger.Definition.Name}' on table '{table.Name}' failed: {ex.Message}",
                ex);
        }
    }

    private sealed class TriggerContext : ITriggerContext
    {
        public TriggerContext(string triggerName, string tableName, TriggerTiming timing, TriggerOperation operation, IRow? old, IWritableRow? @new, ISqlSession sql)
        {
            TriggerName = triggerName;
            TableName = tableName;
            Timing = timing;
            Operation = operation;
            Old = old;
            New = @new;
            Sql = sql;
        }

        public string TriggerName { get; }

        public string TableName { get; }

        public TriggerTiming Timing { get; }

        public TriggerOperation Operation { get; }

        public IRow? Old { get; }

        public IWritableRow? New { get; }

        public ISqlSession Sql { get; }

        // A statement that writes is not cancellable once it has started writing, so nothing fires
        // this token in this version; see the contract.
        public CancellationToken CancellationToken => CancellationToken.None;
    }
}
