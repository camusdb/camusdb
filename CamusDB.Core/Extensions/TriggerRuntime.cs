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
using CamusDB.Core.Extensions.Inline;
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
/// <para><b>Inline triggers.</b> A record with a C# body (<see cref="TriggerDefinition.IsInline"/>) binds to
/// a compiled unit of the engine's <see cref="InlineFunctionRuntime"/> instead. The unit is compiled on
/// this node at the first fire, from the body, the table's columns (the generated <c>NEW</c> and
/// <c>OLD</c> classes) and the inline functions of the database (the generated stubs). The binding is
/// cached on the record and keyed by the column list and the function snapshot it was compiled
/// against, so <c>ADD COLUMN</c> or <c>CREATE OR REPLACE FUNCTION</c> makes the next fire compile again.
/// A binding holds one reference on its unit; it gives it back when it is replaced, when the trigger
/// or its table is dropped, and when the database closes (<see cref="Retire"/>).</para>
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

            BoundTrigger bound = Bind(database, table, definition);

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
            new StatementTriggers.StatementSessions(executor, database.Name, transaction, principal),
            table,
            operation,
            before?.ToArray() ?? [],
            after?.ToArray() ?? [],
            queueBudget);
    }

    private BoundTrigger Bind(DatabaseDescriptor database, TableDescriptor table, TriggerDefinition definition)
    {
        // The form is decided by the body first: a record of the inline form has no type name, and a
        // node must never look one up for it.
        if (definition.IsInline)
            return BindInline(database, table, definition);

        if (definition.TypeName.Length == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.TriggerBindingMismatch,
                $"Trigger '{definition.Name}' on table '{table.Name}' has neither a type nor a body this node can fire" +
                (definition.Language.Length > 0 ? $" (language '{definition.Language}')" : ""));

        ExtensionRegistry registry = executor.Extensions;
        database.Schema.Extensions.TryGetValue(definition.ExtensionName, out ExtensionDefinition? record);

        // Cached per registry and per extension record: a record shared by two engines in one process
        // (never in production) still binds against each engine's own extensions, and an update of the
        // extension, which publishes a new record, makes the trigger bind again.
        //
        // A typed trigger's mapping is also checked against the table's columns. The binding keeps the
        // column list it was checked against, so a schema change binds the trigger again.
        IReadOnlyList<TableColumnSchema> columns = table.Schema.Columns ?? [];

        if (definition.Binding is TriggerBinding cached && ReferenceEquals(cached.Registry, registry) && ReferenceEquals(cached.Record, record)
            && (cached.Bound?.Type?.Typed is null || ReferenceEquals(cached.Columns, columns)))
            return cached.Bound ?? throw cached.Error!;

        TriggerBinding binding = Resolve(executor.ExtensionScopes, database, definition, record, registry, columns);
        definition.Binding = binding;

        return binding.Bound ?? throw binding.Error!;
    }

    private static TriggerBinding Resolve(
        ExtensionScopeFrames scopes,
        DatabaseDescriptor database,
        TriggerDefinition definition,
        ExtensionDefinition? record,
        ExtensionRegistry registry,
        IReadOnlyList<TableColumnSchema> columns)
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

        if (type!.RowType is not null)
        {
            try
            {
                List<string> problems = type.Typed!.Validate(columns);

                if (problems.Count > 0)
                    return TriggerBinding.Failed(registry, record, new CamusDBException(
                        CamusDBErrorCodes.TriggerRowMappingInvalid,
                        $"Trigger '{definition.Name}' cannot fire on this node: its row class {type.RowType.FullName} no longer fits the table ({string.Join("; ", problems)})"),
                        columns);
            }
            catch (CamusDBException ex)
            {
                return TriggerBinding.Failed(registry, record, ex, columns);
            }
        }

        return TriggerBinding.Succeeded(registry, record, new BoundTrigger(definition, type), columns);
    }

    /// <summary>
    /// Binds an inline trigger to its compiled unit, or returns the cached binding when the table's
    /// columns, the function snapshot and the runtime are the ones it was compiled against. A compile
    /// that failed for a reason that can pass (no compile slot in time) is tried again after its retry
    /// time; any other failure is cached and fails each statement until the key changes.
    /// </summary>
    private BoundTrigger BindInline(DatabaseDescriptor database, TableDescriptor table, TriggerDefinition definition)
    {
        InlineFunctionRuntime? runtime = executor.ExtensionScopes.InlineFunctions;
        IReadOnlyList<TableColumnSchema> columns = table.Schema.Columns ?? [];
        IReadOnlyDictionary<string, InlineFunctionDefinition> functions = database.Schema.Functions;

        object? current = definition.Binding;

        if (current is InlineTriggerBinding cached && cached.Matches(runtime, columns, functions))
            return cached.Bound ?? throw cached.Error!;

        InlineTriggerBinding next = ResolveInline(runtime, database, table, definition, columns, functions);

        // A statement that read the trigger list before a DROP TRIGGER or DROP TABLE applied can still
        // fire the dropped record. It fires with code it does not cache: a binding published now would
        // never be retired again.
        if (ReferenceEquals(current, DroppedBinding) && !IsListed(table, definition))
        {
            next.Retire();
            return next.Bound ?? throw next.Error!;
        }

        // One winner per record: a statement that bound the same record at the same time keeps its
        // own result for this statement and gives its unit reference back at once.
        if (ReferenceEquals(definition.ExchangeBinding(next, current), current))
            (current as InlineTriggerBinding)?.Retire();
        else
            next.Retire();

        return next.Bound ?? throw next.Error!;
    }

    private InlineTriggerBinding ResolveInline(
        InlineFunctionRuntime? runtime,
        DatabaseDescriptor database,
        TableDescriptor table,
        TriggerDefinition definition,
        IReadOnlyList<TableColumnSchema> columns,
        IReadOnlyDictionary<string, InlineFunctionDefinition> functions)
    {
        if (runtime is null)
            return InlineTriggerBinding.Failed(runtime, columns, functions, new CamusDBException(
                CamusDBErrorCodes.InlineFunctionUnavailableOnNode,
                $"Trigger '{definition.Name}' on table '{table.Name}' cannot fire on this node: this engine has no runtime for inline C# code"), 0);

        if (!string.Equals(definition.Language, InlineFunctionDefinition.CSharp, StringComparison.OrdinalIgnoreCase))
            return InlineTriggerBinding.Failed(runtime, columns, functions, new CamusDBException(
                CamusDBErrorCodes.TriggerBindingMismatch,
                $"Trigger '{definition.Name}' on table '{table.Name}' is written in '{definition.Language}', which this node cannot run"), 0);

#if CAMUSDB_BROWSER
        return InlineTriggerBinding.Failed(runtime, columns, functions, new CamusDBException(
            CamusDBErrorCodes.InlineFunctionUnavailableOnNode,
            $"Trigger '{definition.Name}' on table '{table.Name}' cannot fire in this build of the server: it has no C# compiler"), 0);
#else
        InlineTriggerSource source = InlineTriggerSources.For(database.Schema, table.Schema, definition.Name, definition.Source);
        string label = database.Name + "." + table.Name + "." + definition.Name;

        InlineFunctionUnit unit = runtime.AcquireTrigger(source, label);

        if (unit.Trigger is not { } runner)
        {
            CamusDBException failure = unit.Failure ?? new CamusDBException(
                CamusDBErrorCodes.InlineFunctionUnavailableOnNode,
                $"Trigger '{definition.Name}' on table '{table.Name}' cannot fire on this node");
            long retryAt = unit.RetryAt;

            runtime.Release(unit, label);
            return InlineTriggerBinding.Failed(runtime, columns, functions, failure, retryAt);
        }

        Func<object?[], object?>[] stubs = InlineFunctionRuntime.FunctionStubs(source.Functions, executor.ExtensionScopes.ResolveInline(database));
        Func<ITriggerContext, ISqlSession, Task> fire = runtime.GuardTrigger(runner, stubs, definition.Name);

        return InlineTriggerBinding.Succeeded(
            runtime, columns, functions,
            new BoundTrigger(definition, null, new InlineTriggerFire(fire, definition.Owner, definition.OwnerId)),
            unit, label);
#endif
    }

    /// <summary>
    /// Gives back the compiled code an inline trigger's binding holds. Called by the schema apply when
    /// the trigger or its table is dropped, and when the database's schema is disposed. Safe to call
    /// for a DLL trigger (it does nothing), more than once, and while a statement still fires the
    /// trigger: the code stays loaded until the last delegate into it is gone, and a record whose
    /// binding was retired binds again at its next fire.
    /// </summary>
    internal static void Retire(TriggerDefinition definition)
    {
        if (!definition.IsInline)
            return;

        object? previous = definition.Binding;
        definition.Binding = DroppedBinding;
        (previous as InlineTriggerBinding)?.Retire();
    }

    /// <summary>The binding of an inline record that <see cref="Retire"/> took the code from.</summary>
    private static readonly object DroppedBinding = new();

    private static bool IsListed(TableDescriptor table, TriggerDefinition definition)
    {
        if (table.Schema.Triggers is not { } triggers)
            return false;

        foreach (TriggerDefinition listed in triggers)
        {
            if (ReferenceEquals(listed, definition))
                return true;
        }

        return false;
    }

    /// <summary>Retires the bindings of every trigger of <paramref name="table"/>; see <see cref="Retire"/>.</summary>
    internal static void RetireAll(TableSchema table)
    {
        if (table.Triggers is not { } triggers)
            return;

        foreach (TriggerDefinition trigger in triggers)
            Retire(trigger);
    }

    /// <summary>
    /// The cached outcome of binding one inline record on one engine: the bound trigger and the unit it
    /// holds a reference on, or the error. Keyed by reference identity of the column list and of the
    /// function snapshot: both are replaced, never edited, when they change.
    /// </summary>
    private sealed class InlineTriggerBinding
    {
        private InlineFunctionUnit? unit;

        private string label = "";

        private int retired;

        public required InlineFunctionRuntime? Runtime { get; init; }

        public required IReadOnlyList<TableColumnSchema> Columns { get; init; }

        public required IReadOnlyDictionary<string, InlineFunctionDefinition> Functions { get; init; }

        public BoundTrigger? Bound { get; init; }

        public CamusDBException? Error { get; init; }

        /// <summary>For a failure that can pass, the tick count from which a fire compiles again; else 0.</summary>
        public long RetryAt { get; init; }

        public bool Matches(InlineFunctionRuntime? runtime, IReadOnlyList<TableColumnSchema> columns, IReadOnlyDictionary<string, InlineFunctionDefinition> functions) =>
            Volatile.Read(ref retired) == 0
            && ReferenceEquals(Runtime, runtime)
            && ReferenceEquals(Columns, columns)
            && ReferenceEquals(Functions, functions)
            && (RetryAt == 0 || Environment.TickCount64 < RetryAt);

        /// <summary>Gives the unit reference back, once.</summary>
        public void Retire()
        {
            if (Interlocked.Exchange(ref retired, 1) != 0)
                return;

            if (unit is { } held && Runtime is { } runtime)
                runtime.Release(held, label);

            unit = null;
        }

        public static InlineTriggerBinding Succeeded(
            InlineFunctionRuntime runtime, IReadOnlyList<TableColumnSchema> columns, IReadOnlyDictionary<string, InlineFunctionDefinition> functions,
            BoundTrigger bound, InlineFunctionUnit unit, string label) =>
            new() { Runtime = runtime, Columns = columns, Functions = functions, Bound = bound, unit = unit, label = label };

        public static InlineTriggerBinding Failed(
            InlineFunctionRuntime? runtime, IReadOnlyList<TableColumnSchema> columns, IReadOnlyDictionary<string, InlineFunctionDefinition> functions,
            CamusDBException error, long retryAt) =>
            new() { Runtime = runtime, Columns = columns, Functions = functions, Error = error, RetryAt = retryAt };
    }

    /// <summary>The cached outcome of binding one record on one engine against one extension record.</summary>
    private sealed class TriggerBinding
    {
        public required ExtensionRegistry Registry { get; init; }

        public required ExtensionDefinition? Record { get; init; }

        public BoundTrigger? Bound { get; init; }

        public CamusDBException? Error { get; init; }

        /// <summary>The table columns a typed trigger's mapping was checked against; null when it was not.</summary>
        public IReadOnlyList<TableColumnSchema>? Columns { get; init; }

        public static TriggerBinding Succeeded(ExtensionRegistry registry, ExtensionDefinition record, BoundTrigger bound, IReadOnlyList<TableColumnSchema> columns) =>
            new() { Registry = registry, Record = record, Bound = bound, Columns = columns };

        public static TriggerBinding Failed(ExtensionRegistry registry, ExtensionDefinition? record, CamusDBException error, IReadOnlyList<TableColumnSchema>? columns = null) =>
            new() { Registry = registry, Record = record, Error = error, Columns = columns };
    }
}

/// <summary>
/// A trigger record and what it fires: the loaded type of the DLL form (<paramref name="Type"/>), or the
/// guarded compiled body of the inline form (<paramref name="Inline"/>). Exactly one is set.
/// </summary>
internal sealed record BoundTrigger(TriggerDefinition Definition, ExtensionTriggerType? Type, InlineTriggerFire? Inline = null);

/// <summary>
/// The fire delegate of an inline trigger (see <c>InlineFunctionRuntime.GuardTrigger</c>) and the owner
/// whose rights its SQL runs with. <paramref name="OwnerId"/> is null for a trigger created while
/// authentication was off; such a trigger's SQL runs with the statement's own rights.
/// </summary>
internal sealed record InlineTriggerFire(Func<ITriggerContext, ISqlSession, Task> Fire, string? Owner, string? OwnerId);

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
/// <para><b>Typed triggers.</b> A trigger of <c>ICamusTrigger&lt;TRow&gt;</c> fires through its
/// <see cref="TypedTriggerAdapter"/>. The AFTER queue still keeps the engine rows, not row objects, so
/// its byte budget is the same for both forms; the objects are built when the trigger fires.</para>
///
/// <para><b>BEFORE output is coerced and then validated.</b> The values a BEFORE trigger set are
/// converted to their column types here, exactly as statement values are; the caller then runs its
/// usual NOT NULL, length and CHECK validation, so a trigger cannot store a value the statement could
/// not.</para>
///
/// <para><b>Rights.</b> A DLL trigger's SQL runs as the user who ran the statement: its author is an
/// operator who installed the extension. An inline trigger's SQL runs as the trigger's owner, so a user
/// who may only alter one table cannot make every writer of it run SQL with the writer's rights
/// (<see cref="StatementSessions"/>). The row changes of a BEFORE trigger need no privilege: they are
/// part of the statement's own write.</para>
///
/// <para><b>Errors.</b> An exception from trigger code ends the statement as
/// <see cref="CamusDBErrorCodes.TriggerFailed"/>. A <see cref="CamusDBException"/> raised by the
/// trigger's own SQL keeps its code (a duplicate key stays a duplicate key), and cancellation passes
/// through unchanged.</para>
/// </summary>
internal sealed class StatementTriggers
{
    private readonly StatementSessions sessions;

    private readonly TableDescriptor table;

    private readonly TriggerOperation operation;

    private readonly BoundTrigger[] before;

    private readonly BoundTrigger[] after;

    private List<(IReadOnlyDictionary<string, ColumnValue>? Old, IReadOnlyDictionary<string, ColumnValue>? New)>? queue;

    private readonly long queueBudget;

    private long queuedBytes;

    internal StatementTriggers(StatementSessions sessions, TableDescriptor table, TriggerEventSet operation, BoundTrigger[] before, BoundTrigger[] after, long queueBudget)
    {
        this.queueBudget = queueBudget;
        this.sessions = sessions;
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

            await InvokeAsync(trigger, TriggerTiming.Before, oldRow, oldAccessor, newRow, newAccessor).ConfigureAwait(false);

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
                await InvokeAsync(trigger, TriggerTiming.After, oldRow, oldAccessor, newRow, newAccessor).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Fires one trigger for one row. An untyped trigger gets the row accessors; a typed one gets row
    /// objects built from the row dictionaries by its adapter, which writes the changed properties of a
    /// BEFORE trigger's NEW back through <paramref name="newRow"/>.
    /// </summary>
    private async Task InvokeAsync(
        BoundTrigger trigger,
        TriggerTiming timing,
        IReadOnlyDictionary<string, ColumnValue>? oldValues,
        TriggerRow? oldRow,
        IReadOnlyDictionary<string, ColumnValue>? newValues,
        TriggerRow? newRow)
    {
        ExtensionNesting.Enter("Trigger '" + trigger.Definition.Name + "'");

        // An inline trigger's SQL runs with its owner's rights; a DLL trigger's with the statement's.
        ExtensionSqlSession session = trigger.Inline is { } owned
            ? await sessions.ForOwnerAsync(owned, trigger.Definition.Name, table.Name).ConfigureAwait(false)
            : sessions.Statement;

        TriggerContext context = new(trigger.Definition.Name, table.Name, timing, operation, oldRow, newRow, session);

        try
        {
            if (trigger.Inline is { } inline)
                await inline.Fire(context, session).ConfigureAwait(false);
            else if (trigger.Type!.Typed is { } typed)
                await typed.FireAsync(
                    trigger.Type.Instance, context, oldValues, newValues,
                    timing == TriggerTiming.Before && newRow is { IsWritable: true } ? newRow : null,
                    trigger.Definition.Name).ConfigureAwait(false);
            else
                await ((ICamusTrigger)trigger.Type.Instance).FireAsync(context).ConfigureAwait(false);

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

    /// <summary>
    /// The SQL sessions the triggers of one statement use: the statement's own session, and one session
    /// per inline trigger owner, made at the first fire that needs it and kept for the statement.
    ///
    /// <para><b>When the owner's rights apply.</b> When the engine has an auth service and the
    /// statement runs as a principal. Without an auth service or a principal there are no rights to
    /// switch between, and the trigger fires with the statement's session. An owner that cannot be
    /// loaded any more (dropped, or a new user under the same name) fails the fire with
    /// <see cref="CamusDBErrorCodes.TriggerOwnerMissing"/>: the trigger never falls back to the
    /// invoker's rights.</para>
    ///
    /// <para><b>A trigger with no owner.</b> A trigger created while authentication was off records no
    /// owner. Under a principal it gets a refusing session: the body still fires and can change
    /// <c>NEW</c>, but each statement it runs through <c>Sql</c> fails with
    /// <see cref="CamusDBErrorCodes.TriggerOwnerMissing"/>. The statement's session would run the
    /// author's SQL with the rights of whoever writes the table, a superuser included, and the author
    /// may be anyone who had API access before authentication was turned on.</para>
    ///
    /// <para>All sessions share the statement's transaction, so a failed statement of any of them
    /// marks that one transaction rollback-only.</para>
    /// </summary>
    internal sealed class StatementSessions
    {
        private readonly CommandExecutor executor;

        private readonly string databaseName;

        private readonly KvTransaction transaction;

        private readonly Principal? principal;

        private Dictionary<string, ExtensionSqlSession>? owners;

        public StatementSessions(CommandExecutor executor, string databaseName, KvTransaction transaction, Principal? principal)
        {
            this.executor = executor;
            this.databaseName = databaseName;
            this.transaction = transaction;
            this.principal = principal;
            Statement = new ExtensionSqlSession(executor, databaseName, transaction, principal);
        }

        /// <summary>The session with the rights of the user who ran the statement.</summary>
        public ExtensionSqlSession Statement { get; }

        /// <summary>The session an inline trigger's SQL runs in; see the class summary for the rules.</summary>
        public async ValueTask<ExtensionSqlSession> ForOwnerAsync(InlineTriggerFire trigger, string triggerName, string tableName)
        {
            if (principal is null || executor.OwnerPrincipalResolver is not { } resolve)
                return Statement;

            if (trigger.OwnerId is not { Length: > 0 } ownerId)
                return Ownerless(triggerName, tableName);

            if (owners is not null && owners.TryGetValue(ownerId, out ExtensionSqlSession? cached))
                return cached;

            Principal owner = await resolve(trigger.Owner ?? "", ownerId).ConfigureAwait(false)
                ?? throw new CamusDBException(
                    CamusDBErrorCodes.TriggerOwnerMissing,
                    $"Trigger '{triggerName}' on table '{tableName}' runs its SQL as its owner '{trigger.Owner}', who no longer exists; " +
                    "drop the trigger and create it again as a user who may run its SQL");

            ExtensionSqlSession session = new(executor, databaseName, transaction, owner);
            (owners ??= new Dictionary<string, ExtensionSqlSession>(StringComparer.Ordinal))[ownerId] = session;
            return session;
        }

        /// <summary>
        /// The refusing session of a trigger with no owner, kept for the statement. The key cannot be
        /// a user id: a user id never starts with a NUL character.
        /// </summary>
        private ExtensionSqlSession Ownerless(string triggerName, string tableName)
        {
            string key = "\0" + tableName + "\0" + triggerName;

            if (owners is not null && owners.TryGetValue(key, out ExtensionSqlSession? cached))
                return cached;

            ExtensionSqlSession session = new(
                executor, databaseName, transaction, principal: null,
                refusal: $"Trigger '{triggerName}' on table '{tableName}' has no owner, because it was created while authentication was off, " +
                         "so its SQL has no rights to run with; a superuser drops the trigger and creates it again as a user who may run its SQL");

            (owners ??= new Dictionary<string, ExtensionSqlSession>(StringComparer.Ordinal))[key] = session;
            return session;
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
