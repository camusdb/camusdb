/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// Runs <c>UPDATE … FROM</c> and <c>DELETE … USING</c> (PostgreSQL forms), and an UPDATE or a DELETE
/// whose target has an alias. The statement changes rows of its target table only; the other relations
/// decide which rows change and, for an UPDATE, supply values.
///
/// <para><b>Two phases.</b> The read phase runs one SELECT over the target and the listed relations,
/// with the statement's WHERE, at the statement timestamp, through the ordinary SELECT pipeline (so
/// every FROM form, every join strategy, views, subqueries and the privilege checks are those of a
/// SELECT). It reads the primary key of the target row and the value of each column of another
/// relation that the statement needs. A joined row carries no row id, so the primary key, which every
/// table has and no UPDATE can change, names the target row; one batched index read per chunk turns the
/// keys into row ids. The read phase finishes, and its rows are buffered (they spill to disk), before
/// the first write, so the statement cannot see its own changes. The write phase then uses the paths of
/// the plain statements: <see cref="RowUpdater.UpdateLocatedRowsAsync"/> and
/// <see cref="RowDeleter.DeleteLocatedRowsAsync"/> lock each row, read it again, re-check the
/// predicate, compute and write, so triggers, foreign keys, CHECK constraints, index maintenance, large
/// values, the mutation limit and RETURNING behave as for a plain UPDATE or DELETE.</para>
///
/// <para><b>Values of the other relations.</b> Each reference to a column of another relation is
/// replaced, in a copy of the SET values and of the re-checked predicate, by a hidden placeholder
/// (<see cref="ParameterPrefix"/> and a number). The lexer accepts only letters, digits and underscores
/// after <c>@</c>, so no statement can name one. For each row, the values that the read phase found are
/// added to the statement parameters under those names. A reference to the target becomes a bare column
/// name, so a SET value reads the row as the write phase locked and read it, never the older value of
/// the read phase: <c>SET balance = a.balance + d.amount</c> does not lose a concurrent change of
/// <c>balance</c>. A SET value that holds a subquery is computed in the read phase as a whole and reads
/// the statement snapshot, as a correlated subquery of a plain UPDATE does.</para>
///
/// <para><b>Re-check.</b> Each conjunct of the WHERE that names a target column and holds no subquery is
/// evaluated again on the locked read, with the values of the other relations that the read phase
/// found. A row that a concurrent commit moved out of the WHERE is left alone and not counted, as
/// PostgreSQL does when it re-checks an updated row.</para>
///
/// <para><b>One change per row.</b> An UPDATE whose target row matches two or more rows of the other
/// relations fails with <see cref="CamusDBErrorCodes.InvalidInput"/> before any write, and the error
/// names the primary key. The new value would depend on which match the statement used, which the plan
/// decides; PostgreSQL uses one of them and does not say which. A DELETE removes such a row once,
/// because the result does not depend on the match.</para>
///
/// <para><b>RETURNING</b> reads the target row only. A column of another relation is refused, and so is
/// a bare <c>*</c> when the statement has a relation list (in PostgreSQL it also returns the columns of
/// the other relations there). With a target alias only, <c>*</c> is the target's columns.</para>
/// </summary>
internal sealed class JoinedDmlExecutor
{
    /// <summary>The start of each hidden placeholder name.</summary>
    internal const string ParameterPrefix = "@#from";

    private readonly ExecutorContext context;

    private readonly SelectStatementExecutor selectExecutor;

    private readonly RowUpdater rowUpdater;

    private readonly RowDeleter rowDeleter;

    internal JoinedDmlExecutor(ExecutorContext context, SelectStatementExecutor selectExecutor, RowUpdater rowUpdater, RowDeleter rowDeleter)
    {
        this.context = context;
        this.selectExecutor = selectExecutor;
        this.rowUpdater = rowUpdater;
        this.rowDeleter = rowDeleter;
    }

    /// <summary>
    /// Runs an UPDATE that has a FROM list or a target alias. <paramref name="statementTimestamp"/> is
    /// the read timestamp of the statement, or null to read the latest committed value.
    /// </summary>
    internal async Task<ExecuteNonSQLResult> UpdateAsync(
        DatabaseDescriptor database,
        NodeAst ast,
        ExecuteSQLTicket ticket,
        HLCTimestamp? statementTimestamp)
    {
        if (ast.extendedTwo is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "LIMIT is not supported in UPDATE ... FROM or in an UPDATE whose target has an alias");

        NodeAst? returningList = StatementScope.GetReturningList(ast);
        if (returningList is not null)
            WriteReturningPlan.Validate(returningList);

        List<(string, NodeAst)> items = new();
        SQLExecutorUpdateCreator.GetUpdateItemList(ast.rightAst!, items);
        Dictionary<string, NodeAst> setValues = AssignmentTargets.ToSetList(items);

        NodeAst where = ast.extendedOne!;
        ConstantDivisionCheck.Check(where);
        foreach (NodeAst value in setValues.Values)
            ConstantDivisionCheck.Check(value);

        KvTransaction tx = ticket.TxnState;

        for (int fenceAttempt = 0; ; fenceAttempt++)
        {
            long writesBefore = tx.WriteCount;

            try
            {
                long writeShapeEpoch = database.WriteShape.Current;

                TableDescriptor table = await context.TableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                SelectStatementExecutor.PinForWrite(database, table, tx, writeShapeEpoch);
                MaterializedViewAccessGuard.RequireWritable(table);

                JoinedDmlPlan plan = JoinedDmlPlan.Build(
                    table, ast, setValues, where, returningList, DmlJoinSlots.RelationList(ast) is null ? "UPDATE" : "UPDATE ... FROM");

                WriteReturningPlan? returning = returningList is null
                    ? null
                    : await WriteReturningPlan.BindAsync(
                        selectExecutor.selectQueryCreator, selectExecutor.queryBinder, database, table,
                        plan.TargetReference, returningList, ticket).ConfigureAwait(false);
                WriteReturningPlan.ReturningRowCollector? collector = returning is not null && !ticket.DiscardReturningRows
                    ? returning.CreateCollector()
                    : null;

                // The order of the plain UPDATE: triggers, then the structural and static checks, then
                // the foreign-key checker, all before the first row is read. The read phase is bound
                // first, so the static checks see the type of each column of another relation.
                Extensions.StatementTriggers? triggers = rowUpdater.CreateStatementTriggers(database, table, tx);
                bool hasBefore = triggers is { HasBefore: true };

                await using SelectRowSource source = await BindSourceAsync(database, plan, ticket, statementTimestamp).ConfigureAwait(false);

                UpdateTicket checkTicket = new(
                    tx, ticket.DatabaseName, table.Name, plainValues: null, plan.SetValues!, plan.Recheck, filters: null, ticket.Parameters);
                StatementExpressionTypes? expressionTypes = RowUpdater.Validate(table, checkTicket, checkNotNull: !hasBefore);

                // Current until the statement returns, for the SET values and the re-check of each row.
                using StatementExpressionTypes.Scope expressionTypesScope = StatementExpressionTypes.Enter(expressionTypes);

                ForeignKeyStatementChecker foreignKeys = await rowUpdater.CreateStatementCheckerAsync(
                    database, table, tx, plan.SetValues!, assignsEveryColumn: hasBefore).ConfigureAwait(false);

                await using SpillableRowList located = new(QueryExecutionContext.For(database, ticket.CancellationToken));

                await LocateAsync(table, plan, source, ticket, statementTimestamp, located, refuseDuplicates: true, database.Options.SpillEffectiveThreshold).ConfigureAwait(false);

                if (ticket.RetryableAborts is { HasAbort: true })
                    return new(database, table, 0);

                IReadOnlySet<string>? evaluated = triggers is not null ? null : EvaluatedColumns(table.Schema, plan, returning);

                // The values of the other relations for the rows of the current chunk, by chunk position.
                List<IReadOnlyDictionary<string, ColumnValue>> chunkCells = new();

                RowUpdater.LocatedRowUpdate update = new()
                {
                    Database = database,
                    Table = table,
                    Transaction = tx,
                    AssignedColumns = RowUpdater.CollectAssignedColumns(checkTicket),
                    EvaluatedColumns = evaluated,
                    ForeignKeys = foreignKeys,
                    Triggers = triggers,
                    RetryableAborts = ticket.RetryableAborts,
                    ComputeNewRow = (position, oldRow) =>
                        new ValueTask<Dictionary<string, ColumnValue>?>(ComputeNewRow(plan, oldRow, chunkCells[position], ticket.Parameters)),
                };

                int updated = 0;
                int chunkSize = Math.Max(1, database.Options.SpillEffectiveThreshold);
                List<ObjectIdValue> chunkRowIds = new(Math.Min(chunkSize, 64));

                await foreach (QueryResultRow row in located.EnumerateAsync(ticket.CancellationToken).ConfigureAwait(false))
                {
                    chunkRowIds.Add(row.RowId);
                    chunkCells.Add(row.Row);

                    if (chunkRowIds.Count < chunkSize)
                        continue;

                    updated += await UpdateChunkAsync(update, chunkRowIds, collector).ConfigureAwait(false);
                    if (ticket.RetryableAborts is { HasAbort: true })
                        return new(database, table, 0);

                    chunkRowIds.Clear();
                    chunkCells.Clear();
                }

                if (chunkRowIds.Count > 0)
                {
                    updated += await UpdateChunkAsync(update, chunkRowIds, collector).ConfigureAwait(false);
                    if (ticket.RetryableAborts is { HasAbort: true })
                        return new(database, table, 0);
                }

                // After the statement's last write, never before: see ForeignKeyStatementChecker.
                if (updated > 0 && foreignKeys.HasWork)
                    await foreignKeys.CompleteAsync(tx).ConfigureAwait(false);

                if (triggers is not null)
                    await triggers.FireAfterAsync().ConfigureAwait(false);

                context.Statistics.TrackUpdate(database, table, updated, updatedValues: null);
                return Complete(database, table, updated, returning, collector);
            }
            catch (CamusDBException ex) when (NonQueryStatementDispatcher.FenceAfterWrite(ex, tx, writesBefore))
            {
                throw NonQueryStatementDispatcher.MustRetryAfterWrite(ex);
            }
            catch (CamusDBException ex) when (NonQueryStatementDispatcher.RetryFenceInSameTransaction(ex, fenceAttempt, tx, writesBefore))
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100 << fenceAttempt)).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Runs a DELETE that has a USING list or a target alias. <paramref name="statementTimestamp"/> is
    /// the read timestamp of the statement, or null to read the latest committed value.
    /// </summary>
    internal async Task<ExecuteNonSQLResult> DeleteAsync(
        DatabaseDescriptor database,
        NodeAst ast,
        ExecuteSQLTicket ticket,
        HLCTimestamp? statementTimestamp)
    {
        if (ast.extendedOne is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "LIMIT is not supported in DELETE ... USING or in a DELETE whose target has an alias");

        NodeAst? returningList = StatementScope.GetReturningList(ast);
        if (returningList is not null)
            WriteReturningPlan.Validate(returningList);

        NodeAst where = ast.rightAst!;
        ConstantDivisionCheck.Check(where);

        KvTransaction tx = ticket.TxnState;

        for (int fenceAttempt = 0; ; fenceAttempt++)
        {
            long writesBefore = tx.WriteCount;

            try
            {
                long writeShapeEpoch = database.WriteShape.Current;

                TableDescriptor table = await context.TableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                SelectStatementExecutor.PinForWrite(database, table, tx, writeShapeEpoch);
                MaterializedViewAccessGuard.RequireWritable(table);

                JoinedDmlPlan plan = JoinedDmlPlan.Build(
                    table, ast, setValues: null, where, returningList, DmlJoinSlots.RelationList(ast) is null ? "DELETE" : "DELETE ... USING");

                WriteReturningPlan? returning = returningList is null
                    ? null
                    : await WriteReturningPlan.BindAsync(
                        selectExecutor.selectQueryCreator, selectExecutor.queryBinder, database, table,
                        plan.TargetReference, returningList, ticket).ConfigureAwait(false);
                WriteReturningPlan.ReturningRowCollector? collector = returning is not null && !ticket.DiscardReturningRows
                    ? returning.CreateCollector()
                    : null;

                await using SelectRowSource source = await BindSourceAsync(database, plan, ticket, statementTimestamp).ConfigureAwait(false);

                // The static checks of the re-checked predicate, as the plain DELETE runs them on its WHERE.
                // The types they decide stay current until the statement returns, for the re-check.
                StatementExpressionTypes expressionTypes = new();
                ConditionalResultTypes.CheckCondition(
                    plan.Recheck, ConditionalResultTypes.LookupFor(table.Schema), expressionTypes, "WHERE", allowAggregates: false);
                using StatementExpressionTypes.Scope expressionTypesScope = StatementExpressionTypes.Enter(expressionTypes);

                // Built before the first write, in the order of the plain DELETE.
                ForeignKeyStatementChecker foreignKeys = await rowDeleter.CreateStatementCheckerAsync(database, table, tx).ConfigureAwait(false);
                Extensions.StatementTriggers? triggers = rowDeleter.CreateStatementTriggers(database, table, tx);

                await using SpillableRowList located = new(QueryExecutionContext.For(database, ticket.CancellationToken));

                await LocateAsync(table, plan, source, ticket, statementTimestamp, located, refuseDuplicates: false, database.Options.SpillEffectiveThreshold).ConfigureAwait(false);

                if (ticket.RetryableAborts is { HasAbort: true })
                    return new(database, table, 0);

                List<IReadOnlyDictionary<string, ColumnValue>> chunkCells = new();

                RowDeleter.LocatedRowDelete delete = new()
                {
                    Database = database,
                    Table = table,
                    Transaction = tx,
                    ForeignKeys = foreignKeys,
                    Triggers = triggers,
                    Returning = collector,
                    KeepColumns = plan.RecheckColumns,
                    Keep = plan.Recheck is null
                        ? null
                        : (position, row) => new ValueTask<bool>(Matches(plan.Recheck, row, Parameters(chunkCells[position], ticket.Parameters))),
                };

                int deleted = 0;
                int chunkSize = Math.Max(1, database.Options.SpillEffectiveThreshold);
                List<ObjectIdValue> chunkRowIds = new(Math.Min(chunkSize, 64));

                await foreach (QueryResultRow row in located.EnumerateAsync(ticket.CancellationToken).ConfigureAwait(false))
                {
                    chunkRowIds.Add(row.RowId);
                    chunkCells.Add(row.Row);

                    if (chunkRowIds.Count < chunkSize)
                        continue;

                    deleted += await rowDeleter.DeleteLocatedRowsAsync(delete, chunkRowIds).ConfigureAwait(false);
                    chunkRowIds.Clear();
                    chunkCells.Clear();
                }

                if (chunkRowIds.Count > 0)
                    deleted += await rowDeleter.DeleteLocatedRowsAsync(delete, chunkRowIds).ConfigureAwait(false);

                if (deleted > 0 && foreignKeys.HasWork)
                    await foreignKeys.CompleteAsync(tx).ConfigureAwait(false);

                if (triggers is not null)
                    await triggers.FireAfterAsync().ConfigureAwait(false);

                context.Statistics.TrackDelete(database, table, deleted);
                return Complete(database, table, deleted, returning, collector);
            }
            catch (CamusDBException ex) when (NonQueryStatementDispatcher.FenceAfterWrite(ex, tx, writesBefore))
            {
                throw NonQueryStatementDispatcher.MustRetryAfterWrite(ex);
            }
            catch (CamusDBException ex) when (NonQueryStatementDispatcher.RetryFenceInSameTransaction(ex, fenceAttempt, tx, writesBefore))
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100 << fenceAttempt)).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Binds the SELECT of the read phase with the statement snapshot open, so a subquery that the bind
    /// runs reads at the same timestamp as the drain, and gives each hidden placeholder the static type
    /// of its column. The bind demands SELECT on every relation it reads, the target included.
    /// </summary>
    private async Task<SelectRowSource> BindSourceAsync(
        DatabaseDescriptor database,
        JoinedDmlPlan plan,
        ExecuteSQLTicket ticket,
        HLCTimestamp? statementTimestamp)
    {
        SelectRowSource source;

        using (StatementSnapshotScope.Open(ticket.TxnState, statementTimestamp))
            source = await selectExecutor.BuildSelectSourceAsync(database, plan.JoinSelect, ticket, plan.StatementName).ConfigureAwait(false);

        plan.RememberTypes(source.StaticTypes);
        return source;
    }

    /// <summary>
    /// The read phase: drains the join at the statement timestamp, turns the primary key of each joined
    /// row into the row id of the target row, and adds one record per target row to
    /// <paramref name="located"/>: the row id, and the values of the other relations under their hidden
    /// names. With <paramref name="refuseDuplicates"/>, a target row that two joined rows name fails the
    /// statement; without it, the second one is dropped. A key with no row (a concurrent delete after the
    /// statement timestamp) is dropped too: there is nothing left to change. The snapshot closes before
    /// the write phase, which must read the latest committed rows.
    /// </summary>
    private static async Task LocateAsync(
        TableDescriptor table,
        JoinedDmlPlan plan,
        SelectRowSource source,
        ExecuteSQLTicket ticket,
        HLCTimestamp? statementTimestamp,
        SpillableRowList located,
        bool refuseDuplicates,
        int maxChunk)
    {
        TableIndexSchema primaryKey = table.Indexes.TryGetValue(CamusDBConstants.PrimaryKeyInternalName, out TableIndexSchema? pk)
            ? pk
            : throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Table '{table.Name}' has no primary key");

        int keyCount = plan.KeyColumns.Length;
        int chunkSize = Math.Max(1, maxChunk);

        HashSet<ObjectIdValue> seen = new();
        List<CompositeColumnValue> keys = new(Math.Min(chunkSize, 64));
        List<Dictionary<string, ColumnValue>> cells = new(Math.Min(chunkSize, 64));

        using StatementSnapshotScope snapshot = StatementSnapshotScope.Open(ticket.TxnState, statementTimestamp);

        IReadOnlyList<DerivedColumnSchema> columns = source.Columns;

        await foreach (QueryResultRow row in source.Cursor.WithCancellation(ticket.CancellationToken).ConfigureAwait(false))
        {
            ColumnValue[] key = new ColumnValue[keyCount];
            for (int i = 0; i < keyCount; i++)
                key[i] = Cell(row, columns[i]);

            Dictionary<string, ColumnValue> rowCells = new(plan.Cells.Count, StringComparer.Ordinal);
            foreach ((string name, int ordinal) in plan.Cells)
                rowCells[name] = Cell(row, columns[ordinal]);

            keys.Add(new CompositeColumnValue(key));
            cells.Add(rowCells);

            if (keys.Count >= chunkSize)
            {
                await AddChunkAsync(table, primaryKey, plan, ticket.TxnState, keys, cells, seen, located, refuseDuplicates).ConfigureAwait(false);
                keys.Clear();
                cells.Clear();
            }

            // A scan that recorded a retryable abort ended early: what it found is not the match set.
            if (ticket.RetryableAborts is { HasAbort: true })
                return;
        }

        if (keys.Count > 0)
            await AddChunkAsync(table, primaryKey, plan, ticket.TxnState, keys, cells, seen, located, refuseDuplicates).ConfigureAwait(false);

        await located.SealAsync(ticket.CancellationToken).ConfigureAwait(false);
    }

    /// <summary>Turns one chunk of primary keys into row ids and buffers the target rows.</summary>
    private static async Task AddChunkAsync(
        TableDescriptor table,
        TableIndexSchema primaryKey,
        JoinedDmlPlan plan,
        KvTransaction tx,
        List<CompositeColumnValue> keys,
        List<Dictionary<string, ColumnValue>> cells,
        HashSet<ObjectIdValue> seen,
        SpillableRowList located,
        bool refuseDuplicates)
    {
        ObjectIdValue?[] rowIds = await table.Store.LookupUniqueRowIdsAsync(tx, primaryKey.KvId, keys).ConfigureAwait(false);

        for (int i = 0; i < rowIds.Length; i++)
        {
            if (rowIds[i] is not { } rowId)
                continue;

            if (!seen.Add(rowId))
            {
                if (!refuseDuplicates)
                    continue;

                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"UPDATE ... FROM matched the row {ForeignKeyKeyText.Format(plan.KeyColumns, keys[i].Values)} of table " +
                    $"'{table.Name}' more than once. Each target row can match one row of the FROM list only, because the " +
                    "new value would otherwise depend on which match the statement used. Make the FROM list or the WHERE " +
                    "match one row for each target row, for example with GROUP BY in a derived table");
            }

            await located.AddAsync(new QueryResultRow(rowId, cells[i])).ConfigureAwait(false);
        }
    }

    /// <summary>Writes one chunk of target rows and hands the new images to the RETURNING collector.</summary>
    private async Task<int> UpdateChunkAsync(
        RowUpdater.LocatedRowUpdate update,
        List<ObjectIdValue> rowIds,
        WriteReturningPlan.ReturningRowCollector? collector)
    {
        Dictionary<string, ColumnValue>?[]? newImages = collector is not null ? new Dictionary<string, ColumnValue>?[rowIds.Count] : null;

        int written = await rowUpdater.UpdateLocatedRowsAsync(update, rowIds, newImages).ConfigureAwait(false);

        if (newImages is not null && update.RetryableAborts is not { HasAbort: true })
        {
            List<QueryResultRow> rows = new(written);
            for (int i = 0; i < newImages.Length; i++)
            {
                if (newImages[i] is { } image)
                    rows.Add(new QueryResultRow(rowIds[i], image));
            }

            await collector!.AddChunkAsync(rows).ConfigureAwait(false);
        }

        return written;
    }

    /// <summary>
    /// The new image of one target row: null when the re-checked predicate no longer holds on the locked
    /// read, else the row with every SET value evaluated against that read and the values of the other
    /// relations that the read phase found for the row.
    /// </summary>
    private static Dictionary<string, ColumnValue>? ComputeNewRow(
        JoinedDmlPlan plan,
        Dictionary<string, ColumnValue> oldRow,
        IReadOnlyDictionary<string, ColumnValue> cells,
        Dictionary<string, ColumnValue>? statementParameters)
    {
        Dictionary<string, ColumnValue>? parameters = Parameters(cells, statementParameters);

        if (plan.Recheck is not null && !Matches(plan.Recheck, oldRow, parameters))
            return null;

        // Every SET value reads the old image only, so SET a = b, b = a swaps the two values.
        Dictionary<string, ColumnValue> newRow = new(oldRow, StringComparer.OrdinalIgnoreCase);

        foreach ((string column, NodeAst value) in plan.SetValues!)
            newRow[column] = SqlExecutor.EvalExpr(value, oldRow, parameters);

        return newRow;
    }

    /// <summary>The statement parameters and the values of one row under their hidden names, in a new dictionary.</summary>
    private static Dictionary<string, ColumnValue>? Parameters(
        IReadOnlyDictionary<string, ColumnValue> cells,
        Dictionary<string, ColumnValue>? statementParameters)
    {
        if (cells.Count == 0)
            return statementParameters;

        Dictionary<string, ColumnValue> parameters = statementParameters is null
            ? new(cells.Count, StringComparer.Ordinal)
            : new(statementParameters, statementParameters.Comparer);

        foreach ((string name, ColumnValue value) in cells)
            parameters[name] = value;

        return parameters;
    }

    /// <summary>True when <paramref name="predicate"/> is true for the locked read, as a WHERE decides.</summary>
    private static bool Matches(NodeAst predicate, Dictionary<string, ColumnValue> row, Dictionary<string, ColumnValue>? parameters)
    {
        ColumnValue result = SqlExecutor.EvalExpr(predicate, row, parameters);
        return result.Type == ColumnType.Bool && result.BoolValue;
    }

    /// <summary>
    /// The target columns that the write phase must decode: those the SET values, the re-check and
    /// RETURNING read. Null decodes every column.
    /// </summary>
    private static IReadOnlySet<string>? EvaluatedColumns(TableSchema schema, JoinedDmlPlan plan, WriteReturningPlan? returning)
    {
        if (plan.TargetColumnsRead is not { } read)
            return null;

        if (returning is null)
            return read;

        if (returning.RequiredColumns is not { } returned)
            return null;

        HashSet<string> columns = new(read, StringComparer.OrdinalIgnoreCase);
        columns.UnionWith(returned);
        return columns;
    }

    /// <summary>The value of one output column of a joined row, read by its row key; see <see cref="DerivedColumnSchema"/>.</summary>
    private static ColumnValue Cell(QueryResultRow row, DerivedColumnSchema column) =>
        row.Row.TryGetValue(column.RowKey, out ColumnValue? value) ? value : ColumnValue.Null;

    private static ExecuteNonSQLResult Complete(
        DatabaseDescriptor database,
        TableDescriptor table,
        int changed,
        WriteReturningPlan? returning,
        WriteReturningPlan.ReturningRowCollector? collector)
    {
        if (returning is null || collector is null)
            return new(database, table, changed);

        return new(database, table, changed, warning: null, returning.Columns, collector.Rows);
    }
}
