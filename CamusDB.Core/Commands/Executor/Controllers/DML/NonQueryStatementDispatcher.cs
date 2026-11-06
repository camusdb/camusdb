
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor.Controllers.Auth;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Statistics;
using CamusDB.Core.Transactions;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DML;

/// <summary>
/// Executes a SQL statement that returns no rows: INSERT, UPDATE, DELETE, INSERT … SELECT, CTAS,
/// REFRESH, cache eviction, comments, and the SET TRANSACTION family.
///
/// <para><b>It also accepts every schema DDL statement</b>, forwarding to the DDL dispatcher rather
/// than rejecting it. That is deliberate: a client routes any non-SELECT statement to whichever
/// endpoint it uses for those, so a statement that works through one entry point and answers
/// "unknown statement" through the other is, to that client, indistinguishable from a feature the
/// server does not support.</para>
///
/// <para><b>Two different retry owners, and mixing them up is the trap.</b> A
/// <c>SchemaCatchingUp</c> failure is retried <em>here</em>, around the table open: the fence fires
/// before any write or schema pin, so the in-flight transaction is untouched and the same one is
/// safely reused on the next attempt. Serialization failures are <em>not</em> retried here — the
/// HTTP controller replays those from a fresh transaction for autocommit statements, and an explicit
/// multi-statement transaction surfaces them to the caller, which owns the retry loop. Retrying a
/// serialization failure in place would reuse a transaction that is already doomed.</para>
///
/// <para>Note where the subquery rewrites of INSERT VALUES, UPDATE and DELETE sit: <b>outside</b>
/// the fence-retry loop, so a retry does not re-execute the inner query.</para>
/// </summary>
internal sealed class NonQueryStatementDispatcher
{
    internal readonly ExecutorContext context;

    internal readonly CatalogsManager catalogs;

    internal readonly SqlExecutor sqlExecutor;

    internal readonly SqlParserCache sqlParserCache;

    internal readonly Auth.StatementAuthorizer statementAuthorizer;

    internal readonly DdlStatementDispatcher ddlDispatcher;

    private readonly ServerLevelStatementDispatcher serverLevelDispatcher;

    internal readonly SelectStatementExecutor selectExecutor;

    internal readonly CreateTableAsSelectExecutor ctasExecutor;

    internal readonly SchemaDdlService schemaDdl;

    private readonly DatabaseLifecycleService databaseLifecycle;

    private readonly UserAdminService userAdmin;

    internal readonly RowInserter rowInserter;

    internal readonly RowUpdater rowUpdater;

    internal readonly RowDeleter rowDeleter;

    internal readonly RowInsertSelector rowInsertSelector;

    /// <summary>Runs an INSERT that carries an ON CONFLICT clause, and UPSERT; built over the inserter and the updater.</summary>
    internal readonly RowUpserter rowUpserter;

    internal readonly QueryExecutor queryExecutor;

    internal readonly SubqueryRewriter subqueryRewriter;

    internal readonly MaterializedViewRefresher matViewRefresher;

    /// <summary>
    /// Reserves the sequence values a statement will draw before it runs. Held here because this
    /// is where an <c>INSERT … SELECT</c> learns its source row count.
    /// </summary>
    internal readonly Functions.SequenceStatementBinder sequenceBinder;

    internal NonQueryStatementDispatcher(
        ExecutorContext context,
        CatalogsManager catalogs,
        SqlExecutor sqlExecutor,
        SqlParserCache sqlParserCache,
        Auth.StatementAuthorizer statementAuthorizer,
        DdlStatementDispatcher ddlDispatcher,
        ServerLevelStatementDispatcher serverLevelDispatcher,
        SelectStatementExecutor selectExecutor,
        CreateTableAsSelectExecutor ctasExecutor,
        SchemaDdlService schemaDdl,
        DatabaseLifecycleService databaseLifecycle,
        UserAdminService userAdmin,
        RowInserter rowInserter,
        RowUpdater rowUpdater,
        RowDeleter rowDeleter,
        RowInsertSelector rowInsertSelector,
        QueryExecutor queryExecutor,
        SubqueryRewriter subqueryRewriter,
        MaterializedViewRefresher matViewRefresher,
        Functions.SequenceStatementBinder sequenceBinder
    )
    {
        // Guarded because these are captured at construction, not read per call: a collaborator
        // built later in the composing constructor would be captured as null here and only fail
        // much later, deep inside a statement, where the cause is far from the mistake.
        ArgumentNullException.ThrowIfNull(statementAuthorizer);
        ArgumentNullException.ThrowIfNull(sequenceBinder);
        ArgumentNullException.ThrowIfNull(ddlDispatcher);
        ArgumentNullException.ThrowIfNull(serverLevelDispatcher);
        ArgumentNullException.ThrowIfNull(selectExecutor);
        ArgumentNullException.ThrowIfNull(ctasExecutor);
        ArgumentNullException.ThrowIfNull(schemaDdl);
        ArgumentNullException.ThrowIfNull(databaseLifecycle);
        ArgumentNullException.ThrowIfNull(userAdmin);

        this.context = context;
        this.catalogs = catalogs;
        this.sqlExecutor = sqlExecutor;
        this.sqlParserCache = sqlParserCache;
        this.statementAuthorizer = statementAuthorizer;
        this.ddlDispatcher = ddlDispatcher;
        this.serverLevelDispatcher = serverLevelDispatcher;
        this.selectExecutor = selectExecutor;
        this.ctasExecutor = ctasExecutor;
        this.schemaDdl = schemaDdl;
        this.databaseLifecycle = databaseLifecycle;
        this.userAdmin = userAdmin;
        this.rowInserter = rowInserter;
        this.rowUpdater = rowUpdater;
        this.rowDeleter = rowDeleter;
        this.rowInsertSelector = rowInsertSelector;
        rowUpserter = new RowUpserter(rowInserter, rowUpdater);
        this.queryExecutor = queryExecutor;
        this.subqueryRewriter = subqueryRewriter;
        this.matViewRefresher = matViewRefresher;
        this.sequenceBinder = sequenceBinder;
    }

    /// <summary>
    /// Binds the RETURNING list of an INSERT, UPDATE or DELETE against the table the current attempt
    /// opened. All three statements hold their target table in <see cref="NodeAst.leftAst"/>. The bind
    /// demands SELECT on the target; see <see cref="WriteReturningPlan"/>.
    /// </summary>
    private Task<WriteReturningPlan> BindReturningAsync(
        DatabaseDescriptor database, TableDescriptor table, NodeAst writeAst, NodeAst returningList, ExecuteSQLTicket ticket) =>
        WriteReturningPlan.BindAsync(
            selectExecutor.selectQueryCreator,
            selectExecutor.queryBinder,
            database,
            table,
            writeAst.leftAst!,
            returningList,
            ticket);

    /// <summary>
    /// The ON CONFLICT clause of an INSERT (or the implied one of an UPSERT), or null for a plain
    /// INSERT. A scalar, IN or EXISTS subquery in the DO UPDATE SET list or WHERE is replaced by its
    /// value here, at one statement timestamp, as UPDATE replaces one. A correlated EXISTS is refused
    /// by the rewrite, and a subquery that is still left afterwards would read the conflicting rows,
    /// which the per-row evaluation cannot run, so it is refused before anything is written.
    /// </summary>
    private async Task<OnConflictClause?> PrepareOnConflictAsync(DatabaseDescriptor database, NodeAst ast, ExecuteSQLTicket ticket)
    {
        if (OnConflictClause.Of(ast) is not { } clause)
            return null;

        if (clause.SetValues is null)
            return clause;

        UpdateTicket assignments = new(
            ticket.TxnState, ticket.DatabaseName, ast.leftAst!.yytext!, null, clause.SetValues, clause.Where, null, ticket.Parameters);

        HLCTimestamp? statementTimestamp = MintStatementTimestamp(database, ticket.TxnState);
        using (StatementSnapshotScope.Open(ticket.TxnState, statementTimestamp))
            assignments = await RewriteUpdateSubqueriesAsync(database, assignments, ticket, "ON CONFLICT DO UPDATE").ConfigureAwait(false);

        bool subqueryLeft = assignments.Where is not null && RequiredColumnAnalyzer.ContainsSubqueryNode(assignments.Where);
        foreach (NodeAst expression in assignments.ExprValues!.Values)
            subqueryLeft |= RequiredColumnAnalyzer.ContainsSubqueryNode(expression);

        if (subqueryLeft)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "A correlated subquery is not supported in ON CONFLICT DO UPDATE; only a subquery that does not read the conflicting row can be used there");

        return clause.WithAssignments(assignments.ExprValues, assignments.Where);
    }

    /// <summary>
    /// Builds the result of an INSERT. With a RETURNING list and no count-only request, the
    /// buffered rows are projected and carried on the result with their schema; otherwise the result
    /// has the row count only, in the same shape as an INSERT without RETURNING.
    /// </summary>
    private static async Task<ExecuteNonSQLResult> CompleteInsertAsync(
        DatabaseDescriptor database,
        TableDescriptor table,
        int inserted,
        string? warning,
        WriteReturningPlan? returning,
        List<QueryResultRow>? insertedRows)
    {
        if (returning is null || insertedRows is null)
            return new(database, table, inserted, warning);

        List<QueryResultRow> projected = await returning.ProjectAsync(insertedRows).ConfigureAwait(false);
        return new(database, table, inserted, warning, returning.Columns, projected);
    }

    /// <summary>
    /// Builds the result of an UPDATE or a DELETE. The collector already holds the projected rows,
    /// because the write path projects each chunk as soon as it is written. Without a collector (no
    /// RETURNING list, or a count-only request) the result has the row count only.
    /// </summary>
    private static ExecuteNonSQLResult CompleteChangeResult(
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

    /// <summary>
    /// Decides whether a <see cref="CamusDBErrorCodes.SchemaCatchingUp"/> fence error may be retried in
    /// the same transaction. The retry reuses the transaction, which is safe only when the failed
    /// attempt wrote nothing: the fence usually fires while the target table opens, before any write,
    /// but a trigger, a procedure, or a related-table open later in the statement can meet it after a
    /// write, and a retry would then repeat that write. Such an attempt is turned into
    /// <see cref="CamusDBErrorCodes.TransactionMustRetry"/>, which the caller retries from a new
    /// transaction.
    /// </summary>
    /// <remarks>Two separate catch clauses use this pair, because an exception thrown inside an
    /// exception filter is swallowed: <see cref="FenceAfterWrite"/> converts, this one retries.</remarks>
    internal static bool RetryFenceInSameTransaction(CamusDBException ex, int fenceAttempt, Transactions.KvTransaction tx, long writesBefore) =>
        ex.Code == CamusDBErrorCodes.SchemaCatchingUp
        && fenceAttempt < SelectStatementExecutor.MaxFenceRetries
        && tx.WriteCount == writesBefore;

    /// <summary>True for a fence error met after the attempt wrote: it must not be retried here.</summary>
    internal static bool FenceAfterWrite(CamusDBException ex, Transactions.KvTransaction tx, long writesBefore) =>
        ex.Code == CamusDBErrorCodes.SchemaCatchingUp && tx.WriteCount != writesBefore;

    private static CamusDBException MustRetryAfterWrite(CamusDBException fence) =>
        new(CamusDBErrorCodes.TransactionMustRetry,
            "A schema change in progress interrupted the statement after it wrote; retry the transaction",
            fence);

    /// <summary>
    /// Executes a SQL statement that returns no rows, and guards the statement boundary with
    /// PostgreSQL's rule: a statement on an aborted transaction is refused before it runs, and a
    /// statement that fails aborts its transaction (marks it rollback-only) before the error leaves
    /// the engine, so no caller can commit the rows of a statement it was told failed, and no later
    /// statement runs on top of them.
    ///
    /// <para>The guard sits here, not in the transports, because every DML path meets this method:
    /// the SQL entry points, the query entry point's <c>RETURNING</c> hand-off, and the SQL an
    /// extension runs. A transactional Kahuna batch stages every key or none, so a duplicate key in a
    /// multi-row INSERT leaves nothing staged; a foreign-key check refused after the row writes, or a
    /// later batch of a large statement, does leave intents behind. The rule does not distinguish the
    /// two, because a client cannot see which one it got (see
    /// <see cref="Transactions.KvTransaction.MarkRollbackOnly(string)"/>).</para>
    ///
    /// <para>A retryable abort carried in <see cref="ExecuteSQLTicket.RetryableAborts"/> instead of
    /// thrown is a failure of the same kind and is guarded the same way.</para>
    /// </summary>
    /// <param name="ticket"></param>
    /// <param name="activity">The statement's entry in the running-statement list, which the parse names; or null.</param>
    /// <returns>The number of inserted/modified/deleted rows</returns>
    internal async Task<ExecuteNonSQLResult> ExecuteNonSQLQuery(
        CommandExecutor executor, ExecuteSQLTicket ticket, Diagnostics.QueryActivityEntry? activity = null)
    {
        // Null for a database-scoped mutation (CREATE USER, GRANT, ...), which writes no transaction.
        Transactions.KvTransaction? tx = ticket.TxnState;
        tx?.ThrowIfRollbackOnly();

        try
        {
            ExecuteNonSQLResult result = await ExecuteNonSQLQueryCore(executor, ticket, activity).ConfigureAwait(false);

            if (tx is not null && ticket.RetryableAborts is { Abort: { } abort })
                tx.MarkRollbackOnly(abort);

            return result;
        }
        catch (Exception ex)
        {
            tx?.MarkRollbackOnly(ex);
            throw;
        }
    }

    /// <summary>The statement dispatch behind <see cref="ExecuteNonSQLQuery"/>; see there for the guard around it.</summary>
    private async Task<ExecuteNonSQLResult> ExecuteNonSQLQueryCore(
        CommandExecutor executor, ExecuteSQLTicket ticket, Diagnostics.QueryActivityEntry? activity)
    {
        context.Validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql, sqlParserCache);
        activity?.Describe(ast.nodeType);

        // Executor stage timing for the write path (parse+plan+stage, exclusive of transport/commit
        // transport). Covers every return path of this method; a no-op when diagnostics are disabled.
        using ServerDiagnostics.ExecuteScope executeScope = ServerDiagnostics.MeasureExecute(
            ServerDiagnostics.Tags.Operation.NonQuery, MapStatementFamily(ast.nodeType));
        using System.Diagnostics.Activity? executeSpan = ServerDiagnostics.StartSpan(ServerDiagnostics.Spans.Execute);
        executeSpan?.SetTag("statement", MapStatementFamily(ast.nodeType));

        statementAuthorizer.SetAuthorizationScope(ticket, ast);
        ticket = StatementScopedValues.Attach(ticket, ast);
        await statementAuthorizer.EnforceAsync(ticket, ast).ConfigureAwait(false);

        // Server-level statements are dispatched before any database is opened — including
        // CREATE DATABASE, which names a database that does not exist yet and so cannot survive the
        // open below. Shared with the DDL entry point so the two lists cannot drift again.
        ServerLevelOutcome serverLevel = await serverLevelDispatcher.TryExecuteAsync(ticket, ast).ConfigureAwait(false);
        if (serverLevel.Handled)
            return default;

        DatabaseDescriptor database = await context.DatabaseOpener.Open(ticket.DatabaseName).ConfigureAwait(false);
        using DatabaseUseHandle _ = database.Use();

        // Mark the transaction as having executed a statement for every statement type except the
        // SET TRANSACTION family — those must be the first statement per standard SQL semantics, so
        // they are exempt from the gate (mirrors ExecuteSQLQuery). A client routes these no-rows
        // statements to whichever endpoint it uses for non-SELECT SQL, which is often this one.
        if (! SetTransactionStatement.IsSetTransactionStatement(ast.nodeType))
            ticket.TxnState.MarkStatementExecuted();

        // Retry boundary: two transient errors, two different retry owners.
        //   CADB0503 SchemaCatchingUp — retried HERE, inside ExecuteNonSQLQuery. The fence fires
        //     in TableOpener.Open before any write or schema-pin, so the in-flight transaction is
        //     unmodified and the same tx is safely reused on each attempt.
        //   CADB0502/CADB0504/CADB0505 serialization failures — retried by the HTTP controller for
        //     autocommit statements (via SerializableRetryHelper). The controller replays from a
        //     fresh BeginAsync each time. Explicit multi-statement transactions surface these codes
        //     to the caller, which owns the retry loop.

        switch (ast.nodeType)
        {
            case NodeType.Insert:
                {
                    // A RETURNING list is checked on the AST before the ticket is built, so a refused
                    // list fails before the statement reserves any sequence value.
                    NodeAst? returningList = StatementScope.GetReturningList(ast);
                    if (returningList is not null)
                        WriteReturningPlan.Validate(returningList);

                    // Read, and its subqueries replaced by their values, once: outside the fence-retry loop,
                    // so a retry does not run an inner query again.
                    OnConflictClause? onConflict = await PrepareOnConflictAsync(database, ast, ticket).ConfigureAwait(false);

                    // A subquery in a VALUES row is replaced by its value the same way, before the rows
                    // are shaped: the shaper evaluates each value synchronously and cannot run a query.
                    ast = await RewriteInsertValuesSubqueriesAsync(database, ast, ticket).ConfigureAwait(false);

                    InsertTicket insertTicket = await sqlExecutor.CreateInsertTicket(executor, database, ticket, ast).ConfigureAwait(false);

                    for (int fenceAttempt = 0; ; fenceAttempt++)
                    {
                        // Read before the attempt: a fence error is retried in this transaction only
                        // when the attempt wrote nothing; see RetryFenceInSameTransaction.
                        long writesBefore = ticket.TxnState.WriteCount;

                        try
                        {
                            // Captured before the table is opened; see WriteShapeClock for why the order matters.
                            long writeShapeEpoch = database.WriteShape.Current;

                            TableDescriptor table = await context.TableOpener.Open(database, insertTicket.TableName).ConfigureAwait(false);
                            SelectStatementExecutor.PinForWrite(database, table, ticket.TxnState, writeShapeEpoch);

                            // Bound inside the attempt, against the table this attempt opened, and before
                            // the first write. The buffer is new on each attempt, so a retried attempt
                            // never returns a row from the attempt that failed.
                            WriteReturningPlan? returning = returningList is null
                                ? null
                                : await BindReturningAsync(database, table, ast, returningList, ticket).ConfigureAwait(false);
                            List<QueryResultRow>? insertedRows = returning is not null && !ticket.DiscardReturningRows
                                ? new(insertTicket.Values.Count)
                                : null;

                            if (onConflict is not null)
                            {
                                RowUpserter.UpsertCounts counts = await rowUpserter.Upsert(
                                    database, table, insertTicket, onConflict, ticket.Parameters, insertedRows,
                                    ticket.CancellationToken).ConfigureAwait(false);

                                context.Statistics.TrackInsert(database, table, counts.Inserted, insertTicket.Values);
                                context.Statistics.TrackUpdate(database, table, counts.Updated, updatedValues: null);
                                return await CompleteInsertAsync(database, table, counts.Affected, warning: null, returning, insertedRows).ConfigureAwait(false);
                            }

                            int inserted = await rowInserter.Insert(database, table, insertTicket, insertedRows: insertedRows).ConfigureAwait(false);
                            // Track statistics on the SQL path too, mirroring the ticket-based Insert()
                            // wrapper — otherwise SQL DML never updates row/mutation counts and auto-analyze
                            // never triggers for the common SQL workload.
                            context.Statistics.TrackInsert(database, table, inserted, insertTicket.Values);
                            return await CompleteInsertAsync(database, table, inserted, warning: null, returning, insertedRows).ConfigureAwait(false);
                        }
                        catch (CamusDBException ex) when (FenceAfterWrite(ex, ticket.TxnState, writesBefore))
                        {
                            throw MustRetryAfterWrite(ex);
                        }
                        catch (CamusDBException ex) when (RetryFenceInSameTransaction(ex, fenceAttempt, ticket.TxnState, writesBefore))
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(100 << fenceAttempt)).ConfigureAwait(false);
                        }
                    }
                }

            case NodeType.InsertSelect:
                {
                    NodeAst? returningList = StatementScope.GetReturningList(ast);
                    if (returningList is not null)
                        WriteReturningPlan.Validate(returningList);

                    InsertSelectTicket insertSelectTicket = sqlExecutor.CreateInsertSelectTicket(ticket, ast);
                    context.Validator.Validate(insertSelectTicket);

                    // Outside the fence-retry loop, as in the VALUES arm.
                    OnConflictClause? onConflict = await PrepareOnConflictAsync(database, ast, ticket).ConfigureAwait(false);

                    // One timestamp for the whole source query, kept across fence retries; see
                    // MintStatementTimestamp.
                    HLCTimestamp? statementTimestamp = MintStatementTimestamp(database, ticket.TxnState);

                    for (int fenceAttempt = 0; ; fenceAttempt++)
                    {
                        // Read before the attempt: a fence error is retried in this transaction only
                        // when the attempt wrote nothing; see RetryFenceInSameTransaction.
                        long writesBefore = ticket.TxnState.WriteCount;

                        try
                        {
                            // Captured before the table is opened; see WriteShapeClock for why the order matters.
                            long writeShapeEpoch = database.WriteShape.Current;

                            TableDescriptor table = await context.TableOpener.Open(database, insertSelectTicket.TableName).ConfigureAwait(false);
                            SelectStatementExecutor.PinForWrite(database, table, ticket.TxnState, writeShapeEpoch);

                            // Bound before the source query runs, so a refused list or a missing SELECT
                            // privilege fails before any row is read or written. See the VALUES arm for
                            // why the buffer is per attempt.
                            WriteReturningPlan? returning = returningList is null
                                ? null
                                : await BindReturningAsync(database, table, ast, returningList, ticket).ConfigureAwait(false);
                            List<QueryResultRow>? insertedRows = returning is not null && !ticket.DiscardReturningRows
                                ? new()
                                : null;

                            await using SelectRowSource source = await BuildInsertSelectSourceAsync(
                                database, insertSelectTicket.SourceSelect, ticket, statementTimestamp).ConfigureAwait(false);

                            int insertedFromSelect = await rowInsertSelector
                                .InsertSelect(
                                    rowInserter, context.Statistics, sequenceBinder, database, table,
                                    insertSelectTicket, ticket, source.Columns, source.Cursor, insertedRows,
                                    sourceTimestamp: statementTimestamp,
                                    upsert: onConflict is null ? null : (rowUpserter, onConflict),
                                    sourceTypes: source.StaticTypes,
                                    sourceProjections: source.Projections)
                                .ConfigureAwait(false);

                            string? insertWarning = ctasExecutor.WarnIfTimeTravelCopyReadNothing(
                                source, insertedFromSelect, insertSelectTicket.TableName);

                            return await CompleteInsertAsync(database, table, insertedFromSelect, insertWarning, returning, insertedRows).ConfigureAwait(false);
                        }
                        catch (CamusDBException ex) when (FenceAfterWrite(ex, ticket.TxnState, writesBefore))
                        {
                            throw MustRetryAfterWrite(ex);
                        }
                        catch (CamusDBException ex) when (RetryFenceInSameTransaction(ex, fenceAttempt, ticket.TxnState, writesBefore))
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(100 << fenceAttempt)).ConfigureAwait(false);
                        }
                    }
                }

            case NodeType.Update:
                {
                    // Checked on the AST before any subquery in the statement runs; see the INSERT arm.
                    NodeAst? returningList = StatementScope.GetReturningList(ast);
                    if (returningList is not null)
                        WriteReturningPlan.Validate(returningList);

                    UpdateTicket updateTicket = sqlExecutor.CreateUpdateTicket(ticket, ast);

                    // The subqueries and the locate scan read at one timestamp; see MintStatementTimestamp.
                    HLCTimestamp? statementTimestamp = MintStatementTimestamp(database, ticket.TxnState);
                    using (StatementSnapshotScope.Open(ticket.TxnState, statementTimestamp))
                        updateTicket = await RewriteUpdateSubqueriesAsync(database, updateTicket, ticket, "an UPDATE").ConfigureAwait(false);

                    for (int fenceAttempt = 0; ; fenceAttempt++)
                    {
                        // Read before the attempt: a fence error is retried in this transaction only
                        // when the attempt wrote nothing; see RetryFenceInSameTransaction.
                        long writesBefore = ticket.TxnState.WriteCount;

                        try
                        {
                            // Captured before the table is opened; see WriteShapeClock for why the order matters.
                            long writeShapeEpoch = database.WriteShape.Current;

                            TableDescriptor table = await context.TableOpener.Open(database, updateTicket.TableName).ConfigureAwait(false);
                            SelectStatementExecutor.PinForWrite(database, table, ticket.TxnState, writeShapeEpoch);

                            // Bound inside the attempt and before the first write, as in the INSERT arm.
                            // The collector is new on each attempt, so a retried attempt never returns a
                            // row from the attempt that failed. A count-only request binds the list (same
                            // errors, same privilege check) but collects nothing.
                            WriteReturningPlan? returning = returningList is null
                                ? null
                                : await BindReturningAsync(database, table, ast, returningList, ticket).ConfigureAwait(false);
                            WriteReturningPlan.ReturningRowCollector? collector = returning is not null && !ticket.DiscardReturningRows
                                ? returning.CreateCollector()
                                : null;

                            int updated = await rowUpdater.Update(
                                queryExecutor, database, table, updateTicket, ticket.RetryableAborts, collector,
                                statementTimestamp).ConfigureAwait(false);

                            // A retryable abort recorded instead of thrown: the statement did not
                            // complete, and the transport that owns the sink reports it.
                            if (ticket.RetryableAborts is { HasAbort: true })
                                return new(database, table, 0);

                            context.Statistics.TrackUpdate(database, table, updated, updateTicket.PlainValues);
                            return CompleteChangeResult(database, table, updated, returning, collector);
                        }
                        catch (CamusDBException ex) when (FenceAfterWrite(ex, ticket.TxnState, writesBefore))
                        {
                            throw MustRetryAfterWrite(ex);
                        }
                        catch (CamusDBException ex) when (RetryFenceInSameTransaction(ex, fenceAttempt, ticket.TxnState, writesBefore))
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(100 << fenceAttempt)).ConfigureAwait(false);
                        }
                    }
                }

            case NodeType.Delete:
                {
                    // Checked on the AST before the WHERE subquery runs; see the INSERT arm.
                    NodeAst? returningList = StatementScope.GetReturningList(ast);
                    if (returningList is not null)
                        WriteReturningPlan.Validate(returningList);

                    DeleteTicket deleteTicket = sqlExecutor.CreateDeleteTicket(ticket, ast);

                    // Before the rewrite below, which turns a subquery into a literal.
                    Queries.ConstantDivisionCheck.Check(deleteTicket.Where);

                    // The subqueries and the locate scan read at one timestamp; see MintStatementTimestamp.
                    HLCTimestamp? statementTimestamp = MintStatementTimestamp(database, ticket.TxnState);

                    // Resolve any scalar / IN / NOT IN / EXISTS subquery in the WHERE clause to a
                    // literal before evaluation (DELETE has no per-row EXISTS stage, unlike SELECT, so a
                    // correlated EXISTS is refused). Done once here, outside the fence-retry loop, so
                    // the inner query is not re-executed on each SchemaCatchingUp retry.
                    if (deleteTicket.Where is not null)
                    {
                        NodeAst rewrittenWhere;
                        using (StatementSnapshotScope.Open(ticket.TxnState, statementTimestamp))
                            rewrittenWhere = await subqueryRewriter
                                .RewriteExpressionAsync(database, deleteTicket.Where, ticket, "the WHERE of a DELETE")
                                .ConfigureAwait(false);

                        if (!ReferenceEquals(rewrittenWhere, deleteTicket.Where))
                            deleteTicket = new DeleteTicket(
                                txnState: deleteTicket.TxnState,
                                databaseName: deleteTicket.DatabaseName,
                                tableName: deleteTicket.TableName,
                                where: rewrittenWhere,
                                filters: deleteTicket.Filters,
                                parameters: deleteTicket.Parameters,
                                limit: deleteTicket.Limit,
                                probe: deleteTicket.Probe);
                    }

                    for (int fenceAttempt = 0; ; fenceAttempt++)
                    {
                        // Read before the attempt: a fence error is retried in this transaction only
                        // when the attempt wrote nothing; see RetryFenceInSameTransaction.
                        long writesBefore = ticket.TxnState.WriteCount;

                        try
                        {
                            // Captured before the table is opened; see WriteShapeClock for why the order matters.
                            long writeShapeEpoch = database.WriteShape.Current;

                            TableDescriptor table = await context.TableOpener.Open(database, deleteTicket.TableName).ConfigureAwait(false);
                            SelectStatementExecutor.PinForWrite(database, table, ticket.TxnState, writeShapeEpoch);

                            // Bound and collected per attempt, as in the UPDATE arm. The plan and the
                            // collector go to the deleter as arguments, not on the ticket: the ticket is
                            // rebuilt field by field after the WHERE subquery rewrite above.
                            WriteReturningPlan? returning = returningList is null
                                ? null
                                : await BindReturningAsync(database, table, ast, returningList, ticket).ConfigureAwait(false);
                            WriteReturningPlan.ReturningRowCollector? collector = returning is not null && !ticket.DiscardReturningRows
                                ? returning.CreateCollector()
                                : null;

                            int deleted = await rowDeleter.Delete(
                                queryExecutor, database, table, deleteTicket, returning: collector,
                                statementTimestamp: statementTimestamp).ConfigureAwait(false);
                            context.Statistics.TrackDelete(database, table, deleted);
                            return CompleteChangeResult(database, table, deleted, returning, collector);
                        }
                        catch (CamusDBException ex) when (FenceAfterWrite(ex, ticket.TxnState, writesBefore))
                        {
                            throw MustRetryAfterWrite(ex);
                        }
                        catch (CamusDBException ex) when (RetryFenceInSameTransaction(ex, fenceAttempt, ticket.TxnState, writesBefore))
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(100 << fenceAttempt)).ConfigureAwait(false);
                        }
                    }
                }

            case NodeType.Call:
                {
                    // Runs inside this statement's transaction; the transport commits or rolls it back.
                    int affected = await Extensions.ProcedureRunner.CallAsync(executor, database, ticket, ast).ConfigureAwait(false);
                    return new(database, null!, affected);
                }

            case NodeType.EvictCache:
                {
                    string cacheName = SqlStringLiteral.Decode(ast.yytext ?? string.Empty);
                    // Normalize to lowercase to match the hint grammar's ToLowerInvariant on identifier tokens.
                    database.Cache?.InvalidateCacheName(database.Id, cacheName.ToLowerInvariant());
                    return new(database, null!, 0);
                }

            case NodeType.EvictCacheAll:
                {
                    database.Cache?.InvalidateDatabase(database.Id);
                    return new(database, null!, 0);
                }

            case NodeType.CommentOnTable:
            case NodeType.CommentOnColumn:
            case NodeType.CommentOnIndex:
                {
                    CommentTicket commentTicket = sqlExecutor.CreateCommentTicket(ticket, ast);
                    context.Validator.Validate(commentTicket);
                    await schemaDdl.Comment(database, commentTicket).ConfigureAwait(false);
                    return new(database, null!, 0);
                }

            // A sequence comment lives on the sequence record, not on a table, so it has its own
            // implementation; the DDL entry point runs the same one.
            case NodeType.CommentOnSequence:
                await ddlDispatcher.CommentOnSequenceAsync(database, ast).ConfigureAwait(false);
                return new(database, null!, 0);

            case NodeType.SetTransaction:
            case NodeType.SetTransactionLocking:
            case NodeType.SetTransactionPriority:
                 SetTransactionStatement.Apply(ast, ticket);
                return new(database, null!, 0);

            // Reachable here too: a client routes any non-SELECT statement to whichever endpoint it
            // uses for those, so CTAS must behave the same through either one.
            case NodeType.CreateTableAsSelect:
            case NodeType.CreateTableAsSelectIfNotExists:
                {
                    (bool _, int ctasRows, string? ctasWarning) =
                        await ctasExecutor.ExecuteCreateTableAsSelectAsync(database, ast, ticket).ConfigureAwait(false);

                    TableDescriptor ctasTable = await context.TableOpener.Open(database, ast.leftAst!.yytext!).ConfigureAwait(false);
                    return new ExecuteNonSQLResult(database, ctasTable, ctasRows, ctasWarning);
                }

            // REFRESH is routed here rather than through the DDL path because it is a write: it
            // replaces a relation's contents and reports how many rows it wrote, the same shape a
            // caller already gets from INSERT … SELECT.
            case NodeType.RefreshMaterializedView:
                {
                    string refreshedName = ast.leftAst!.yytext!;
                    string refreshOptions = ast.yytext ?? "";

                    int refreshedRows = await matViewRefresher.RefreshAsync(
                        executor, catalogs, context.Registry, database, refreshedName,
                        concurrently: refreshOptions.StartsWith("concurrently", StringComparison.Ordinal),
                        withNoData: refreshOptions.EndsWith("no data", StringComparison.Ordinal),
                        ticket, context.Logger).ConfigureAwait(false);

                    return new ExecuteNonSQLResult(database, null!, refreshedRows);
                }

            default:
                // Same reason CTAS has an arm above: a client routes every non-SELECT statement to
                // whichever endpoint it uses for those, so schema DDL arrives here as readily as at the
                // DDL entry point. A statement that works through one and answers "unknown statement"
                // through the other is, to that client, indistinguishable from one the server does not
                // support. Both accept it, with one implementation behind them.
                if (StatementScope.IsSchemaDdl(ast.nodeType))
                {
                    ExecuteDDLSQLResult ddlResult = await ddlDispatcher.ExecuteDDLSQL(executor, ticket, activity).ConfigureAwait(false);

                    // No descriptor: DDL returns no relation to the caller, and the row count is
                    // meaningful only for a CREATE that also populated one.
                    return new ExecuteNonSQLResult(database, null!, ddlResult.ModifiedRows);
                }

                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown non-query AST stmt: " + ast.nodeType);
        }
    }

    /// <summary>
    /// Mints the read timestamp of a write statement (<c>UPDATE</c>, <c>DELETE</c>,
    /// <c>INSERT … SELECT</c>), or returns null when the transaction does not take statement snapshots
    /// (<see cref="KvTransaction.TakesStatementSnapshot"/>), in which case the statement reads the latest
    /// committed value as before.
    ///
    /// <para>The statement's read phase reads at this timestamp: the subquery rewrite, then the locate
    /// scan of an <c>UPDATE</c> or <c>DELETE</c>, or the source query of an <c>INSERT … SELECT</c>. So
    /// the statement sees one consistent view, as a SELECT does, and a row that a concurrent transfer
    /// changes is located from one state, never from half of it. Minted once, before the fence-retry
    /// loop, so a retried attempt reads the same view as the subqueries did.</para>
    ///
    /// <para>The write phase keeps the latest read: the lock-then-read and the re-check of each located
    /// row, the uniqueness and the foreign-key checks. Each snapshot scope closes before that phase
    /// begins (<see cref="StatementSnapshotScope"/>). A row that a later commit moved out of the WHERE is
    /// skipped by the re-check; a row that a commit after the timestamp moved into the WHERE is not
    /// located, as if the statement had run at its timestamp.</para>
    /// </summary>
    private static HLCTimestamp? MintStatementTimestamp(DatabaseDescriptor database, KvTransaction tx) =>
        database.Transactions.TryMintStatementTimestamp(tx, out HLCTimestamp timestamp) ? timestamp : null;

    /// <summary>
    /// Binds the source query of an <c>INSERT … SELECT</c> with the statement snapshot open, so a
    /// subquery that the bind runs reads at the same timestamp as the drain
    /// (<see cref="RowInsertSelector.InsertSelect"/>). The snapshot closes when the bind returns.
    /// </summary>
    private async Task<SelectRowSource> BuildInsertSelectSourceAsync(
        DatabaseDescriptor database, NodeAst sourceSelect, ExecuteSQLTicket ticket, HLCTimestamp? statementTimestamp)
    {
        using StatementSnapshotScope _ = StatementSnapshotScope.Open(ticket.TxnState, statementTimestamp);
        return await selectExecutor.BuildSelectSourceAsync(database, sourceSelect, ticket, "INSERT ... SELECT").ConfigureAwait(false);
    }

    /// <summary>
    /// Replaces every subquery of an INSERT's VALUES rows with its value, at one statement timestamp,
    /// and returns the INSERT node with the rewritten rows in the same slot (its other slots, the
    /// ON CONFLICT and RETURNING clauses among them, are carried over). A VALUES row has no outer row,
    /// so every subquery there is uncorrelated by construction; one that names a column anyway fails
    /// inside the inner query. Returns the same node when the rows hold no subquery, which is the
    /// common INSERT, so the statement pays only the scan.
    ///
    /// <para>The row spine (<see cref="NodeType.InsertBatchList"/>, left-deep, one node per row) is
    /// walked with a loop, never by recursion: the parser admits a statement of ten thousand rows
    /// on the promise that every walker on that path is iterative (<see cref="StatementDepthGuard"/>),
    /// and a recursive walk ended the process with a stack overflow. Each row is an expression tree
    /// of bounded depth, so the row itself goes through the ordinary rewrite.</para>
    /// </summary>
    private async Task<NodeAst> RewriteInsertValuesSubqueriesAsync(
        DatabaseDescriptor database, NodeAst ast, ExecuteSQLTicket ticket)
    {
        if (ast.extendedOne is null)
            return ast;

        // The list nodes from the root down; the node below the last one is the first row.
        List<NodeAst> spine = new();
        NodeAst firstRow = ast.extendedOne;
        while (firstRow.nodeType == NodeType.InsertBatchList && firstRow.leftAst is not null)
        {
            spine.Add(firstRow);
            firstRow = firstRow.leftAst;
        }

        Stack<NodeAst> scratch = new();
        bool anySubquery = SubqueryNodeScan.Contains(firstRow, SubqueryNodeKinds.All, scratch);
        for (int i = 0; i < spine.Count && !anySubquery; i++)
            anySubquery = SubqueryNodeScan.Contains(spine[i].rightAst, SubqueryNodeKinds.All, scratch);

        if (!anySubquery)
            return ast;

        HLCTimestamp? statementTimestamp = MintStatementTimestamp(database, ticket.TxnState);
        using StatementSnapshotScope _ = StatementSnapshotScope.Open(ticket.TxnState, statementTimestamp);

        // Rebuilt bottom-up. A list node above an unchanged prefix is reused as it is; from the first
        // changed row upward every node is new, because its left child is.
        NodeAst current = await subqueryRewriter.RewriteExpressionAsync(database, firstRow, ticket, "INSERT VALUES").ConfigureAwait(false);
        bool changed = !ReferenceEquals(current, firstRow);

        for (int i = spine.Count - 1; i >= 0; i--)
        {
            NodeAst listNode = spine[i];
            NodeAst row = listNode.rightAst!;
            NodeAst rewrittenRow = await subqueryRewriter.RewriteExpressionAsync(database, row, ticket, "INSERT VALUES").ConfigureAwait(false);

            if (!changed && ReferenceEquals(rewrittenRow, row))
            {
                current = listNode;
                continue;
            }

            changed = true;
            current = new NodeAst(
                NodeType.InsertBatchList, current, rewrittenRow,
                listNode.extendedOne, listNode.extendedTwo, listNode.extendedThree,
                listNode.extendedFour, listNode.extendedFive, listNode.yytext,
                listNode.extendedSix, listNode.extendedSeven);
        }

        if (!changed)
            return ast;

        return new NodeAst(
            ast.nodeType,
            ast.leftAst,
            ast.rightAst,
            current,
            ast.extendedTwo,
            ast.extendedThree,
            ast.extendedFour,
            ast.extendedFive,
            ast.yytext,
            ast.extendedSix,
            ast.extendedSeven);
    }

    /// <summary>
    /// Pre-materializes uncorrelated scalar / <c>IN</c> / <c>NOT IN</c> / <c>EXISTS</c> subqueries in
    /// an <c>UPDATE</c>'s <c>WHERE</c> clause and each <c>SET</c> value into literals, mirroring the
    /// subquery rewrite that <c>SELECT</c> already performs, so the synchronous evaluator in
    /// <see cref="RowUpdater"/> never sees an unresolved subquery node. A correlated <c>EXISTS</c>
    /// is refused, because an UPDATE has no per-row EXISTS stage (see
    /// <see cref="SubqueryRewriter.RewriteExpressionAsync"/>); <paramref name="statementKind"/>
    /// names the statement in that refusal. Runs once before the fence-retry loop so inner queries
    /// are not re-executed per retry. Rebuilds the ticket only when something actually changed.
    /// </summary>
    internal async Task<UpdateTicket> RewriteUpdateSubqueriesAsync(
        DatabaseDescriptor database, UpdateTicket ticket, ExecuteSQLTicket sqlTicket, string statementKind)
    {
        // Before the rewrite below, which turns a subquery into a literal.
        Queries.ConstantDivisionCheck.Check(ticket.Where);

        if (ticket.ExprValues is not null)
            foreach (NodeAst value in ticket.ExprValues.Values)
                Queries.ConstantDivisionCheck.Check(value);

        bool changed = false;

        NodeAst? newWhere = ticket.Where;
        if (newWhere is not null)
        {
            newWhere = await subqueryRewriter
                .RewriteExpressionAsync(database, newWhere, sqlTicket, $"the WHERE of {statementKind}")
                .ConfigureAwait(false);
            changed |= !ReferenceEquals(newWhere, ticket.Where);
        }

        Dictionary<string, NodeAst>? newExprValues = ticket.ExprValues;
        if (ticket.ExprValues is not null)
        {
            // Materialized only when a SET expression actually rewrote: the common parameterized
            // UPDATE has no subquery anywhere, and eagerly copying the dictionary made every
            // execution pay for the rare case.
            Dictionary<string, NodeAst>? rewritten = null;
            foreach (KeyValuePair<string, NodeAst> entry in ticket.ExprValues)
            {
                NodeAst resolved = await subqueryRewriter
                    .RewriteExpressionAsync(database, entry.Value, sqlTicket, $"a SET value of {statementKind}")
                    .ConfigureAwait(false);

                if (rewritten is not null)
                {
                    rewritten[entry.Key] = resolved;
                    continue;
                }

                if (ReferenceEquals(resolved, entry.Value))
                    continue;

                // First rewritten entry: copy the whole original dictionary and take over. Entries
                // visited before this one were resolved to their original references (or this branch
                // would have run earlier), and entries after it are overwritten as the loop reaches
                // them, so the copy is exact.
                rewritten = new(ticket.ExprValues, ticket.ExprValues.Comparer);
                rewritten[entry.Key] = resolved;
                changed = true;
            }

            if (rewritten is not null)
                newExprValues = rewritten;
        }

        if (!changed)
            return ticket;

        return new UpdateTicket(
            txnState: ticket.TxnState,
            databaseName: ticket.DatabaseName,
            tableName: ticket.TableName,
            plainValues: ticket.PlainValues,
            exprValues: newExprValues,
            where: newWhere,
            filters: ticket.Filters,
            parameters: ticket.Parameters,
            limit: ticket.Limit,
            probe: ticket.Probe);
    }

    /// <summary>Maps a parsed statement's node type to the bounded <c>statement</c> metric family tag.</summary>
    internal static string MapStatementFamily(NodeType nodeType) => nodeType switch
    {
        NodeType.Select or NodeType.SelectForUpdate => ServerDiagnostics.Tags.Statement.Select,
        NodeType.Insert or NodeType.InsertSelect => ServerDiagnostics.Tags.Statement.Insert,
        NodeType.Update => ServerDiagnostics.Tags.Statement.Update,
        NodeType.Delete => ServerDiagnostics.Tags.Statement.Delete,
        _ => ServerDiagnostics.Tags.Statement.Other,
    };}
