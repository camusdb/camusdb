
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
/// <para>Note where the DELETE subquery rewrite sits: <b>outside</b> the fence-retry loop, so a
/// retry does not re-execute the inner query.</para>
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

    internal readonly QueryExecutor queryExecutor;

    internal readonly SubqueryRewriter subqueryRewriter;

    internal readonly MaterializedViewRefresher matViewRefresher;

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
        MaterializedViewRefresher matViewRefresher
    )
    {
        // Guarded because these are captured at construction, not read per call: a collaborator
        // built later in the composing constructor would be captured as null here and only fail
        // much later, deep inside a statement, where the cause is far from the mistake.
        ArgumentNullException.ThrowIfNull(statementAuthorizer);
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
        this.queryExecutor = queryExecutor;
        this.subqueryRewriter = subqueryRewriter;
        this.matViewRefresher = matViewRefresher;
    }

    /// <summary>
    /// Execute a SQL statement that doesn't return rows
    /// </summary>
    /// <param name="ticket"></param>
    /// <returns>The number of inserted/modified/deleted rows</returns>
    internal async Task<ExecuteNonSQLResult> ExecuteNonSQLQuery(CommandExecutor executor, ExecuteSQLTicket ticket)
    {
        context.Validator.Validate(ticket);

        NodeAst ast = SQLParserProcessor.Parse(ticket.Sql, sqlParserCache);

        // Executor stage timing for the write path (parse+plan+stage, exclusive of transport/commit
        // transport). Covers every return path of this method; a no-op when diagnostics are disabled.
        using ServerDiagnostics.ExecuteScope executeScope = ServerDiagnostics.MeasureExecute(
            ServerDiagnostics.Tags.Operation.NonQuery, MapStatementFamily(ast.nodeType));
        using System.Diagnostics.Activity? executeSpan = ServerDiagnostics.StartSpan(ServerDiagnostics.Spans.Execute);
        executeSpan?.SetTag("statement", MapStatementFamily(ast.nodeType));

        statementAuthorizer.SetAuthorizationScope(ticket, ast);
        ticket = SessionScalarFunctions.AttachSessionValues(ticket, ast);
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
                    InsertTicket insertTicket = await sqlExecutor.CreateInsertTicket(executor, database, ticket, ast).ConfigureAwait(false);

                    for (int fenceAttempt = 0; ; fenceAttempt++)
                    {
                        try
                        {
                            TableDescriptor table = await context.TableOpener.Open(database, insertTicket.TableName).ConfigureAwait(false);
                            SelectStatementExecutor.PinSchemaVersion(database, table, ticket.TxnState);
                            int inserted = await rowInserter.Insert(database, table, insertTicket).ConfigureAwait(false);
                            // Track statistics on the SQL path too, mirroring the ticket-based Insert()
                            // wrapper — otherwise SQL DML never updates row/mutation counts and auto-analyze
                            // never triggers for the common SQL workload.
                            context.Statistics.TrackInsert(database, table, inserted, insertTicket.Values);
                            return new(database, table, inserted);
                        }
                        catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.SchemaCatchingUp && fenceAttempt < SelectStatementExecutor.MaxFenceRetries)
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(100 << fenceAttempt)).ConfigureAwait(false);
                        }
                    }
                }

            case NodeType.InsertSelect:
                {
                    InsertSelectTicket insertSelectTicket = sqlExecutor.CreateInsertSelectTicket(ticket, ast);
                    context.Validator.Validate(insertSelectTicket);

                    for (int fenceAttempt = 0; ; fenceAttempt++)
                    {
                        try
                        {
                            TableDescriptor table = await context.TableOpener.Open(database, insertSelectTicket.TableName).ConfigureAwait(false);
                            SelectStatementExecutor.PinSchemaVersion(database, table, ticket.TxnState);

                            await using SelectRowSource source = await selectExecutor.BuildSelectSourceAsync(
                                database, insertSelectTicket.SourceSelect, ticket, "INSERT ... SELECT").ConfigureAwait(false);

                            int insertedFromSelect = await rowInsertSelector
                                .InsertSelect(rowInserter, context.Statistics, database, table, insertSelectTicket, source.Columns, source.Cursor)
                                .ConfigureAwait(false);

                            string? insertWarning = ctasExecutor.WarnIfTimeTravelCopyReadNothing(
                                source, insertedFromSelect, insertSelectTicket.TableName);

                            return new(database, table, insertedFromSelect, insertWarning);
                        }
                        catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.SchemaCatchingUp && fenceAttempt < SelectStatementExecutor.MaxFenceRetries)
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(100 << fenceAttempt)).ConfigureAwait(false);
                        }
                    }
                }

            case NodeType.Update:
                {
                    UpdateTicket updateTicket = sqlExecutor.CreateUpdateTicket(ticket, ast);
                    updateTicket = await RewriteUpdateSubqueriesAsync(database, updateTicket, ticket).ConfigureAwait(false);

                    for (int fenceAttempt = 0; ; fenceAttempt++)
                    {
                        try
                        {
                            TableDescriptor table = await context.TableOpener.Open(database, updateTicket.TableName).ConfigureAwait(false);
                            SelectStatementExecutor.PinSchemaVersion(database, table, ticket.TxnState);
                            int updated = await rowUpdater.Update(queryExecutor, database, table, updateTicket).ConfigureAwait(false);
                            context.Statistics.TrackUpdate(database, table, updated, updateTicket.PlainValues);
                            return new(database, table, updated);
                        }
                        catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.SchemaCatchingUp && fenceAttempt < SelectStatementExecutor.MaxFenceRetries)
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(100 << fenceAttempt)).ConfigureAwait(false);
                        }
                    }
                }

            case NodeType.Delete:
                {
                    DeleteTicket deleteTicket = sqlExecutor.CreateDeleteTicket(ticket, ast);

                    // Resolve any scalar / IN / NOT IN subquery in the WHERE clause to a literal
                    // before evaluation (DELETE has no subquery rewrite of its own, unlike SELECT).
                    // Done once here, outside the fence-retry loop, so the inner query is not
                    // re-executed on each SchemaCatchingUp retry.
                    if (deleteTicket.Where is not null)
                    {
                        NodeAst rewrittenWhere = await subqueryRewriter
                            .RewriteWhereExpressionAsync(database, deleteTicket.Where, ticket)
                            .ConfigureAwait(false);

                        if (!ReferenceEquals(rewrittenWhere, deleteTicket.Where))
                            deleteTicket = new DeleteTicket(
                                txnState: deleteTicket.TxnState,
                                databaseName: deleteTicket.DatabaseName,
                                tableName: deleteTicket.TableName,
                                where: rewrittenWhere,
                                filters: deleteTicket.Filters,
                                parameters: deleteTicket.Parameters,
                                limit: deleteTicket.Limit);
                    }

                    for (int fenceAttempt = 0; ; fenceAttempt++)
                    {
                        try
                        {
                            TableDescriptor table = await context.TableOpener.Open(database, deleteTicket.TableName).ConfigureAwait(false);
                            SelectStatementExecutor.PinSchemaVersion(database, table, ticket.TxnState);
                            int deleted = await rowDeleter.Delete(queryExecutor, database, table, deleteTicket).ConfigureAwait(false);
                            context.Statistics.TrackDelete(database, table, deleted);
                            return new(database, table, deleted);
                        }
                        catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.SchemaCatchingUp && fenceAttempt < SelectStatementExecutor.MaxFenceRetries)
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(100 << fenceAttempt)).ConfigureAwait(false);
                        }
                    }
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
                    ExecuteDDLSQLResult ddlResult = await ddlDispatcher.ExecuteDDLSQL(executor, ticket).ConfigureAwait(false);

                    // No descriptor: DDL returns no relation to the caller, and the row count is
                    // meaningful only for a CREATE that also populated one.
                    return new ExecuteNonSQLResult(database, null!, ddlResult.ModifiedRows);
                }

                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown non-query AST stmt: " + ast.nodeType);
        }
    }

    /// <summary>
    /// Pre-materializes uncorrelated scalar / <c>IN</c> / <c>NOT IN</c> subqueries in an
    /// <c>UPDATE</c>'s <c>WHERE</c> clause and each <c>SET</c> value into literals, mirroring the
    /// subquery rewrite that <c>SELECT</c> already performs, so the synchronous evaluator in
    /// <see cref="RowUpdater"/> never sees an unresolved subquery node. Runs once before the
    /// fence-retry loop so inner queries are not re-executed per retry. Rebuilds the ticket only
    /// when something actually changed; <c>EXISTS</c> is left intact (see
    /// <see cref="SubqueryRewriter.RewriteWhereExpressionAsync"/>).
    /// </summary>
    internal async Task<UpdateTicket> RewriteUpdateSubqueriesAsync(
        DatabaseDescriptor database, UpdateTicket ticket, ExecuteSQLTicket sqlTicket)
    {
        bool changed = false;

        NodeAst? newWhere = ticket.Where;
        if (newWhere is not null)
        {
            newWhere = await subqueryRewriter
                .RewriteWhereExpressionAsync(database, newWhere, sqlTicket)
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
                    .RewriteWhereExpressionAsync(database, entry.Value, sqlTicket)
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
            limit: ticket.Limit);
    }

    /// <summary>Maps a parsed statement's node type to the bounded <c>statement</c> metric family tag.</summary>
    internal static string MapStatementFamily(NodeType nodeType) => nodeType switch
    {
        NodeType.Select => ServerDiagnostics.Tags.Statement.Select,
        NodeType.Insert or NodeType.InsertSelect => ServerDiagnostics.Tags.Statement.Insert,
        NodeType.Update => ServerDiagnostics.Tags.Statement.Update,
        NodeType.Delete => ServerDiagnostics.Tags.Statement.Delete,
        _ => ServerDiagnostics.Tags.Statement.Other,
    };}
