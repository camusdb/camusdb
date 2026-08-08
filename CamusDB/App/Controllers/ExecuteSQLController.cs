
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.Auth;
using System.Text.Json;
using System.Diagnostics;
using CamusDB.App.Models;
using Microsoft.AspNetCore.Mvc;
using CamusDB.Core.Cache;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Transactions;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.SQLParser;
using CamusDB.App.Services;
using Kahuna.Shared.KeyValue;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class ExecuteSQLController : CommandsController
{
    private readonly PreparedStatementRegistry preparedStatements;

    public ExecuteSQLController(
        CommandExecutor executor,
        HttpTransactionCoordinator transactions,
        PreparedStatementRegistry preparedStatements,
        ILogger<ICamusDB> logger, CamusDBOptions options) : base(executor, transactions, logger, options)
    {
        this.preparedStatements = preparedStatements;
    }

    /// <summary>
    /// What a request executes, after either reading it off the body (inline) or recovering it from
    /// a prepared statement. <see cref="RootType"/> is what makes the indirection worth having: the
    /// handlers below need the statement's root node to route it, and an inline request can only
    /// answer that by parsing the SQL again — uncached — on every single request.
    /// </summary>
    private readonly record struct ResolvedSql(
        string Database,
        string Sql,
        Dictionary<string, ColumnValue>? Parameters,
        NodeType RootType);

    /// <summary>
    /// Resolves one request to the (database, sql, parameters, root node) it executes.
    ///
    /// <para>Call this <b>once</b>, outside any serializable retry body. Re-resolving inside a retry
    /// would let a concurrent close or an idle expiry turn a retryable conflict into an
    /// unknown-statement failure part-way through a replay.</para>
    /// </summary>
    private ResolvedSql Resolve(ExecuteSQLRequest request, Principal? principal)
    {
        if (string.IsNullOrEmpty(request.StatementId))
        {
            if (request.PositionalParameters is { Count: > 0 })
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    "'positionalParameters' requires a 'statementId'; an inline request binds by name");

            string sql = request.Sql ?? "";
            return new ResolvedSql(
                request.DatabaseName ?? "", sql, request.Parameters, SQLParserProcessor.Parse(sql).nodeType);
        }

        PreparedStatementBinder.ValidateNoInlineFields(
            hasSql: !string.IsNullOrEmpty(request.Sql),
            hasDatabase: !string.IsNullOrEmpty(request.DatabaseName),
            hasParameters: request.Parameters is { Count: > 0 });

        PreparedStatement statement = preparedStatements.Resolve(principal, request.StatementId);

        return new ResolvedSql(
            statement.Database,
            statement.Sql,
            PreparedStatementBinder.Bind(statement, request.PositionalParameters, static value => value),
            statement.RootNodeType);
    }

    private static List<ColumnSchemaDto> ToColumnDtos(IReadOnlyList<DerivedColumnSchema> schema)
    {
        List<ColumnSchemaDto> dtos = new(schema.Count);
        foreach (DerivedColumnSchema col in schema)
            dtos.Add(new ColumnSchemaDto { Name = col.Name, Type = col.Type });
        return dtos;
    }

    // The credential redaction runs only when the log level is enabled, so it is not "expensive and
    // unnecessary". CA1873's guard recognition does not extend to the source-generated Log.* form, so
    // it is suppressed here rather than on every call site.
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1873",
        Justification = "Redaction is guarded by IsEnabled; analyzer does not recognize the source-generated Log method.")]
    private void LogExecutingSqlRedacted(string sql)
    {
        if (logger.IsEnabled(LogLevel.Debug))
            Log.LogExecutingSql(logger, SqlCredentialRedactor.Redact(sql));
    }

    /// <summary>
    /// Logs a deserialized request, never the raw body.
    ///
    /// <para>The raw JSON must not be logged even through <see cref="SqlCredentialRedactor"/>: that
    /// helper reads SQL, and a JSON document is not SQL. JSON doubles a backslash, so an
    /// <c>E'a\'tail'</c> password arrives as <c>E'a\\'tail'</c> and the escape rules terminate the
    /// literal one quote early, leaving real password text in the log. Worse, the body also carries
    /// <c>parameters</c>, and <c>IDENTIFIED BY @p</c> is a supported form — so a bound password is
    /// present in the body with no literal for any SQL-aware redactor to find.</para>
    ///
    /// <para>Only the SQL is logged, redacted; parameter values are never logged — a value is only
    /// identifiable as a credential by the statement it binds to, so no value is safe to log here.
    /// Their count is logged instead, which is what diagnostics actually need.</para>
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Performance", "CA1873",
        Justification = "Redaction is guarded by IsEnabled; analyzer does not recognize the source-generated Log method.")]
    private void LogRequestRedacted(ExecuteSQLRequest? request)
    {
        if (!logger.IsEnabled(LogLevel.Information))
            return;

        int parameterCount = (request?.Parameters?.Count ?? 0) + (request?.PositionalParameters?.Count ?? 0);

        Log.LogRequestBody(
            logger,
            $"db={request?.DatabaseName ?? ""} params={parameterCount} sql={SqlCredentialRedactor.Redact(request?.Sql)}");
    }

    [HttpPost]
    [Route("/execute-sql-query")]
    public async Task<JsonResult> ExecuteSQLQuery()
    {
        // Server-side wall clock for the whole handler, reported back as ServerTimeMs so a caller
        // can subtract it from its own observed latency to isolate network/connection overhead.
        Stopwatch stopwatch = Stopwatch.StartNew();

        try
        {
            ExecuteSQLRequest? request = await JsonSerializer.DeserializeAsync<ExecuteSQLRequest>(Request.Body, jsonOptions).ConfigureAwait(false);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ExecuteSQLQuery request is not valid");

            (CamusIsolationLevel? reqLevel, CamusTransactionMode? reqMode, _, TransactionPriority? reqPriority) = ParseRequestLevelMode(request);

            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            // Resolved once, before any retry body: a prepared handle must not be re-looked-up
            // mid-replay, where a concurrent close would turn a conflict into a 404.
            ResolvedSql resolved = Resolve(request, principal);
            string sql = resolved.Sql;

            LogExecutingSqlRedacted(sql);

            // SHOW DATABASES / BRANCHES / ANCESTORS operate on the registry and need no
            // database context or transaction.
            if (resolved.RootType is NodeType.ShowDatabases or NodeType.ShowBranches or NodeType.ShowAncestors
                                   or NodeType.ShowOrphanDatabases or NodeType.ShowEngineStats
                                   or NodeType.ShowVariables)
            {
                QuerySchemaHolder schemaHolder = new();
                ExecuteSQLTicket ticket = new(
                    txnState: null!,
                    database: resolved.Database,
                    sql: sql,
                    parameters: resolved.Parameters,
                        principal: principal
                );
                (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket, schemaOut: schemaHolder).ConfigureAwait(false);
                List<QueryResultRow> rows = [];
                await foreach (QueryResultRow row in cursor)
                    rows.Add(row);
                PositionalRowSet rowSet = new(rows, schemaHolder.Schema);
                return new JsonResult(new ExecuteSQLQueryResponse("ok", rowSet.Count, ToColumnDtos(schemaHolder.Schema), rowSet) { ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds });
            }

            // Explicit (caller-supplied) transaction — client handles retry and lifecycle.
            if (request.TxnIdPT > 0)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(request.TxnIdPT, request.TxnIdCounter);
                    QuerySchemaHolder schemaHolder = new();
                    ExecuteSQLTicket ticket = new(
                        txnState: txnState,
                        database: resolved.Database,
                        sql: sql,
                        parameters: resolved.Parameters,
                        principal: principal
                    );
                    List<QueryResultRow> rows = [];
                    (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket, schemaOut: schemaHolder).ConfigureAwait(false);
                    await foreach (QueryResultRow row in cursor)
                        rows.Add(row);
                    PositionalRowSet rowSet = new(rows, schemaHolder.Schema);
                    return new JsonResult(new ExecuteSQLQueryResponse("ok", rowSet.Count, ToColumnDtos(schemaHolder.Schema), rowSet) { ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds });
                }
                catch (Exception)
                {
                    if (txnState is not null)
                        await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
                    throw;
                }
            }

            // Autocommit: retry transparently on transient serialization failures when the
            // resolved level is Serializable; run once for Read Committed.
            List<QueryResultRow> resultRows = [];
            List<ColumnSchemaDto> resultColumns = [];
            IReadOnlyList<DerivedColumnSchema> resultSchema = [];
            Kommander.Time.HLCTimestamp causalToken = default;
            CacheMetadataHolder cacheMeta = new();

            async Task AutocommitBody(CancellationToken ct)
            {
                KvTransaction tx = await transactions.BeginReadOnlyAsync(
                    resolved.Database, promote: true, request.CausalToken, priority: reqPriority,
                    cancellationToken: ct).ConfigureAwait(false);
                try
                {
                    QuerySchemaHolder schemaHolder = new();
                    ExecuteSQLTicket ticket = new(
                        txnState: tx,
                        database: resolved.Database,
                        sql: sql,
                        parameters: resolved.Parameters,
                        principal: principal
                    );
                    // Fully buffer the decoded, transaction-independent rows, THEN commit — so a
                    // serializable retry can restart cleanly (no bytes are written until the
                    // response is serialized after this handler returns, well after commit).
                    List<QueryResultRow> rows = [];
                    (DatabaseDescriptor db, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket, cacheMeta, schemaHolder).ConfigureAwait(false);
                    await foreach (QueryResultRow row in cursor)
                        rows.Add(row);
                    causalToken = await transactions.CommitAsync(db, tx, ct).ConfigureAwait(false);
                    resultRows = rows;
                    resultColumns = ToColumnDtos(schemaHolder.Schema);
                    resultSchema = schemaHolder.Schema;
                }
                catch
                {
                    await transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                    throw;
                }
            }

            CamusIsolationLevel resolvedLevel = reqLevel ?? options.DefaultIsolationLevel;
            if (resolvedLevel == CamusIsolationLevel.Serializable)
                await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitBody).ConfigureAwait(false);
            else
                await AutocommitBody(CancellationToken.None).ConfigureAwait(false);

            PositionalRowSet resultSet = new(resultRows, resultSchema);
            ExecuteSQLQueryResponse queryResponse = new("ok", resultSet.Count, resultColumns, resultSet)
            {
                CausalToken = causalToken.IsNull() ? null : causalToken
            };

            // Surface cache metadata only when the query went through the cache path.
            if (cacheMeta.CacheName is not null)
            {
                queryResponse.CacheStatus = CacheMetadataHolder.ToStatusString(cacheMeta.Status);
                queryResponse.CacheBypassReason = CacheMetadataHolder.ToBypassReasonString(cacheMeta.BypassReason);
                queryResponse.CachedAtHlc = cacheMeta.CachedAtHlc;
                queryResponse.AgeMs = cacheMeta.AgeMs;
                queryResponse.CacheName = cacheMeta.CacheName;
            }

            queryResponse.ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds;
            return new JsonResult(queryResponse);
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteSQLQueryResponse("failed", e.Code, e.Message) { ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds }) { StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code) };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteSQLQueryResponse("failed", "CA0000", e.Message) { ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds }) { StatusCode = 500 };
        }
    }

    /// <summary>
    /// Streaming counterpart of <see cref="ExecuteSQLQuery"/>: executes a SELECT / SHOW statement and
    /// writes the result set as newline-delimited JSON (<see cref="QueryStreamNdjsonWriter.ContentType"/>)
    /// instead of one buffered <see cref="ExecuteSQLQueryResponse"/>. Rows are pushed to the transport as
    /// they are pulled from the cursor, so a multi-thousand-row / multi-MB result never materializes in
    /// server memory. This is an additive endpoint — <c>/execute-sql-query</c> keeps its exact buffered
    /// shape, so existing clients are unaffected; only clients that opt into this route see the stream.
    ///
    /// <para>Because bytes can reach the wire before the autocommit transaction commits, a serializable
    /// conflict that surfaces after streaming has started cannot be transparently retried — it is
    /// reported through the failure trailer (see <see cref="QueryStreamNdjsonWriter"/>). Errors that
    /// occur before the first line (parse errors, unknown database) still produce a normal JSON error
    /// body with the correct HTTP status, since nothing has been sent yet.</para>
    /// </summary>
    [HttpPost]
    [Route("/execute-sql-query-stream")]
    public async Task ExecuteSQLQueryStream()
    {
        Stopwatch stopwatch = Stopwatch.StartNew();
        CancellationToken ct = HttpContext.RequestAborted;

        ExecuteSQLRequest? request;
        string sql;
        ResolvedSql resolved;
        Principal? principal = null;

        // Setup phase: anything that throws here happens before a single byte is on the wire, so it
        // can still be reported as a normal JSON error response with the correct HTTP status code.
        try
        {
            request = await JsonSerializer.DeserializeAsync<ExecuteSQLRequest>(Request.Body, jsonOptions, ct).ConfigureAwait(false);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ExecuteSQLQuery request is not valid");

            // Resolve the principal in the setup phase so an auth failure is a clean 401 before any
            // stream bytes are written.
            principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            // Resolving a prepared handle belongs in the setup phase: an unknown or expired handle
            // must fail before the 200 header goes out, so it stays a clean JSON 404 instead of an
            // in-band trailer error the client can only discover at the end of the stream.
            resolved = Resolve(request, principal);
            sql = resolved.Sql;
            LogExecutingSqlRedacted(sql);
        }
        catch (Exception e)
        {
            await WriteSetupErrorAsync(e, stopwatch).ConfigureAwait(false);
            return;
        }

        await using Utf8JsonWriter jsonWriter = new(Response.BodyWriter, new JsonWriterOptions { SkipValidation = true });
        QueryStreamNdjsonWriter ndjson = new(jsonWriter, Response.BodyWriter);

        int total = 0;
        Kommander.Time.HLCTimestamp causalToken = default;

        try
        {
            // SHOW DATABASES / BRANCHES / ANCESTORS operate on the registry and need no database
            // context or transaction.
            if (resolved.RootType is NodeType.ShowDatabases or NodeType.ShowBranches or NodeType.ShowAncestors
                                   or NodeType.ShowOrphanDatabases or NodeType.ShowEngineStats
                                   or NodeType.ShowVariables)
            {
                ExecuteSQLTicket ticket = new(
                    txnState: null!,
                    database: resolved.Database,
                    sql: sql,
                    parameters: resolved.Parameters,
                        principal: principal
                );
                (_, total) = await StreamQueryRowsAsync(ticket, ndjson, ct).ConfigureAwait(false);
            }
            // Explicit (caller-supplied) transaction — client owns commit/rollback and retry.
            else if (request.TxnIdPT > 0)
            {
                KvTransaction txnState = transactions.GetState(request.TxnIdPT, request.TxnIdCounter);
                try
                {
                    ExecuteSQLTicket ticket = new(
                        txnState: txnState,
                        database: resolved.Database,
                        sql: sql,
                        parameters: resolved.Parameters,
                        principal: principal
                    );
                    (_, total) = await StreamQueryRowsAsync(ticket, ndjson, ct).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
                    throw;
                }
            }
            // Autocommit: single attempt — streaming forfeits the buffered path's transparent
            // serializable retry, because rows may already be on the wire before commit.
            else
            {
                KvTransaction tx = await transactions.BeginReadOnlyAsync(
                    resolved.Database, promote: true, request.CausalToken, cancellationToken: ct).ConfigureAwait(false);
                try
                {
                    ExecuteSQLTicket ticket = new(
                        txnState: tx,
                        database: resolved.Database,
                        sql: sql,
                        parameters: resolved.Parameters,
                        principal: principal
                    );
                    (DatabaseDescriptor? db, int count) = await StreamQueryRowsAsync(ticket, ndjson, ct).ConfigureAwait(false);
                    total = count;
                    causalToken = await transactions.CommitAsync(db!, tx, ct).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    await transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                    throw;
                }
            }

            ndjson.WriteTrailer(new QueryStreamTrailer
            {
                Status       = "ok",
                Total        = total,
                CausalToken  = causalToken.IsNull() ? null : causalToken,
                ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds,
            });
            await Response.BodyWriter.FlushAsync(ct).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            if (ndjson.HeaderWritten)
            {
                // The 200 header (and possibly rows) is already on the wire — the status can no longer
                // change, so report the failure in-band as the terminal trailer line.
                (string code, string message) = ToErrorCodeMessage(e);
                try
                {
                    ndjson.WriteTrailer(new QueryStreamTrailer
                    {
                        Status       = "failed",
                        Total        = total,
                        Code         = code,
                        Message      = message,
                        ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds,
                    });
                    await Response.BodyWriter.FlushAsync(ct).ConfigureAwait(false);
                }
                catch
                {
                    // Client already gone — nothing more we can do.
                }
            }
            else
            {
                // Failure before the first line (e.g. unknown database/table): nothing is on the wire,
                // so emit a normal JSON error body with the correct HTTP status.
                await WriteSetupErrorAsync(e, stopwatch).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Runs the query ticket, writes the schema header, then streams each row through
    /// <paramref name="ndjson"/>, flushing to the network periodically so memory stays bounded and the
    /// client receives rows incrementally. Returns the resolved <see cref="DatabaseDescriptor"/> (so an
    /// autocommit caller can commit) and the streamed row count. The header is written only after
    /// <see cref="CommandExecutor.ExecuteSQLQuery"/> succeeds, so a setup failure leaves the response
    /// unstarted and reportable as a clean HTTP error.
    /// </summary>
    private async Task<(DatabaseDescriptor? Database, int Count)> StreamQueryRowsAsync(
        ExecuteSQLTicket ticket,
        QueryStreamNdjsonWriter ndjson,
        CancellationToken ct)
    {
        QuerySchemaHolder schemaHolder = new();
        (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(ticket, schemaOut: schemaHolder).ConfigureAwait(false);

        Response.StatusCode  = 200;
        Response.ContentType = QueryStreamNdjsonWriter.ContentType;
        ndjson.WriteHeader(schemaHolder.Schema);

        int count = 0;
        await foreach (QueryResultRow row in cursor.WithCancellation(ct).ConfigureAwait(false))
        {
            ndjson.WriteRow(row, schemaHolder.Schema);
            count++;

            // Push buffered rows to the client every 128 rows: bounds server-held memory and delivers
            // the stream incrementally instead of at completion.
            if ((count & 0x7F) == 0)
                await Response.BodyWriter.FlushAsync(ct).ConfigureAwait(false);
        }

        return (database, count);
    }

    /// <summary>
    /// Writes a normal single-object JSON error response for a streaming request that failed before any
    /// stream bytes were sent, using the same <see cref="ExecuteSQLQueryResponse"/> failure shape and
    /// HTTP status mapping as the buffered endpoint.
    /// </summary>
    private async Task WriteSetupErrorAsync(Exception e, Stopwatch stopwatch)
    {
        logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

        (string code, string message) = ToErrorCodeMessage(e);
        int status = e is CamusDBException cdb ? CamusDBErrorCodes.GetHttpStatus(cdb.Code) : 500;

        Response.StatusCode  = status;
        Response.ContentType = "application/json";

        ExecuteSQLQueryResponse body = new("failed", code, message) { ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds };
        await JsonSerializer.SerializeAsync(Response.Body, body, jsonOptions).ConfigureAwait(false);
    }

    private static (string Code, string Message) ToErrorCodeMessage(Exception e)
        => e is CamusDBException cdb ? (cdb.Code, cdb.Message) : ("CA0000", e.Message);

    [HttpPost]
    [Route("/execute-sql-non-query")]
    public async Task<JsonResult> ExecuteNonSQLQuery()
    {
        // Server-side wall clock for the whole handler (parse + execute + commit), returned as
        // ServerTimeMs so a caller can separate processing time from network round-trip cost.
        Stopwatch stopwatch = Stopwatch.StartNew();

        try
        {
            ExecuteSQLRequest? request = await JsonSerializer.DeserializeAsync<ExecuteSQLRequest>(Request.Body, jsonOptions).ConfigureAwait(false);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ExecuteNonSQLQuery request is not valid");

            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            // Resolved once, before the retry body below — see Resolve.
            ResolvedSql resolved = Resolve(request, principal);

            LogExecutingSqlRedacted(resolved.Sql);

            (CamusIsolationLevel? reqLevel2, CamusTransactionMode? reqMode2, KeyValueTransactionLocking? reqLocking2, TransactionPriority? reqPriority2) = ParseRequestLevelMode(request);

            // Clients route no-rows statements to whichever endpoint they use for non-SELECT SQL,
            // so a database-scoped statement can arrive here as easily as at /execute-sql-ddl. It
            // returns no descriptor, so it must bypass both the transaction and the commit below.
            if (StatementScope.IsDatabaseScopedMutation(resolved.RootType))
            {
                ExecuteSQLTicket dbScopedTicket = new(
                    txnState: null!,
                    database: resolved.Database,
                    sql: resolved.Sql,
                    parameters: resolved.Parameters,
                        principal: principal
                );

                await executor.ExecuteNonSQLQuery(dbScopedTicket).ConfigureAwait(false);
                return new JsonResult(new ExecuteNonSQLQueryResponse("ok", 0) { ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds });
            }

            // Explicit (caller-supplied) transaction — client handles retry and lifecycle.
            if (request.TxnIdPT > 0)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(request.TxnIdPT, request.TxnIdCounter);
                    ExecuteSQLTicket ticket = new(
                        txnState: txnState,
                        database: resolved.Database,
                        sql: resolved.Sql,
                        parameters: resolved.Parameters,
                        principal: principal
                    );
                    ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(ticket).ConfigureAwait(false);
                    return new JsonResult(new ExecuteNonSQLQueryResponse("ok", result.ModifiedRows) { Warning = result.Warning, ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds });
                }
                catch (Exception)
                {
                    if (txnState is not null)
                        await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);
                    throw;
                }
            }

            // Autocommit: retry transparently on transient serialization failures when the
            // resolved level is Serializable; run once for Read Committed.
            int modifiedRows = 0;
            // Reset on every attempt below, not accumulated: a retried autocommit statement reports the
            // warning of the attempt that actually committed, not one left over from an aborted try.
            string? warning = null;
            Kommander.Time.HLCTimestamp causalToken2 = default;

            async Task AutocommitDmlBody(CancellationToken ct)
            {
                KvTransaction tx = await transactions.StartAsync(
                    resolved.Database, reqLevel2, reqMode2, reqLocking2, priority: reqPriority2,
                    cancellationToken: ct).ConfigureAwait(false);
                try
                {
                    ExecuteSQLTicket ticket = new(
                        txnState: tx,
                        database: resolved.Database,
                        sql: resolved.Sql,
                        parameters: resolved.Parameters,
                        principal: principal
                    );
                    ExecuteNonSQLResult r = await executor.ExecuteNonSQLQuery(ticket).ConfigureAwait(false);
                    causalToken2 = await transactions.CommitAsync(r.Database, tx, ct).ConfigureAwait(false);
                    modifiedRows = r.ModifiedRows;
                    warning = r.Warning;
                }
                catch
                {
                    await transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                    throw;
                }
            }

            CamusIsolationLevel resolvedLevel2 = reqLevel2 ?? options.DefaultIsolationLevel;
            if (resolvedLevel2 == CamusIsolationLevel.Serializable)
                await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitDmlBody).ConfigureAwait(false);
            else
                await AutocommitDmlBody(CancellationToken.None).ConfigureAwait(false);

            return new JsonResult(new ExecuteNonSQLQueryResponse("ok", modifiedRows) { Warning = warning, CausalToken = causalToken2.IsNull() ? null : causalToken2, ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds });
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteNonSQLQueryResponse("failed", e.Code, e.Message) { ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds }) { StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code) };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteNonSQLQueryResponse("failed", "CA0000", e.Message) { ServerTimeMs = stopwatch.Elapsed.TotalMilliseconds }) { StatusCode = 500 };
        }
    }

    [HttpPost]
    [Route("/execute-sql-ddl")]
    public async Task<JsonResult> ExecuteSQLDDL()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            ExecuteSQLRequest? request = JsonSerializer.Deserialize<ExecuteSQLRequest>(body, jsonOptions);

            LogRequestRedacted(request);

            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ExecuteSQL-DDL request is not valid");

            // DDL cannot be prepared: schema statements are one-shot, so a handle would buy nothing
            // and would need a lifetime nothing here can give it.
            if (!string.IsNullOrEmpty(request.StatementId) || request.PositionalParameters is { Count: > 0 })
                throw PreparedStatementBinder.NotSupportedHere("/execute-sql-ddl");

            Principal? principal = await ResolveRequestPrincipalAsync().ConfigureAwait(false);

            KvTransaction? txnState = null;
            bool newTransaction = false;

            try
            {
                string sql = request.Sql ?? "";
                NodeAst ast = SQLParserProcessor.Parse(sql);

                // Database-scoped statements need neither a context database nor a transaction:
                // CommandExecutor handles them before databaseOpener.Open and returns no descriptor,
                // so starting a transaction here would hand a null descriptor to the commit below.
                if (!StatementScope.IsDatabaseScopedMutation(ast.nodeType))
                {
                    (CamusIsolationLevel? reqLevel3, CamusTransactionMode? reqMode3, KeyValueTransactionLocking? reqLocking3, TransactionPriority? reqPriority3) = ParseRequestLevelMode(request);
                    (newTransaction, txnState) = await BeginOrResumeAsync(
                        request.DatabaseName,
                        request.TxnIdPT,
                        request.TxnIdCounter,
                        isolationLevel: reqLevel3,
                        transactionMode: reqMode3,
                        locking: reqLocking3,
                        priority: reqPriority3
                    ).ConfigureAwait(false);
                }

                ExecuteSQLTicket ticket = new(
                    txnState: txnState!,
                    database: request.DatabaseName ?? "",
                    sql: sql,
                    parameters: request.Parameters,
                        principal: principal
                );

                ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(ticket).ConfigureAwait(false);

                if (newTransaction)
                    await transactions.CommitAsync(result.Database, txnState!).ConfigureAwait(false);

                return new JsonResult(new ExecuteDDLSQLResponse("ok", result.ModifiedRows, result.Warning));
            }
            catch (Exception)
            {
                if (txnState is not null)
                    await transactions.RollbackIfNotCompletedAsync(txnState).ConfigureAwait(false);

                throw;
            }
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteDDLSQLResponse("failed", e.Code, e.Message))
            {
                StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code)
            };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteDDLSQLResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
