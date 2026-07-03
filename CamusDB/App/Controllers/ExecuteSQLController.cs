
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using System.Text.Json;
using System.Diagnostics;
using CamusDB.App.Models;
using Microsoft.AspNetCore.Mvc;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.SQLParser;
using CamusDB.App.Services;

namespace CamusDB.App.Controllers;

[ApiController]
public sealed class ExecuteSQLController : CommandsController
{
    public ExecuteSQLController(CommandExecutor executor, HttpTransactionCoordinator transactions, ILogger<ICamusDB> logger) : base(executor, transactions, logger)
    {
        
    }

    [HttpPost]
    [Route("/execute-sql-query")]
    public async Task<JsonResult> ExecuteSQLQuery()
    {
        try
        {
            //Stopwatch stopwatch = Stopwatch.StartNew();

            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            Log.LogRequestBody(logger, body);

            ExecuteSQLRequest? request = JsonSerializer.Deserialize<ExecuteSQLRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ExecuteSQLQuery request is not valid");

            (CamusIsolationLevel? reqLevel, CamusTransactionMode? reqMode) = ParseRequestLevelMode(request);

            string sql = request.Sql ?? "";
            NodeAst ast = SQLParserProcessor.Parse(sql);

            // SHOW DATABASES / BRANCHES / ANCESTORS operate on the registry and need no
            // database context or transaction.
            if (ast.nodeType is NodeType.ShowDatabases or NodeType.ShowBranches or NodeType.ShowAncestors)
            {
                ExecuteSQLTicket ticket = new(
                    txnState: null!,
                    database: request.DatabaseName ?? "",
                    sql: sql,
                    parameters: request.Parameters
                );
                (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket).ConfigureAwait(false);
                List<Dictionary<string, ColumnValue>> rows = [];
                await foreach (QueryResultRow row in cursor)
                    rows.Add(row.Row);
                return new JsonResult(new ExecuteSQLQueryResponse("ok", rows.Count, rows));
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
                        database: request.DatabaseName ?? "",
                        sql: sql,
                        parameters: request.Parameters
                    );
                    List<Dictionary<string, ColumnValue>> rows = [];
                    (DatabaseDescriptor database, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket).ConfigureAwait(false);
                    await foreach (QueryResultRow row in cursor)
                        rows.Add(row.Row);
                    return new JsonResult(new ExecuteSQLQueryResponse("ok", rows.Count, rows));
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
            List<Dictionary<string, ColumnValue>> resultRows = [];
            Kommander.Time.HLCTimestamp causalToken = default;

            async Task AutocommitBody(CancellationToken ct)
            {
                KvTransaction tx = await transactions.BeginReadOnlyAsync(
                    request.DatabaseName ?? "", promote: true, request.CausalToken, ct).ConfigureAwait(false);
                try
                {
                    ExecuteSQLTicket ticket = new(
                        txnState: tx,
                        database: request.DatabaseName ?? "",
                        sql: sql,
                        parameters: request.Parameters
                    );
                    List<Dictionary<string, ColumnValue>> rows = [];
                    (DatabaseDescriptor db, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket).ConfigureAwait(false);
                    await foreach (QueryResultRow row in cursor)
                        rows.Add(row.Row);
                    causalToken = await transactions.CommitAsync(db, tx, ct).ConfigureAwait(false);
                    resultRows = rows;
                }
                catch
                {
                    await transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                    throw;
                }
            }

            CamusIsolationLevel resolvedLevel = reqLevel ?? CamusDBConfig.DefaultIsolationLevel;
            if (resolvedLevel == CamusIsolationLevel.Serializable)
                await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitBody).ConfigureAwait(false);
            else
                await AutocommitBody(CancellationToken.None).ConfigureAwait(false);

            return new JsonResult(new ExecuteSQLQueryResponse("ok", resultRows.Count, resultRows) { CausalToken = causalToken.IsNull() ? null : causalToken });
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteSQLQueryResponse("failed", e.Code, e.Message)) { StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code) };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteSQLQueryResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }

    [HttpPost]
    [Route("/execute-sql-non-query")]
    public async Task<JsonResult> ExecuteNonSQLQuery()
    {
        try
        {
            using StreamReader reader = new(Request.Body);
            string body = await reader.ReadToEndAsync().ConfigureAwait(false);

            Log.LogRequestBody(logger, body);

            ExecuteSQLRequest? request = JsonSerializer.Deserialize<ExecuteSQLRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ExecuteNonSQLQuery request is not valid");

            (CamusIsolationLevel? reqLevel2, CamusTransactionMode? reqMode2) = ParseRequestLevelMode(request);

            // Explicit (caller-supplied) transaction — client handles retry and lifecycle.
            if (request.TxnIdPT > 0)
            {
                KvTransaction? txnState = null;
                try
                {
                    txnState = transactions.GetState(request.TxnIdPT, request.TxnIdCounter);
                    ExecuteSQLTicket ticket = new(
                        txnState: txnState,
                        database: request.DatabaseName ?? "",
                        sql: request.Sql ?? "",
                        parameters: request.Parameters
                    );
                    ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(ticket).ConfigureAwait(false);
                    return new JsonResult(new ExecuteNonSQLQueryResponse("ok", result.ModifiedRows));
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
            Kommander.Time.HLCTimestamp causalToken2 = default;

            async Task AutocommitDmlBody(CancellationToken ct)
            {
                KvTransaction tx = await transactions.StartAsync(request.DatabaseName ?? "", reqLevel2, reqMode2, ct).ConfigureAwait(false);
                try
                {
                    ExecuteSQLTicket ticket = new(
                        txnState: tx,
                        database: request.DatabaseName ?? "",
                        sql: request.Sql ?? "",
                        parameters: request.Parameters
                    );
                    ExecuteNonSQLResult r = await executor.ExecuteNonSQLQuery(ticket).ConfigureAwait(false);
                    causalToken2 = await transactions.CommitAsync(r.Database, tx, ct).ConfigureAwait(false);
                    modifiedRows = r.ModifiedRows;
                }
                catch
                {
                    await transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                    throw;
                }
            }

            CamusIsolationLevel resolvedLevel2 = reqLevel2 ?? CamusDBConfig.DefaultIsolationLevel;
            if (resolvedLevel2 == CamusIsolationLevel.Serializable)
                await SerializableRetryHelper.ExecuteAutocommitAsync(AutocommitDmlBody).ConfigureAwait(false);
            else
                await AutocommitDmlBody(CancellationToken.None).ConfigureAwait(false);

            return new JsonResult(new ExecuteNonSQLQueryResponse("ok", modifiedRows) { CausalToken = causalToken2.IsNull() ? null : causalToken2 });
        }
        catch (CamusDBException e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteNonSQLQueryResponse("failed", e.Code, e.Message)) { StatusCode = CamusDBErrorCodes.GetHttpStatus(e.Code) };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteNonSQLQueryResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
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

            Log.LogRequestBody(logger, body);

            ExecuteSQLRequest? request = JsonSerializer.Deserialize<ExecuteSQLRequest>(body, jsonOptions);
            if (request == null)
                throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "ExecuteSQL-DDL request is not valid");

            KvTransaction? txnState = null;
            bool newTransaction = false;

            try
            {
                string sql = request.Sql ?? "";
                NodeAst ast = SQLParserProcessor.Parse(sql);

                // CREATE/DROP/RENAME DATABASE do not require a database context or a transaction —
                // they are handled in CommandExecutor before databaseOpener.Open is called.
                bool isDbManagement = ast.nodeType is
                    NodeType.CreateDatabase or NodeType.CreateDatabaseIfNotExists or
                    NodeType.CreateDatabaseBranch or NodeType.CreateDatabaseBranchIfNotExists or
                    NodeType.DropDatabase or NodeType.DropDatabaseIfExists or
                    NodeType.RenameDatabase;

                if (!isDbManagement)
                {
                    (CamusIsolationLevel? reqLevel3, CamusTransactionMode? reqMode3) = ParseRequestLevelMode(request);
                    (newTransaction, txnState) = await BeginOrResumeAsync(
                        request.DatabaseName,
                        request.TxnIdPT,
                        request.TxnIdCounter,
                        isolationLevel: reqLevel3,
                        transactionMode: reqMode3
                    ).ConfigureAwait(false);
                }

                ExecuteSQLTicket ticket = new(
                    txnState: txnState!,
                    database: request.DatabaseName ?? "",
                    sql: sql,
                    parameters: request.Parameters
                );

                ExecuteDDLSQLResult result = await executor.ExecuteDDLSQL(ticket).ConfigureAwait(false);

                if (newTransaction)
                    await transactions.CommitAsync(result.Database, txnState!).ConfigureAwait(false);

                return new JsonResult(new ExecuteDDLSQLResponse("ok"));
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

            return new JsonResult(new ExecuteDDLSQLResponse("failed", e.Code, e.Message)) { StatusCode = 500 };
        }
        catch (Exception e)
        {
            logger.LogError("{Name}: {Message}\n{StackTrace}", e.GetType().Name, e.Message, e.StackTrace);

            return new JsonResult(new ExecuteDDLSQLResponse("failed", "CA0000", e.Message)) { StatusCode = 500 };
        }
    }
}
