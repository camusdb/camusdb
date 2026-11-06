/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Text.Json;

using Kahuna;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Wasm;

/// <summary>
/// Runs one SQL statement on one node's <see cref="CommandExecutor"/> and writes the outcome as a
/// JSON object. The single-node playground has one of these; the cluster playground has one for
/// each node, so a visitor can send the same statement to a different member.
///
/// <para>Each statement is autocommitted the way the REST transport does it: the database-scoped
/// statements and the server-level queries run with no transaction, row-returning statements in a
/// read-only one, and everything else in a read-write one that is committed, or rolled back on
/// failure. Serializable conflicts are retried by <see cref="SerializableRetryHelper"/>.</para>
///
/// <para>A stopped cluster member gets the same retry. Every transaction starts on the leader of a
/// partition chosen by the hash of a random key, and a statement routes to the last leader its node
/// knows, so a statement can name a stopped member even when the data it needs is healthy. A real
/// server sees that as a retryable gRPC failure. The browser's in-memory transport throws
/// <see cref="KahunaServerException"/> instead, before it delivers anything, and nothing above this
/// class treats that as retryable. Each new attempt starts with a new random key, and a new election
/// can finish between attempts.</para>
/// </summary>
internal sealed class StatementRunner
{
    /// <summary>
    /// Attempts for one autocommit statement. The helper's backoff adds up to about 4.6 s over these
    /// attempts, which is longer than the cluster's 3 s maximum election timeout.
    /// </summary>
    private const int MaxAutocommitAttempts = 16;

    /// <summary>
    /// How long one attempt waits to start its transaction when the statement has a budget. A node
    /// that hosts a coordinator partition with no leader waits for an election that may never finish.
    /// Giving up costs nothing, because the next attempt picks a new partition.
    /// </summary>
    private static readonly TimeSpan BeginBudget = TimeSpan.FromSeconds(2);

    /// <summary>A rollback that cannot finish in this time leaves its session to the reaper.</summary>
    private static readonly TimeSpan RollbackBudget = TimeSpan.FromSeconds(2);

    private const string NodePrefix = "The node ";

    private readonly CommandExecutor executor;

    private readonly CamusDBOptions options;

    /// <summary>
    /// How long one statement may run, or null for no limit. The playground runs one call at a time,
    /// so a statement that waits for a partition with no majority would also block every restart
    /// the visitor needs to end that wait.
    /// </summary>
    private readonly TimeSpan? statementBudget;

    internal StatementRunner(CommandExecutor executor, CamusDBOptions options, TimeSpan? statementBudget = null)
    {
        this.executor = executor;
        this.options = options;
        this.statementBudget = statementBudget;
    }

    /// <summary>
    /// Runs one statement and writes its outcome object. Returns false when the statement failed,
    /// which ends the script that contains it.
    /// </summary>
    public async Task<bool> RunAsync(string sql, string databaseName, Utf8JsonWriter writer)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();

        using CancellationTokenSource? deadline = statementBudget is { } budget ? new(budget) : null;
        CancellationToken cancellationToken = deadline?.Token ?? CancellationToken.None;

        writer.WriteStartObject();
        writer.WriteString("sql", sql);
        writer.WriteString("database", databaseName);

        try
        {
            NodeAst ast = executor.ParseSql(sql);
            NodeType root = ast.nodeType;

            // The AST form, not the root type: an INSERT, UPDATE or DELETE with RETURNING returns rows too.
            if (StatementScope.ReturnsRows(ast))
                await QueryAsync(sql, databaseName, root, writer, cancellationToken).ConfigureAwait(false);
            else
                await NonQueryAsync(sql, databaseName, root, writer, cancellationToken).ConfigureAwait(false);

            writer.WriteNumber("ms", stopwatch.Elapsed.TotalMilliseconds);
            writer.WriteEndObject();
            return true;
        }
        catch (Exception ex)
        {
            // Paths without a retry (server-level queries, database-scoped statements) reach this
            // point with the transport's raw exception.
            Exception reported = ex switch
            {
                KahunaServerException kahuna when IsUnreachableNode(kahuna) => NodeUnreachable(kahuna),
                OperationCanceledException when cancellationToken.IsCancellationRequested => new CamusDBException(
                    CamusDBErrorCodes.TransactionMustRetry,
                    $"The statement did not finish within {statementBudget!.Value.TotalSeconds:0} s. " +
                    "The usual cause is a partition with no leader, because too many of its replicas are stopped."
                ),
                _ => ex,
            };

            writer.WriteBoolean("ok", false);
            writer.WriteString("code", reported is CamusDBException camus ? camus.Code : reported.GetType().Name);
            writer.WriteString("message", reported.Message);
            writer.WriteNumber("ms", stopwatch.Elapsed.TotalMilliseconds);
            writer.WriteEndObject();
            return false;
        }
    }

    private async Task QueryAsync(string sql, string databaseName, NodeType root, Utf8JsonWriter writer, CancellationToken cancellationToken)
    {
        QuerySchemaHolder schema = new();
        List<QueryResultRow> rows = [];

        if (StatementScope.IsServerLevelQuery(root))
        {
            // Reads the registry or this process's own state: no database, no transaction.
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: null!, database: databaseName, sql: sql, parameters: null, cancellationToken: cancellationToken),
                schemaOut: schema).ConfigureAwait(false);

            await foreach (QueryResultRow row in cursor.WithCancellation(cancellationToken).ConfigureAwait(false))
                rows.Add(row);
        }
        else
        {
            await RetryAsync(async ct =>
            {
                rows.Clear();

                DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);
                // A row-returning statement can still have a bounded side effect: SELECT nextval(...)
                // reserves sequence values before evaluating the projection. Use a normal
                // transaction so each reservation gets a unique idempotency key.
                KvTransaction tx = await BeginAttemptAsync(database, ct).ConfigureAwait(false);
                try
                {
                    (DatabaseDescriptor? db, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                        new ExecuteSQLTicket(txnState: tx, database: databaseName, sql: sql, parameters: null, cancellationToken: ct),
                        schemaOut: schema).ConfigureAwait(false);

                    // Buffer every row before the commit, so a retried attempt starts from nothing.
                    await foreach (QueryResultRow row in cursor.WithCancellation(ct).ConfigureAwait(false))
                        rows.Add(row);

                    await CommitOrReleaseAsync(db, database, tx, ct).ConfigureAwait(false);
                }
                catch
                {
                    await RollbackAfterFailureAsync(database, tx).ConfigureAwait(false);
                    throw;
                }
            }, cancellationToken).ConfigureAwait(false);
        }

        IReadOnlyList<DerivedColumnSchema> columns = schema.Schema;

        writer.WriteBoolean("ok", true);
        writer.WriteStartArray("columns");
        foreach (DerivedColumnSchema column in columns)
        {
            writer.WriteStartObject();
            writer.WriteString("name", column.Name);
            writer.WriteString("type", column.Type.ToString());
            writer.WriteEndObject();
        }
        writer.WriteEndArray();

        writer.WriteStartArray("rows");
        RowLayout? boundLayout = null;
        int[]? ordinals = null;
        foreach (QueryResultRow row in rows)
            CompactRowJsonWriter.WriteRow(writer, row, columns, ref boundLayout, ref ordinals);
        writer.WriteEndArray();
    }

    private async Task NonQueryAsync(string sql, string databaseName, NodeType root, Utf8JsonWriter writer, CancellationToken cancellationToken)
    {
        int affected = 0;
        string? warning = null;

        if (StatementScope.IsDatabaseScopedMutation(root))
        {
            // Names its own target and writes the shared registry: no database, no transaction.
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
                new ExecuteSQLTicket(txnState: null!, database: databaseName, sql: sql, parameters: null, cancellationToken: cancellationToken)).ConfigureAwait(false);
            warning = result.Warning;
        }
        else
        {
            await RetryAsync(async ct =>
            {
                DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);
                KvTransaction tx = await BeginAttemptAsync(database, ct).ConfigureAwait(false);
                try
                {
                    ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
                        new ExecuteSQLTicket(txnState: tx, database: databaseName, sql: sql, parameters: null, cancellationToken: ct)).ConfigureAwait(false);

                    await CommitOrReleaseAsync(result.Database, database, tx, ct).ConfigureAwait(false);

                    affected = result.ModifiedRows;
                    warning = result.Warning;
                }
                catch
                {
                    await RollbackAfterFailureAsync(database, tx).ConfigureAwait(false);
                    throw;
                }
            }, cancellationToken).ConfigureAwait(false);
        }

        writer.WriteBoolean("ok", true);
        writer.WriteNumber("affected", affected);
        if (!string.IsNullOrEmpty(warning))
            writer.WriteString("warning", warning);
    }

    /// <summary>
    /// Commits through the descriptor the statement returned. A statement that returns none wrote
    /// nothing through <paramref name="tx"/>, so releasing it is correct — the same rule as the
    /// server's transaction coordinator.
    /// </summary>
    private static async Task CommitOrReleaseAsync(
        DatabaseDescriptor? resultDatabase, DatabaseDescriptor database, KvTransaction tx, CancellationToken ct)
    {
        try
        {
            if (resultDatabase is not null)
                await resultDatabase.Transactions.CommitAsync(tx, ct).ConfigureAwait(false);
            else
                await database.Transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
        }
        catch (KahunaServerException ex) when (IsUnreachableNode(ex))
        {
            throw CommitOutcomeUnknown($"could not reach {UnreachableEndpoint(ex)}, which is stopped or unreachable");
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw CommitOutcomeUnknown("did not finish in time");
        }
    }

    private async Task<KvTransaction> BeginAttemptAsync(DatabaseDescriptor database, CancellationToken ct)
    {
        if (statementBudget is null)
            return await database.Transactions.BeginAsync(cancellationToken: ct).ConfigureAwait(false);

        using CancellationTokenSource attempt = CancellationTokenSource.CreateLinkedTokenSource(ct);
        attempt.CancelAfter(BeginBudget);

        try
        {
            return await database.Transactions.BeginAsync(cancellationToken: attempt.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new CamusDBException(
                CamusDBErrorCodes.TransactionMustRetry,
                "The transaction could not start, because its coordinator partition has no known leader."
            );
        }
    }

    /// <summary>
    /// The failure of a commit that stopped partway. A commit runs several steps, so an earlier step
    /// can apply before a later one fails. This code is not retryable, because a replay could apply
    /// the statement twice.
    /// </summary>
    private static CamusDBException CommitOutcomeUnknown(string reason) =>
        new(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"The commit {reason}. The statement may or may not have been applied: read the data before you run it again."
        );

    /// <summary>
    /// Rolls back after a failed attempt, with its own budget, because the statement's deadline can be
    /// the reason for the failure. If the session's leader is gone, the rollback cannot reach it
    /// either, and the server-side reaper reclaims the session. Its failure must not replace the
    /// statement's own failure, which decides whether the statement is retried.
    /// </summary>
    private static async Task RollbackAfterFailureAsync(DatabaseDescriptor database, KvTransaction tx)
    {
        using CancellationTokenSource budget = new(RollbackBudget);

        try
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx, budget.Token).ConfigureAwait(false);
        }
        catch (KahunaServerException ex) when (IsUnreachableNode(ex))
        {
        }
        catch (OperationCanceledException) when (budget.IsCancellationRequested)
        {
        }
    }

    private Task RetryAsync(Func<CancellationToken, Task> attempt, CancellationToken cancellationToken)
    {
        async Task Guarded(CancellationToken ct)
        {
            try
            {
                await attempt(ct).ConfigureAwait(false);
            }
            catch (KahunaServerException ex) when (IsUnreachableNode(ex))
            {
                throw NodeUnreachable(ex);
            }
        }

        return options.DefaultIsolationLevel == CamusIsolationLevel.Serializable
            ? SerializableRetryHelper.ExecuteAutocommitAsync(Guarded, MaxAutocommitAttempts, cancellationToken)
            : Guarded(cancellationToken);
    }

    /// <summary>
    /// True for the failure of an in-memory forward to a member that is stopped or behind a blocked
    /// link. The transport has no type for this failure, so the message is the only signal.
    /// </summary>
    private static bool IsUnreachableNode(KahunaServerException ex) =>
        ex.ResponseType is null
        && ex.Message.StartsWith(NodePrefix, StringComparison.Ordinal)
        && (ex.Message.EndsWith(" does not exist.", StringComparison.Ordinal) || ex.Message.Contains(" is not reachable from ", StringComparison.Ordinal));

    private static string UnreachableEndpoint(KahunaServerException ex) =>
        ex.Message[NodePrefix.Length..].Split(' ')[0];

    private static CamusDBException NodeUnreachable(KahunaServerException ex) =>
        new(
            CamusDBErrorCodes.TransactionMustRetry,
            $"This statement needs {UnreachableEndpoint(ex)}, which is stopped or unreachable. " +
            "A partition that keeps a majority of its replicas elects a new leader in a few seconds. " +
            "A partition without a majority stays unavailable until enough of its replicas restart."
        );
}
