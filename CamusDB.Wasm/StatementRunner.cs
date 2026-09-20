/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Text.Json;

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
/// </summary>
internal sealed class StatementRunner
{
    private readonly CommandExecutor executor;

    private readonly CamusDBOptions options;

    internal StatementRunner(CommandExecutor executor, CamusDBOptions options)
    {
        this.executor = executor;
        this.options = options;
    }

    /// <summary>
    /// Runs one statement and writes its outcome object. Returns false when the statement failed,
    /// which ends the script that contains it.
    /// </summary>
    public async Task<bool> RunAsync(string sql, string databaseName, Utf8JsonWriter writer)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();

        writer.WriteStartObject();
        writer.WriteString("sql", sql);
        writer.WriteString("database", databaseName);

        try
        {
            NodeType root = executor.ParseSql(sql).nodeType;

            if (StatementScope.ReturnsRows(root))
                await QueryAsync(sql, databaseName, root, writer).ConfigureAwait(false);
            else
                await NonQueryAsync(sql, databaseName, root, writer).ConfigureAwait(false);

            writer.WriteNumber("ms", stopwatch.Elapsed.TotalMilliseconds);
            writer.WriteEndObject();
            return true;
        }
        catch (Exception ex)
        {
            writer.WriteBoolean("ok", false);
            writer.WriteString("code", ex is CamusDBException camus ? camus.Code : ex.GetType().Name);
            writer.WriteString("message", ex.Message);
            writer.WriteNumber("ms", stopwatch.Elapsed.TotalMilliseconds);
            writer.WriteEndObject();
            return false;
        }
    }

    private async Task QueryAsync(string sql, string databaseName, NodeType root, Utf8JsonWriter writer)
    {
        QuerySchemaHolder schema = new();
        List<QueryResultRow> rows = [];

        if (StatementScope.IsServerLevelQuery(root))
        {
            // Reads the registry or this process's own state: no database, no transaction.
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(txnState: null!, database: databaseName, sql: sql, parameters: null),
                schemaOut: schema).ConfigureAwait(false);

            await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
                rows.Add(row);
        }
        else
        {
            await RetryAsync(async ct =>
            {
                rows.Clear();

                DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);
                KvTransaction tx = await database.Transactions.BeginReadOnlyAsync(promote: true, cancellationToken: ct).ConfigureAwait(false);
                try
                {
                    (DatabaseDescriptor? db, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                        new ExecuteSQLTicket(txnState: tx, database: databaseName, sql: sql, parameters: null),
                        schemaOut: schema).ConfigureAwait(false);

                    // Buffer every row before the commit, so a retried attempt starts from nothing.
                    await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
                        rows.Add(row);

                    await CommitOrReleaseAsync(db, database, tx, ct).ConfigureAwait(false);
                }
                catch
                {
                    await database.Transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                    throw;
                }
            }).ConfigureAwait(false);
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

    private async Task NonQueryAsync(string sql, string databaseName, NodeType root, Utf8JsonWriter writer)
    {
        int affected = 0;
        string? warning = null;

        if (StatementScope.IsDatabaseScopedMutation(root))
        {
            // Names its own target and writes the shared registry: no database, no transaction.
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
                new ExecuteSQLTicket(txnState: null!, database: databaseName, sql: sql, parameters: null)).ConfigureAwait(false);
            warning = result.Warning;
        }
        else
        {
            await RetryAsync(async ct =>
            {
                DatabaseDescriptor database = await executor.OpenDatabase(databaseName).ConfigureAwait(false);
                KvTransaction tx = await database.Transactions.BeginAsync(cancellationToken: ct).ConfigureAwait(false);
                try
                {
                    ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
                        new ExecuteSQLTicket(txnState: tx, database: databaseName, sql: sql, parameters: null)).ConfigureAwait(false);

                    await CommitOrReleaseAsync(result.Database, database, tx, ct).ConfigureAwait(false);

                    affected = result.ModifiedRows;
                    warning = result.Warning;
                }
                catch
                {
                    await database.Transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
                    throw;
                }
            }).ConfigureAwait(false);
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
        if (resultDatabase is not null)
            await resultDatabase.Transactions.CommitAsync(tx, ct).ConfigureAwait(false);
        else
            await database.Transactions.RollbackIfNotCompletedAsync(tx, ct).ConfigureAwait(false);
    }

    private Task RetryAsync(Func<CancellationToken, Task> attempt) =>
        options.DefaultIsolationLevel == CamusIsolationLevel.Serializable
            ? SerializableRetryHelper.ExecuteAutocommitAsync(attempt)
            : attempt(CancellationToken.None);
}
