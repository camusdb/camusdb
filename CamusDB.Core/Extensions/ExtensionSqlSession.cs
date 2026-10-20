/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Extensions;

namespace CamusDB.Core.Extensions;

/// <summary>
/// The SQL session a trigger or a procedure receives: it runs SELECT, INSERT, UPDATE and DELETE inside
/// the transaction of the statement that called the extension, as the user who ran that statement.
///
/// <para><b>Same entry points as a client.</b> A nested statement goes through
/// <see cref="CommandExecutor.ExecuteNonSQLQuery"/> or <see cref="CommandExecutor.ExecuteSQLQuery"/>
/// with a ticket that carries the caller's <see cref="KvTransaction"/> and principal. It therefore
/// meets the same validation, privilege checks (invoker rights), write pins, constraint checks and
/// triggers as any statement, and its writes are tracked mutations of the caller's transaction:
/// they commit or roll back with it and count against its mutation limit. The authorization scope a
/// nested statement sets is undone when it returns, because it is set inside an awaited method.</para>
///
/// <para><b>Refused statements.</b> DDL and transaction control are refused before parsing goes any
/// further (<see cref="CamusDBErrorCodes.ExtensionStatementRefused"/>): DDL inside a write has no
/// schema-fence design, and a nested COMMIT would end the caller's transaction under it.</para>
///
/// <para><b>A failure ends the calling statement.</b> There are no savepoints, and a failed statement
/// can leave part of its writes staged (a foreign-key check runs after the row writes). So when SQL run
/// through this session fails, the session marks the transaction rollback-only and records the failure.
/// The caller of the extension (<see cref="StatementTriggers"/>, <see cref="ProcedureRunner"/>) checks
/// <see cref="Failure"/> after the extension returns: an extension that caught the error and returned
/// normally still fails its statement with <see cref="CamusDBErrorCodes.ExtensionCaughtSqlError"/>,
/// and a commit of the marked transaction is refused. The session's own refusals (an empty or
/// refused statement) are raised before anything runs and do not mark the transaction.</para>
///
/// <para><b>Schema fence.</b> The outer statement retries <see cref="CamusDBErrorCodes.SchemaCatchingUp"/>
/// in the same transaction, which is safe only because the fence fires before the outer statement's
/// first write. A nested statement opens its tables after the outer one wrote, so its fence error is
/// turned into <see cref="CamusDBErrorCodes.TransactionMustRetry"/>: the caller retries from a new
/// transaction instead of repeating writes in this one.</para>
/// </summary>
internal sealed class ExtensionSqlSession : ISqlSession
{
    private readonly CommandExecutor executor;

    private readonly string databaseName;

    private readonly KvTransaction transaction;

    private readonly Principal? principal;

    private long affectedRows;

    private string? failure;

    /// <summary>The first error a statement of this session raised, or null. See the class summary.</summary>
    public string? Failure => Volatile.Read(ref failure);

    /// <summary>The rows the session's INSERT, UPDATE and DELETE statements changed, in total: what
    /// <c>CALL</c> reports as its own affected-row count.</summary>
    public long AffectedRows => Interlocked.Read(ref affectedRows);

    public ExtensionSqlSession(CommandExecutor executor, string databaseName, KvTransaction transaction, Principal? principal)
    {
        this.executor = executor;
        this.databaseName = databaseName;
        this.transaction = transaction;
        this.principal = principal;
    }

    public async Task<long> ExecuteAsync(string sql, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        NodeType kind = Classify(sql);

        if (kind is not (NodeType.Insert or NodeType.InsertSelect or NodeType.Update or NodeType.Delete))
            throw Refused(sql, "ExecuteAsync runs INSERT, UPDATE or DELETE");

        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
                new ExecuteSQLTicket(transaction, databaseName, sql, ToParameters(parameters), principal)).ConfigureAwait(false);

            Interlocked.Add(ref affectedRows, result.ModifiedRows);
            return result.ModifiedRows;
        }
        catch (Exception ex)
        {
            throw Fail(sql, ex);
        }
    }

    public async Task<IReadOnlyList<IRow>> QueryAsync(string sql, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        if (Classify(sql) != NodeType.Select)
            throw Refused(sql, "QueryAsync runs SELECT");

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(transaction, databaseName, sql, ToParameters(parameters), principal)).ConfigureAwait(false);

            List<IRow> rows = [];

            // Copied: a cursor row may be backed by a buffer the next row reuses, and the contract
            // promises rows that outlive the call.
            await foreach (QueryResultRow row in cursor.ConfigureAwait(false))
                rows.Add(new TriggerRow(new Dictionary<string, ColumnValue>(row.Row, StringComparer.OrdinalIgnoreCase)));

            return rows;
        }
        catch (Exception ex)
        {
            throw Fail(sql, ex);
        }
    }

    private NodeType Classify(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw Refused(sql, "the statement is empty");

        return executor.ParseSql(sql).nodeType;
    }

    /// <summary>Converts the contract's parameter dictionary: <c>name</c> or <c>@name</c> becomes the
    /// <c>@name</c> key the evaluator looks up.</summary>
    private static Dictionary<string, ColumnValue>? ToParameters(IReadOnlyDictionary<string, object?>? parameters)
    {
        if (parameters is null || parameters.Count == 0)
            return null;

        Dictionary<string, ColumnValue> converted = new(parameters.Count);

        foreach (KeyValuePair<string, object?> entry in parameters)
        {
            string key = entry.Key.StartsWith('@') ? entry.Key : "@" + entry.Key;
            converted[key] = ExtensionValueMapping.FromObject(entry.Value);
        }

        return converted;
    }

    /// <summary>
    /// Records a failure of a statement this session ran and marks the transaction rollback-only, then
    /// returns the exception to throw: the original one, except a schema fence error, which becomes
    /// <see cref="CamusDBErrorCodes.TransactionMustRetry"/> (see the class summary).
    /// </summary>
    private Exception Fail(string sql, Exception ex)
    {
        string reason = $"a statement run by a C# extension failed ({ex.Message}): {sql}";
        Interlocked.CompareExchange(ref failure, reason, null);
        transaction.MarkRollbackOnly(reason);

        return ex is CamusDBException { Code: CamusDBErrorCodes.SchemaCatchingUp } fence ? MustRetry(fence) : ex;
    }

    private static CamusDBException Refused(string sql, string rule) =>
        new(CamusDBErrorCodes.ExtensionStatementRefused, $"An extension's SQL session refused the statement ({rule}): {sql}");

    private static CamusDBException MustRetry(CamusDBException fence) =>
        new(CamusDBErrorCodes.TransactionMustRetry,
            "A statement run by an extension met a schema change in progress after the outer statement wrote; retry the transaction",
            fence);
}
