/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;

using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.SqlFuzz.Tests.Engine;

namespace CamusDB.SqlFuzz.Tests.Runner;

/// <summary>Which <see cref="CamusDB.Core.CommandsExecutor.CommandExecutor"/> entry point a statement goes through.</summary>
internal enum StatementRoute
{
    /// <summary><c>ExecuteSQLQuery</c>: SELECT, EXPLAIN and SHOW. Without a transaction it runs in a read-only snapshot.</summary>
    Query,

    /// <summary><c>ExecuteSQLQuery</c> in a read-write transaction: ANALYZE writes statistics.</summary>
    Analyze,

    /// <summary><c>ExecuteNonSQLQuery</c>: INSERT, UPDATE, DELETE and UPSERT.</summary>
    NonQuery,

    /// <summary><c>ExecuteDDLSQL</c>: CREATE TABLE and CREATE INDEX.</summary>
    Ddl,
}

/// <summary>
/// The seam between the oracles and the engine. An oracle sends SQL text and reads a
/// <see cref="StatementOutcome"/>; it never sees the engine itself. A unit test puts a fake between
/// the two that changes one answer, and so proves that the oracle reports the disagreement.
/// </summary>
internal interface IStatementExecutor
{
    /// <summary>The name of the engine configuration, for a finding.</summary>
    string ArmName { get; }

    /// <summary>
    /// Runs one statement. With <paramref name="transaction"/> null, the statement runs on its own:
    /// a read in a read-only snapshot, a write or DDL in a transaction that commits at once. With a
    /// transaction, the statement joins it, and the caller commits or rolls back.
    /// </summary>
    Task<StatementOutcome> ExecuteAsync(StatementRoute route, string sql, KvTransaction? transaction = null);

    /// <summary>Begins an explicit transaction on the database of this engine.</summary>
    Task<KvTransaction> BeginAsync();

    /// <summary>Commits <paramref name="transaction"/>. A refused commit is an error outcome, not an exception.</summary>
    Task<StatementOutcome> CommitAsync(KvTransaction transaction);

    /// <summary>Rolls back <paramref name="transaction"/> when it is still open.</summary>
    Task RollbackAsync(KvTransaction transaction);

    /// <summary>The integrity problems of one table (<see cref="CamusDB.Tests.SqlFuzz.TableIntegrityAudit"/>); empty when the table is consistent.</summary>
    Task<IReadOnlyList<string>> CheckIntegrityAsync(string table);

    /// <summary>True after a hang: an operation of this engine still runs in the background, so the engine must not be used again, not even to drop its database.</summary>
    bool Poisoned { get; }
}

/// <summary>
/// Thrown by <see cref="SqlRunner"/> when an operation that has no outcome of its own (BEGIN, ROLLBACK,
/// an integrity scan) does not finish within the time limit. <see cref="Oracles.RoundContext.RunGuardedAsync"/>
/// turns it into a hang finding.
/// </summary>
internal sealed class StatementHangException(string operation, TimeSpan limit)
    : Exception($"{operation} did not finish within {limit.TotalSeconds:0.#} s")
{
    public string Operation { get; } = operation;
}

/// <summary>
/// Sends statements through <see cref="CamusDB.Core.CommandsExecutor.CommandExecutor"/> with
/// <see cref="ExecuteSQLTicket"/>, on one <see cref="FuzzArm"/>, and turns each result or exception
/// into a <see cref="StatementOutcome"/>.
///
/// <para><b>The time limit.</b> Every operation on the engine has it: a statement, BEGIN, COMMIT,
/// ROLLBACK and an integrity scan. An operation that is not done after <see cref="StatementTimeout"/> is
/// a hang. Its token is then cancelled and it gets <see cref="CancelGrace"/> more to stop. An operation
/// that is still running after that is left behind, because nothing else can stop it, so the campaign
/// stops to use this engine (<see cref="Poisoned"/>). After that, every operation fails at once instead
/// of waiting on the stuck one.</para>
///
/// <para><b>Rows are read in full</b> before the outcome returns. A failure while the cursor streams
/// is an outcome of the statement, the same as a failure at plan time.</para>
/// </summary>
internal sealed class SqlRunner : IStatementExecutor
{
    private readonly FuzzArm arm;

    public SqlRunner(FuzzArm arm, TimeSpan? statementTimeout = null)
    {
        this.arm = arm;
        StatementTimeout = statementTimeout ?? TimeSpan.FromSeconds(10);
    }

    public string ArmName => arm.Name;

    public TimeSpan StatementTimeout { get; }

    public TimeSpan CancelGrace { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// When true, every row of a successful read is also written in the HTTP JSON row format, and a
    /// failure there is recorded on the outcome (<see cref="StatementOutcome.WireError"/>).
    /// </summary>
    public bool WireCheck { get; init; } = true;

    /// <summary>True after a hang: a statement of this engine still runs in the background.</summary>
    public bool Poisoned { get; private set; }

    /// <summary>The count of statements this runner sent, for the per-oracle execution counts.</summary>
    public long StatementsExecuted { get; private set; }

    public async Task<KvTransaction> BeginAsync()
    {
        (bool done, KvTransaction? tx) = await BoundedAsync(_ => arm.Database.Transactions.BeginAsync()).ConfigureAwait(false);
        return done ? tx! : throw new StatementHangException("BEGIN", StatementTimeout);
    }

    public async Task<StatementOutcome> CommitAsync(KvTransaction transaction)
    {
        StatementsExecuted++;
        (bool done, StatementOutcome? outcome) = await BoundedAsync(async _ =>
        {
            try
            {
                await arm.Database.Transactions.CommitAsync(transaction).ConfigureAwait(false);
                return StatementOutcome.FromAffected("COMMIT", 0);
            }
            catch (Exception ex)
            {
                return StatementOutcome.FromException("COMMIT", ex);
            }
        }).ConfigureAwait(false);
        if (!done)
            return StatementOutcome.FromHang("COMMIT", StatementTimeout);
        if (!outcome!.IsSuccess)
            await RollbackAsync(transaction).ConfigureAwait(false);
        return outcome;
    }

    /// <summary>Rolls back when the transaction is still open. A hang here poisons the engine but is not thrown: a rollback is cleanup, and the operation that needed it already has its outcome.</summary>
    public async Task RollbackAsync(KvTransaction transaction)
    {
        if (Poisoned)
            return;
        await BoundedAsync<bool>(async _ =>
        {
            try { await arm.Database.Transactions.RollbackIfNotCompletedAsync(transaction).ConfigureAwait(false); }
            catch { }
            return true;
        }).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<string>> CheckIntegrityAsync(string table)
    {
        (bool done, IReadOnlyList<string>? problems) = await BoundedAsync(_ =>
            CamusDB.Tests.SqlFuzz.TableIntegrityAudit.CheckAsync(arm.Executor, arm.Database, arm.DatabaseName, table)).ConfigureAwait(false);
        return done ? problems! : throw new StatementHangException("the integrity scan of " + table, StatementTimeout);
    }

    /// <summary>
    /// Runs <paramref name="operation"/> under the time limit (see the class summary). Returns false when
    /// it did not finish; an exception of the operation propagates. A poisoned engine runs nothing.
    /// </summary>
    private async Task<(bool Done, T? Result)> BoundedAsync<T>(Func<CancellationToken, Task<T>> operation)
    {
        if (Poisoned)
            return (false, default);

        using CancellationTokenSource cancel = new();
        Task<T> run = Task.Run(() => operation(cancel.Token));
        Task first = await Task.WhenAny(run, Task.Delay(StatementTimeout)).ConfigureAwait(false);
        if (first != run)
        {
            cancel.Cancel();
            first = await Task.WhenAny(run, Task.Delay(CancelGrace)).ConfigureAwait(false);
            if (first != run)
            {
                Poisoned = true;
                // The task keeps running. Observe its end, so a late exception is not unobserved.
                _ = run.ContinueWith(static t => _ = t.Exception, TaskScheduler.Default);
            }
            return (false, default);
        }
        return (true, await run.ConfigureAwait(false));
    }

    public async Task<StatementOutcome> ExecuteAsync(StatementRoute route, string sql, KvTransaction? transaction = null)
    {
        StatementsExecuted++;
        Stopwatch watch = Stopwatch.StartNew();

        // Not done at the time limit: a hang, whether or not the cancellation then stops it. The
        // cancellation only decides if this engine is still usable.
        (bool done, StatementOutcome? outcome) = await BoundedAsync(token => RunCoreAsync(route, sql, transaction, token)).ConfigureAwait(false);
        return done
            ? outcome!.WithElapsed(watch.Elapsed)
            : StatementOutcome.FromHang(sql, StatementTimeout).WithElapsed(watch.Elapsed);
    }

    private async Task<StatementOutcome> RunCoreAsync(StatementRoute route, string sql, KvTransaction? transaction, CancellationToken token)
    {
        bool ownTransaction = transaction is null && route is StatementRoute.NonQuery or StatementRoute.Ddl or StatementRoute.Analyze;
        KvTransaction? tx = transaction;
        try
        {
            if (ownTransaction)
                tx = await arm.Database.Transactions.BeginAsync().ConfigureAwait(false);

            StatementOutcome outcome = route switch
            {
                StatementRoute.Query or StatementRoute.Analyze => await QueryCoreAsync(sql, tx ?? KvTransaction.CreateReadOnly(), token).ConfigureAwait(false),
                StatementRoute.NonQuery => await NonQueryCoreAsync(sql, tx!, token).ConfigureAwait(false),
                StatementRoute.Ddl => await DdlCoreAsync(sql, tx!, token).ConfigureAwait(false),
                _ => throw new ArgumentOutOfRangeException(nameof(route)),
            };

            if (ownTransaction)
            {
                await arm.Database.Transactions.CommitAsync(tx!).ConfigureAwait(false);
                tx = null;
            }

            return outcome;
        }
        catch (Exception ex)
        {
            return StatementOutcome.FromException(sql, ex);
        }
        finally
        {
            if (ownTransaction && tx is not null)
            {
                try { await arm.Database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false); }
                catch { }
            }
        }
    }

    private async Task<StatementOutcome> QueryCoreAsync(string sql, KvTransaction tx, CancellationToken token)
    {
        QuerySchemaHolder schema = new();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await arm.Executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, arm.DatabaseName, sql, null, cancellationToken: token), schemaOut: schema).ConfigureAwait(false);

        List<ColumnValue[]> rows = new();
        OutcomeColumn[]? columns = null;
        string[]? keys = null;
        string? wireError = null;
        CamusDB.Tests.SqlFuzz.SqlFuzzWire? wire = WireCheck ? new CamusDB.Tests.SqlFuzz.SqlFuzzWire() : null;

        await foreach (QueryResultRow row in cursor.WithCancellation(token).ConfigureAwait(false))
        {
            if (keys is null)
                (columns, keys) = ResolveColumns(schema.Schema, row);

            if (wire is not null && wireError is null)
                wireError = wire.TryWrite(row, schema.Schema);

            ColumnValue[] cells = new ColumnValue[keys.Length];
            for (int i = 0; i < keys.Length; i++)
                cells[i] = row.Row.TryGetValue(keys[i], out ColumnValue? value) ? value : ColumnValue.Null;
            rows.Add(cells);
        }

        if (columns is null)
            (columns, _) = ResolveColumns(schema.Schema, null);

        return StatementOutcome.FromRows(sql, columns, rows, wireError);
    }

    /// <summary>
    /// The output columns in projection order. The bound schema gives the order and the types; a
    /// statement with no bound schema (the SHOW family) uses the cell order of its first row.
    /// </summary>
    private static (OutcomeColumn[] Columns, string[] Keys) ResolveColumns(IReadOnlyList<DerivedColumnSchema> schema, QueryResultRow? first)
    {
        List<OutcomeColumn> columns = new(schema.Count);
        List<string> keys = new(schema.Count);
        foreach (DerivedColumnSchema column in schema)
        {
            if (column.Hidden)
                continue;
            columns.Add(new OutcomeColumn(column.Name, column.Type));
            keys.Add(column.RowKey);
        }

        if (columns.Count == 0 && first is { } row)
        {
            foreach ((string key, ColumnValue value) in row.Row)
            {
                columns.Add(new OutcomeColumn(key, value.Type));
                keys.Add(key);
            }
        }

        return (columns.ToArray(), keys.ToArray());
    }

    private async Task<StatementOutcome> NonQueryCoreAsync(string sql, KvTransaction tx, CancellationToken token)
    {
        ExecuteNonSQLResult result = await arm.Executor.ExecuteNonSQLQuery(
            new ExecuteSQLTicket(tx, arm.DatabaseName, sql, null, cancellationToken: token)).ConfigureAwait(false);
        return StatementOutcome.FromAffected(sql, result.ModifiedRows);
    }

    private async Task<StatementOutcome> DdlCoreAsync(string sql, KvTransaction tx, CancellationToken token)
    {
        await arm.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, arm.DatabaseName, sql, null, cancellationToken: token)).ConfigureAwait(false);
        return StatementOutcome.FromAffected(sql, 0);
    }
}
