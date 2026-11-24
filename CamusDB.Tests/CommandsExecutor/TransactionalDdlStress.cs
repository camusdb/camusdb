/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>One node of a stress run: an engine and the database it holds open.</summary>
internal sealed record StressNode(CommandExecutor Executor, DatabaseDescriptor Database);

/// <summary>
/// Concurrent transactions that change the schema, beside autocommit schema changes and transactions that
/// write rows, on one node or on every node of a cluster. Each worker keeps a model of what it committed, and
/// the end of the run compares the model with the schema and the rows of every node. A worker accepts only the
/// errors that a client is told to retry; any other error ends the run as a failure.
///
/// <para>The interleavings are random, so one pass proves nothing: the fixtures run each scenario with several
/// seeds and rounds. A failure prints the seed.</para>
/// </summary>
internal sealed class TransactionalDdlStress(string db, IReadOnlyList<StressNode> nodes, int seed)
{
    /// <summary>A run that has not ended by then is a deadlock.</summary>
    private static readonly TimeSpan Deadline = TimeSpan.FromMinutes(4);

    private const int Accounts = 40;

    private const int InitialBalance = 100;

    private enum Outcome
    {
        Committed,

        /// <summary>A definite non-commit: the transaction was refused, or rolled back on purpose.</summary>
        NotCommitted,
    }

    private readonly ConcurrentQueue<string> failures = new();

    private readonly ConcurrentDictionary<string, int> refusals = new();

    private int committed;

    // ── statements ────────────────────────────────────────────────────────────

    private static bool IsRetryable(CamusDBException ex) =>
        ex.Code is CamusDBErrorCodes.TransactionConflict or CamusDBErrorCodes.TransactionMustRetry;

    private async Task StatementAsync(StressNode node, KvTransaction tx, string sql)
    {
        ExecuteSQLTicket ticket = new(txnState: tx, database: db, sql: sql, parameters: null);

        if (sql.StartsWith("INSERT", StringComparison.Ordinal) || sql.StartsWith("UPDATE", StringComparison.Ordinal))
            await node.Executor.ExecuteNonSQLQuery(ticket);
        else
            await node.Executor.ExecuteDDLSQL(ticket);
    }

    private async Task<List<Dictionary<string, ColumnValue>>> QueryAsync(StressNode node, string sql)
    {
        KvTransaction tx = await node.Database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await node.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(txnState: tx, database: db, sql: sql, parameters: null));

            List<Dictionary<string, ColumnValue>> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(new Dictionary<string, ColumnValue>(row.Row));

            return rows;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>
    /// Runs <paramref name="statements"/> in one explicit transaction and ends it. A COMMIT whose outcome is
    /// not resolved is retried on the same transaction, as a client does, so the returned outcome is definite.
    /// </summary>
    private async Task<Outcome> TransactionAsync(StressNode node, bool commit, params string[] statements)
    {
        KvTransactionsManager transactions = node.Database.Transactions;
        KvTransaction tx = await transactions.BeginAsync();
        tx.MarkSessionOwned();

        try
        {
            foreach (string sql in statements)
                await StatementAsync(node, tx, sql);
        }
        catch (CamusDBException ex) when (IsRetryable(ex))
        {
            CountRefusal("statement", ex);
            await RollbackAsync(transactions, tx);
            return Outcome.NotCommitted;
        }
        catch
        {
            await RollbackAsync(transactions, tx);
            throw;
        }

        if (!commit)
        {
            await RollbackAsync(transactions, tx);
            return Outcome.NotCommitted;
        }

        while (true)
        {
            try
            {
                await transactions.CommitAsync(tx);
                Interlocked.Increment(ref committed);
                return Outcome.Committed;
            }
            catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.TransactionFinalizeUnresolved)
            {
                CountRefusal("unresolved commit", ex);
                await Task.Delay(50);
            }
            catch (CamusDBException ex) when (IsRetryable(ex))
            {
                CountRefusal("commit", ex);
                return Outcome.NotCommitted;
            }
        }
    }

    private static async Task RollbackAsync(KvTransactionsManager transactions, KvTransaction tx)
    {
        if (tx.Status == KvTransactionStatus.Active)
            await transactions.RollbackAsync(tx);
    }

    /// <summary>Runs one autocommit schema statement; returns whether it took effect.</summary>
    private async Task<Outcome> AutocommitDdlAsync(StressNode node, string sql)
    {
        KvTransaction tx = await node.Database.Transactions.BeginAsync();
        try
        {
            await StatementAsync(node, tx, sql);
            await node.Database.Transactions.CommitAsync(tx);
            Interlocked.Increment(ref committed);
            return Outcome.Committed;
        }
        catch (CamusDBException ex) when (IsRetryable(ex))
        {
            CountRefusal("autocommit", ex);
            return Outcome.NotCommitted;
        }
        finally
        {
            await node.Database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private void CountRefusal(string where, CamusDBException ex) =>
        refusals.AddOrUpdate($"{where} {ex.Code}", 1, static (_, count) => count + 1);

    private StressNode NodeOf(Random random) => nodes[random.Next(nodes.Count)];

    private async Task RunWorkersAsync(IEnumerable<Func<Task>> workers)
    {
        Task all = Task.WhenAll(workers.Select(worker => Task.Run(async () =>
        {
            try
            {
                await worker();
            }
            catch (Exception ex)
            {
                failures.Enqueue(ex.ToString());
            }
        })));

        if (await Task.WhenAny(all, Task.Delay(Deadline)) != all)
            Assert.Fail($"seed {seed}: the workers did not end within {Deadline}; refusals: {Refusals}");

        Assert.IsTrue(failures.IsEmpty, $"seed {seed}: {string.Join("\n", failures.Take(3))}");
        TestContext.Progress.WriteLine($"seed {seed}: {committed} committed; refusals: {Refusals}");
    }

    private string Refusals => string.Join(", ", refusals.OrderBy(r => r.Key).Select(r => $"{r.Key} x{r.Value}"));

    /// <summary>A run in which every attempt was refused tests nothing.</summary>
    private void AssertMadeProgress(int atLeast) =>
        Assert.GreaterOrEqual(committed, atLeast, $"seed {seed}: too few transactions committed; refusals: {Refusals}");

    // ── the model of one worker ───────────────────────────────────────────────

    /// <summary>What one worker committed on the shared table. Workers use names of their own, so models never overlap.</summary>
    private sealed class Model
    {
        public readonly HashSet<string> Columns = new(StringComparer.Ordinal);

        public readonly HashSet<string> Indexes = new(StringComparer.Ordinal);

        public readonly HashSet<string> Checks = new(StringComparer.Ordinal);

        public readonly HashSet<string> Tables = new(StringComparer.Ordinal);

        /// <summary>The value that a row got in a column of the model, by row id and column.</summary>
        public readonly Dictionary<(long Id, string Column), long> Cells = [];

        public readonly HashSet<long> RowIds = [];
    }

    private static string? Pick(Random random, HashSet<string> names) =>
        names.Count == 0 ? null : names.ElementAt(random.Next(names.Count));

    /// <summary>One random schema transaction of a worker on the table <c>t</c>, and its effect on the model.</summary>
    private async Task SchemaTransactionAsync(Random random, Model model, int worker, int round)
    {
        StressNode node = NodeOf(random);
        bool commit = random.Next(4) != 0;
        string name = $"{worker}_{round}";
        long id = 1_000_000L * (worker + 1) + round;

        switch (random.Next(9))
        {
            case 0: // a column with no fill, and a row that uses it
            {
                string column = "c" + name;
                if (await TransactionAsync(node, commit, $"ALTER TABLE t ADD COLUMN {column} int64", $"INSERT INTO t (id, owner, {column}) VALUES ({id}, {worker}, {round})") == Outcome.Committed)
                {
                    model.Columns.Add(column);
                    model.RowIds.Add(id);
                    model.Cells[(id, column)] = round;
                }

                break;
            }

            case 1: // a column that the online build fills
            {
                string column = "f" + name;
                if (await TransactionAsync(node, commit, $"ALTER TABLE t ADD COLUMN {column} int64 NOT NULL DEFAULT ({round + 1})") == Outcome.Committed)
                {
                    model.Columns.Add(column);
                    foreach (long row in model.RowIds)
                        model.Cells[(row, column)] = round + 1;
                }

                break;
            }

            case 2 when Pick(random, model.Columns) is { } dropped:
                if (await TransactionAsync(node, commit, $"ALTER TABLE t DROP COLUMN {dropped}") == Outcome.Committed)
                {
                    model.Columns.Remove(dropped);
                    foreach ((long Id, string Column) cell in model.Cells.Keys.Where(c => c.Column == dropped).ToList())
                        model.Cells.Remove(cell);
                }

                break;

            case 3 when Pick(random, model.Columns) is { } renamed:
            {
                string to = "r" + name;
                if (await TransactionAsync(node, commit, $"ALTER TABLE t RENAME COLUMN {renamed} TO {to}") == Outcome.Committed)
                {
                    model.Columns.Remove(renamed);
                    model.Columns.Add(to);
                    foreach ((long Id, string Column) cell in model.Cells.Keys.Where(c => c.Column == renamed).ToList())
                    {
                        model.Cells[(cell.Id, to)] = model.Cells[cell];
                        model.Cells.Remove(cell);
                    }
                }

                break;
            }

            case 4: // the online build of an index
                if (await TransactionAsync(node, commit, $"CREATE INDEX i{name} ON t (owner)") == Outcome.Committed)
                    model.Indexes.Add("i" + name);
                break;

            case 5 when Pick(random, model.Indexes) is { } index:
                if (await TransactionAsync(node, commit, $"DROP INDEX {index} ON t") == Outcome.Committed)
                    model.Indexes.Remove(index);
                break;

            case 6: // a staged constraint
                if (await TransactionAsync(node, commit, $"ALTER TABLE t ADD CONSTRAINT k{name} CHECK (owner >= 0)") == Outcome.Committed)
                    model.Checks.Add("k" + name);
                break;

            case 7 when Pick(random, model.Checks) is { } check:
                if (await TransactionAsync(node, commit, $"ALTER TABLE t DROP CONSTRAINT {check}") == Outcome.Committed)
                    model.Checks.Remove(check);
                break;

            default: // a table of the transaction, with an index and rows
            {
                string table = "t" + name;
                if (await TransactionAsync(node, commit,
                        $"CREATE TABLE {table} (id int64 PRIMARY KEY, v int64)",
                        $"INSERT INTO {table} (id, v) VALUES (1, {round}), (2, {round})",
                        $"CREATE INDEX {table}_v ON {table} (v)") == Outcome.Committed)
                    model.Tables.Add(table);
                break;
            }
        }
    }

    /// <summary>Compares the models with the schema and the rows of every node.</summary>
    private async Task AssertModelsAsync(IReadOnlyCollection<Model> models)
    {
        string[] columns = [.. models.SelectMany(m => m.Columns).Concat(["id", "owner"]).OrderBy(n => n, StringComparer.Ordinal)];
        string[] indexes = [.. models.SelectMany(m => m.Indexes).OrderBy(n => n, StringComparer.Ordinal)];
        string[] checks = [.. models.SelectMany(m => m.Checks).OrderBy(n => n, StringComparer.Ordinal)];
        string[] tables = [.. models.SelectMany(m => m.Tables).Concat(["t"]).OrderBy(n => n, StringComparer.Ordinal)];
        long[] ids = [.. models.SelectMany(m => m.RowIds).Concat([1L]).Order()];

        await AssertSettledAsync();

        for (int i = 0; i < nodes.Count; i++)
        {
            StressNode node = nodes[i];
            string where = $"seed {seed}, node {i}";
            Schema schema = node.Database.SharedSchema;
            TableSchema table = schema.Tables["t"];

            CollectionAssert.AreEqual(tables, schema.Tables.Keys.OrderBy(n => n, StringComparer.Ordinal).ToArray(), $"{where}: the tables");
            CollectionAssert.AreEqual(columns, table.Columns!.Select(c => c.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray(), $"{where}: the columns");
            CollectionAssert.AreEqual(indexes,
                (table.Indexes ?? []).Where(x => x.Type == IndexType.Multi).Select(x => x.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray(), $"{where}: the indexes");
            CollectionAssert.AreEqual(checks, (table.CheckConstraints ?? []).Select(c => c.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray(), $"{where}: the checks");

            List<Dictionary<string, ColumnValue>> rows = await QueryAsync(node, "SELECT * FROM t ORDER BY id");
            CollectionAssert.AreEqual(ids, rows.Select(r => r["id"].LongValue).ToArray(), $"{where}: the rows");

            foreach (((long id, string column), long value) in models.SelectMany(m => m.Cells))
                Assert.AreEqual(value, rows.Single(r => r["id"].LongValue == id)[column].LongValue, $"{where}: row {id}, column {column}");

            foreach (string index in indexes)
                Assert.AreEqual(ids.Length, (await QueryAsync(node, $"SELECT id FROM t@{{FORCE_INDEX={index}}} WHERE owner >= 0")).Count, $"{where}: index {index} holds every row");

            foreach (string created in tables.Where(n => n != "t"))
                Assert.AreEqual(2, (await QueryAsync(node, $"SELECT id FROM {created}@{{FORCE_INDEX={created}_v}} WHERE v >= 0")).Count, $"{where}: table {created}");
        }
    }

    /// <summary>Nothing is pending on any node, and the schema holds no record of unfinished work.</summary>
    private async Task AssertSettledAsync()
    {
        Stopwatch timer = Stopwatch.StartNew();
        string? unsettled;

        do
        {
            unsettled = await DescribeUnsettledAsync();
            if (unsettled is not null)
                await Task.Delay(100);
        }
        while (unsettled is not null && timer.Elapsed < TimeSpan.FromSeconds(30));

        Assert.IsNull(unsettled, $"seed {seed}: {unsettled}");
    }

    private async Task<string?> DescribeUnsettledAsync()
    {
        long version = nodes.Max(n => n.Database.SharedSchema.SchemaVersion);

        for (int i = 0; i < nodes.Count; i++)
        {
            if (nodes[i].Database.SharedSchema.PendingChangeSet is { } pending)
                return $"node {i} has change set {pending.ChangeSetId} pending";

            if (nodes[i].Database.SharedSchema.SchemaVersion != version)
                return $"node {i} is behind schema version {version}";
        }

        DatabaseDescriptor database = nodes[0].Database;

        foreach (string prefix in new[]
        {
            MetaKeys.CoordinatorKeyPrefix(database.Id),
            MetaKeys.PurgeKeyPrefix(database.Id),
            MetaKeys.ChangeSetKeyPrefix(database.Id),
            MetaKeys.ChangeSetDecisionKeyPrefix(database.Id),
        })
        {
            int count = 0;
            await foreach ((string key, ReadOnlyKeyValueEntry entry) in database.Kahuna.Kahuna.LocateAndScanRange(
                HLCTimestamp.Zero, MetaKeys.MetaBucketPrefix(database.Id), null, true, null, true, 1000,
                HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
            {
                if (entry.Value is not null && key.StartsWith(prefix, StringComparison.Ordinal))
                    count++;
            }

            if (count > 0)
                return $"{count} key(s) are left under '{prefix}'";
        }

        return null;
    }

    // ── scenarios ─────────────────────────────────────────────────────────────

    /// <summary>Workers change one table in transactions of their own: one commits, and the others wait or are told to retry.</summary>
    public async Task DdlTransactionsOnOneTableAsync(int workers, int rounds)
    {
        await AutocommitDdlAsync(nodes[0], "CREATE TABLE t (id int64 PRIMARY KEY, owner int64)");
        await TransactionAsync(nodes[0], commit: true, "INSERT INTO t (id, owner) VALUES (1, 0)");

        Model[] models = [.. Enumerable.Range(0, workers).Select(_ => new Model())];

        await RunWorkersAsync(models.Select((model, worker) => (Func<Task>)(async () =>
        {
            Random random = new(seed * 1000 + worker);
            for (int round = 0; round < rounds; round++)
                await SchemaTransactionAsync(random, model, worker, round);
        })));

        AssertMadeProgress(workers * rounds / 4);
        await AssertModelsAsync(models);
    }

    /// <summary>The refusal of <c>ADD COLUMN</c> for a name that is taken: the invalid-input code, with the name in the message.</summary>
    private static bool IsDuplicateColumn(CamusDBException ex, string column) =>
        ex.Code == CamusDBErrorCodes.InvalidInput && ex.Message.Contains($"Duplicate column '{column}'", StringComparison.Ordinal);

    /// <summary>In each round every worker adds the same column in a transaction. Exactly one commits.</summary>
    public async Task SameColumnAsync(int workers, int rounds)
    {
        await AutocommitDdlAsync(nodes[0], "CREATE TABLE t (id int64 PRIMARY KEY, owner int64)");
        await TransactionAsync(nodes[0], commit: true, "INSERT INTO t (id, owner) VALUES (1, 0)");

        Model model = new();

        for (int round = 0; round < rounds; round++)
        {
            string column = $"c{round}";
            int winners = 0;
            int current = round;

            await RunWorkersAsync(Enumerable.Range(0, workers).Select(worker => (Func<Task>)(async () =>
            {
                long id = 1_000_000L * (worker + 1) + current;
                StressNode node = nodes[worker % nodes.Count];

                while (true)
                {
                    try
                    {
                        if (await TransactionAsync(node, commit: true, $"ALTER TABLE t ADD COLUMN {column} int64", $"INSERT INTO t (id, owner, {column}) VALUES ({id}, {worker}, 1)") == Outcome.Committed)
                        {
                            Interlocked.Increment(ref winners);
                            lock (model)
                            {
                                model.Columns.Add(column);
                                model.RowIds.Add(id);
                                model.Cells[(id, column)] = 1;
                            }

                            return;
                        }
                    }
                    catch (CamusDBException ex) when (IsDuplicateColumn(ex, column))
                    {
                        return;
                    }
                }
            })));

            Assert.AreEqual(1, winners, $"seed {seed}: exactly one transaction adds {column}");
        }

        await AssertModelsAsync([model]);
    }

    /// <summary>Transactions that change the schema beside autocommit schema changes of the same table.</summary>
    public async Task DdlTransactionsBesideAutocommitDdlAsync(int workers, int rounds)
    {
        await AutocommitDdlAsync(nodes[0], "CREATE TABLE t (id int64 PRIMARY KEY, owner int64)");
        await TransactionAsync(nodes[0], commit: true, "INSERT INTO t (id, owner) VALUES (1, 0)");

        Model[] models = [.. Enumerable.Range(0, 2 * workers).Select(_ => new Model())];

        await RunWorkersAsync(models.Select((model, worker) => (Func<Task>)(async () =>
        {
            Random random = new(seed * 1000 + worker);

            for (int round = 0; round < rounds; round++)
            {
                if (worker < workers)
                    await SchemaTransactionAsync(random, model, worker, round);
                else
                    await AutocommitSchemaChangeAsync(random, model, worker, round);
            }
        })));

        AssertMadeProgress(workers * rounds / 4);
        await AssertModelsAsync(models);
    }

    private async Task AutocommitSchemaChangeAsync(Random random, Model model, int worker, int round)
    {
        StressNode node = NodeOf(random);
        string name = $"{worker}_{round}";

        switch (random.Next(4))
        {
            case 0:
                if (await AutocommitDdlAsync(node, $"ALTER TABLE t ADD COLUMN c{name} int64") == Outcome.Committed)
                    model.Columns.Add("c" + name);
                break;

            case 1 when Pick(random, model.Columns) is { } dropped:
                if (await AutocommitDdlAsync(node, $"ALTER TABLE t DROP COLUMN {dropped}") == Outcome.Committed)
                    model.Columns.Remove(dropped);
                break;

            case 2 when Pick(random, model.Indexes) is { } index:
                if (await AutocommitDdlAsync(node, $"DROP INDEX {index} ON t") == Outcome.Committed)
                    model.Indexes.Remove(index);
                break;

            default:
                if (await AutocommitDdlAsync(node, $"CREATE INDEX i{name} ON t (owner)") == Outcome.Committed)
                    model.Indexes.Add("i" + name);
                break;
        }
    }

    /// <summary>
    /// Transfers between the rows of one table, readers of the whole table, and transactions that change that
    /// table, at the same time. Every transfer keeps the sum of the balances and adds two to the sum of the
    /// versions, so a lost write, a write applied twice and a row decoded with the wrong layout all show.
    /// </summary>
    public async Task DdlTransactionsBesideWritersAsync(int writers, int schemaWorkers, int readers, TimeSpan duration, Func<CancellationToken, Task>? disturb = null)
    {
        await AutocommitDdlAsync(nodes[0], "CREATE TABLE t (id int64 PRIMARY KEY, owner int64, balance int64, ver int64)");
        await TransactionAsync(nodes[0], commit: true,
            "INSERT INTO t (id, owner, balance, ver) VALUES " + string.Join(", ", Enumerable.Range(1, Accounts).Select(i => $"({i}, 0, {InitialBalance}, 0)")));

        Model[] models = [.. Enumerable.Range(0, schemaWorkers).Select(_ => new Model())];
        Stopwatch timer = Stopwatch.StartNew();
        using CancellationTokenSource stop = new();
        int transfers = 0;
        int reads = 0;
        int schemaRounds = 0;

        List<Func<Task>> workers = [];

        for (int w = 0; w < writers; w++)
        {
            int worker = w;
            workers.Add(async () =>
            {
                Random random = new(seed * 1000 + worker);
                StressNode node = nodes[worker % nodes.Count];

                while (timer.Elapsed < duration)
                {
                    // In id order, so two transfers never wait for each other.
                    int a = random.Next(1, Accounts);
                    int b = random.Next(a + 1, Accounts + 1);
                    int amount = random.Next(1, 10) - 5;

                    if (await TransactionAsync(node, commit: random.Next(8) != 0,
                            $"UPDATE t SET balance = balance - {amount}, ver = ver + 1 WHERE id = {a}",
                            $"UPDATE t SET balance = balance + {amount}, ver = ver + 1 WHERE id = {b}") == Outcome.Committed)
                        Interlocked.Increment(ref transfers);
                }
            });
        }

        for (int r = 0; r < readers; r++)
        {
            int worker = r;
            workers.Add(async () =>
            {
                StressNode node = nodes[worker % nodes.Count];

                while (timer.Elapsed < duration)
                {
                    List<Dictionary<string, ColumnValue>> rows;

                    try
                    {
                        rows = await QueryAsync(node, "SELECT * FROM t WHERE id <= " + Accounts);
                    }
                    catch (CamusDBException ex) when (IsRetryable(ex))
                    {
                        CountRefusal("read", ex);
                        continue;
                    }

                    Assert.AreEqual(Accounts, rows.Count, "every read returns every row");
                    Assert.AreEqual(Accounts * InitialBalance, rows.Sum(row => row["balance"].LongValue), "every read keeps the sum of the balances");
                    Interlocked.Increment(ref reads);
                }
            });
        }

        for (int s = 0; s < schemaWorkers; s++)
        {
            int worker = s;
            workers.Add(async () =>
            {
                Random random = new(seed * 1000 + 500 + worker);

                for (int round = 0; timer.Elapsed < duration; round++)
                {
                    await SchemaTransactionAsync(random, models[worker], worker, round);
                    Interlocked.Increment(ref schemaRounds);
                }
            });
        }

        Task disturbance = disturb is null ? Task.CompletedTask : Task.Run(() => disturb(stop.Token));

        await RunWorkersAsync(workers);
        await stop.CancelAsync();
        await disturbance;

        TestContext.Progress.WriteLine($"seed {seed}: {transfers} transfers, {reads} reads, {schemaRounds} schema transactions");
        Assert.Greater(transfers, 0);
        Assert.Greater(reads, 0);
        Assert.Greater(models.Sum(m => m.Columns.Count + m.Indexes.Count + m.Checks.Count + m.Tables.Count), 0, $"seed {seed}: some schema transactions committed");

        await AssertSettledAsync();

        string[] indexes = [.. models.SelectMany(m => m.Indexes)];
        string[] added = [.. models.SelectMany(m => m.Columns)];

        for (int i = 0; i < nodes.Count; i++)
        {
            string where = $"seed {seed}, node {i}";
            TableSchema table = nodes[i].Database.SharedSchema.Tables["t"];

            CollectionAssert.AreEquivalent(added.Concat(["id", "owner", "balance", "ver"]), table.Columns!.Select(c => c.Name), $"{where}: the columns");
            CollectionAssert.AreEquivalent(indexes, (table.Indexes ?? []).Where(x => x.Type == IndexType.Multi).Select(x => x.Name), $"{where}: the indexes");

            List<Dictionary<string, ColumnValue>> rows = await QueryAsync(nodes[i], "SELECT * FROM t");
            List<Dictionary<string, ColumnValue>> accounts = [.. rows.Where(r => r["id"].LongValue <= Accounts)];

            Assert.AreEqual(Accounts, accounts.Count, where);
            Assert.AreEqual(Accounts * InitialBalance, accounts.Sum(r => r["balance"].LongValue), $"{where}: the sum of the balances");
            Assert.AreEqual(2L * transfers, accounts.Sum(r => r["ver"].LongValue), $"{where}: every committed transfer, once");

            foreach (((long id, string column), long value) in models.SelectMany(m => m.Cells))
                Assert.AreEqual(value, rows.Single(r => r["id"].LongValue == id)[column].LongValue, $"{where}: row {id}, column {column}");

            foreach (string index in indexes)
                Assert.AreEqual(rows.Count, (await QueryAsync(nodes[i], $"SELECT id FROM t@{{FORCE_INDEX={index}}} WHERE owner >= 0")).Count, $"{where}: index {index}");
        }
    }
}
