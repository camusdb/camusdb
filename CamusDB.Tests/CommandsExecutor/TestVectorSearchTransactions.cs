
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A nearest-neighbour query is an ordinary read, so it must obey the ordinary visibility rules:
/// its own transaction's uncommitted writes, nothing from a rolled-back one, and the historical
/// contents under <c>AS OF SYSTEM TIME</c>.
///
/// <para>Ranking is where this could go wrong quietly. The ordering is computed after rows are
/// produced, so a visibility bug would not raise — it would return a confidently ordered list of the
/// wrong rows.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestVectorSearchTransactions : SharedNodeBaseTest
{
    private static string OID => ObjectIdGenerator.Generate().ToString();

    private static ColumnValue Pack(params float[] elements)
    {
        byte[] bytes = new byte[elements.Length * 4];

        for (int i = 0; i < elements.Length; i++)
            BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(i * 4, 4), elements[i]);

        return new ColumnValue(bytes);
    }

    /// <summary>Query vector pointing along the first axis; "near" rows are the ones close to it.</summary>
    private static Dictionary<string, ColumnValue> Query() => new() { { "@q", Pack(1f, 0f, 0f, 0f) } };

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddl = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, dbname,
            "CREATE TABLE docs (id OID NOT NULL, tag string, embedding bytes(16), PRIMARY KEY (id))", null));

        return (dbname, db, executor);
    }

    private static async Task InsertAsync(
        CommandExecutor executor, DatabaseDescriptor db, KvTransaction tx, string tag, params float[] embedding)
    {
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name,
            "INSERT INTO docs (id, tag, embedding) VALUES (@id, @tag, @e)",
            new()
            {
                { "@id", new(ColumnType.Id, OID) },
                { "@tag", new(ColumnType.String, tag) },
                { "@e", Pack(embedding) },
            }));
    }

    private static async Task<List<string>> NearestAsync(
        CommandExecutor executor, DatabaseDescriptor db, KvTransaction tx, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db.Name, sql, Query()));

        List<QueryResultRow> rows = await cursor.ToListAsync();
        return rows.Select(r => r.Row["tag"].StrValue!).ToList();
    }

    private const string NearestSql =
        "SELECT tag FROM docs ORDER BY l2_distance(embedding, @q) LIMIT 3";

    // ── Read-your-own-writes ─────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task RanksUncommittedRowsWrittenByTheSameTransaction()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupAsync();

        KvTransaction committed = await db.Transactions.BeginAsync();
        await InsertAsync(executor, db, committed, "far", 0f, 0f, 1f, 0f);
        await db.Transactions.CommitAsync(committed);

        KvTransaction tx = await db.Transactions.BeginAsync();
        await InsertAsync(executor, db, tx, "near-uncommitted", 1f, 0.05f, 0f, 0f);

        // The uncommitted row is the nearest, so it must both be visible and rank first.
        List<string> ownView = await NearestAsync(executor, db, tx, NearestSql);
        CollectionAssert.AreEqual(new[] { "near-uncommitted", "far" }, ownView);

        await db.Transactions.CommitAsync(tx);
    }

    [Test]
    [NonParallelizable]
    public async Task DoesNotRankRowsFromARolledBackTransaction()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupAsync();

        KvTransaction committed = await db.Transactions.BeginAsync();
        await InsertAsync(executor, db, committed, "far", 0f, 0f, 1f, 0f);
        await db.Transactions.CommitAsync(committed);

        KvTransaction doomed = await db.Transactions.BeginAsync();
        await InsertAsync(executor, db, doomed, "never-committed", 1f, 0f, 0f, 0f);
        await db.Transactions.RollbackAsync(doomed);

        KvTransaction reader = await db.Transactions.BeginAsync();
        List<string> visible = await NearestAsync(executor, db, reader, NearestSql);

        // The discarded row was the closest match; ranking it would be the most convincing possible
        // wrong answer.
        CollectionAssert.AreEqual(new[] { "far" }, visible);
        await db.Transactions.CommitAsync(reader);
    }

    // ── Time travel ──────────────────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task RanksTheRowsThatExistedAtAPastSnapshot()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupAsync();

        KvTransaction first = await db.Transactions.BeginAsync();
        await InsertAsync(executor, db, first, "original", 0.5f, 0f, 0f, 0f);
        await db.Transactions.CommitAsync(first);

        await Task.Delay(60);
        long snapshotMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await Task.Delay(60);

        KvTransaction second = await db.Transactions.BeginAsync();
        await InsertAsync(executor, db, second, "added-later", 1f, 0f, 0f, 0f);
        await db.Transactions.CommitAsync(second);

        // "added-later" is nearer than "original", so a snapshot that leaked it would change the
        // ranking, not merely lengthen the list.
        (_, IAsyncEnumerable<QueryResultRow> historic) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), dbname,
                $"SELECT tag FROM docs AS OF SYSTEM TIME {snapshotMs} ORDER BY l2_distance(embedding, @q) LIMIT 3",
                Query()));

        List<string> asOf = (await historic.ToListAsync()).Select(r => r.Row["tag"].StrValue!).ToList();
        CollectionAssert.AreEqual(new[] { "original" }, asOf);

        KvTransaction now = await db.Transactions.BeginAsync();
        List<string> current = await NearestAsync(executor, db, now, NearestSql);
        CollectionAssert.AreEqual(new[] { "added-later", "original" }, current);
        await db.Transactions.CommitAsync(now);
    }

    // ── A parameterized LIMIT still bounds the sort ──────────────────────────

    [Test]
    [NonParallelizable]
    public async Task ParameterizedLimitBindsBeforeThePlanIsBuilt()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SetupAsync();

        KvTransaction seed = await db.Transactions.BeginAsync();
        await InsertAsync(executor, db, seed, "a", 1f, 0f, 0f, 0f);
        await InsertAsync(executor, db, seed, "b", 0f, 1f, 0f, 0f);
        await InsertAsync(executor, db, seed, "c", 0f, 0f, 1f, 0f);
        await db.Transactions.CommitAsync(seed);

        Dictionary<string, ColumnValue> parameters = Query();
        parameters["@k"] = new(ColumnType.Integer64, 2L);

        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbname,
                "SELECT tag FROM docs ORDER BY l2_distance(embedding, @q) LIMIT @k", parameters));

        List<QueryResultRow> rows = await cursor.ToListAsync();
        Assert.AreEqual(2, rows.Count, "a bound LIMIT must still limit");
        Assert.AreEqual("a", rows[0].Row["tag"].StrValue);

        // The bound is read from the evaluated limit, so a parameter reaches the plan the same way a
        // literal does.
        (_, IAsyncEnumerable<QueryResultRow> plan) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbname,
                "EXPLAIN SELECT tag FROM docs ORDER BY l2_distance(embedding, @q) LIMIT @k", parameters));

        List<QueryResultRow> planRows = await plan.ToListAsync();
        QueryResultRow topk = planRows.Single(r => r.Row["node"].StrValue == "topk");
        StringAssert.Contains("k: 2", topk.Row["detail"].StrValue!);

        await db.Transactions.CommitAsync(tx);
    }
}
