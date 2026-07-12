/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Native (non-terminated) Id key encoding: <see cref="ColumnType.Id"/> now encodes as a fixed-width
/// base-125 number instead of sharing String's terminated path. This breaks String/Id encoding
/// sharing, so a bare string literal compared to an Id column must be coerced to Id at the query
/// layer for equality/range/lookup to still match. It also unlocks descending Id indexes. These tests
/// exercise the interchange and the descending path end to end.
/// </summary>
[NonParallelizable]
internal sealed class TestNativeIdEncoding : BaseTest
{
    // Explicit, ordered 24-hex ObjectIds for deterministic assertions.
    private const string Id1 = "000000000000000000000001";
    private const string Id2 = "0000000000000000000000a0";
    private const string Id3 = "7fffffffffffffffffffffff";
    private const string Id4 = "fffffffffffffffffffffffe";

    [Test]
    [NonParallelizable]
    public async Task WhereIdEquality_StringLiteral_MatchesNativeEncodedKey()
    {
        (string dbname, _, CommandExecutor executor) = await CreateRefTable();
        await InsertRefs(executor, dbname, new[] { Id1, Id2, Id3 });

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, $"SELECT ref FROM docs WHERE ref = '{Id2}'");

        Assert.AreEqual(1, rows.Count, "equality on an Id column must match through the native key");
        Assert.AreEqual(Id2, rows[0].Row["ref"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task IdRangePredicate_StringLiteral_ReturnsCorrectRows()
    {
        (string dbname, _, CommandExecutor executor) = await CreateRefTable();
        await InsertRefs(executor, dbname, new[] { Id1, Id2, Id3, Id4 });

        List<string> refs = (await ExecSelect(executor, dbname, $"SELECT ref FROM docs WHERE ref > '{Id2}'"))
            .Select(r => r.Row["ref"].StrValue!).OrderBy(x => x).ToList();

        CollectionAssert.AreEqual(new[] { Id3, Id4 }, refs);
    }

    [Test]
    [NonParallelizable]
    public async Task IdInList_StringLiterals_MatchNativeEncodedKeys()
    {
        (string dbname, _, CommandExecutor executor) = await CreateRefTable();
        await ExecDDL(executor, dbname, "CREATE INDEX ref_idx ON docs (ref)");
        await InsertRefs(executor, dbname, new[] { Id1, Id2, Id3, Id4 });

        List<string> refs = (await ExecSelect(executor, dbname, $"SELECT ref FROM docs WHERE ref IN ('{Id1}', '{Id3}')"))
            .Select(r => r.Row["ref"].StrValue!).OrderBy(x => x).ToList();

        CollectionAssert.AreEqual(new[] { Id1, Id3 }, refs);
    }

    [Test]
    [NonParallelizable]
    public async Task PrimaryKeyLookup_ByIdLiteral_Works()
    {
        (string dbname, _, CommandExecutor executor) = await CreateRefTable();
        await InsertRefs(executor, dbname, new[] { Id1, Id2 });

        // Grab a real primary-key id, then look it up by literal.
        List<QueryResultRow> all = await ExecSelect(executor, dbname, "SELECT id FROM docs");
        string pk = all[0].Row["id"].StrValue!;

        List<QueryResultRow> rows = await ExecSelect(executor, dbname, $"SELECT id FROM docs WHERE id = '{pk}'");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(pk, rows[0].Row["id"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task DescendingIdIndex_OrderByDesc_StreamsInDescendingOrder()
    {
        (string dbname, _, CommandExecutor executor) = await CreateRefTable();
        await ExecDDL(executor, dbname, "CREATE INDEX ref_desc ON docs (ref DESC)");
        await InsertRefs(executor, dbname, new[] { Id2, Id4, Id1, Id3 });

        List<string> refs = (await ExecSelect(executor, dbname, "SELECT ref FROM docs ORDER BY ref DESC"))
            .Select(r => r.Row["ref"].StrValue!).ToList();

        CollectionAssert.AreEqual(new[] { Id4, Id3, Id2, Id1 }, refs);
    }

    [Test]
    [NonParallelizable]
    public async Task DescendingIdIndex_ElidesSort()
    {
        (string dbname, _, CommandExecutor executor) = await CreateRefTable();
        await ExecDDL(executor, dbname, "CREATE INDEX ref_desc ON docs (ref DESC)");
        await InsertRefs(executor, dbname, new[] { Id1, Id2, Id3 });

        List<string> nodes = await ExplainNodes(executor, dbname, "SELECT ref FROM docs ORDER BY ref DESC");

        Assert.IsFalse(nodes.Contains("sort"),
            "ORDER BY ref DESC must stream from the descending Id index; plan nodes: " + string.Join(", ", nodes));
    }

    [Test]
    [NonParallelizable]
    public async Task DescendingIdPrimaryKey_IsAcceptedAndPersists()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname, "CREATE TABLE t (id oid, name string(20) not null, PRIMARY KEY (id DESC))");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        TableIndexSchema pk = db2.Schema.Tables["t"].Indexes!.First(ix => ix.DirectionAt(0) == OrderType.Descending);
        Assert.AreEqual(OrderType.Descending, pk.DirectionAt(0), "descending Id primary key must persist");
    }

    // ── Helpers ────────────────────────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateRefTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname, "CREATE TABLE docs (id oid primary key, ref oid not null)");
        return (dbname, database, executor);
    }

    private static async Task InsertRefs(CommandExecutor executor, string dbname, string[] refs)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        foreach (string r in refs)
        {
            KvTransaction tx = await db.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
                $"INSERT INTO docs (id, ref) VALUES (gen_id(), '{r}')", null));
            await db.Transactions.CommitAsync(tx);
        }
    }

    private static async Task ExecDDL(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecSelect(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<List<string>> ExplainNodes(CommandExecutor executor, string dbname, string sql)
    {
        List<QueryResultRow> rows = await ExecSelect(executor, dbname, "EXPLAIN " + sql);
        List<string> nodes = new();
        foreach (QueryResultRow r in rows)
            if (r.Row.TryGetValue("node", out ColumnValue? node) && node.StrValue is not null)
                nodes.Add(node.StrValue);
        return nodes;
    }
}
