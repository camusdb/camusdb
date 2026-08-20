
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
using CamusDB.Core.Cache;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Acceptance tests for executing an ORDER BY whose key is computed per row, through the full sorter.
/// Covers the nearest-neighbour query the vector work exists for, the in-memory and spilled sort
/// paths agreeing, and the carrier column never reaching a result set.
/// </summary>
internal sealed class TestOrderByExpressionExecution : SharedNodeBaseTest
{
    private static string OID => ObjectIdGenerator.Generate().ToString();

    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
    }

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, parameters));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Select(
        CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db.Name, sql, parameters));
        return await cursor.ToListAsync();
    }

    /// <summary>Names of varying length, so length() orders them differently from the text itself.</summary>
    private static readonly string[] Names = ["dddd", "a", "ccc", "bb"];

    private async Task<(DatabaseDescriptor db, CommandExecutor executor)> SetupNames()
        => await SetupNames(Options);

    private async Task<(DatabaseDescriptor db, CommandExecutor executor)> SetupNames(CamusDBOptions options)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase(options);

        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, n int64, name string, PRIMARY KEY (id))");

        for (int i = 0; i < Names.Length; i++)
        {
            await Exec(executor, db, "INSERT INTO t (id, n, name) VALUES (@id, @n, @name)",
                new()
                {
                    { "@id", new(ColumnType.Id, OID) },
                    { "@n", new(ColumnType.Integer64, (long)i) },
                    { "@name", new(ColumnType.String, Names[i]) },
                });
        }

        return (db, executor);
    }

    // ── The query the whole feature exists for ───────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task NearestNeighbourOrdering_ReturnsRowsNearestFirst()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db,
            "CREATE TABLE docs (id OID NOT NULL, tag string, embedding bytes(16), PRIMARY KEY (id))");

        // Distances from the query vector [1,0,0,0]: near 0.1, mid 1.0, far ~1.41.
        await InsertDoc(executor, db, "near", [1f, 0.1f, 0f, 0f]);
        await InsertDoc(executor, db, "far", [0f, 1f, 1f, 0f]);
        await InsertDoc(executor, db, "mid", [0f, 0f, 0f, 0f]);

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT tag FROM docs ORDER BY l2_distance(embedding, @q)",
            new() { { "@q", Pack([1f, 0f, 0f, 0f]) } });

        CollectionAssert.AreEqual(new[] { "near", "mid", "far" }, rows.Select(r => r.Row["tag"].StrValue).ToArray());
    }

    [Test]
    [NonParallelizable]
    public async Task NearestNeighbourOrdering_WorksThroughAProjectionAlias()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db,
            "CREATE TABLE docs (id OID NOT NULL, tag string, embedding bytes(16), PRIMARY KEY (id))");

        await InsertDoc(executor, db, "near", [1f, 0.1f, 0f, 0f]);
        await InsertDoc(executor, db, "far", [0f, 1f, 1f, 0f]);

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT tag, l2_distance(embedding, @q) AS distance FROM docs ORDER BY distance",
            new() { { "@q", Pack([1f, 0f, 0f, 0f]) } });

        CollectionAssert.AreEqual(new[] { "near", "far" }, rows.Select(r => r.Row["tag"].StrValue).ToArray());
        Assert.Less(rows[0].Row["distance"].FloatValue, rows[1].Row["distance"].FloatValue);
    }

    private static ColumnValue Pack(float[] elements)
    {
        byte[] bytes = new byte[elements.Length * 4];

        for (int i = 0; i < elements.Length; i++)
            BinaryPrimitives.WriteSingleLittleEndian(bytes.AsSpan(i * 4, 4), elements[i]);

        return new ColumnValue(bytes);
    }

    private static Task InsertDoc(CommandExecutor executor, DatabaseDescriptor db, string tag, float[] embedding)
        => Exec(executor, db, "INSERT INTO docs (id, tag, embedding) VALUES (@id, @tag, @e)",
            new()
            {
                { "@id", new(ColumnType.Id, OID) },
                { "@tag", new(ColumnType.String, tag) },
                { "@e", Pack(embedding) },
            });

    // ── General expression ordering ──────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task ComputedOrdering_OrdersByTheComputedValue()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupNames();

        List<QueryResultRow> rows = await Select(executor, db, "SELECT name FROM t ORDER BY length(name)");

        CollectionAssert.AreEqual(new[] { "a", "bb", "ccc", "dddd" },
            rows.Select(r => r.Row["name"].StrValue).ToArray());
    }

    [Test]
    [NonParallelizable]
    public async Task ComputedOrdering_HonoursDescending()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupNames();

        List<QueryResultRow> rows = await Select(executor, db, "SELECT name FROM t ORDER BY length(name) DESC");

        CollectionAssert.AreEqual(new[] { "dddd", "ccc", "bb", "a" },
            rows.Select(r => r.Row["name"].StrValue).ToArray());
    }

    [Test]
    [NonParallelizable]
    public async Task MixedKeys_ComputedThenColumn_UseOneComparisonPath()
    {
        // Two names of equal length, so the second key decides. A mixed ordering that resolved its
        // two keys through different rules would be where the paths diverge.
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, n int64, name string, PRIMARY KEY (id))");

        await Exec(executor, db, "INSERT INTO t (id, n, name) VALUES (@id, 2, 'bb')",
            new() { { "@id", new(ColumnType.Id, OID) } });
        await Exec(executor, db, "INSERT INTO t (id, n, name) VALUES (@id, 1, 'cc')",
            new() { { "@id", new(ColumnType.Id, OID) } });
        await Exec(executor, db, "INSERT INTO t (id, n, name) VALUES (@id, 3, 'a')",
            new() { { "@id", new(ColumnType.Id, OID) } });

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT n FROM t ORDER BY length(name), n");

        CollectionAssert.AreEqual(new[] { 3L, 1L, 2L }, rows.Select(r => r.Row["n"].LongValue).ToArray());
    }

    [Test]
    [NonParallelizable]
    public async Task ComputedOrdering_PlacesNullsLikeAColumnOrdering()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, "CREATE TABLE t (id OID NOT NULL, n int64, name string, PRIMARY KEY (id))");

        await Exec(executor, db, "INSERT INTO t (id, n, name) VALUES (@id, 1, 'bb')",
            new() { { "@id", new(ColumnType.Id, OID) } });
        await Exec(executor, db, "INSERT INTO t (id, n, name) VALUES (@id, 2, NULL)",
            new() { { "@id", new(ColumnType.Id, OID) } });
        await Exec(executor, db, "INSERT INTO t (id, n, name) VALUES (@id, 3, 'a')",
            new() { { "@id", new(ColumnType.Id, OID) } });

        // length(NULL) is NULL. The computed ordering must place it exactly where a nullable column
        // ordering would, so the two forms stay interchangeable.
        List<QueryResultRow> computed = await Select(executor, db, "SELECT n FROM t ORDER BY length(name)");
        List<QueryResultRow> column = await Select(executor, db, "SELECT n FROM t ORDER BY name");

        CollectionAssert.AreEqual(
            column.Select(r => r.Row["n"].LongValue).ToArray(),
            computed.Select(r => r.Row["n"].LongValue).ToArray());
    }

    // ── The carrier column must never escape ─────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task SelectStar_ExposesNoInternalSortKey()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupNames();

        List<QueryResultRow> rows = await Select(executor, db, "SELECT * FROM t ORDER BY length(name)");

        Assert.AreEqual(Names.Length, rows.Count);

        foreach (QueryResultRow row in rows)
        {
            CollectionAssert.AreEquivalent(new[] { "id", "n", "name" }, row.Row.Keys.ToArray());
            Assert.IsFalse(row.Row.Keys.Any(k => k.StartsWith('~')), "internal sort carrier leaked into the result");
        }
    }

    // ── A column read only by the ordering must still be decoded ─────────────

    [Test]
    [NonParallelizable]
    public async Task ComputedOrdering_ReadsAColumnThatIsNotProjected()
    {
        // "name" appears only inside the ordering expression. If projection narrowing dropped it,
        // every key would evaluate to NULL and the rows would come back in scan order.
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupNames();

        List<QueryResultRow> rows = await Select(executor, db, "SELECT n FROM t ORDER BY length(name)");

        CollectionAssert.AreEqual(new[] { 1L, 3L, 2L, 0L }, rows.Select(r => r.Row["n"].LongValue).ToArray());
    }

    // ── In-memory and spilled sorts must agree ───────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task SpilledAndInMemorySorts_ReturnTheSameOrder()
    {
        // Two engines, because an engine fixes its configuration when it is constructed: one sorts
        // in memory, the other is forced to spill on a tiny input so the carrier makes a real
        // round trip through the spill encoder and the k-way merge.
        (DatabaseDescriptor inMemoryDb, CommandExecutor inMemoryExecutor) =
            await SetupNames(Options with { SpillEnabled = false });

        (DatabaseDescriptor spilledDb, CommandExecutor spilledExecutor) =
            await SetupNames(Options with { SpillEnabled = true, ForceSpillThresholdRows = 2 });

        const string sql = "SELECT name FROM t ORDER BY length(name)";

        List<QueryResultRow> inMemory = await Select(inMemoryExecutor, inMemoryDb, sql);
        List<QueryResultRow> spilled = await Select(spilledExecutor, spilledDb, sql);

        CollectionAssert.AreEqual(new[] { "a", "bb", "ccc", "dddd" },
            inMemory.Select(r => r.Row["name"].StrValue).ToArray());
        CollectionAssert.AreEqual(
            inMemory.Select(r => r.Row["name"].StrValue).ToArray(),
            spilled.Select(r => r.Row["name"].StrValue).ToArray());

        foreach (QueryResultRow row in spilled)
            Assert.IsFalse(row.Row.Keys.Any(k => k.StartsWith('~')), "carrier survived the spill round trip");
    }

    // ── The key is materialized once per row, not once per comparison ────────

    [Test]
    [NonParallelizable]
    public async Task VolatileOrdering_ProducesAConsistentPermutation()
    {
        // random() is volatile. Evaluated once per row it yields a fixed key and therefore a valid
        // permutation; evaluated inside the comparer it would return a different value on every
        // comparison, violating the comparer contract and dropping or duplicating rows.
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupNames();

        for (int attempt = 0; attempt < 5; attempt++)
        {
            List<QueryResultRow> rows = await Select(executor, db, "SELECT n FROM t ORDER BY random()");

            Assert.AreEqual(Names.Length, rows.Count, "row count must survive a volatile ordering");
            CollectionAssert.AreEquivalent(
                new[] { 0L, 1L, 2L, 3L },
                rows.Select(r => r.Row["n"].LongValue).ToArray(),
                "every row must appear exactly once");
        }
    }

    [Test]
    [NonParallelizable]
    public async Task VolatileOrdering_BypassesTheResultCache()
    {
        // A cached volatile ordering would freeze one permutation forever and replay it as though it
        // had been recomputed. The ordering AST is now part of what the cache eligibility check reads,
        // so the query reports itself as bypassed for non-determinism rather than being stored.
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupNames();

        CacheMetadataHolder volatileMeta = new();
        (_, IAsyncEnumerable<QueryResultRow> volatileCursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), db.Name,
                "SELECT n FROM t {cache=vec} ORDER BY random()", null), volatileMeta);
        _ = await volatileCursor.ToListAsync();

        Assert.AreEqual(QueryCacheBypassReason.NonDeterministic, volatileMeta.BypassReason);

        // A deterministic ordering expression must stay cacheable — the check has to discriminate,
        // not blanket-disable the cache for every computed key.
        CacheMetadataHolder stableMeta = new();
        (_, IAsyncEnumerable<QueryResultRow> stableCursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), db.Name,
                "SELECT n FROM t {cache=vec} ORDER BY length(name)", null), stableMeta);
        _ = await stableCursor.ToListAsync();

        Assert.AreNotEqual(QueryCacheBypassReason.NonDeterministic, stableMeta.BypassReason);
    }
}
