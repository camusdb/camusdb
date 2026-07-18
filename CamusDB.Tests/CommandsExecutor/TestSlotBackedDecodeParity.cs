
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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Proves the slot-backed decode path (<see cref="CamusDBConfig.SlotBackedDecode"/> == true) produces
/// results identical to the eager path across representative query shapes. The flag ships off by default
/// (a selectivity-dependent perf trade), so without this test the slot path would never be exercised;
/// here every shape is run under both flag states and compared cell-for-cell.
/// </summary>
[NonParallelizable]
public sealed class TestSlotBackedDecodeParity : SharedNodeBaseTest
{
    private bool _original;

    [SetUp]
    public void SaveFlag() => _original = CamusDBConfig.SlotBackedDecode;

    [TearDown]
    public void RestoreFlag() => CamusDBConfig.SlotBackedDecode = _original;

    private static async Task<List<object?[]>> RunAsync(
        CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        QuerySchemaHolder schemaHolder = new();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket, schemaOut: schemaHolder);
        List<object?[]> encoded = [];
        await foreach (QueryResultRow row in cursor)
            encoded.Add(CompactRowEncoder.EncodeRow(row.Row, schemaHolder.Schema));
        await database.Transactions.CommitAsync(tx);
        return encoded;
    }

    private static void AssertRowsEqual(List<object?[]> eager, List<object?[]> slot)
    {
        Assert.AreEqual(eager.Count, slot.Count, "row count");
        for (int r = 0; r < eager.Count; r++)
            CollectionAssert.AreEqual(eager[r], slot[r], $"row {r}");
    }

    private async Task AssertParity(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        CamusDBConfig.SlotBackedDecode = false;
        List<object?[]> eager = await RunAsync(executor, database, dbname, sql);

        CamusDBConfig.SlotBackedDecode = true;
        List<object?[]> slot = await RunAsync(executor, database, dbname, sql);

        AssertRowsEqual(eager, slot);
    }

    [Test]
    public async Task SlotPath_MatchesEager_AcrossQueryShapes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, dbname,
            "CREATE TABLE t (id INT64 NOT NULL, cat STRING NOT NULL, val INT64 NOT NULL, score FLOAT64, PRIMARY KEY (id))", null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, dbname,
            "CREATE TABLE u (id INT64 NOT NULL, tid INT64 NOT NULL, note STRING, PRIMARY KEY (id))", null));
        await database.Transactions.CommitAsync(ddl);

        KvTransaction ins = await database.Transactions.BeginAsync();
        for (int i = 0; i < 40; i++)
        {
            // Invariant formatting so the decimal separator is '.' regardless of the test host locale.
            string score = (i + 0.5).ToString(System.Globalization.CultureInfo.InvariantCulture);
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(ins, dbname,
                $"INSERT INTO t (id, cat, val, score) VALUES ({i}, \"c{i % 4}\", {i * 3}, {score})", null));
        }
        for (int i = 0; i < 20; i++)
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(ins, dbname,
                $"INSERT INTO u (id, tid, note) VALUES ({i}, {i}, \"n{i}\")", null));
        await database.Transactions.CommitAsync(ins);

        // Scan + projection, SELECT *, selective filter, ORDER BY, GROUP BY, DISTINCT, and a join.
        await AssertParity(executor, database, dbname, "SELECT id, cat, val FROM t");
        await AssertParity(executor, database, dbname, "SELECT * FROM t");
        await AssertParity(executor, database, dbname, "SELECT id, val FROM t WHERE val < 30");
        await AssertParity(executor, database, dbname, "SELECT id, val FROM t ORDER BY val DESC");
        await AssertParity(executor, database, dbname, "SELECT cat, COUNT(*), SUM(val) FROM t GROUP BY cat");
        await AssertParity(executor, database, dbname, "SELECT DISTINCT cat FROM t");
        await AssertParity(executor, database, dbname, "SELECT t.id, t.cat, u.note FROM t JOIN u ON t.id = u.tid");
    }
}
