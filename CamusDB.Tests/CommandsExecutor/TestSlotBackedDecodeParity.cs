
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
/// Proves the slot-backed decode path (<see cref="CamusDBOptions.SlotBackedDecode"/> == true) produces
/// results identical to the eager path across representative query shapes. The flag ships off by default
/// (a selectivity-dependent perf trade), so without this test the slot path would never be exercised;
/// here every shape is run under both flag states and compared cell-for-cell.
/// </summary>
// Serial: shares one embedded Kahuna node across the fixture, so concurrent fixtures would
// interleave transactions and database names on the same node.
[NonParallelizable]
public sealed class TestSlotBackedDecodeParity : SharedNodeBaseTest
{
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

    /// <summary>
    /// Runs <paramref name="sql"/> through two engines that differ only in whether slot-backed decode is
    /// enabled, and requires identical rows. Two engines rather than one: a decode path is fixed when the
    /// engine is built, so a single engine could only ever exercise one of the two arms.
    /// </summary>
    private static async Task AssertParity(
        CommandExecutor eagerEngine, DatabaseDescriptor eagerDb,
        CommandExecutor slotEngine, DatabaseDescriptor slotDb,
        string dbname, string sql)
    {
        List<object?[]> eager = await RunAsync(eagerEngine, eagerDb, dbname, sql);
        List<object?[]> slot  = await RunAsync(slotEngine,  slotDb,  dbname, sql);

        AssertRowsEqual(eager, slot);
    }

    [Test]
    public async Task SlotPath_MatchesEager_AcrossQueryShapes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor)
            = await CreateDatabase(Options with { SlotBackedDecode = false });

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

        // A second engine over the same data, differing only in the decode path under test.
        CommandExecutor slotEngine = CreateCommandExecutor(Options with { SlotBackedDecode = true });
        DatabaseDescriptor slotDb = await slotEngine.OpenDatabase(dbname);

        Task Parity(string sql) => AssertParity(executor, database, slotEngine, slotDb, dbname, sql);

        // Scan + projection, SELECT *, selective filter, ORDER BY, GROUP BY, DISTINCT, and a join.
        await Parity("SELECT id, cat, val FROM t");
        await Parity("SELECT * FROM t");
        await Parity("SELECT id, val FROM t WHERE val < 30");
        await Parity("SELECT id, val FROM t ORDER BY val DESC");
        await Parity("SELECT cat, COUNT(*), SUM(val) FROM t GROUP BY cat");
        await Parity("SELECT DISTINCT cat FROM t");
        await Parity("SELECT t.id, t.cat, u.note FROM t JOIN u ON t.id = u.tid");
    }
}
