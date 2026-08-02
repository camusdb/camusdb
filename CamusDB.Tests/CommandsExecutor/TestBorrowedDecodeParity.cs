
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
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
/// Proves the borrowed (zero-copy) decode path (<see cref="CamusDBOptions.BorrowedDecode"/> forced on)
/// produces results identical to the eager path across representative query shapes, end to end through
/// the real pipeline. The flag ships off by default (a selectivity-dependent perf trade pending Phase 6
/// benchmarks), so without this test the borrowed path would never be exercised by the suite. It also
/// guards the borrowed-view lifetime: ORDER BY / GROUP BY / JOIN retain decoded rows across scan
/// iterations, so if a <c>RowView</c>'s backing bytes were reused or freed early, these shapes would
/// diverge from the eager path.
/// </summary>
// Serial: shares one embedded Kahuna node across the fixture, so concurrent fixtures would
// interleave transactions and database names on the same node.
[NonParallelizable]
public sealed class TestBorrowedDecodeParity : SharedNodeBaseTest
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

    private static void AssertRowsEqual(List<object?[]> eager, List<object?[]> borrowed)
    {
        Assert.AreEqual(eager.Count, borrowed.Count, "row count");
        for (int r = 0; r < eager.Count; r++)
            CollectionAssert.AreEqual(eager[r], borrowed[r], $"row {r}");
    }

    /// <summary>
    /// Runs <paramref name="sql"/> through two engines that differ only in the decode path, and requires
    /// identical rows. Each engine forces its policy explicitly: ForceEager rather than the default
    /// Adaptive matters, because under Adaptive the scanner would turn borrowed decode on for these
    /// filtered queries and the "eager" arm would silently be borrowed-vs-borrowed. ForceBorrowed
    /// likewise exercises borrowing on every shape, including the row-retaining ones.
    /// </summary>
    private static async Task AssertParity(
        CommandExecutor eagerEngine, DatabaseDescriptor eagerDb,
        CommandExecutor borrowedEngine, DatabaseDescriptor borrowedDb,
        string dbname, string sql)
    {
        List<object?[]> eager    = await RunAsync(eagerEngine,    eagerDb,    dbname, sql);
        List<object?[]> borrowed = await RunAsync(borrowedEngine, borrowedDb, dbname, sql);

        AssertRowsEqual(eager, borrowed);
    }

    [Test]
    public async Task BorrowedPath_MatchesEager_AcrossQueryShapes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor)
            = await CreateDatabase(Options with { BorrowedDecode = BorrowedDecodePolicy.ForceEager });

        KvTransaction ddl = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, dbname,
            "CREATE TABLE t (id INT64 NOT NULL, cat STRING NOT NULL, val INT64 NOT NULL, score FLOAT64, PRIMARY KEY (id))", null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(ddl, dbname,
            "CREATE TABLE u (id INT64 NOT NULL, tid INT64 NOT NULL, note STRING, PRIMARY KEY (id))", null));
        await database.Transactions.CommitAsync(ddl);

        KvTransaction ins = await database.Transactions.BeginAsync();
        for (int i = 0; i < 40; i++)
        {
            string score = (i + 0.5).ToString(System.Globalization.CultureInfo.InvariantCulture);
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(ins, dbname,
                $"INSERT INTO t (id, cat, val, score) VALUES ({i}, \"c{i % 4}\", {i * 3}, {score})", null));
        }
        for (int i = 0; i < 20; i++)
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(ins, dbname,
                $"INSERT INTO u (id, tid, note) VALUES ({i}, {i}, \"n{i}\")", null));
        // A NULL note so the string-equality fast path's NULL fallback is exercised.
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(ins, dbname,
            "INSERT INTO u (id, tid, note) VALUES (99, 99, NULL)", null));
        await database.Transactions.CommitAsync(ins);

        // A second engine over the same data, differing only in the decode path under test.
        CommandExecutor borrowedEngine = CreateCommandExecutor(
            Options with { BorrowedDecode = BorrowedDecodePolicy.ForceBorrowed });
        DatabaseDescriptor borrowedDb = await borrowedEngine.OpenDatabase(dbname);

        Task Parity(string sql) => AssertParity(executor, database, borrowedEngine, borrowedDb, dbname, sql);

        // Scan + projection, SELECT *, selective filter, ORDER BY, GROUP BY, DISTINCT, and a join —
        // the last four retain decoded rows across scan iterations (borrowed-view lifetime coverage).
        await Parity("SELECT id, cat, val FROM t");
        await Parity("SELECT * FROM t");
        await Parity("SELECT id, val FROM t WHERE val < 30");
        // String-equality fast path (byte-native compare on the borrowed path): equality, inequality,
        // a literal that matches nothing, and a NULL cell that must not equal any literal.
        await Parity("SELECT id, cat FROM t WHERE cat = \"c1\"");
        await Parity("SELECT id, cat FROM t WHERE cat <> \"c1\"");
        await Parity("SELECT id FROM t WHERE cat = \"nope\"");
        await Parity("SELECT id, note FROM u WHERE note = \"n5\"");
        await Parity("SELECT id FROM u WHERE note <> \"n5\"");
        await Parity("SELECT id, val FROM t ORDER BY val DESC");
        await Parity("SELECT cat, COUNT(*), SUM(val) FROM t GROUP BY cat");
        await Parity("SELECT DISTINCT cat FROM t");
        await Parity("SELECT t.id, t.cat, u.note FROM t JOIN u ON t.id = u.tid");
    }
}
