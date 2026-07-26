
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
/// Proves the borrowed (zero-copy) decode path (<see cref="CamusDBConfig.BorrowedDecode"/> == true)
/// produces results identical to the eager path across representative query shapes, end to end through
/// the real pipeline. The flag ships off by default (a selectivity-dependent perf trade pending Phase 6
/// benchmarks), so without this test the borrowed path would never be exercised by the suite. It also
/// guards the borrowed-view lifetime: ORDER BY / GROUP BY / JOIN retain decoded rows across scan
/// iterations, so if a <c>RowView</c>'s backing bytes were reused or freed early, these shapes would
/// diverge from the eager path.
/// </summary>
[NonParallelizable]
public sealed class TestBorrowedDecodeParity : SharedNodeBaseTest
{
    private BorrowedDecodePolicy _original;

    [SetUp]
    public void SaveFlag() => _original = CamusDBConfig.BorrowedDecode;

    [TearDown]
    public void RestoreFlag() => CamusDBConfig.BorrowedDecode = _original;

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

    private async Task AssertParity(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        // Force each path explicitly. ForceEager (not the default Adaptive) is essential: under Adaptive
        // the scanner would enable borrowed decode for these filtered queries, so the "eager" arm would
        // actually run borrowed and the comparison would be borrowed-vs-borrowed. ForceBorrowed likewise
        // exercises borrowed on every shape, including the retaining ones.
        CamusDBConfig.BorrowedDecode = BorrowedDecodePolicy.ForceEager;
        List<object?[]> eager = await RunAsync(executor, database, dbname, sql);

        CamusDBConfig.BorrowedDecode = BorrowedDecodePolicy.ForceBorrowed;
        List<object?[]> borrowed = await RunAsync(executor, database, dbname, sql);

        AssertRowsEqual(eager, borrowed);
    }

    [Test]
    public async Task BorrowedPath_MatchesEager_AcrossQueryShapes()
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

        // Scan + projection, SELECT *, selective filter, ORDER BY, GROUP BY, DISTINCT, and a join —
        // the last four retain decoded rows across scan iterations (borrowed-view lifetime coverage).
        await AssertParity(executor, database, dbname, "SELECT id, cat, val FROM t");
        await AssertParity(executor, database, dbname, "SELECT * FROM t");
        await AssertParity(executor, database, dbname, "SELECT id, val FROM t WHERE val < 30");
        // String-equality fast path (byte-native compare on the borrowed path): equality, inequality,
        // a literal that matches nothing, and a NULL cell that must not equal any literal.
        await AssertParity(executor, database, dbname, "SELECT id, cat FROM t WHERE cat = \"c1\"");
        await AssertParity(executor, database, dbname, "SELECT id, cat FROM t WHERE cat <> \"c1\"");
        await AssertParity(executor, database, dbname, "SELECT id FROM t WHERE cat = \"nope\"");
        await AssertParity(executor, database, dbname, "SELECT id, note FROM u WHERE note = \"n5\"");
        await AssertParity(executor, database, dbname, "SELECT id FROM u WHERE note <> \"n5\"");
        await AssertParity(executor, database, dbname, "SELECT id, val FROM t ORDER BY val DESC");
        await AssertParity(executor, database, dbname, "SELECT cat, COUNT(*), SUM(val) FROM t GROUP BY cat");
        await AssertParity(executor, database, dbname, "SELECT DISTINCT cat FROM t");
        await AssertParity(executor, database, dbname, "SELECT t.id, t.cat, u.note FROM t JOIN u ON t.id = u.tid");
    }
}
