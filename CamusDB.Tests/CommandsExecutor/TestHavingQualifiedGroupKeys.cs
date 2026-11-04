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
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A HAVING predicate names a group key the way the GROUP BY and the select list name it, with its
/// alias qualifier (<c>HAVING a0.c1 &gt; 1</c>). The binder resolved such a name to the select item
/// it stands for, but the aggregate workspace stored the group's cell under the item's output name
/// only, and the evaluator read the cell by the name's own text. So the query failed with
/// <see cref="CamusDBErrorCodes.UnknownColumn"/> whenever the two differed: an alias
/// (<c>a0.c1 AS g0</c>), or the bare row key of a qualified reference (<c>a0.c1</c> keyed as
/// <c>c1</c>). The workspace now stores a cell under the name's own text as well, so every reader of
/// the predicate finds it. IN and NOT IN are also accepted in HAVING. These tests drive the shapes
/// through SQL and check the rows.
/// </summary>
public sealed class TestHavingQualifiedGroupKeys : SharedNodeBaseTest
{
    private const string Grouped = "SELECT a0.c1 AS g0, COUNT(*) AS x0 FROM t0 AS a0 GROUP BY a0.c1";

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Ddl(database, executor, dbname, "CREATE TABLE t0 (c0 int64 PRIMARY KEY, c1 int64, c2 string)");
        await Ddl(database, executor, dbname, "CREATE TABLE t1 (c0 int64 PRIMARY KEY, c1 int64)");

        // Groups by c1: 1 (one row), 2 (two rows), 3 (one row) and NULL (one row).
        await Write(database, executor, dbname, "INSERT INTO t0 (c0, c1, c2) VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 2, 'c'), (4, NULL, 'd'), (5, 3, 'e')");
        await Write(database, executor, dbname, "INSERT INTO t1 (c0, c1) VALUES (1, 10), (2, 20), (3, 30)");

        return (dbname, database, executor);
    }

    private static async Task Ddl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task Write(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    /// <summary>The rows of <paramref name="sql"/> as "a|b" text ("NULL" for a NULL), in the order the query returned them.</summary>
    private static async Task<List<string>> RowsInOrder(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows.Select(r => string.Join("|", r.Row.Values.Select(Text))).ToList();
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    /// <summary>The rows of <paramref name="sql"/>, sorted, so a result compares without an ORDER BY.</summary>
    private static async Task<List<string>> Rows(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        List<string> rows = await RowsInOrder(database, executor, dbname, sql);
        return rows.OrderBy(x => x, System.StringComparer.Ordinal).ToList();
    }

    private static string Text(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "NULL",
        ColumnType.Bool => value.BoolValue ? "true" : "false",
        ColumnType.Integer64 => value.LongValue.ToString(),
        ColumnType.String => value.StrValue ?? "",
        _ => value.ToString() ?? "",
    };

    [Test]
    public async Task QualifiedGroupKeyUnderAnAlias_FiltersTheGroups()
    {
        // The query of the fuzzer finding: the group key is projected as g0, and HAVING names it a0.c1.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<string> rows = await Rows(database, executor, dbname, Grouped + " HAVING (a0.c1 > 1)");

        Assert.That(rows, Is.EqualTo(new List<string> { "2|2", "3|1" }));
    }

    [Test]
    public async Task QualifiedGroupKeyProjectedUnderItsBareName_FiltersTheGroups()
    {
        // Without an alias the row key of a0.c1 is the bare name c1, which also differs from the text of the reference.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<string> rows = await Rows(database, executor, dbname, "SELECT a0.c1, COUNT(*) FROM t0 AS a0 GROUP BY a0.c1 HAVING a0.c1 > 1");

        Assert.That(rows, Is.EqualTo(new List<string> { "2|2", "3|1" }));
    }

    [Test]
    public async Task QualifiedGroupKeyNotProjected_FiltersTheGroups()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<string> rows = await Rows(database, executor, dbname, "SELECT COUNT(*) AS n FROM t0 AS a0 GROUP BY a0.c1 HAVING a0.c1 > 1");

        Assert.That(rows, Is.EqualTo(new List<string> { "1", "2" }));
    }

    [Test]
    public async Task QualifiedGroupKeyInsideArithmeticAndAFunction_FiltersTheGroups()
    {
        // The reference sits below nodes the HAVING evaluator hands to the general expression
        // evaluator, which reads the cell by the same text.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<string> rows = await Rows(database, executor, dbname, Grouped + " HAVING abs(a0.c1 - 3) < 1 OR a0.c1 * 10 = 10");

        Assert.That(rows, Is.EqualTo(new List<string> { "1|1", "3|1" }));
    }

    [Test]
    public async Task QualifiedGroupKeyWithAnAggregate_FiltersTheGroups()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<string> rows = await Rows(database, executor, dbname, Grouped + " HAVING a0.c1 > 1 AND COUNT(*) > 1");

        Assert.That(rows, Is.EqualTo(new List<string> { "2|2" }));
    }

    [Test]
    public async Task QualifiedGroupKeyOverAJoin_FiltersTheGroups()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<string> rows = await Rows(
            database, executor, dbname,
            "SELECT a0.c1 AS g0, SUM(a1.c1) AS s FROM t0 AS a0 INNER JOIN t1 AS a1 ON a0.c0 = a1.c0 GROUP BY a0.c1 HAVING a0.c1 >= 2");

        Assert.That(rows, Is.EqualTo(new List<string> { "2|50" }));
    }

    [Test]
    public async Task InListInHaving_OnTheGroupKeyAndOnAnAggregate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        Assert.That(await Rows(database, executor, dbname, Grouped + " HAVING a0.c1 IN (1, 3)"), Is.EqualTo(new List<string> { "1|1", "3|1" }));
        Assert.That(await Rows(database, executor, dbname, Grouped + " HAVING COUNT(*) NOT IN (1)"), Is.EqualTo(new List<string> { "2|2" }));

        // The shape the TLP oracle writes: the NULL group is the one whose membership is UNKNOWN.
        Assert.That(await Rows(database, executor, dbname, Grouped + " HAVING (a0.c1 IN (1, 3)) IS NULL"), Is.EqualTo(new List<string> { "NULL|1" }));

        // A NULL item makes every non-match UNKNOWN, so NOT IN keeps no group.
        Assert.That(await Rows(database, executor, dbname, Grouped + " HAVING a0.c1 NOT IN (1, NULL)"), Is.Empty);
    }

    [Test]
    public async Task QualifiedGroupKeyInOrderByAndHaving_SortsTheFilteredGroups()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<string> rows = await RowsInOrder(database, executor, dbname, Grouped + " HAVING a0.c1 > 1 ORDER BY a0.c1 DESC");

        Assert.That(rows, Is.EqualTo(new List<string> { "3|1", "2|2" }));
    }

    [Test]
    public async Task AliasOfAnAggregateWithoutGroupBy_StillResolves()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        List<string> rows = await Rows(database, executor, dbname, "SELECT COUNT(*) AS n FROM t0 AS a0 HAVING n > 1");

        Assert.That(rows, Is.EqualTo(new List<string> { "5" }));
    }

    [Test]
    public async Task NameThatIsNeitherAGroupKeyNorAProjection_IsStillRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        foreach (string having in new[] { "a9.c1 > 1", "a0.c2 > 'a'" })
        {
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(() => Rows(database, executor, dbname, Grouped + " HAVING " + having));
            Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput), having);
            Assert.That(ex.Message, Does.Contain("HAVING must reference"), having);
        }
    }
}
