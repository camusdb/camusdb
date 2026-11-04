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
using CamusDB.Tests.SqlFuzz;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An IN or NOT IN subquery in a SELECT WHERE runs as a semi-join or an anti-join when the inner
/// column has an index: each outer value probes the index, and the inner WHERE is evaluated against
/// the decoded inner row. That row is keyed by bare column names, and the filter was evaluated with
/// no name scope, so a reference the subquery qualified with its own alias (<c>s.c3</c>) was an
/// unknown column, while the same statement without the index, or under an OR, ran through the
/// general path and worked. The SQL fuzzer always qualifies inner columns, so it met the refusal in
/// every indexed IN subquery with a WHERE. The filter now resolves names in the inner scope, and a
/// name that scope cannot resolve leaves the predicate to the general path, which reports it.
/// </summary>
[TestFixture]
internal sealed class TestSemiJoinInnerAliasFilter : BaseTest
{
    private sealed record Fixture(string Name, DatabaseDescriptor Database, CommandExecutor Executor, SqlFuzzReplay Db);

    private async Task<Fixture> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        SqlFuzzReplay db = new(executor, database, dbname);

        await db.Ddl("CREATE TABLE t0 (c0 int64 PRIMARY KEY, c1 bool, c3 int64)");
        await db.Write("INSERT INTO t0 (c0, c1, c3) VALUES (1, true, 10), (2, false, 20), (3, NULL, 30)");

        // c1 is nullable and has a non-unique index: the null-aware anti-join and the non-unique probe.
        await db.Ddl("CREATE TABLE t1 (c0 int64 PRIMARY KEY, c1 int64)");
        await db.Ddl("CREATE INDEX t1_c1 ON t1 (c1)");
        await db.Write("INSERT INTO t1 (c0, c1) VALUES (1, 10), (2, NULL), (3, 30), (4, 30)");

        return new Fixture(dbname, database, executor, db);
    }

    private static List<string> Keys(params long[] keys) =>
        SqlFuzzValues.Multiset(keys.Select(k => (IReadOnlyList<ColumnValue>)new[] { new ColumnValue(ColumnType.Integer64, k) }));

    private static async Task<string> Plan(SqlFuzzReplay db, string sql)
    {
        SqlFuzzRows rows = await db.Query("EXPLAIN " + sql);
        return string.Join("\n", rows.Rows.Select(r => string.Join(" ", r.Select(v => v.StrValue ?? ""))));
    }

    [Test]
    [NonParallelizable]
    public async Task TheFindingRunsThroughTheUniqueProbe()
    {
        Fixture f = await SetupAsync();
        const string sql = "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c0 IN (SELECT s4.c0 FROM t0 AS s4 WHERE s4.c3 >= 20)";

        Assert.That(await Plan(f.Db, sql), Does.Contain("semi-join"), "the primary key makes it a semi-join");
        Assert.That(await f.Db.Rows(sql), Is.EqualTo(Keys(2, 3)));
    }

    [Test]
    [NonParallelizable]
    public async Task NotInRunsThroughTheUniqueProbeAsAnAntiJoin()
    {
        Fixture f = await SetupAsync();
        const string sql = "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c0 NOT IN (SELECT s4.c0 FROM t0 AS s4 WHERE s4.c3 >= 20)";

        Assert.That(await Plan(f.Db, sql), Does.Contain("anti-join"));
        Assert.That(await f.Db.Rows(sql), Is.EqualTo(Keys(1)));
    }

    [Test]
    [NonParallelizable]
    public async Task InRunsThroughANonUniqueIndexProbe()
    {
        Fixture f = await SetupAsync();
        // Inner rows 2, 3 and 4 pass the filter, with the values NULL, 30 and 30. Only the outer row
        // with c3 = 30 matches.
        const string sql = "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c3 IN (SELECT s.c1 FROM t1 AS s WHERE s.c0 <> 1)";

        Assert.That(await Plan(f.Db, sql), Does.Contain("semi-join").And.Contain("t1_c1"));
        Assert.That(await f.Db.Rows(sql), Is.EqualTo(Keys(3)));
    }

    [Test]
    [NonParallelizable]
    public async Task NullAwareNotInEvaluatesTheFilterInItsNullScan()
    {
        Fixture f = await SetupAsync();

        // The filter removes the inner NULL (row 2), so the anti-join keeps the outer rows whose value
        // is neither 10 nor 30.
        const string withoutNull = "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c3 NOT IN (SELECT s.c1 FROM t1 AS s WHERE s.c0 <> 2)";
        Assert.That(await Plan(f.Db, withoutNull), Does.Contain("null-aware-anti-join"));
        Assert.That(await f.Db.Rows(withoutNull), Is.EqualTo(Keys(2)));

        // The filter keeps the inner NULL, so NOT IN is UNKNOWN for every outer row.
        const string withNull = "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c3 NOT IN (SELECT s.c1 FROM t1 AS s WHERE s.c0 >= 2)";
        Assert.That(await f.Db.Rows(withNull), Is.Empty);
    }

    private static IEnumerable<TestCaseData> Filters()
    {
        yield return new TestCaseData("(s4.c3 >= 20) IS FALSE", new long[] { 1 }).SetName("IsFalse");
        yield return new TestCaseData("NOT s4.c1", new long[] { 2 }).SetName("NotOnABoolColumn");
        yield return new TestCaseData("CASE WHEN s4.c1 THEN s4.c3 ELSE 0 END > 5", new long[] { 1 }).SetName("CaseOperand");
        yield return new TestCaseData("abs(s4.c3 - 25) < 10", new long[] { 2, 3 }).SetName("FunctionArgument");
        yield return new TestCaseData("s4.c3 IN (s4.c0, 20)", new long[] { 2 }).SetName("InListWithAColumnItem");
        yield return new TestCaseData("s4.c3 BETWEEN s4.c0 AND 25", new long[] { 1, 2 }).SetName("BetweenWithAColumnBound");
        yield return new TestCaseData("c3 >= 20 AND s4.c1 IS NOT NULL", new long[] { 2 }).SetName("BareAndQualifiedMixed");
        yield return new TestCaseData("S4.C3 >= 20", new long[] { 2, 3 }).SetName("AliasInAnotherCase");
    }

    [Test]
    [NonParallelizable]
    [TestCaseSource(nameof(Filters))]
    public async Task EveryFilterShapeResolvesTheInnerAlias(string filter, long[] expected)
    {
        Fixture f = await SetupAsync();
        string sql = $"SELECT a0.c0 FROM t0 AS a0 WHERE a0.c0 IN (SELECT s4.c0 FROM t0 AS s4 WHERE {filter})";

        Assert.That(await Plan(f.Db, sql), Does.Contain("semi-join"));
        Assert.That(await f.Db.Rows(sql), Is.EqualTo(Keys(expected)));
    }

    [Test]
    [NonParallelizable]
    public async Task TheTableNameQualifiesTheFilterWhenThereIsNoAlias()
    {
        Fixture f = await SetupAsync();
        const string sql = "SELECT c0 FROM t0 WHERE c0 IN (SELECT t0.c0 FROM t0 WHERE t0.c3 >= 20)";

        Assert.That(await Plan(f.Db, sql), Does.Contain("semi-join"));
        Assert.That(await f.Db.Rows(sql), Is.EqualTo(Keys(2, 3)));
    }

    [Test]
    [NonParallelizable]
    public async Task AParameterNextToTheQualifiedColumn()
    {
        Fixture f = await SetupAsync();
        const string sql = "SELECT a0.c0 FROM t0 AS a0 WHERE a0.c0 IN (SELECT s4.c0 FROM t0 AS s4 WHERE s4.c3 >= @p)";
        Dictionary<string, ColumnValue> parameters = new() { { "@p", new ColumnValue(ColumnType.Integer64, 20) } };

        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(new ExecuteSQLTicket(txn, f.Name, sql, parameters));
        List<IReadOnlyList<ColumnValue>> rows = new();
        await foreach (QueryResultRow row in cursor)
            rows.Add(row.Row.Values.ToList());
        await f.Database.Transactions.CommitAsync(txn);

        Assert.That(SqlFuzzValues.Multiset(rows), Is.EqualTo(Keys(2, 3)));
    }

    [Test]
    [NonParallelizable]
    public async Task AnUnknownInnerColumnIsRefusedWithTheCodeOfTheGeneralPath()
    {
        Fixture f = await SetupAsync();

        CamusDBException? qualified = Assert.ThrowsAsync<CamusDBException>(async () =>
            await f.Db.Rows("SELECT a0.c0 FROM t0 AS a0 WHERE a0.c0 IN (SELECT s4.c0 FROM t0 AS s4 WHERE s4.c9 >= 1)"));
        Assert.That(qualified!.Code, Is.EqualTo(CamusDBErrorCodes.UnknownColumn));
        Assert.That(qualified.Message, Does.Contain("s4.c9"));

        CamusDBException? bare = Assert.ThrowsAsync<CamusDBException>(async () =>
            await f.Db.Rows("SELECT a0.c0 FROM t0 AS a0 WHERE a0.c0 IN (SELECT s4.c0 FROM t0 AS s4 WHERE c9 >= 1)"));
        Assert.That(bare!.Code, Is.EqualTo(CamusDBErrorCodes.UnknownColumn));
    }

    [Test]
    [NonParallelizable]
    public async Task AnOuterReferenceInTheFilterIsCorrelatedAndStaysRefused()
    {
        Fixture f = await SetupAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await f.Db.Rows("SELECT a0.c0 FROM t0 AS a0 WHERE a0.c0 IN (SELECT s4.c0 FROM t0 AS s4 WHERE s4.c3 >= a0.c3)"));
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        Assert.That(ex.Message, Does.Contain("Correlated"));
    }
}
