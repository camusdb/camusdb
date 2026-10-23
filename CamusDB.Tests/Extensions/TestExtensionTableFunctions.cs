/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Extensions;
using CamusDB.Extensions;

namespace CamusDB.Tests.Extensions;

/// <summary>
/// Table-valued extension functions in <c>FROM</c>: the named column, joins on both sides, LIMIT and
/// WHERE, disposal of the user enumerator when the scan stops early or fails, the per-statement spool
/// of a volatile function, EXPLAIN's leaf estimate, the boot refusals, and the per-database scope.
/// </summary>
[NonParallelizable]
public sealed class TestExtensionTableFunctions : ExtensionTestBase
{
    private static async Task<List<string>> Words(CommandExecutor executor, string db, string sql, string column = "word") =>
        (await ExecQuery(executor, db, sql)).Select(r => r[column].StrValue!).ToList();

    private static async Task<List<long>> Longs(CommandExecutor executor, string db, string sql, string column) =>
        (await ExecQuery(executor, db, sql)).Select(r => r[column].LongValue).ToList();

    [Test]
    public async Task InFrom_NamedColumn_Alias_Where_AndLimit()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();

        CollectionAssert.AreEqual(new[] { "a", "bb", "ccc" }, await Words(executor, db, "SELECT word FROM rx_split('a bb ccc')"),
            "Column = \"word\" names the output column");

        CollectionAssert.AreEqual(new[] { "a", "ccc" }, await Words(executor, db, "SELECT word FROM rx_split('a bb ccc') WHERE word <> 'bb'"));

        CollectionAssert.AreEqual(new[] { "bb" }, await Words(executor, db, "SELECT w.word FROM rx_split('a bb') AS w WHERE w.word = 'bb'"));

        CollectionAssert.AreEqual(new[] { "x", "y" }, await Words(executor, db, "SELECT t FROM rx_split('x y') AS s(t)", column: "t"),
            "a column alias renames the column");

        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, await Longs(executor, db, "SELECT rx_count_to FROM rx_count_to(3)", "rx_count_to"),
            "without Column, the function name names the column");

        CollectionAssert.AreEqual(new long[] { 3, 2 }, await Longs(executor, db, "SELECT n FROM rx_count_to(5) AS n WHERE n < 4 ORDER BY n DESC LIMIT 2", "n"));

        Assert.AreEqual(0, (await ExecQuery(executor, db, "SELECT rx_count_to FROM rx_count_to(NULL)")).Count,
            "a NULL argument for a non-nullable parameter gives zero rows");

        Assert.AreEqual(0, (await ExecQuery(executor, db, "SELECT rx_maybe FROM rx_maybe(NULL)")).Count, "a null enumerable is zero rows");

        List<Dictionary<string, ColumnValue>> withNulls = await ExecQuery(executor, db, "SELECT rx_maybe FROM rx_maybe('nulls')");
        Assert.AreEqual(3, withNulls.Count);
        Assert.AreEqual(ColumnType.Null, withNulls[1]["rx_maybe"].Type, "a null element is a NULL row value");

        Dictionary<string, ColumnValue> list = (await ExecQuery(executor, db, "SELECT rx_lists FROM rx_lists(2)")).Last();
        CollectionAssert.AreEqual(new long[] { 2, 20 }, list["rx_lists"].ArrayValues!.Select(v => v.LongValue), "an array element type yields array rows");
    }

    [Test]
    public async Task Joins_OnBothSides()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();

        await ExecDdl(executor, db, "CREATE TABLE dict (id INT64 PRIMARY KEY, term STRING, weight INT64)");
        await ExecNonQuery(executor, db, "INSERT INTO dict (id, term, weight) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'zz', 30)");

        List<Dictionary<string, ColumnValue>> right = await ExecQuery(executor, db,
            "SELECT w.word AS word, d.weight AS weight FROM rx_split('b a q') AS w JOIN dict d ON d.term = w.word ORDER BY d.weight");
        CollectionAssert.AreEqual(new long[] { 10, 20 }, right.Select(r => r["weight"].LongValue));

        List<Dictionary<string, ColumnValue>> left = await ExecQuery(executor, db,
            "SELECT d.term AS term, w.word AS word FROM dict d JOIN rx_split('zz a') AS w ON w.word = d.term ORDER BY d.term");
        CollectionAssert.AreEqual(new[] { "a", "zz" }, left.Select(r => r["word"].StrValue));

        Assert.AreEqual(6, (await Scalar(executor, db, "SELECT COUNT(*) AS c FROM dict CROSS JOIN rx_count_to(2) n")).LongValue);

        List<Dictionary<string, ColumnValue>> both = await ExecQuery(executor, db,
            "SELECT a.word AS word FROM rx_split('p q r') AS a JOIN rx_split('q r s') AS b ON a.word = b.word ORDER BY a.word");
        CollectionAssert.AreEqual(new[] { "q", "r" }, both.Select(r => r["word"].StrValue));
    }

    [Test]
    public async Task Enumerator_IsDisposed_OnLimit_AndOnError()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();

        long before = (await Scalar(executor, db, "SELECT rx_disposals()")).LongValue;

        // rx_tracked never ends on its own: only the LIMIT stops it, and stopping must dispose it.
        CollectionAssert.AreEqual(new long[] { 5, 6, 7 }, await Longs(executor, db, "SELECT n FROM rx_tracked(5) AS n LIMIT 3", "n"));
        Assert.AreEqual(before + 1, (await Scalar(executor, db, "SELECT rx_disposals()")).LongValue, "the iterator's finally ran on LIMIT");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => ExecQuery(executor, db, "SELECT n FROM rx_fail_after(2) AS n"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionFunctionFailed, ex.Code);
        StringAssert.Contains("rx_fail_after", ex.Message, "the error names the function");
        StringAssert.Contains("reached 2", ex.Message);
        Assert.AreEqual(before + 2, (await Scalar(executor, db, "SELECT rx_disposals()")).LongValue, "the iterator's finally ran on the error");
    }

    [Test]
    public async Task Volatile_InnerSideOfANestedLoop_SeesTheSameRowsOnEachReRead()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();

        await ExecDdl(executor, db, "CREATE TABLE outer_rows (id INT64 PRIMARY KEY)");
        await ExecNonQuery(executor, db, "INSERT INTO outer_rows (id) VALUES (1), (2), (3), (4)");

        List<Dictionary<string, ColumnValue>> rows = await ExecQuery(executor, db,
            "SELECT o.id AS id, v.seq AS seq FROM outer_rows o CROSS JOIN rx_volatile_seq(3) AS v(seq)");

        Assert.AreEqual(12, rows.Count);

        // Each call of rx_volatile_seq starts where the last one stopped, so a second read would give
        // three new values. One statement must see one set of three.
        List<long> values = rows.Select(r => r["seq"].LongValue).Distinct().ToList();
        Assert.AreEqual(3, values.Count, "every re-read of the volatile function returned the same rows");

        // The next statement reads the function again.
        List<long> next = await Longs(executor, db, "SELECT seq FROM rx_volatile_seq(3) AS s(seq)", "seq");
        CollectionAssert.IsEmpty(next.Intersect(values), "a new statement calls the function again");
    }

    [Test]
    public async Task Explain_ShowsTheLeafAndItsEstimate()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabaseWithRichFixture();

        Dictionary<string, ColumnValue> counted = (await ExecQuery(executor, db, "EXPLAIN SELECT n FROM rx_count_to(7) AS n"))
            .Single(r => r["node"].StrValue == "function-scan");
        StringAssert.Contains("rx_count_to(7)", counted["detail"].StrValue!);
        Assert.AreEqual(7L, counted["estimated_rows"].LongValue, "the CountRowsMethod gives the estimate");

        Dictionary<string, ColumnValue> uncounted = (await ExecQuery(executor, db, "EXPLAIN SELECT word FROM rx_split('a b')"))
            .Single(r => r["node"].StrValue == "function-scan");
        StringAssert.Contains("rx_split", uncounted["detail"].StrValue!);
        Assert.Greater(uncounted["estimated_rows"].LongValue, 0, "a function without a count method gets the default estimate");
    }

    [Test]
    public async Task Scope_OnlyWhereInstalled_AndShowExtensionsListsIt()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase(WithExtensions("Fixture.Rich"));

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => ExecQuery(executor, db, "SELECT word FROM rx_split('a')"))!;
        StringAssert.Contains("rx_split", ex.Message, "a loaded but not installed table function does not resolve");

        await ExecDdl(executor, db, "CREATE EXTENSION rich");
        CollectionAssert.AreEqual(new[] { "a" }, await Words(executor, db, "SELECT word FROM rx_split('a')"));

        Dictionary<string, ColumnValue> rich = (await ExecQuery(executor, db, "SHOW EXTENSIONS")).Single(r => r["name"].StrValue == "rich");
        StringAssert.Contains("rx_split", rich["table_functions"].StrValue);
        StringAssert.Contains("rx_split(STRING) -> SETOF STRING", rich["signatures"].StrValue);

        ExtensionDefinition record = (await executor.OpenDatabase(db)).Schema.Extensions["rich"];
        Assert.IsTrue(record.HasMember(ExtensionMemberKind.TableFunction, "rx_split"), "the installed record lists the table function");

        // A view over the table function is a dependent: DROP EXTENSION refuses.
        await ExecDdl(executor, db, "CREATE VIEW words AS SELECT word FROM rx_split('v w')");
        CollectionAssert.AreEqual(new[] { "v", "w" }, await Words(executor, db, "SELECT word FROM words"));

        CamusDBException drop = Assert.ThrowsAsync<CamusDBException>(() => ExecDdl(executor, db, "DROP EXTENSION rich"))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionHasDependents, drop.Code);
        StringAssert.Contains("view words", drop.Message);
    }

    [Test]
    public void Boot_RefusesACollisionWithABuiltInTableFunction()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() => CreateCommandExecutor(WithExtensions("Fixture.TableFunctionCollision")))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionDuplicateMember, ex.Code);
        StringAssert.Contains("generate_series", ex.Message);
    }

    public static class BadTableFunctions
    {
        [CamusTableFunction("bad_element")]
        public static IEnumerable<int> UnmappedElement(long n) => [];

        [CamusTableFunction("bad_shape")]
        public static long NotAnEnumerable(long n) => n;

        [CamusTableFunction("bad_count", CountRowsMethod = "Missing")]
        public static IEnumerable<long> MissingCount(long n) => [];
    }

    [TestCase(nameof(BadTableFunctions.UnmappedElement), "System.Int32")]
    [TestCase(nameof(BadTableFunctions.NotAnEnumerable), "IEnumerable<T>")]
    [TestCase(nameof(BadTableFunctions.MissingCount), "CountRowsMethod 'Missing'")]
    public void Binder_RefusesABadSignature_AndNamesTheMember(string methodName, string reason)
    {
        MethodInfo method = typeof(BadTableFunctions).GetMethod(methodName)!;

        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            ExtensionTableFunctionBinder.Bind(method, method.GetCustomAttribute<CamusTableFunctionAttribute>()!))!;

        Assert.AreEqual(CamusDBErrorCodes.ExtensionInvalidSignature, ex.Code);
        StringAssert.Contains(methodName, ex.Message);
        StringAssert.Contains(reason, ex.Message);
    }
}
