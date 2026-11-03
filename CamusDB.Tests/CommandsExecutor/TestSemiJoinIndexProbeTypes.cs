/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Tests.SqlFuzz;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// An IN or NOT IN subquery whose inner column has a non-unique index runs as a semi-join or an
/// anti-join that probes that index for each outer value. The probe compares the value it read back
/// with the outer value. That comparison knew INT64, STRING, OID, FLOAT64 and BOOL only, and every
/// other type compared as "not equal", so every probe of a NUMERIC, DATE, DATETIME, FLOAT32 or BYTES
/// index missed: <c>x IN (SELECT …)</c> lost its matches and <c>x NOT IN (SELECT …)</c> kept them. The
/// SQL fuzzer found it through a DELETE whose WHERE selected one row in a SELECT and none in the DELETE.
/// Without the index, the subquery result becomes a list of literals, and a DATE or DATETIME value had
/// no literal form there, so the statement failed with the internal-error code. Each case is checked
/// with the index and without it, and both must give the expected rows.
/// </summary>
[TestFixture]
internal sealed class TestSemiJoinIndexProbeTypes : BaseTest
{
    private static IEnumerable<TestCaseData> Types()
    {
        yield return new TestCaseData("numeric", "CAST('0.5' AS numeric)", "CAST('7.25' AS numeric)").SetName("Numeric");
        yield return new TestCaseData("date", "CAST('2024-01-01' AS date)", "CAST('2024-02-29' AS date)").SetName("Date");
        yield return new TestCaseData("datetime", "CAST('2024-01-01T10:00:00Z' AS datetime)", "CAST('2024-02-29T00:00:00Z' AS datetime)").SetName("DateTime");
        yield return new TestCaseData("float32", "CAST(1.5 AS float32)", "CAST(2.5 AS float32)").SetName("Float32");
        yield return new TestCaseData("bytes", "X'01ff'", "X'02'").SetName("Bytes");
        yield return new TestCaseData("int64", "5", "7").SetName("Int64");
    }

    [Test]
    [NonParallelizable]
    [TestCaseSource(nameof(Types))]
    public async Task InAndNotInSubqueriesMatchThroughAnIndex(string type, string matched, string unmatched)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        SqlFuzzReplay db = new(executor, database, dbname);

        await db.Ddl($"CREATE TABLE outer_rows (id int64 PRIMARY KEY, v {type})");
        await db.Ddl($"CREATE TABLE inner_rows (id int64 PRIMARY KEY, v {type})");
        await db.Write($"INSERT INTO outer_rows (id, v) VALUES (1, {matched}), (2, {unmatched}), (3, NULL)");
        await db.Write($"INSERT INTO inner_rows (id, v) VALUES (10, {matched})");

        await AssertRows(db);

        await db.Ddl("CREATE INDEX inner_rows_v ON inner_rows (v)");
        await AssertRows(db);

        // The same table on both sides, with the index, is the shape the fuzzer found.
        await db.Ddl("CREATE INDEX outer_rows_v ON outer_rows (v)");
        Assert.That(await db.Rows("SELECT id FROM outer_rows WHERE v IN (SELECT o.v FROM outer_rows AS o)"),
            Is.EqualTo(new List<string> { "i:1", "i:2" }), "IN over the table itself");
        Assert.That(await db.Rows("SELECT id FROM outer_rows WHERE v NOT IN (SELECT o.v FROM outer_rows AS o)"),
            Is.Empty, "NOT IN over the table itself, whose subquery holds a NULL");
    }

    private static async Task AssertRows(SqlFuzzReplay db)
    {
        Assert.That(await db.Rows("SELECT id FROM outer_rows WHERE v IN (SELECT i.v FROM inner_rows AS i)"),
            Is.EqualTo(new List<string> { "i:1" }), "IN");
        // Row 3 has a NULL, so NOT IN is UNKNOWN for it and the row is not returned.
        Assert.That(await db.Rows("SELECT id FROM outer_rows WHERE v NOT IN (SELECT i.v FROM inner_rows AS i)"),
            Is.EqualTo(new List<string> { "i:2" }), "NOT IN");
    }
}
