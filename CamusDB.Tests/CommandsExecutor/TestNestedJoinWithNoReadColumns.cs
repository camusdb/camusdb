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
/// A join whose left input is itself a join qualifies each left row with the alias it finds in that
/// row. When the query reads no column of the inner join, projection pushdown keeps no cell, so the
/// row holds no key to find an alias in, and the join failed with the internal-error code
/// <c>CADB0099</c> ("Could not resolve alias for nested join left row"). The SQL fuzzer found both shapes
/// below: three CROSS joins of a one-row table, and a padded RIGHT JOIN under a CROSS JOIN.
/// </summary>
[TestFixture]
internal sealed class TestNestedJoinWithNoReadColumns : BaseTest
{
    [Test]
    [NonParallelizable]
    public async Task ThreeCrossJoinsThatReadOnlyTheLastTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        SqlFuzzReplay db = new(executor, database, dbname);

        await db.Ddl("CREATE TABLE t0 (c0 int64 PRIMARY KEY)");
        await db.Write("INSERT INTO t0 (c0) VALUES (1), (2)");

        Assert.That(await db.Rows("SELECT a2.c0 FROM t0 AS a0 CROSS JOIN t0 AS a1 CROSS JOIN t0 AS a2"),
            Is.EqualTo(new List<string> { "i:1", "i:1", "i:1", "i:1", "i:2", "i:2", "i:2", "i:2" }));
        Assert.That(await db.Rows("SELECT COUNT(*) FROM t0 AS a0 CROSS JOIN t0 AS a1 CROSS JOIN t0 AS a2"),
            Is.EqualTo(new List<string> { "i:8" }));
    }

    [Test]
    [NonParallelizable]
    public async Task APaddedRightJoinUnderACrossJoin()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        SqlFuzzReplay db = new(executor, database, dbname);

        await db.Ddl("CREATE TABLE t0 (c0 string PRIMARY KEY)");
        await db.Ddl("CREATE TABLE t1 (c0 int64 PRIMARY KEY, c4 int64)");
        await db.Write("INSERT INTO t0 (c0) VALUES ('k1')");
        await db.Write("INSERT INTO t1 (c0, c4) VALUES (1, 5), (2, 5), (3, NULL)");

        // t0 RIGHT JOIN t0 ON FALSE gives one padded row, so each t1 row appears once.
        Assert.That(await db.Rows("SELECT a2.c4 AS g0, SUM(a2.c4) AS x0 FROM t0 AS a0 RIGHT JOIN t0 AS a1 ON FALSE CROSS JOIN t1 AS a2 GROUP BY a2.c4"),
            Is.EqualTo(new List<string> { "NULL␟NULL", "i:5␟i:10" }));
    }
}
