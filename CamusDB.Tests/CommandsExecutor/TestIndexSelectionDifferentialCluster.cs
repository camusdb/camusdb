/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The composite-unique-index cases from <see cref="TestIndexSelectionDifferential"/> on a
/// clustered engine. The planner is the same, but a clustered node backfills a new unique index
/// through the coordinator-driven path rather than the standalone one, and the rule that a
/// NULL-keyed row carries no unique entry is enforced separately in each. The standalone fixture
/// alone cannot prove the cluster backfill omits the same rows the planner must account for.
/// </summary>
[NonParallelizable]
public class TestIndexSelectionDifferentialCluster : SharedNodeBaseTest
{
    private async Task RunCase(string query, string indexDdl, string expected, bool extraRowInGroupOne = false)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await IndexDifferentialProbe.CreateFixture(executor, database, dbname, extraRowInGroupOne);
        await IndexDifferentialProbe.AssertSameRowsBeforeAndAfterIndex(executor, database, dbname, query, indexDdl, expected);
    }

    [Test]
    public async Task EqualityPrefix_UniqueCompositeWithNullableTrailing_KeepsNullRow()
    {
        await RunCase("SELECT a FROM probe WHERE a = 1", "CREATE UNIQUE INDEX ab ON probe (a, b)", "a=1");
    }

    [Test]
    public async Task StreamingDistinct_UniqueCompositeWithNullableTrailing_KeepsAllValues()
    {
        await RunCase("SELECT DISTINCT a FROM probe", "CREATE UNIQUE INDEX ab ON probe (a, b)", "a=1;a=2");
    }

    [Test]
    public async Task StreamingGroupBy_UniqueCompositeWithNullableTrailing_KeepsGroupsAndCounts()
    {
        await RunCase(
            "SELECT a, count(*) AS n FROM probe GROUP BY a",
            "CREATE UNIQUE INDEX ab ON probe (a, b)",
            "a=1,n=2;a=2,n=1",
            extraRowInGroupOne: true);
    }

    [Test]
    public async Task FloatLiteral_EqualityOnIntColumn_UniqueIndex_KeepsRow()
    {
        await RunCase("SELECT a FROM probe WHERE a = 1.0", "CREATE UNIQUE INDEX ai ON probe (a)", "a=1");
    }
}
