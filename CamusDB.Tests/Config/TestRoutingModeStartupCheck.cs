/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Config;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Config;

/// <summary>
/// The rule of <see cref="RoutingModeStartupCheck.Decide"/>, without a node: the record wins over an
/// unset key, an explicit key that disagrees refuses, and a cluster without a record takes its existing
/// data's mode or, with no database, the configured value.
/// </summary>
[TestFixture]
public sealed class TestRoutingModeDecision
{
    [TestCase(true, false, false, true)]
    [TestCase(true, true, false, true)]
    [TestCase(false, true, false, false)]
    [TestCase(false, false, true, false)]
    public void Recorded_IsFollowed_WhenTheKeyIsUnsetOrAgrees(bool recorded, bool configured, bool explicitlySet, bool expected)
    {
        if (explicitlySet && configured != recorded)
            Assert.Ignore("covered by the refusal case");

        RoutingModeStartupCheck.RoutingModeDecision decision =
            RoutingModeStartupCheck.Decide(recorded, existing: null, configured, explicitlySet);

        Assert.That(decision.KeyRange, Is.EqualTo(expected));
        Assert.That(decision.Persist, Is.False, "a recorded mode is never written again");
    }

    [TestCase(true, false)]
    [TestCase(false, true)]
    public void Recorded_RefusesAnExplicitKeyThatDisagrees(bool recorded, bool configured)
    {
        CamusDBException e = Assert.Throws<CamusDBException>(
            () => RoutingModeStartupCheck.Decide(recorded, existing: null, configured, explicitlySet: true))!;

        Assert.That(e.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(e.Message, Does.Contain("was created with " + (recorded ? "key-range" : "hash")));
        Assert.That(e.Message, Does.Contain("logical-dump-and-reimport"));
    }

    [TestCase(true, false)]
    [TestCase(false, true)]
    public void Existing_IsRecorded_AndAnUnsetKeyFollowsIt(bool existing, bool configured)
    {
        RoutingModeStartupCheck.RoutingModeDecision decision =
            RoutingModeStartupCheck.Decide(recorded: null, existing, configured, explicitlySet: false);

        Assert.That(decision, Is.EqualTo(new RoutingModeStartupCheck.RoutingModeDecision(existing, Persist: true)));
    }

    [TestCase(true, false)]
    [TestCase(false, true)]
    public void Existing_RefusesAnExplicitKeyThatDisagrees(bool existing, bool configured)
    {
        CamusDBException e = Assert.Throws<CamusDBException>(
            () => RoutingModeStartupCheck.Decide(recorded: null, existing, configured, explicitlySet: true))!;

        Assert.That(e.Message, Does.Contain("holds data placed under " + (existing ? "key-range" : "hash")));
    }

    [TestCase(true, true)]
    [TestCase(false, false)]
    [TestCase(true, false)]
    [TestCase(false, true)]
    public void NewCluster_RecordsTheConfiguredValue(bool configured, bool explicitlySet)
    {
        RoutingModeStartupCheck.RoutingModeDecision decision =
            RoutingModeStartupCheck.Decide(recorded: null, existing: null, configured, explicitlySet);

        Assert.That(decision, Is.EqualTo(new RoutingModeStartupCheck.RoutingModeDecision(configured, Persist: true)));
    }
}

/// <summary>
/// <see cref="RoutingModeStartupCheck.ResolveAsync"/> against a live node: the record is written once
/// and followed afterwards, a cluster without a record is recorded from its data, concurrent first boots
/// agree, and a table stored under key-range routing cannot be opened by a hash-routed engine.
/// </summary>
[TestFixture]
// Serial: boots a per-test embedded node with two partitions for key-range routing.
[NonParallelizable]
public sealed class TestRoutingModeStartupCheck : BaseTest
{
    protected override int NodeInitialPartitions => 2;

    private Task<bool> ResolveAsync(bool configured, bool explicitlySet) =>
        RoutingModeStartupCheck.ResolveAsync(TestNode!, configured, explicitlySet, logger, CancellationToken.None);

    private void AssertRefused(bool configured, string expectedReason)
    {
        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(() => ResolveAsync(configured, explicitlySet: true))!;
        Assert.That(e.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(e.Message, Does.Contain(expectedReason));
    }

    private static async Task CreateTableAsync(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, "CREATE TABLE items (id INT64 PRIMARY KEY, v INT64)", null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        KvTransaction insert = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(insert, dbname, "INSERT INTO items (id, v) VALUES (1, 10)", null));
        await database.Transactions.CommitAsync(insert);
    }

    [Test]
    public async Task NewCluster_RecordsAnExplicitMode_ThatAnUnsetKeyThenFollows()
    {
        Assert.That(await ResolveAsync(configured: true, explicitlySet: true), Is.True);

        Assert.That(await ResolveAsync(configured: false, explicitlySet: false), Is.True,
            "an unset key follows the recorded key-range mode, whatever the compiled default");
        Assert.That(await ResolveAsync(configured: true, explicitlySet: true), Is.True, "an agreeing key starts");

        AssertRefused(configured: false, "was created with key-range");
    }

    [Test]
    public async Task NewCluster_WithTheKeyUnset_RecordsTheDefault()
    {
        Assert.That(await ResolveAsync(configured: false, explicitlySet: false), Is.False);

        Assert.That(await ResolveAsync(configured: true, explicitlySet: false), Is.False,
            "a changed default does not move a cluster created under the old one");

        AssertRefused(configured: true, "was created with hash");
    }

    [Test]
    public async Task ConcurrentFirstBoots_AgreeOnOneMode()
    {
        List<Task<bool>> boots = [];
        for (int i = 0; i < 6; i++)
            boots.Add(ResolveAsync(configured: i % 2 == 0, explicitlySet: false));

        bool[] modes = await Task.WhenAll(boots);

        Assert.That(modes.Distinct().Count(), Is.EqualTo(1), "every node runs in the one recorded mode");
        Assert.That(await ResolveAsync(configured: !modes[0], explicitlySet: false), Is.EqualTo(modes[0]));
    }

    [Test]
    public async Task ExistingHashCluster_WithoutARecord_IsRecordedAsHash()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { KeyRangeShardingEnabled = false });
        await CreateTableAsync(executor, database, dbname);

        AssertRefused(configured: true, "holds data placed under hash");

        Assert.That(await ResolveAsync(configured: true, explicitlySet: false), Is.False,
            "an unset key on a cluster with hash-routed data stays on hash routing");

        AssertRefused(configured: true, "was created with hash");
    }

    [Test]
    public async Task ExistingKeyRangeCluster_WithoutARecord_IsRecordedAsKeyRange()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { KeyRangeShardingEnabled = true });
        await CreateTableAsync(executor, database, dbname);

        AssertRefused(configured: false, "holds data placed under key-range");

        Assert.That(await ResolveAsync(configured: false, explicitlySet: false), Is.True,
            "an unset key on a cluster with key-range data stays on key-range routing");

        AssertRefused(configured: false, "was created with key-range");
    }

    /// <summary>
    /// The split state the startup check prevents on the server host, caught where a table opens: a
    /// table created under key-range routing, then opened by an engine with the flag off.
    /// </summary>
    [Test]
    public async Task HashEngine_RefusesToOpenATableStoredUnderKeyRange()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor keyRangeEngine) =
            await CreateDatabase(Options with { KeyRangeShardingEnabled = true });
        await CreateTableAsync(keyRangeEngine, database, dbname);

        CommandExecutor hashEngine = CreateCommandExecutor(Options with { KeyRangeShardingEnabled = false });
        DatabaseDescriptor hashView = await hashEngine.OpenDatabase(dbname);

        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await hashEngine.ExecuteSQLQuery(
                new ExecuteSQLTicket(hashView.Transactions.BeginReadOnly(), dbname, "SELECT v FROM items", null));
            await foreach (QueryResultRow _ in cursor) { }
        })!;

        Assert.That(e.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(e.Message, Does.Contain("'items' is stored under key-range routing"));

        await hashEngine.CloseDatabase(new CloseDatabaseTicket(dbname));
    }
}
