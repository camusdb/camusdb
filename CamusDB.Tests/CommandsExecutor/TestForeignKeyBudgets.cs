/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The cost budgets of foreign-key enforcement, proved with counters rather than inferred: what a
/// parent DELETE reads from the child table, and how many keys the validation of ADD CONSTRAINT
/// checks. Each test fills the child table with 10,000 rows, so a full scan in an enforcement path
/// shows as thousands of entries read and fails the bound.
///
/// <para>The other budgets have their tests next to the path they measure: no work without a
/// constraint (<c>TestForeignKeyInsert.TableWithoutConstraintsDoesNoForeignKeyWork</c>), no work for an
/// UPDATE of other columns (<c>TestForeignKeyUpdate.UpdateOfOtherColumnsDoesNoForeignKeyWork</c>), one
/// lock per distinct parent key and one batched read (<c>TestForeignKeyInsert</c>, 1000 rows and 3
/// parents), no whole-bucket lock (<c>RendezvousLocksNeverEscalateToTheWholeBucket</c>), one batched
/// read per ancestry level on a branch (<c>TestForeignKeyBranch</c>), and the one-phase commit of a
/// pessimistic child write (<c>TestClusterForeignKeyOnePhaseCommit</c>, three nodes).</para>
/// </summary>
internal static class ForeignKeyBudgetScenarios
{
    private const int ChildRows = 10_000;

    /// <summary>
    /// A parent DELETE runs one bounded probe per removed key. Five parents without children are
    /// deleted while 10,000 children reference a sixth: the statement reads the parent rows and a few
    /// index entries, and no child row. The DELETE of the referenced parent then reads at most one probe
    /// page of the child index, not its 10,000 entries.
    /// </summary>
    public static async Task ParentDeleteProbesOncePerKeyAndReadsNoChildRow(CommandExecutor executor, string dbname)
    {
        await ForeignKeyAlterScenarios.Ddl(executor, dbname,
            "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))");
        await ForeignKeyAlterScenarios.Ddl(executor, dbname,
            "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (name))");

        await ForeignKeyAlterScenarios.Dml(executor, dbname,
            "INSERT INTO cities (id, name) VALUES (1, 'lima'), (2, 'p2'), (3, 'p3'), (4, 'p4'), (5, 'p5'), (6, 'p6')");
        await InsertChildrenAsync(executor, dbname, _ => "lima");

        // The control: a scan of the child table shows as 10,000 entries read, so the bounds below
        // would catch one.
        using (KvScanEntryCounter control = KvScanEntryCounter.Start())
        {
            await ForeignKeyAlterScenarios.Query(executor, dbname, "SELECT id, city FROM weather");
            Assert.That(control.Total(), Is.GreaterThanOrEqualTo(ChildRows), "The counter must see a full scan");
        }

        using (ForeignKeyOperationCounter work = ForeignKeyOperationCounter.Start())
        using (KvScanEntryCounter scans = KvScanEntryCounter.Start())
        {
            await ForeignKeyAlterScenarios.Dml(executor, dbname, "DELETE FROM cities WHERE id >= 2");

            Assert.AreEqual(5, work.Count("parent_probe"), "One probe per removed key");
            TestContext.Out.WriteLine($"DELETE of 5 parents: {scans.Count("row")} row entries, {scans.Count("index")} index entries read");
            Assert.That(scans.Count("row"), Is.LessThan(100), "Only the parent rows are scanned, never the child table");
            Assert.That(scans.Count("index"), Is.LessThan(100), "A probe of a key with no children reads no child entry");
        }

        using (ForeignKeyOperationCounter work = ForeignKeyOperationCounter.Start())
        using (KvScanEntryCounter scans = KvScanEntryCounter.Start())
        {
            CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(async () =>
                await ForeignKeyAlterScenarios.Dml(executor, dbname, "DELETE FROM cities WHERE id = 1"))!;
            Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictDelete, refused.Code, refused.Message);

            Assert.AreEqual(1, work.Count("parent_probe"));
            TestContext.Out.WriteLine($"DELETE of the referenced parent: {scans.Count("row")} row entries, {scans.Count("index")} index entries read");
            Assert.That(scans.Count("index"), Is.LessThanOrEqualTo(KvStoreConstants.ForeignKeyProbePageSize + 1),
                "The probe stops at the first child: one page of the child index at most");
            Assert.That(scans.Count("row"), Is.LessThan(100));
        }
    }

    /// <summary>
    /// ADD CONSTRAINT over 10,000 children that reference 50 parents checks 50 distinct keys, not
    /// 10,000, and reads the parents in batches rather than one read per child row.
    /// </summary>
    public static async Task AddConstraintValidatesOneKeyPerDistinctParent(CommandExecutor executor, string dbname)
    {
        await ForeignKeyAlterScenarios.Ddl(executor, dbname,
            "CREATE TABLE cities (id int64 PRIMARY KEY NOT NULL, name string NOT NULL, UNIQUE KEY cities_name (name))");
        await ForeignKeyAlterScenarios.Ddl(executor, dbname, "CREATE TABLE weather (id int64 PRIMARY KEY NOT NULL, city string)");

        StringBuilder parents = new("INSERT INTO cities (id, name) VALUES ");
        for (int i = 0; i < 50; i++)
            parents.Append(i == 0 ? "" : ", ").Append('(').Append(i).Append(", 'c").Append(i).Append("')");
        await ForeignKeyAlterScenarios.Dml(executor, dbname, parents.ToString());

        await InsertChildrenAsync(executor, dbname, row => "c" + (row % 50));

        using ForeignKeyOperationCounter work = ForeignKeyOperationCounter.Start();
        await ForeignKeyAlterScenarios.Ddl(executor, dbname, ForeignKeyAlterScenarios.AddConstraint);

        TestContext.Out.WriteLine($"ADD CONSTRAINT: {work.Count("validation_key")} keys, {work.Count("child_probe_key")} parent reads in {work.Count("child_probe_batch")} batches");
        Assert.AreEqual(50, work.Count("validation_key"), "One check per distinct parent key");
        Assert.That(work.Count("child_probe_key"), Is.LessThanOrEqualTo(50), "Each parent key is read once");
        Assert.That(work.Count("child_probe_batch"), Is.LessThan(50), "The parent reads are batched");
    }

    /// <summary>
    /// The path without a constraint allocates nothing. The checker factories of INSERT, DELETE and
    /// UPDATE complete at once and return the shared empty checker, and the per-row and completion calls
    /// on it do no work. Counted with the allocation counter of the current thread, which no other test
    /// can move; every call here completes synchronously, so the work stays on this thread. Run first in
    /// a database with no constraint, then for a table that no constraint touches in a database that has
    /// some.
    /// </summary>
    public static async Task PathWithoutAConstraintAllocatesNothing(CommandExecutor executor, string dbname)
    {
        await ForeignKeyAlterScenarios.Ddl(executor, dbname, "CREATE TABLE plain (id int64 PRIMARY KEY NOT NULL, v string)");

        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        TableDescriptor plain = await executor.OpenTable(new OpenTableTicket(dbname, "plain"));

        Assert.IsTrue(database.Schema.ForeignKeys.IsEmpty);
        AssertNoAllocation(database, plain, "a database with no constraint");

        await ForeignKeyAlterScenarios.Seed(executor, dbname, withUserIndex: false);
        await ForeignKeyAlterScenarios.Ddl(executor, dbname, ForeignKeyAlterScenarios.AddConstraint);

        Assert.IsFalse(database.Schema.ForeignKeys.IsEmpty);
        AssertNoAllocation(database, plain, "a table that no constraint touches");
    }

    private static void AssertNoAllocation(DatabaseDescriptor database, TableDescriptor table, string subject)
    {
        // The factories never open a table or touch the transaction when no constraint applies, so
        // neither is passed.
        Dictionary<string, ColumnValue> row = new() { ["id"] = new ColumnValue(ColumnType.Integer64, 1L) };
        Dictionary<string, ColumnValue> assigned = new() { ["v"] = new ColumnValue(ColumnType.String, "x") };

        // Once to compile every path, then measured.
        RunNoConstraintPath(database, table, row, assigned);

        long before = GC.GetAllocatedBytesForCurrentThread();
        RunNoConstraintPath(database, table, row, assigned);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.AreEqual(0, allocated, $"The foreign-key path for {subject} must allocate nothing");
    }

    private static void RunNoConstraintPath(
        DatabaseDescriptor database, TableDescriptor table, Dictionary<string, ColumnValue> row, Dictionary<string, ColumnValue> assigned)
    {
        ValueTask<ForeignKeyStatementChecker> insert = ForeignKeyStatementChecker.ForChildWritesAsync(database, table, null!, null!);
        ValueTask<ForeignKeyStatementChecker> delete = ForeignKeyStatementChecker.ForParentDeletesAsync(database, table, null!, null!);
        ValueTask<ForeignKeyStatementChecker> update = ForeignKeyStatementChecker.ForUpdatesAsync(database, table, null!, null!, assigned, null);

        if (!insert.IsCompletedSuccessfully || !delete.IsCompletedSuccessfully || !update.IsCompletedSuccessfully)
            throw new AssertionException("A factory did not complete at once");

        foreach (ForeignKeyStatementChecker checker in (ReadOnlySpan<ForeignKeyStatementChecker>)[insert.Result, delete.Result, update.Result])
        {
            if (!ReferenceEquals(checker, ForeignKeyStatementChecker.None) || checker.HasWork)
                throw new AssertionException("A factory built a checker with work");

            checker.AddChildRow(row);
            checker.AddRemovedParentRow(row);
            checker.AddUpdatedRow(row, row);

            if (!checker.CompleteAsync(null!).IsCompletedSuccessfully)
                throw new AssertionException("The empty checker did not complete at once");
        }
    }

    /// <summary>10,000 child rows in statements of 1000, with the city each row number names.</summary>
    private static async Task InsertChildrenAsync(CommandExecutor executor, string dbname, Func<int, string> cityOf)
    {
        for (int start = 0; start < ChildRows; start += 1000)
        {
            StringBuilder sql = new("INSERT INTO weather (id, city) VALUES ");
            for (int row = start; row < start + 1000; row++)
                sql.Append(row == start ? "" : ", ").Append('(').Append(row + 1).Append(", '").Append(cityOf(row)).Append("')");

            await ForeignKeyAlterScenarios.Dml(executor, dbname, sql.ToString());
        }
    }
}

/// <summary>The foreign-key cost budgets on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyBudgets : BaseTest
{
    [Test] public async Task ParentDeleteProbesOncePerKeyAndReadsNoChildRow() => await Run(ForeignKeyBudgetScenarios.ParentDeleteProbesOncePerKeyAndReadsNoChildRow);
    [Test] public async Task AddConstraintValidatesOneKeyPerDistinctParent() => await Run(ForeignKeyBudgetScenarios.AddConstraintValidatesOneKeyPerDistinctParent);
    [Test] public async Task PathWithoutAConstraintAllocatesNothing() => await Run(ForeignKeyBudgetScenarios.PathWithoutAConstraintAllocatesNothing);

    private async Task Run(Func<CommandExecutor, string, Task> scenario)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, dbname);
    }
}

/// <summary>The foreign-key cost budgets on a cluster-mode engine.</summary>
[TestFixture]
[NonParallelizable]
public sealed class TestForeignKeyBudgetsCluster : SharedNodeBaseTest
{
    [Test] public async Task ParentDeleteProbesOncePerKeyAndReadsNoChildRow() => await Run(ForeignKeyBudgetScenarios.ParentDeleteProbesOncePerKeyAndReadsNoChildRow);
    [Test] public async Task AddConstraintValidatesOneKeyPerDistinctParent() => await Run(ForeignKeyBudgetScenarios.AddConstraintValidatesOneKeyPerDistinctParent);
    [Test] public async Task PathWithoutAConstraintAllocatesNothing() => await Run(ForeignKeyBudgetScenarios.PathWithoutAConstraintAllocatesNothing);

    private async Task Run(Func<CommandExecutor, string, Task> scenario)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, dbname);
    }
}
