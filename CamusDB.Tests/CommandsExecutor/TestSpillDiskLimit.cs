/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <see cref="CamusDBOptions.SpillMaxTotalBytes"/> end to end through SQL, for every operator that
/// spills: a statement that would write more spill bytes than the limit fails with
/// <see cref="CamusDBErrorCodes.SpillLimitExceeded"/>, leaves no spill file and no reserved byte,
/// and the engine still runs the next statement.
///
/// <para>[NonParallelizable] because <see cref="SpillFileManager.AcquireInstanceLock"/> holds one
/// process-wide lock file.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestSpillDiskLimit : SharedNodeBaseTest
{
    private const int RowCount = 60;

    /// <summary>Far less than the encoded rows of any query here, so the first few spill writes pass it.</summary>
    private const long SmallLimit = 512;

    private string _dataDir = null!;

    [SetUp]
    public void SetUpSpill()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), "camusdb_spill_limit_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dataDir);

        SpillFileManager.AcquireInstanceLock(_dataDir);
    }

    [TearDown]
    public void TearDownSpill()
    {
        SpillFileManager.ReleaseInstanceLock();

        try { Directory.Delete(_dataDir, recursive: true); } catch { }
    }

    private CamusDBOptions SpillOn(long maxTotalBytes) =>
        Options with
        {
            SpillEnabled = true,
            ForceSpillThresholdRows = 4,
            SpillMergeFanIn = 4,
            SpillMaxTotalBytes = maxTotalBytes,
            DataDirectory = _dataDir,
        };

    private sealed record Fixture(string DbName, DatabaseDescriptor Database, CommandExecutor Executor);

    /// <summary>
    /// Table <c>t</c> has <see cref="RowCount"/> rows with a unique 100-character <c>payload</c>, so
    /// every row is its own group and its own distinct tuple. Table <c>u</c> has one row per
    /// <c>grp</c> of <c>t</c> for the join. No index covers <c>grp</c>, so the join is a hash join.
    /// </summary>
    private async Task<Fixture> Setup(CamusDBOptions options)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(options);
        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "t",
            columns:
            [
                new("id",      ColumnType.Id),
                new("grp",     ColumnType.Integer64, notNull: true),
                new("payload", ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "u",
            columns:
            [
                new("id",    ColumnType.Id),
                new("grp",   ColumnType.Integer64, notNull: true),
                new("label", ColumnType.String, notNull: true),
            ],
            constraints: [new(ConstraintType.PrimaryKey, "~pk", [new("id", OrderType.Ascending)])],
            ifNotExists: false));

        List<Dictionary<string, ColumnValue>> rows = new(RowCount);
        List<Dictionary<string, ColumnValue>> labels = new(RowCount);
        for (int i = 0; i < RowCount; i++)
        {
            rows.Add(new()
            {
                { "id",      new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                { "grp",     new(ColumnType.Integer64, (long)i) },
                { "payload", new(ColumnType.String,    i.ToString("D3") + new string('x', 97)) },
            });
            labels.Add(new()
            {
                { "id",    new(ColumnType.Id,        ObjectIdGenerator.Generate().ToString()) },
                { "grp",   new(ColumnType.Integer64, (long)i) },
                { "label", new(ColumnType.String,    "label-" + i) },
            });
        }

        await executor.Insert(new InsertTicket(txn, dbname, "t", values: rows));
        await executor.Insert(new InsertTicket(txn, dbname, "u", values: labels));
        await database.Transactions.CommitAsync(txn);

        return new Fixture(dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> Query(Fixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        try
        {
            ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null);
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await f.Executor.ExecuteSQLQuery(ticket);
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await f.Database.Transactions.CommitAsync(txn);
            return rows;
        }
        catch
        {
            await f.Database.Transactions.RollbackAsync(txn);
            throw;
        }
    }

    private static async Task NonQuery(Fixture f, string sql)
    {
        KvTransaction txn = await f.Database.Transactions.BeginAsync();
        try
        {
            ExecuteSQLTicket ticket = new(txnState: txn, database: f.DbName, sql: sql, parameters: null);
            await f.Executor.ExecuteNonSQLQuery(ticket);
            await f.Database.Transactions.CommitAsync(txn);
        }
        catch
        {
            await f.Database.Transactions.RollbackAsync(txn);
            throw;
        }
    }

    private string[] SpillFiles()
    {
        string spillRoot = Path.Combine(_dataDir, "tmp", "spill");
        return Directory.Exists(spillRoot)
            ? Directory.GetFiles(spillRoot, "*.spill", SearchOption.AllDirectories)
            : [];
    }

    private void AssertNothingLeft(string what)
    {
        Assert.IsEmpty(SpillFiles(), $"{what}: a refused spill must delete every spill file of the statement.");
        Assert.That(SpillFileManager.BudgetFor(_dataDir).UsedBytes, Is.Zero,
            $"{what}: a refused spill must give back every byte the statement reserved.");
    }

    private static readonly string[] SpillingQueries =
    [
        "SELECT payload FROM t ORDER BY payload DESC",
        "SELECT payload, COUNT(*) FROM t GROUP BY payload",
        "SELECT DISTINCT payload FROM t",
        "SELECT t.payload, u.label FROM t JOIN u ON t.grp = u.grp",
        // No index covers u.grp, so the subquery is materialized into a spillable value list.
        "SELECT payload FROM t WHERE grp IN (SELECT grp FROM u)",
        // The derived table is materialized into a spillable row list before the join reads it.
        "SELECT x.payload, u.label FROM (SELECT grp, payload FROM t) x JOIN u ON x.grp = u.grp",
    ];

    [TestCaseSource(nameof(SpillingQueries))]
    public async Task QueryPastTheLimit_FailsCleanly_AndTheNextStatementRuns(string sql)
    {
        Fixture f = await Setup(SpillOn(SmallLimit));

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Query(f, sql))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SpillLimitExceeded), ex.Message);

        AssertNothingLeft(sql);

        List<QueryResultRow> count = await Query(f, "SELECT COUNT(*) FROM t");
        Assert.That(count, Has.Count.EqualTo(1), "A statement that does not spill must still run.");
        Assert.IsEmpty(SpillFiles());
    }

    [TestCaseSource(nameof(SpillingQueries))]
    public async Task QueryUnderTheLimit_Spills_AndGivesBackEveryByte(string sql)
    {
        Fixture f = await Setup(SpillOn(CamusDBOptions.Default.SpillMaxTotalBytes));

        List<QueryResultRow> rows = await Query(f, sql);

        Assert.That(rows, Has.Count.EqualTo(RowCount), "Each query returns one row per row of t.");
        AssertNothingLeft(sql);
    }

    [Test]
    public async Task DeletePastTheLimit_FailsCleanly_AndDeletesNothing()
    {
        Fixture f = await Setup(SpillOn(SmallLimit));

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => NonQuery(f, "DELETE FROM t WHERE grp >= 0"))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SpillLimitExceeded), ex.Message);

        AssertNothingLeft("DELETE");

        List<QueryResultRow> rows = await Query(f, "SELECT id FROM t");
        Assert.That(rows, Has.Count.EqualTo(RowCount), "The refused DELETE must not remove any row.");
    }

    [Test]
    public async Task UpdatePastTheLimit_FailsCleanly_AndUpdatesNothing()
    {
        Fixture f = await Setup(SpillOn(SmallLimit));

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(
            () => NonQuery(f, "UPDATE t SET payload = 'changed' WHERE grp >= 0"))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.SpillLimitExceeded), ex.Message);

        AssertNothingLeft("UPDATE");

        List<QueryResultRow> changed = await Query(f, "SELECT id FROM t WHERE payload = 'changed'");
        Assert.IsEmpty(changed, "The refused UPDATE must not change any row.");
    }

    [Test]
    public async Task LimitOff_LargeSpillSucceeds()
    {
        Fixture f = await Setup(SpillOn(maxTotalBytes: 0));

        List<QueryResultRow> rows = await Query(f, "SELECT payload FROM t ORDER BY payload DESC");

        Assert.That(rows, Has.Count.EqualTo(RowCount));
        Assert.That(rows.First().Row.TryGetValue("payload", out var first) ? first.StrValue : null,
            Does.StartWith("059"), "Spill must still sort correctly with the limit off.");
        AssertNothingLeft("limit off");
    }
}
