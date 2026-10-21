/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// DDL on one database while a staged schema change backfills a table of it. The backfill used to
/// hold the database's DDL semaphore from its first row to its last, so a <c>CREATE TABLE</c> of an
/// unrelated table waited for the whole build. Now the change holds only its table: DDL on another
/// table runs at once, and DDL on the same table waits until the change is done, as before.
///
/// <para>The backfill is paused at a seam between two of its batches, so the test observes the
/// other statements against a change that is provably in its long phase, with no timing guess.</para>
/// </summary>
[NonParallelizable]
internal abstract class DdlDuringStagedChangeScenarios : SharedNodeBaseTest
{
    /// <summary>Rows in the backfilled table: three index batches of 500 and three column batches of 256.</summary>
    private const int Rows = 1_200;

    private static readonly TimeSpan Soon = TimeSpan.FromSeconds(20);

    private static readonly TimeSpan NotYet = TimeSpan.FromSeconds(1);

    protected abstract bool ClusterMode { get; }

    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: ClusterMode);
    }

    private static async Task ExecuteDdl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task ExecuteNonQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<bool> CompletesWithin(Task task, TimeSpan within)
        => ReferenceEquals(await Task.WhenAny(task, Task.Delay(within)), task);

    /// <summary>A database with table <c>items</c> holding <see cref="Rows"/> rows, <c>val = id</c>.</summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE items (id INT64 PRIMARY KEY, val INT64 NOT NULL)");

        const int rowsPerStatement = 200;
        for (int start = 1; start <= Rows; start += rowsPerStatement)
        {
            int end = Math.Min(Rows, start + rowsPerStatement - 1);
            string values = string.Join(", ", Enumerable.Range(start, end - start + 1).Select(i => $"({i}, {i})"));
            await ExecuteNonQuery(executor, dbname, "INSERT INTO items (id, val) VALUES " + values);
        }

        return (dbname, database, executor);
    }

    /// <summary>
    /// A pause of a backfill at one of its seams: the first time the seam fires it reports that the
    /// backfill reached it and waits until the test lets it go; later fires pass through.
    /// </summary>
    private sealed class BackfillPause
    {
        private readonly TaskCompletionSource reached = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int fires;

        public Task Reached => reached.Task;

        public void Release() => released.TrySetResult();

        public async Task OnSeam()
        {
            if (Interlocked.Increment(ref fires) != 1)
                return;

            reached.TrySetResult();
            await released.Task;
        }
    }

    private static async Task<int> CountIndexEntries(CommandExecutor executor, string dbname, DatabaseDescriptor database, string indexName)
    {
        TableIndexSchema index = database.Schema.Tables["items"].Indexes!.First(ix => ix.Name == indexName);
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(dbname, "items"));

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            int entries = 0;
            await foreach (var _ in table.Store.ScanIndex(tx, index.KvId, [ColumnType.Integer64], from: null, to: null, unique: false))
                entries++;
            return entries;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static TableColumnSchema? Column(DatabaseDescriptor database, string table, string column)
        => database.Schema.Tables[table].Columns?.Find(c => string.Equals(c.Name, column, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    /// <c>CREATE INDEX</c> is paused between its first and second batch. A <c>CREATE TABLE</c> of another
    /// table completes meanwhile; an <c>ALTER TABLE</c> of the indexed table does not start until the
    /// index is Public. Afterwards the index holds one entry per row and the column is Public.
    /// </summary>
    [Test]
    public async Task CreateTableRunsDuringAnIndexBackfillWhileSameTableDdlWaits()
    {
        if (!ClusterMode)
            Assert.Ignore("A standalone node builds an index in two phases under the DDL gate; only the staged cluster build releases it");

        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        BackfillPause pause = new();
        executor.TestInterceptAfterBackfillCheckpoint = pause.OnSeam;

        Task createIndex = ExecuteDdl(executor, dbname, "CREATE INDEX items_val ON items (val)");
        Task? alterSameTable = null;

        try
        {
            Assert.That(await CompletesWithin(pause.Reached, Soon), Is.True, "the backfill reaches its first checkpoint");
            Assert.That(database.DdlGate.IsTableHeld("items"), Is.True, "the index build holds its table");
            Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1), "and not the DDL semaphore");

            Task createOther = ExecuteDdl(executor, dbname, "CREATE TABLE holds (id INT64 PRIMARY KEY, amount INT64 NOT NULL)");
            Assert.That(await CompletesWithin(createOther, Soon), Is.True, "CREATE TABLE of another table completes during the backfill");
            await createOther;
            Assert.That(database.Schema.Tables.ContainsKey("holds"), Is.True);

            alterSameTable = ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN tier INT64 NOT NULL DEFAULT (7)");
            Assert.That(await CompletesWithin(alterSameTable, NotYet), Is.False, "ADD COLUMN on the indexed table waits for the build");
            Assert.That(Column(database, "items", "tier"), Is.Null, "it has not started: the column does not exist in any state");
        }
        finally
        {
            pause.Release();
            executor.TestInterceptAfterBackfillCheckpoint = null;
        }

        await createIndex.WaitAsync(Soon);
        await alterSameTable!.WaitAsync(Soon);

        Assert.That(database.DdlGate.IsTableHeld("items"), Is.False, "nothing holds the table once both statements are done");
        Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1), "and the semaphore is free");

        TableIndexSchema index = database.Schema.Tables["items"].Indexes!.First(ix => ix.Name == "items_val");
        Assert.That(index.State, Is.EqualTo(SchemaElementState.Public));
        Assert.That(await CountIndexEntries(executor, dbname, database, "items_val"), Is.EqualTo(Rows), "one entry per row");

        TableColumnSchema? tier = Column(database, "items", "tier");
        Assert.That(tier, Is.Not.Null);
        Assert.That(tier!.State, Is.EqualTo(SchemaElementState.Public));
    }

    /// <summary>
    /// <c>ALTER TABLE ... ADD COLUMN</c> is paused before its first fill batch. A <c>CREATE TABLE</c> of
    /// another table completes meanwhile; a <c>DROP TABLE</c> of the table being filled waits until the
    /// column is Public, then drops it. The fill runs through the coordinator in both modes.
    /// </summary>
    [Test]
    public async Task CreateTableRunsDuringAColumnFillWhileDropOfTheSameTableWaits()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        BackfillPause pause = new();
        executor.TestInterceptBeforeFillBatchLock = pause.OnSeam;

        Task addColumn = ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN tier INT64 NOT NULL DEFAULT (7)");
        Task? dropSameTable = null;

        try
        {
            Assert.That(await CompletesWithin(pause.Reached, Soon), Is.True, "the fill reaches its first batch");
            Assert.That(database.DdlGate.IsTableHeld("items"), Is.True, "the column fill holds its table");
            Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1), "and not the DDL semaphore");
            Assert.That(Column(database, "items", "tier")?.State, Is.EqualTo(SchemaElementState.WriteOnly), "the column is being filled");

            Task createOther = ExecuteDdl(executor, dbname, "CREATE TABLE holds (id INT64 PRIMARY KEY, amount INT64 NOT NULL)");
            Assert.That(await CompletesWithin(createOther, Soon), Is.True, "CREATE TABLE of another table completes during the fill");
            await createOther;

            dropSameTable = ExecuteDdl(executor, dbname, "DROP TABLE items");
            Assert.That(await CompletesWithin(dropSameTable, NotYet), Is.False, "DROP TABLE of the filled table waits for the fill");
            Assert.That(database.Schema.Tables.ContainsKey("items"), Is.True);
        }
        finally
        {
            pause.Release();
            executor.TestInterceptBeforeFillBatchLock = null;
        }

        await addColumn.WaitAsync(Soon);
        await dropSameTable!.WaitAsync(Soon);

        Assert.That(database.Schema.Tables.ContainsKey("items"), Is.False, "the drop ran after the fill");
        Assert.That(database.Schema.Tables.ContainsKey("holds"), Is.True);
        Assert.That(database.DdlGate.HeldTableCount, Is.EqualTo(0));
        Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1));
    }
}

[TestFixture]
internal sealed class TestDdlDuringStagedChangeClusterMode : DdlDuringStagedChangeScenarios
{
    protected override bool ClusterMode => true;
}

[TestFixture]
internal sealed class TestDdlDuringStagedChangeStandalone : DdlDuringStagedChangeScenarios
{
    protected override bool ClusterMode => false;
}
