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
using Kahuna.Shared.KeyValue;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// DDL on one database while a staged schema change visits the rows of a table of it: an index build, a
/// column fill, or the validation of a CHECK or a NOT NULL. Such a pass used to hold the database's DDL
/// semaphore from its first row to its last, so a <c>CREATE TABLE</c> of an unrelated table waited for the
/// whole pass. The change holds only its table: DDL on another table runs at once, and DDL on the same
/// table waits until the change is done. A standalone node and a cluster-mode node behave the same.
///
/// <para>The pass is paused at a seam, so the test observes the other statements against a change that is
/// provably in its long phase, with no timing guess.</para>
/// </summary>
[NonParallelizable]
internal abstract class DdlDuringStagedChangeScenarios : SharedNodeBaseTest
{
    /// <summary>Rows in the backfilled table: three index batches of 500 and three column batches of 256.</summary>
    private const int Rows = 1_200;

    private static readonly TimeSpan Soon = TimeSpan.FromSeconds(20);

    private static readonly TimeSpan NotYet = TimeSpan.FromSeconds(1);

    protected abstract bool ClusterMode { get; }

    /// <summary>
    /// Whether transactional DDL is on. The schema statements outside a transaction then hold the DDL lock of
    /// the database, which they share, so they must run beside each other as they do with the setting off.
    /// </summary>
    protected virtual bool TransactionalDdl => false;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        TransactionalDdl ? defaults with { TransactionalDdlEnabled = true } : defaults;

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

        await ExecuteDdl(executor, dbname, "CREATE TABLE items (id INT64 PRIMARY KEY, val INT64 NOT NULL, note STRING)");

        const int rowsPerStatement = 200;
        for (int start = 1; start <= Rows; start += rowsPerStatement)
        {
            int end = Math.Min(Rows, start + rowsPerStatement - 1);
            string values = string.Join(", ", Enumerable.Range(start, end - start + 1).Select(i => $"({i}, {i}, 'n')"));
            await ExecuteNonQuery(executor, dbname, "INSERT INTO items (id, val, note) VALUES " + values);
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
    /// <c>CREATE INDEX</c> needs the lock of a row that an open transaction holds, and waits until that
    /// transaction ends. A <c>CREATE TABLE</c> of another table completes meanwhile, and so do other
    /// statements on that table. One forgotten COMMIT must not stop every DDL of the database.
    /// </summary>
    [Test]
    public async Task CreateTableRunsWhileAnIndexBuildWaitsForAnOpenTransaction()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        KvTransaction open = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite, locking: KeyValueTransactionLocking.Pessimistic);
        Task createIndex;

        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(open, dbname, "UPDATE items SET note = 'z' WHERE id = 123", null));

            createIndex = ExecuteDdl(executor, dbname, "CREATE INDEX items_val ON items (val)");
            Assert.That(await CompletesWithin(createIndex, NotYet), Is.False, "the build waits for the open transaction's row");
            Assert.That(database.DdlGate.IsTableHeld("items"), Is.True, "the build holds its table while it waits");
            Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1), "and not the DDL semaphore");

            foreach (string other in new[]
            {
                "CREATE TABLE holds (id INT64 PRIMARY KEY, amount INT64 NOT NULL)",
                "COMMENT ON TABLE holds IS 'unrelated'",
                "CREATE INDEX holds_amount ON holds (amount)",
                "ALTER TABLE holds ADD CONSTRAINT amount_positive CHECK (amount > 0)",
                "TRUNCATE TABLE holds",
                "DROP TABLE holds",
            })
            {
                Task statement = ExecuteDdl(executor, dbname, other);
                Assert.That(await CompletesWithin(statement, Soon), Is.True, $"{other} completes while the build waits");
                await statement;
            }

            Assert.That(createIndex.IsCompleted, Is.False, "the build still waits");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(open);
        }

        await createIndex.WaitAsync(Soon);

        Assert.That(database.Schema.Tables["items"].Indexes!.First(ix => ix.Name == "items_val").State, Is.EqualTo(SchemaElementState.Public));
        Assert.That(await CountIndexEntries(executor, dbname, database, "items_val"), Is.EqualTo(Rows), "one entry per row");
        Assert.That(database.DdlGate.HeldTableCount, Is.EqualTo(0));
        Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1));
    }

    /// <summary>
    /// <c>ADD CHECK</c> and <c>SET NOT NULL</c> are paused before they read the existing rows. A
    /// <c>CREATE TABLE</c> of another table completes meanwhile. Each statement on the same table waits,
    /// the ones that read no row included: they persist the whole table, as the staged change does.
    /// </summary>
    [TestCase("ALTER TABLE items ADD CONSTRAINT val_positive CHECK (val > 0)", "ALTER TABLE items ADD COLUMN tier INT64 NOT NULL DEFAULT (7)")]
    [TestCase("ALTER TABLE items ADD CONSTRAINT val_positive CHECK (val > 0)", "ALTER TABLE items DROP CONSTRAINT val_positive")]
    [TestCase("ALTER TABLE items ADD CONSTRAINT val_positive CHECK (val > 0)", "ALTER TABLE items ALTER COLUMN val DROP NOT NULL")]
    [TestCase("ALTER TABLE items ADD CONSTRAINT val_positive CHECK (val > 0)", "ALTER TABLE items ALTER COLUMN note SET STORAGE EXTERNAL")]
    [TestCase("ALTER TABLE items ALTER COLUMN note SET NOT NULL", "CREATE INDEX items_val ON items (val)")]
    [TestCase("ALTER TABLE items ALTER COLUMN note SET NOT NULL", "ALTER TABLE items ALTER COLUMN note DROP NOT NULL")]
    public async Task CreateTableRunsDuringAConstraintValidationWhileSameTableDdlWaits(string constraint, string sameTable)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        BackfillPause pause = new();
        executor.TestInterceptBeforeRowConstraintValidation = pause.OnSeam;

        Task addConstraint = ExecuteDdl(executor, dbname, constraint);
        Task? sameTableStatement = null;

        try
        {
            Assert.That(await CompletesWithin(pause.Reached, Soon), Is.True, "the statement reaches its row scan");
            Assert.That(database.DdlGate.IsTableHeld("items"), Is.True, "the validation holds its table");
            Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1), "and not the DDL semaphore");

            Task createOther = ExecuteDdl(executor, dbname, "CREATE TABLE holds (id INT64 PRIMARY KEY, amount INT64 NOT NULL)");
            Assert.That(await CompletesWithin(createOther, Soon), Is.True, "CREATE TABLE of another table completes during the scan");
            await createOther;

            sameTableStatement = ExecuteDdl(executor, dbname, sameTable);
            Assert.That(await CompletesWithin(sameTableStatement, NotYet), Is.False, $"{sameTable} waits for the scan");
        }
        finally
        {
            pause.Release();
            executor.TestInterceptBeforeRowConstraintValidation = null;
        }

        await addConstraint.WaitAsync(Soon);
        await sameTableStatement!.WaitAsync(Soon);

        Assert.That(database.Schema.Tables.ContainsKey("holds"), Is.True);
        Assert.That(database.DdlGate.HeldTableCount, Is.EqualTo(0));
        Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1));
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty, "no job is left behind");
    }

    /// <summary>
    /// A foreign key with no index to serve it builds one. The build is paused; a <c>CREATE TABLE</c> of
    /// another table completes meanwhile, and the constraint is there once the statement ends.
    /// </summary>
    [Test]
    public async Task CreateTableRunsDuringTheIndexBuildOfAForeignKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        await ExecuteDdl(executor, dbname, "CREATE TABLE parents (id INT64 PRIMARY KEY)");

        for (int start = 1; start <= Rows; start += 200)
        {
            string values = string.Join(", ", Enumerable.Range(start, 200).Select(i => $"({i})"));
            await ExecuteNonQuery(executor, dbname, "INSERT INTO parents (id) VALUES " + values);
        }

        BackfillPause pause = new();
        executor.TestInterceptAfterBackfillCheckpoint = pause.OnSeam;

        Task addForeignKey = ExecuteDdl(executor, dbname, "ALTER TABLE items ADD CONSTRAINT items_fk FOREIGN KEY (val) REFERENCES parents (id)");

        try
        {
            Assert.That(await CompletesWithin(pause.Reached, Soon), Is.True, "the build of the backing index reaches its first checkpoint");

            Task createOther = ExecuteDdl(executor, dbname, "CREATE TABLE holds (id INT64 PRIMARY KEY, amount INT64 NOT NULL)");
            Assert.That(await CompletesWithin(createOther, Soon), Is.True, "CREATE TABLE of another table completes during the build");
            await createOther;
        }
        finally
        {
            pause.Release();
            executor.TestInterceptAfterBackfillCheckpoint = null;
        }

        await addForeignKey.WaitAsync(Soon);

        TableSchema items = database.Schema.Tables["items"];
        Assert.That(items.ForeignKeys!.Single(fk => fk.Name == "items_fk").State, Is.EqualTo(SchemaElementState.Public));
        Assert.That(database.DdlGate.HeldTableCount, Is.EqualTo(0));
        Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1));
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

/// <summary><see cref="DdlDuringStagedChangeScenarios"/> in cluster mode with transactional DDL on.</summary>
[TestFixture]
internal sealed class TestDdlDuringStagedChangeClusterModeTransactionalDdl : DdlDuringStagedChangeScenarios
{
    protected override bool ClusterMode => true;

    protected override bool TransactionalDdl => true;
}

/// <summary><see cref="DdlDuringStagedChangeScenarios"/> on a standalone node with transactional DDL on.</summary>
[TestFixture]
internal sealed class TestDdlDuringStagedChangeStandaloneTransactionalDdl : DdlDuringStagedChangeScenarios
{
    protected override bool ClusterMode => false;

    protected override bool TransactionalDdl => true;
}
