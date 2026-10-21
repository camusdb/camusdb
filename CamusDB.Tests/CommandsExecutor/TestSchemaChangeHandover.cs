/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A staged schema change whose driver loses the schema leadership while it fills the table. The job
/// record carries the term of the node that drives it; the next leader's resume stamps its own, newer
/// term, and from then on the former driver's record writes are refused. The former driver must not
/// take the column back over the successor's work, and its statement must be answered from what the
/// successor did: success when the column was published, the successor's recorded error when it took
/// the column back. The successor is played on the same node, through a coordinator that reads a
/// newer term; the real leadership change is covered by the cluster tests.
/// </summary>
[NonParallelizable]
internal abstract class SchemaChangeHandoverScenarios : SharedNodeBaseTest
{
    private const int Rows = 40;
    private static readonly TimeSpan Soon = TimeSpan.FromSeconds(30);

    protected abstract bool ClusterMode { get; }

    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options)
    {
        CommandValidator validator = new(options);
        CatalogsManager catalogsManager = new(logger);
        return new(validator, catalogsManager, logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: ClusterMode);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

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

    private static async Task<List<Dictionary<string, ColumnValue>>> Query(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

            List<Dictionary<string, ColumnValue>> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(new Dictionary<string, ColumnValue>(row.Row, StringComparer.OrdinalIgnoreCase));
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>A database with table <c>items</c> holding <see cref="Rows"/> rows, ids 1..Rows.</summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await ExecuteDdl(executor, dbname, "CREATE TABLE items (id INT64 PRIMARY KEY, name STRING)");
        for (int i = 1; i <= Rows; i++)
            await ExecuteNonQuery(executor, dbname, $"INSERT INTO items (id, name) VALUES ({i}, 'n{i}')");
        return (dbname, database, executor);
    }

    private static TableColumnSchema? Column(DatabaseDescriptor database, string name) =>
        database.Schema.Tables["items"].Columns!.Find(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase));

    private static string TableId(DatabaseDescriptor database) => database.Schema.Tables["items"].Id!;

    private static async Task<bool> CompletesWithin(Task task, TimeSpan within)
        => ReferenceEquals(await Task.WhenAny(task, Task.Delay(within)), task);

    /// <summary>Waits until no staged change holds <c>items</c>: the driver gave the job up.</summary>
    private static async Task WaitForTableReleasedAsync(DatabaseDescriptor database)
    {
        DateTime deadline = DateTime.UtcNow.Add(Soon);
        while (database.DdlGate.IsTableHeld("items"))
        {
            Assert.That(DateTime.UtcNow, Is.LessThan(deadline), "the driver must release the table once it hands the job over");
            await Task.Delay(20);
        }
    }

    /// <summary>
    /// The successor, on this node: a coordinator that reads a schema-leader term one above the
    /// driver's, and resumes the jobs of the database exactly as a new leader does.
    /// </summary>
    private SchemaChangeCoordinator Successor(CommandExecutor executor, long term) => new(executor.Catalogs, logger)
    {
        BackfillAsync = executor.BackfillColumnDefaultsAsync,
        ReleaseOwnedSequenceAsync = executor.ReleaseOwnedSequenceAsync,
        ReadLeaderTermForTesting = _ => term + 1,
    };

    // -----------------------------------------------------------------------
    // The record's term fence
    // -----------------------------------------------------------------------

    /// <summary>
    /// The store refuses a job write or delete whose term is older than the record's, accepts a newer
    /// or equal one, and does not fence a writer that states no term.
    /// </summary>
    [Test]
    public async Task JobRecord_RefusesWritesOfAnOlderTerm()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        string tableId = TableId(database);

        PersistedCoordinatorJob Record(long term, int attempts) => new()
        {
            TableName = "items",
            TableId = tableId,
            ElementName = "qty",
            TargetState = SchemaElementState.Public,
            ElementKind = SchemaElementKind.Column,
            ColumnType = ColumnType.Integer64,
            LeaderTerm = term,
            Attempts = attempts,
        };

        await executor.Catalogs.PersistCoordinatorJobAsync(database, Record(term: 5, attempts: 0));

        CoordinatorJobTakenOverException refused = Assert.ThrowsAsync<CoordinatorJobTakenOverException>(
            () => executor.Catalogs.PersistCoordinatorJobAsync(database, Record(term: 4, attempts: 1)))!;
        Assert.That(refused.WriterTerm, Is.EqualTo(4));
        Assert.That(refused.OwnerTerm, Is.EqualTo(5));

        PersistedCoordinatorJob? stored = await executor.Catalogs.TryLoadCoordinatorJobAsync(database, tableId, "qty");
        Assert.That(stored, Is.Not.Null);
        Assert.That(stored!.Attempts, Is.EqualTo(0), "the refused write changed nothing");
        Assert.That(stored.LeaderTerm, Is.EqualTo(5));

        await executor.Catalogs.PersistCoordinatorJobAsync(database, Record(term: 5, attempts: 1));
        await executor.Catalogs.PersistCoordinatorJobAsync(database, Record(term: 6, attempts: 2));
        stored = await executor.Catalogs.TryLoadCoordinatorJobAsync(database, tableId, "qty");
        Assert.That(stored!.LeaderTerm, Is.EqualTo(6), "an equal and a newer term both land");

        Assert.ThrowsAsync<CoordinatorJobTakenOverException>(
            () => executor.Catalogs.DeleteCoordinatorJobAsync(database, tableId, "qty", leaderTerm: 5));
        Assert.That(await executor.Catalogs.TryLoadCoordinatorJobAsync(database, tableId, "qty"), Is.Not.Null, "a delete of an older term is refused");

        // A writer that states no term is not fenced: records from before the field existed, and
        // cleanups that act on no leadership, still land.
        await executor.Catalogs.PersistCoordinatorJobAsync(database, Record(term: 0, attempts: 3));
        stored = await executor.Catalogs.TryLoadCoordinatorJobAsync(database, tableId, "qty");
        Assert.That(stored!.Attempts, Is.EqualTo(3));

        await executor.Catalogs.DeleteCoordinatorJobAsync(database, tableId, "qty");
        Assert.That(await executor.Catalogs.TryLoadCoordinatorJobAsync(database, tableId, "qty"), Is.Null);
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty);

        _ = dbname;
    }

    // -----------------------------------------------------------------------
    // A fill handed over to a successor
    // -----------------------------------------------------------------------

    /// <summary>
    /// The successor takes the job over during the driver's fill and publishes the column. The driver's
    /// fill fails; it must not take the column back, and the statement must succeed, because its change
    /// was made.
    /// </summary>
    [Test]
    public async Task AddColumn_TakenOverDuringTheFill_SucceedsWhenTheSuccessorPublishesIt()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        string tableId = TableId(database);

        long term = database.Kahuna.SchemaLeaderTerm(database.Id);
        Assert.That(term, Is.GreaterThan(0), "the node reads a term it can fence with");

        int fires = 0;
        executor.TestInterceptBeforeFillBatchLock = async () =>
        {
            if (Interlocked.Increment(ref fires) != 1)
                return;

            // The next leader's resume: it stamps the record with its newer term before it drives.
            PersistedCoordinatorJob? record = await executor.Catalogs.TryLoadCoordinatorJobAsync(database, tableId, "qty");
            Assert.That(record, Is.Not.Null, "the driver recorded the job before its first step");
            Assert.That(record!.LeaderTerm, Is.EqualTo(term), "the record carries the driver's term");
            record.LeaderTerm = term + 1;
            record.Attempts = 1;
            await executor.Catalogs.PersistCoordinatorJobAsync(database, record);

            // The driver's fill stops, as it does when the leadership-lost callback cancels it.
            throw new OperationCanceledException("the schema leadership moved");
        };

        Task statement;
        Task successor;
        try
        {
            statement = ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN qty INT64 NOT NULL DEFAULT (7)");

            successor = Task.Run(async () =>
            {
                await WaitForTableReleasedAsync(database);
                await Successor(executor, term).ResumeJobsAsync(database);
            });

            Assert.That(await CompletesWithin(Task.WhenAll(statement, successor), Soon), Is.True, "the statement is answered once the successor is done");
            await successor;
            await statement;
        }
        finally
        {
            executor.TestInterceptBeforeFillBatchLock = null;
        }

        Assert.That(fires, Is.EqualTo(2), "the driver's one batch, then the successor's");

        TableColumnSchema? qty = Column(database, "qty");
        Assert.That(qty, Is.Not.Null, "the successor published the column");
        Assert.That(qty!.State, Is.EqualTo(SchemaElementState.Public));

        List<Dictionary<string, ColumnValue>> rows = await Query(executor, dbname, "SELECT id, qty FROM items ORDER BY id");
        Assert.That(rows, Has.Count.EqualTo(Rows));
        Assert.That(rows.Select(r => r["qty"].LongValue), Is.All.EqualTo(7), "every row holds the default");

        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty, "the successor deleted the record");
        Assert.That(database.DdlGate.HeldTableCount, Is.EqualTo(0));
        Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1), "the statement left the gate as it found it");
    }

    /// <summary>
    /// The successor takes the job over and takes the column back, recording why. The driver's statement
    /// must fail with the successor's error, not with "nothing was committed", and must not retry: the
    /// column is gone, and a retry would start the change again.
    /// </summary>
    [Test]
    public async Task AddColumn_TakenOverDuringTheFill_FailsWithTheReasonTheSuccessorRecorded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        string tableId = TableId(database);

        long term = database.Kahuna.SchemaLeaderTerm(database.Id);
        Assert.That(term, Is.GreaterThan(0));

        const string reason = "column \"qty\" of table \"items\" contains null values (the successor found a row it could not fill)";

        int fires = 0;
        executor.TestInterceptBeforeFillBatchLock = async () =>
        {
            if (Interlocked.Increment(ref fires) != 1)
                return;

            // The next leader's take-back: the removal intent, with the error that caused it.
            PersistedCoordinatorJob? record = await executor.Catalogs.TryLoadCoordinatorJobAsync(database, tableId, "qty");
            Assert.That(record, Is.Not.Null);
            record!.LeaderTerm = term + 1;
            record.Attempts = 1;
            record.TargetState = SchemaElementState.Absent;
            record.FailureCode = CamusDBErrorCodes.NotNullViolation;
            record.FailureMessage = reason;
            await executor.Catalogs.PersistCoordinatorJobAsync(database, record);

            throw new OperationCanceledException("the schema leadership moved");
        };

        Task<SchemaChangeHandedOverException?> statement;
        Task successor;
        try
        {
            statement = Task.Run(async () =>
            {
                try
                {
                    await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN qty INT64 NOT NULL DEFAULT (7)");
                    return null;
                }
                catch (SchemaChangeHandedOverException ex)
                {
                    return ex;
                }
            });

            successor = Task.Run(async () =>
            {
                await WaitForTableReleasedAsync(database);
                await Successor(executor, term).ResumeJobsAsync(database);
            });

            Assert.That(await CompletesWithin(Task.WhenAll(statement, successor), Soon), Is.True);
            await successor;
        }
        finally
        {
            executor.TestInterceptBeforeFillBatchLock = null;
        }

        SchemaChangeHandedOverException? failure = await statement;
        Assert.That(failure, Is.Not.Null, "the statement fails with the handed-over outcome");
        Assert.That(failure!.Code, Is.EqualTo(CamusDBErrorCodes.NotNullViolation), "with the code the successor recorded");
        Assert.That(failure.Message, Does.Contain(reason));
        Assert.That(failure.Message, Does.Not.Contain("nothing was committed"));

        Assert.That(Column(database, "qty"), Is.Null, "the successor removed the column");
        Assert.That(await executor.Catalogs.LoadCoordinatorJobsAsync(database), Is.Empty);
        Assert.That(database.DdlGate.HeldTableCount, Is.EqualTo(0));
        Assert.That(database.SchemaDdlSemaphore.CurrentCount, Is.EqualTo(1));

        // The name is free: the statement can be run again, and this time it completes.
        await ExecuteDdl(executor, dbname, "ALTER TABLE items ADD COLUMN qty INT64 NOT NULL DEFAULT (7)");
        Assert.That(Column(database, "qty")!.State, Is.EqualTo(SchemaElementState.Public));
    }

    /// <summary>
    /// A resume on a node whose term is older than the record's leaves the job alone: the record is the
    /// newer leader's, and the older node's write is refused before it drives anything.
    /// </summary>
    [Test]
    public async Task Resume_LeavesAJobOwnedByANewerTerm()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();
        string tableId = TableId(database);

        long term = database.Kahuna.SchemaLeaderTerm(database.Id);
        Assert.That(term, Is.GreaterThan(0));

        await executor.Catalogs.PersistCoordinatorJobAsync(database, new PersistedCoordinatorJob
        {
            TableName = "items",
            TableId = tableId,
            ElementName = "qty",
            TargetState = SchemaElementState.Public,
            ElementKind = SchemaElementKind.Column,
            ColumnType = ColumnType.Integer64,
            ColumnNotNull = true,
            ColumnDefault = new ColumnValue(ColumnType.Integer64, 7L),
            LeaderTerm = term + 1,
        });

        SchemaChangeCoordinator older = new(executor.Catalogs, logger)
        {
            BackfillAsync = executor.BackfillColumnDefaultsAsync,
            ReleaseOwnedSequenceAsync = executor.ReleaseOwnedSequenceAsync,
        };

        await older.ResumeJobsAsync(database);

        Assert.That(Column(database, "qty"), Is.Null, "the older node added nothing");
        PersistedCoordinatorJob? record = await executor.Catalogs.TryLoadCoordinatorJobAsync(database, tableId, "qty");
        Assert.That(record, Is.Not.Null, "the record stays for the newer leader");
        Assert.That(record!.Attempts, Is.EqualTo(0));

        await executor.Catalogs.DeleteCoordinatorJobAsync(database, tableId, "qty");
    }
}

/// <summary><see cref="SchemaChangeHandoverScenarios"/> on a standalone engine.</summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestSchemaChangeHandoverStandalone : SchemaChangeHandoverScenarios
{
    protected override bool ClusterMode => false;
}

/// <summary><see cref="SchemaChangeHandoverScenarios"/> on a cluster-mode engine.</summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestSchemaChangeHandoverCluster : SchemaChangeHandoverScenarios
{
    protected override bool ClusterMode => true;
}
