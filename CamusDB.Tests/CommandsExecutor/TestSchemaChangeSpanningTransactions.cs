/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A transaction that writes a row before a staged schema change, and commits after the change has
/// read the table. The index backfill and the foreign-key validation pass read committed rows only,
/// and the transaction planned its writes against the schema it started with: it wrote no entry for
/// the new index and ran no check for the new constraint.
///
/// <para>The write-shape fence closes the gap in two parts, and each has its scenarios here. A commit
/// that reaches its gate after the change is refused with a retryable conflict. A commit that passed
/// its gate before the change is awaited by the change, so its rows are visible to the backfill or
/// the validation pass. The remaining scenarios show what the fence must not refuse.</para>
/// </summary>
internal static class SchemaChangeSpanningTransactionScenarios
{
    private const string LateIndex = "CREATE INDEX weather_city_late ON weather (city)";

    private const string ThroughLateIndex = "SELECT id FROM weather@{FORCE_INDEX=weather_city_late} WHERE city = ";

    /// <summary>How long a schema change must stay blocked before a test accepts that it waits.</summary>
    private static readonly TimeSpan BlockedFor = TimeSpan.FromMilliseconds(750);

    /// <summary>The orphan must not survive under a Public constraint: its commit is refused.</summary>
    public static async Task OrphanCommittedAfterTheValidationIsCaught(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ForeignKeyAlterScenarios.Seed(executor, dbname, withUserIndex: true);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO weather (id, city) VALUES (60, 'atlantis')", null));

        await ForeignKeyAlterScenarios.Ddl(executor, dbname, ForeignKeyAlterScenarios.AddConstraint);

        AssertRefusedAndRolledBack(await Capture(database.Transactions.CommitAsync(tx)), tx);

        Assert.AreEqual(SchemaElementState.Public, database.Schema.Tables["weather"].ForeignKeys!.Single().State);
        Assert.IsEmpty(await ForeignKeyAlterScenarios.Orphans(executor, dbname), "A Public constraint must not hold over an orphan");
    }

    /// <summary>
    /// The parent side of the same gap: a DELETE of a referenced parent, staged before the constraint
    /// existed, ran no check for children. Its commit after the validation would leave an orphan.
    /// </summary>
    public static async Task ParentDeleteCommittedAfterTheValidationIsCaught(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ForeignKeyAlterScenarios.Seed(executor, dbname, withUserIndex: true);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "DELETE FROM cities WHERE id = 2", null));

        Exception? alterError = await Capture(ForeignKeyAlterScenarios.Ddl(executor, dbname, ForeignKeyAlterScenarios.AddConstraint));
        Exception? commitError = await Capture(database.Transactions.CommitAsync(tx));

        // The validation pass can be refused by the lock of the staged DELETE. That is a correct
        // outcome too; what must never happen is a commit after a validation that passed.
        if (alterError is null)
            AssertRefusedAndRolledBack(commitError, tx);

        bool constraintExists = database.Schema.Tables["weather"].ForeignKeys is { Count: > 0 };
        List<long> orphans = await ForeignKeyAlterScenarios.Orphans(executor, dbname);

        Assert.IsFalse(constraintExists && orphans.Count > 0,
            $"A constraint must not hold over an orphan. ALTER: {alterError?.Message ?? "ok"}; commit: {commitError?.Message ?? "ok"}; orphans: {string.Join(",", orphans)}");
    }

    /// <summary>The new index must hold an entry for every committed row: the late commit is refused.</summary>
    public static async Task RowCommittedAfterTheIndexBackfillIsIndexed(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ForeignKeyAlterScenarios.Seed(executor, dbname, withUserIndex: false);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO weather (id, city) VALUES (60, 'atlantis')", null));

        await ForeignKeyAlterScenarios.Ddl(executor, dbname, LateIndex);

        AssertRefusedAndRolledBack(await Capture(database.Transactions.CommitAsync(tx)), tx);

        Assert.IsEmpty(await ForeignKeyAlterScenarios.Query(executor, dbname, "SELECT id FROM weather WHERE id = 60"), "The refused row must not exist");

        // The retry that the error asks for plans against the new index.
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO weather (id, city) VALUES (60, 'atlantis')");

        Assert.AreEqual(1, (await ForeignKeyAlterScenarios.Query(executor, dbname, ThroughLateIndex + "'atlantis'")).Count,
            "The retried row must be found through the new index");
    }

    /// <summary>
    /// A commit that passed its gate before the index existed cannot be refused any more, so the build
    /// waits for its outcome, and the backfill then indexes the row.
    /// </summary>
    public static async Task IndexBuildWaitsForACommitInFlight(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ForeignKeyAlterScenarios.Seed(executor, dbname, withUserIndex: false);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO weather (id, city) VALUES (60, 'atlantis')", null));

        // The first half of CommitAsync: the gate is passed, and no commit request has left yet.
        Assert.IsTrue(tx.TryEnterCommit(out _));

        Task build = ForeignKeyAlterScenarios.Ddl(executor, dbname, LateIndex);

        Assert.AreNotSame(build, await Task.WhenAny(build, Task.Delay(BlockedFor)),
            "The index build must not read the table while a commit planned before it is in flight");

        // The second half: CommitAsync resumes a transaction that is already Finalizing.
        await database.Transactions.CommitAsync(tx);
        await build;

        Assert.AreEqual(1, (await ForeignKeyAlterScenarios.Query(executor, dbname, ThroughLateIndex + "'atlantis'")).Count,
            "A row whose commit landed during the build must be in the index");
    }

    /// <summary>
    /// The same wait in front of the validation pass: the orphan lands first, the pass finds it, and
    /// the constraint is refused and removed.
    /// </summary>
    public static async Task ValidationWaitsForACommitInFlight(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ForeignKeyAlterScenarios.Seed(executor, dbname, withUserIndex: true);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO weather (id, city) VALUES (60, 'atlantis')", null));

        Assert.IsTrue(tx.TryEnterCommit(out _));

        Task alter = ForeignKeyAlterScenarios.Ddl(executor, dbname, ForeignKeyAlterScenarios.AddConstraint);

        Assert.AreNotSame(alter, await Task.WhenAny(alter, Task.Delay(BlockedFor)),
            "The validation pass must not read the table while a commit planned before it is in flight");

        await database.Transactions.CommitAsync(tx);

        CamusDBException violation = Assert.ThrowsAsync<CamusDBException>(async () => await alter)!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, violation.Code, violation.Message);
        Assert.IsFalse(database.Schema.Tables["weather"].ForeignKeys is { Count: > 0 }, "A constraint that failed its validation must be removed");
    }

    /// <summary>
    /// CREATE TABLE with a foreign key adds a duty to the parent: its DELETE must look for children.
    /// A DELETE staged before the child table existed looked for none, so its commit after the
    /// CREATE, and after a child row took the parent, would leave an orphan.
    /// </summary>
    public static async Task ParentDeleteCommittedAfterCreateTableIsCaught(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ForeignKeyAlterScenarios.Seed(executor, dbname, withUserIndex: false);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "DELETE FROM cities WHERE id = 3", null));

        await ForeignKeyAlterScenarios.Ddl(executor, dbname,
            "CREATE TABLE forecasts (id int64 PRIMARY KEY NOT NULL, city string REFERENCES cities (name))");

        AssertRefusedAndRolledBack(await Capture(database.Transactions.CommitAsync(tx)), tx);

        // The parent is still there, so the child is accepted, and the parent is now protected.
        await ForeignKeyAlterScenarios.Dml(executor, dbname, "INSERT INTO forecasts (id, city) VALUES (1, 'bogota')");

        CamusDBException restricted = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ForeignKeyAlterScenarios.Dml(executor, dbname, "DELETE FROM cities WHERE id = 3"))!;
        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictDelete, restricted.Code, restricted.Message);
    }

    /// <summary>
    /// The fence is per table and per statement. A transaction that began before the change, wrote
    /// another table before it, and writes the changed table only afterwards planned every write
    /// against the right shape, so it commits.
    /// </summary>
    public static async Task WritesPlannedAfterTheChangeCommit(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ForeignKeyAlterScenarios.Seed(executor, dbname, withUserIndex: false);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO cities (id, name) VALUES (4, 'cusco')", null));

        await ForeignKeyAlterScenarios.Ddl(executor, dbname, LateIndex);

        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO weather (id, city) VALUES (60, 'cusco')", null));
        await database.Transactions.CommitAsync(tx);

        Assert.AreEqual(1, (await ForeignKeyAlterScenarios.Query(executor, dbname, ThroughLateIndex + "'cusco'")).Count);
        Assert.AreEqual(1, (await ForeignKeyAlterScenarios.Query(executor, dbname, "SELECT id FROM cities WHERE id = 4")).Count);
    }

    /// <summary>
    /// ADD COLUMN with a default has the same shape, and a different guard: the column moves the
    /// table's layout version, and the schema-version pin refuses the commit. Either the commit is
    /// refused, or the row reads the default; a row that reads NULL would be the gap.
    /// </summary>
    public static async Task RowCommittedAfterAColumnBackfillReadsTheDefault(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ForeignKeyAlterScenarios.Seed(executor, dbname, withUserIndex: false);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO weather (id, city) VALUES (60, 'lima')", null));

        await ForeignKeyAlterScenarios.Ddl(executor, dbname, "ALTER TABLE weather ADD COLUMN temp INT64 DEFAULT (7)");

        Exception? commitError = await Capture(database.Transactions.CommitAsync(tx));
        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        List<QueryResultRow> rows = await ForeignKeyAlterScenarios.Query(executor, dbname, "SELECT temp FROM weather WHERE id = 60");

        if (commitError is not null)
        {
            Assert.IsEmpty(rows, "A refused commit must leave no row");
            return;
        }

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(7, rows[0].Row["temp"].LongValue, "A row committed after the column backfill must read the default");
    }

    private static void AssertRefusedAndRolledBack(Exception? commitError, KvTransaction tx)
    {
        Assert.IsInstanceOf<CamusDBException>(commitError, "The commit of a write planned before the change must be refused");
        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, ((CamusDBException)commitError!).Code, commitError.Message);

        // The refusal is a definite non-commit, so the caller can replay from a new transaction
        // without a rollback of its own.
        Assert.AreEqual(KvTransactionStatus.RolledBack, tx.Status);
    }

    private static async Task<Exception?> Capture(Task task)
    {
        try
        {
            await task;
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }
}

[TestFixture]
[NonParallelizable]
public sealed class TestSchemaChangeSpanningTransactions : BaseTest
{
    [Test] public async Task OrphanCommittedAfterTheValidationIsCaught() => await Run(SchemaChangeSpanningTransactionScenarios.OrphanCommittedAfterTheValidationIsCaught);
    [Test] public async Task ParentDeleteCommittedAfterTheValidationIsCaught() => await Run(SchemaChangeSpanningTransactionScenarios.ParentDeleteCommittedAfterTheValidationIsCaught);
    [Test] public async Task ParentDeleteCommittedAfterCreateTableIsCaught() => await Run(SchemaChangeSpanningTransactionScenarios.ParentDeleteCommittedAfterCreateTableIsCaught);
    [Test] public async Task RowCommittedAfterTheIndexBackfillIsIndexed() => await Run(SchemaChangeSpanningTransactionScenarios.RowCommittedAfterTheIndexBackfillIsIndexed);
    [Test] public async Task IndexBuildWaitsForACommitInFlight() => await Run(SchemaChangeSpanningTransactionScenarios.IndexBuildWaitsForACommitInFlight);
    [Test] public async Task ValidationWaitsForACommitInFlight() => await Run(SchemaChangeSpanningTransactionScenarios.ValidationWaitsForACommitInFlight);
    [Test] public async Task WritesPlannedAfterTheChangeCommit() => await Run(SchemaChangeSpanningTransactionScenarios.WritesPlannedAfterTheChangeCommit);
    [Test] public async Task RowCommittedAfterAColumnBackfillReadsTheDefault() => await Run(SchemaChangeSpanningTransactionScenarios.RowCommittedAfterAColumnBackfillReadsTheDefault);

    /// <summary>
    /// The oldest statement decides. A later statement of the same transaction plans against the new
    /// shape, but the writes of the first one are still staged without the new index entry.
    /// </summary>
    [Test]
    public async Task TheFirstPinOfATableDecides()
    {
        (_, DatabaseDescriptor database, _) = await CreateDatabase();

        WriteShapeClock clock = new();
        TableSchema table = new() { Id = ObjectIdGenerator.Generate().ToString(), Name = "t" };

        KvTransaction tx = await database.Transactions.BeginAsync();

        tx.PinWriteShape(table, clock.Current);
        clock.Advance(table);
        tx.PinWriteShape(table, clock.Current);

        Assert.IsFalse(tx.TryEnterCommit(out IWriteShapeSource? changed));
        Assert.AreSame(table, changed);
        Assert.AreEqual(KvTransactionStatus.Active, tx.Status, "A refused gate must change nothing");
        Assert.IsFalse(tx.IsCommitInFlight);

        await database.Transactions.RollbackAsync(tx);
    }

    /// <summary>A statement that read the epoch after the change planned with the new shape.</summary>
    [Test]
    public async Task APinTakenAfterTheChangeHolds()
    {
        (_, DatabaseDescriptor database, _) = await CreateDatabase();

        WriteShapeClock clock = new();
        TableSchema table = new() { Id = ObjectIdGenerator.Generate().ToString(), Name = "t" };
        TableSchema other = new() { Id = ObjectIdGenerator.Generate().ToString(), Name = "u" };

        KvTransaction tx = await database.Transactions.BeginAsync();

        long epoch = clock.Advance(table);
        tx.PinWriteShape(table, clock.Current);

        // A later change of another table does not concern this transaction.
        clock.Advance(other);

        Assert.IsTrue(tx.TryEnterCommit(out _));
        Assert.IsTrue(tx.IsCommitInFlight);
        Assert.IsFalse(tx.IsCommitInFlightPlannedBefore([table], epoch), "The pin is not older than the change");
        Assert.IsTrue(tx.IsCommitInFlightPlannedBefore([table], epoch + 1));
        Assert.IsTrue(tx.IsCommitInFlightPlannedBefore([other, table], epoch + 1));
        Assert.IsFalse(tx.IsCommitInFlightPlannedBefore([other], long.MaxValue), "The transaction did not write that table");

        await database.Transactions.RollbackAsync(tx);
    }

    /// <summary>A table instance that the node replaced fails every pin, old or new.</summary>
    [Test]
    public async Task ARetiredTableRefusesEveryPin()
    {
        (_, DatabaseDescriptor database, _) = await CreateDatabase();

        WriteShapeClock clock = new();
        TableSchema table = new() { Id = ObjectIdGenerator.Generate().ToString(), Name = "t" };

        KvTransaction tx = await database.Transactions.BeginAsync();
        tx.PinWriteShape(table, clock.Current);

        clock.Retire(table);

        Assert.IsFalse(tx.TryEnterCommit(out IWriteShapeSource? changed));
        Assert.AreSame(table, changed);

        await database.Transactions.RollbackAsync(tx);
    }

    /// <summary>
    /// A client can leave after a commit that returned no outcome, and never come back. The schema
    /// change must not wait for that commit for ever: past the longest life of a session nothing the
    /// session started can still arrive, so the wait ends by itself. The age is compressed here by
    /// the same setting that compresses the release of abandoned holdings.
    /// </summary>
    [Test]
    public async Task TheWaitForAnAbandonedCommitIsBounded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { AbandonedTransactionReleaseAfterMs = 1_500 });

        await ForeignKeyAlterScenarios.Seed(executor, dbname, withUserIndex: false);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO weather (id, city) VALUES (60, 'atlantis')", null));

        // The commit passed its gate, and then the client left.
        Assert.IsTrue(tx.TryEnterCommit(out _));

        Task build = ForeignKeyAlterScenarios.Ddl(executor, dbname, "CREATE INDEX weather_city_late ON weather (city)");

        Assert.AreNotSame(build, await Task.WhenAny(build, Task.Delay(500)), "The build must wait while the session can still be alive");

        await build.WaitAsync(TimeSpan.FromSeconds(20));

        await database.Transactions.RollbackAsync(tx);
    }

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}

[TestFixture]
[NonParallelizable]
public sealed class TestSchemaChangeSpanningTransactionsCluster : SharedNodeBaseTest
{
    [Test] public async Task OrphanCommittedAfterTheValidationIsCaught() => await Run(SchemaChangeSpanningTransactionScenarios.OrphanCommittedAfterTheValidationIsCaught);
    [Test] public async Task ParentDeleteCommittedAfterTheValidationIsCaught() => await Run(SchemaChangeSpanningTransactionScenarios.ParentDeleteCommittedAfterTheValidationIsCaught);
    [Test] public async Task ParentDeleteCommittedAfterCreateTableIsCaught() => await Run(SchemaChangeSpanningTransactionScenarios.ParentDeleteCommittedAfterCreateTableIsCaught);
    [Test] public async Task RowCommittedAfterTheIndexBackfillIsIndexed() => await Run(SchemaChangeSpanningTransactionScenarios.RowCommittedAfterTheIndexBackfillIsIndexed);
    [Test] public async Task IndexBuildWaitsForACommitInFlight() => await Run(SchemaChangeSpanningTransactionScenarios.IndexBuildWaitsForACommitInFlight);
    [Test] public async Task ValidationWaitsForACommitInFlight() => await Run(SchemaChangeSpanningTransactionScenarios.ValidationWaitsForACommitInFlight);
    [Test] public async Task WritesPlannedAfterTheChangeCommit() => await Run(SchemaChangeSpanningTransactionScenarios.WritesPlannedAfterTheChangeCommit);
    [Test] public async Task RowCommittedAfterAColumnBackfillReadsTheDefault() => await Run(SchemaChangeSpanningTransactionScenarios.RowCommittedAfterAColumnBackfillReadsTheDefault);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}
