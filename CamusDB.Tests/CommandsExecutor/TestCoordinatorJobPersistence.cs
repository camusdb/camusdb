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

using Kahuna;
using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Transactions;
using CamusDB.Tests.Storage;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The durable record of a schema-change coordinator job: what may be written, and that the write
/// and the delete survive a commit that the transaction coordinator aborted.
///
/// <para>A resume matches a record to a live table through the table id only. A record without one
/// is deleted as stale by the next leader, and the element it described stays in its intermediate
/// state with no job left to finish it, so such a record must be refused when it is written.</para>
///
/// <para>The abort scenarios swap the database's transaction manager for one whose commits are
/// aborted a fixed number of times. The abort also rolls the real transaction back, as a real abort
/// does; an abort that left the write intent in place would block the replay on the key's lock and
/// test the lock timeout instead of the retry.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestCoordinatorJobPersistence : SharedNodeBaseTest
{
    private const string TableId = "T1";

    private const string ElementName = "score";

    /// <summary>Aborts the first <c>abortCount</c> commits, after rolling the real transaction back.</summary>
    private sealed class AbortingCommitKahuna(IKahuna inner, int abortCount) : DelegatingKahuna(inner)
    {
        private int abortsRemaining = abortCount;

        private int commitCalls;

        public int CommitCalls => Volatile.Read(ref commitCalls);

        public override async Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(
            TransactionHandle handle, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref commitCalls);

            if (Interlocked.Decrement(ref abortsRemaining) >= 0)
            {
                await inner.LocateAndRollbackTransaction(handle, cancellationToken);
                return (KeyValueResponseType.Aborted, null);
            }

            return await inner.LocateAndCommitTransaction(handle, cancellationToken);
        }
    }

    private static PersistedCoordinatorJob Job(string tableId = TableId, string elementName = ElementName, int attempts = 0) => new()
    {
        TableName = "robots",
        TableId = tableId,
        ElementName = elementName,
        TargetState = SchemaElementState.Public,
        Attempts = attempts,
    };

    /// <summary>
    /// The same database, with commits routed through <paramref name="fault"/>. It shares the id, so it
    /// reads and writes the same coordinator keys as <paramref name="database"/>.
    /// </summary>
    private DatabaseDescriptor WithFaultyCommits(DatabaseDescriptor database, AbortingCommitKahuna fault) =>
        new(database.Id, database.Name, database.Kahuna, new KvTransactionsManager(fault, Options), new(), Options);

    [Test]
    public async Task JobWithoutTableIdIsRefused()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Catalogs.PersistCoordinatorJobAsync(database, Job(tableId: "")))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInternalOperation, exception.Code);
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database),
            "A refused job must leave no record behind");
    }

    [Test]
    public async Task JobWithoutElementNameIsRefused()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Catalogs.PersistCoordinatorJobAsync(database, Job(elementName: "")))!;

        Assert.AreEqual(CamusDBErrorCodes.InvalidInternalOperation, exception.Code);
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database));
    }

    /// <summary>
    /// A resume that finds an added column still Absent re-creates it from the record, so every part
    /// of the column definition must come back. A field that is lost here creates the column without,
    /// for example, its sequence default, and its existing rows are then filled with NULL.
    /// </summary>
    [Test]
    public async Task ColumnDefinitionRoundTrips()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        PersistedCoordinatorJob written = Job();
        written.ColumnType = ColumnType.Integer64;
        written.ColumnNotNull = true;
        written.ColumnMaxLength = 40;
        written.ColumnArrayElementType = ColumnType.String;
        written.ColumnDefaultFunction = "gen_uuid_v7";
        written.ColumnDefaultSequenceId = "seq-1";
        written.ColumnIdentityAlways = true;
        written.ColumnNotNullConstraintName = "score_nn";
        written.ColumnComment = "a comment";
        written.ColumnStorage = ColumnStorageStrategy.External;

        await executor.Catalogs.PersistCoordinatorJobAsync(database, written);

        PersistedCoordinatorJob read = (await executor.Catalogs.LoadCoordinatorJobsAsync(database)).Single();
        Assert.AreEqual(ColumnType.Integer64, read.ColumnType);
        Assert.IsTrue(read.ColumnNotNull);
        Assert.AreEqual(40, read.ColumnMaxLength);
        Assert.AreEqual(ColumnType.String, read.ColumnArrayElementType);
        Assert.AreEqual("gen_uuid_v7", read.ColumnDefaultFunction);
        Assert.AreEqual("seq-1", read.ColumnDefaultSequenceId);
        Assert.IsTrue(read.ColumnIdentityAlways);
        Assert.AreEqual("score_nn", read.ColumnNotNullConstraintName);
        Assert.AreEqual("a comment", read.ColumnComment);
        Assert.AreEqual(ColumnStorageStrategy.External, read.ColumnStorage);
    }

    [Test]
    public async Task PersistSurvivesAbortedCommits()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        AbortingCommitKahuna fault = new(database.Kahuna.Kahuna, abortCount: 2);
        using DatabaseDescriptor faulty = WithFaultyCommits(database, fault);

        await executor.Catalogs.PersistCoordinatorJobAsync(faulty, Job(attempts: 3));

        Assert.AreEqual(3, fault.CommitCalls, "Two aborted commits, then the one that lands");

        List<PersistedCoordinatorJob> jobs = await executor.Catalogs.LoadCoordinatorJobsAsync(database);
        PersistedCoordinatorJob job = jobs.Single();
        Assert.AreEqual(TableId, job.TableId);
        Assert.AreEqual(ElementName, job.ElementName);
        Assert.AreEqual(3, job.Attempts);
    }

    [Test]
    public async Task DeleteSurvivesAbortedCommits()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await executor.Catalogs.PersistCoordinatorJobAsync(database, Job());

        AbortingCommitKahuna fault = new(database.Kahuna.Kahuna, abortCount: 2);
        using DatabaseDescriptor faulty = WithFaultyCommits(database, fault);

        await executor.Catalogs.DeleteCoordinatorJobAsync(faulty, TableId, ElementName);

        Assert.AreEqual(3, fault.CommitCalls, "Two aborted commits, then the one that lands");
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database));
    }

    /// <summary>
    /// The retry is bounded: when every commit aborts, the conflict reaches the caller and no record
    /// is written.
    /// </summary>
    [Test]
    public async Task PersistReportsTheConflictWhenEveryCommitAborts()
    {
        (_, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        AbortingCommitKahuna fault = new(database.Kahuna.Kahuna, abortCount: int.MaxValue);
        using DatabaseDescriptor faulty = WithFaultyCommits(database, fault);

        CamusDBException exception = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.Catalogs.PersistCoordinatorJobAsync(faulty, Job()))!;

        Assert.AreEqual(CamusDBErrorCodes.TransactionConflict, exception.Code);
        Assert.AreEqual(SerializableRetryHelper.DefaultMaxAttempts, fault.CommitCalls);
        Assert.IsEmpty(await executor.Catalogs.LoadCoordinatorJobsAsync(database));
    }
}
