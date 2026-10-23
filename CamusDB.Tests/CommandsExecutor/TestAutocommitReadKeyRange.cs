/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using NUnit.Framework;

using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.App.Controllers;
using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Under key-range sharding the autocommit SELECT of the HTTP and gRPC transports gets the same
/// transaction as under hash routing: a zero-identity snapshot pinned at a local timestamp. It makes
/// no Kahuna round trip to begin or end, and it takes no range lock, so a writer neither refuses it nor
/// waits for it.
///
/// <para>The reads begin through <see cref="HttpTransactionCoordinator.BeginReadOnlyAsync"/>, the call
/// every autocommit read path makes, so a statement can be interleaved with a writer. Every writer
/// starts after the read began: an older in-flight writer is waited for by every snapshot read through
/// MVCC intent resolution, lock or no lock, and that rule is not what these tests are about.</para>
/// </summary>
[TestFixture]
// Serial: boots a per-test embedded node with two partitions for key-range routing.
[NonParallelizable]
public sealed class TestAutocommitReadKeyRange : BaseTest
{
    private const int Slots = 20;

    /// <summary>
    /// A hang guard on each statement. The engine's lock-wait deadline is raised far above it, so a
    /// statement that waits on a lock fails this bound instead of being refused with a conflict.
    /// </summary>
    private static readonly TimeSpan StatementBudget = TimeSpan.FromSeconds(5);

    private static readonly JsonSerializerOptions CamelCase = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

    protected override int NodeInitialPartitions => 2;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { KeyRangeShardingEnabled = true, LockWaitDeadlineMs = 30_000 };

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

    /// <summary>
    /// Creates <c>tokens</c> with an index on <c>slot</c> (an Integer64 index, so it is range-routed)
    /// and one row per slot.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecuteDdl(executor, dbname, "CREATE TABLE tokens (id INT64 PRIMARY KEY, slot INT64, token STRING)");
        await ExecuteDdl(executor, dbname, "CREATE INDEX tokens_slot ON tokens (slot)");

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            for (int slot = 1; slot <= Slots; slot++)
            {
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
                    $"INSERT INTO tokens (id, slot, token) VALUES ({slot}, {slot}, 'token-{slot}')", null));
            }

            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        return (dbname, database, executor);
    }

    /// <summary>Runs <paramref name="sql"/> in <paramref name="tx"/> inside the statement budget and returns the tokens.</summary>
    private static async Task<List<string>> ReadTokensPromptly(CommandExecutor executor, string dbname, KvTransaction tx, string sql)
    {
        long started = Stopwatch.GetTimestamp();

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbname, sql, null));

        List<string> tokens = [];

        await foreach (QueryResultRow row in cursor)
            tokens.Add(row.Row["token"].StrValue!);

        Assert.That(Stopwatch.GetElapsedTime(started), Is.LessThan(StatementBudget),
            "an autocommit read must not wait on a writer");

        return tokens;
    }

    /// <summary>Runs a write statement in <paramref name="tx"/> inside the statement budget.</summary>
    private static async Task WritePromptly(CommandExecutor executor, string dbname, KvTransaction tx, string sql)
    {
        long started = Stopwatch.GetTimestamp();

        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

        Assert.That(Stopwatch.GetElapsedTime(started), Is.LessThan(StatementBudget),
            "a writer must not wait on an autocommit read");
    }

    /// <summary>
    /// The transaction every autocommit read path begins has no identity in key-range mode, so it
    /// cannot own a lock and has nothing to finalize. The coordinator registers nothing for it.
    /// </summary>
    [Test]
    public async Task AutocommitReadBegin_IsAZeroIdentitySnapshot()
    {
        (string dbname, _, CommandExecutor executor) = await SetupAsync();
        HttpTransactionCoordinator coordinator = new(executor);

        KvTransaction tx = await coordinator.BeginReadOnlyAsync(dbname);

        Assert.Multiple(() =>
        {
            Assert.That(tx.TransactionId, Is.EqualTo(HLCTimestamp.Zero), "the autocommit read has no Kahuna identity");
            Assert.That(tx.ReadTimestamp.IsNull(), Is.False, "under the Serializable default the read is pinned");
            Assert.That(tx.IsReadOnly, Is.True);
            Assert.That(coordinator.ActiveCount, Is.Zero, "nothing is registered for the autocommit read");
        });
    }

    /// <summary>
    /// The read scans the whole row range and one slot through the index, and stays open. A younger
    /// pessimistic Serializable writer then updates that slot. With the shared range locks a promoted
    /// read used to hold until its commit, the writer's exclusive lock waited for the read; now the read
    /// holds nothing and the writer goes straight through. The read still sees its pinned instant.
    /// </summary>
    [Test]
    public async Task AutocommitRead_DoesNotMakeAYoungerWriterWait()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        HttpTransactionCoordinator coordinator = new(executor);

        KvTransaction read = await coordinator.BeginReadOnlyAsync(dbname);
        Assert.That((await ReadTokensPromptly(executor, dbname, read, "SELECT token FROM tokens")).Count, Is.EqualTo(Slots));
        CollectionAssert.AreEqual(new[] { "token-7" },
            await ReadTokensPromptly(executor, dbname, read, "SELECT token FROM tokens WHERE slot = 7"));
        Assert.AreEqual(0, read.GetAcquiredRangeLocks().Count, "the autocommit read took no range lock");

        KvTransaction writer = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, locking: KeyValueTransactionLocking.Pessimistic);
        try
        {
            await WritePromptly(executor, dbname, writer, "UPDATE tokens SET token = 'moved' WHERE slot = 7");
            await database.Transactions.CommitAsync(writer);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(writer);
        }

        CollectionAssert.AreEqual(new[] { "token-7" },
            await ReadTokensPromptly(executor, dbname, read, "SELECT token FROM tokens WHERE slot = 7"),
            "the read stays pinned at its instant after the writer commits");
        await coordinator.CommitOrReleaseAsync(database, read);

        KvTransaction next = await coordinator.BeginReadOnlyAsync(dbname);
        CollectionAssert.AreEqual(new[] { "moved" },
            await ReadTokensPromptly(executor, dbname, next, "SELECT token FROM tokens WHERE slot = 7"),
            "a later autocommit read sees the committed change");
    }

    /// <summary>
    /// A younger writer updates a slot and stays open with its exclusive range lock and live intents.
    /// The read then scans the whole row range and looks the slot up. A promoted read used to ask for a
    /// shared lock over that range and was refused with a conflict after the lock-wait deadline; now it
    /// asks for nothing and reads its pinned instant. The writer's commit is not fenced by the read.
    /// </summary>
    [Test]
    public async Task AutocommitRead_IsNotRefusedByAYoungerWritersLock()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();
        HttpTransactionCoordinator coordinator = new(executor);

        KvTransaction read = await coordinator.BeginReadOnlyAsync(dbname);

        KvTransaction writer = await database.Transactions.BeginAsync(
            CamusIsolationLevel.Serializable, locking: KeyValueTransactionLocking.Pessimistic);
        try
        {
            await WritePromptly(executor, dbname, writer, "UPDATE tokens SET token = 'moved' WHERE slot = 7");

            Assert.That((await ReadTokensPromptly(executor, dbname, read, "SELECT token FROM tokens")).Count, Is.EqualTo(Slots),
                "a full scan under the writer's lock returns every row");
            CollectionAssert.AreEqual(new[] { "token-7" },
                await ReadTokensPromptly(executor, dbname, read, "SELECT token FROM tokens WHERE slot = 7"),
                "the index lookup under the writer's lock reads the pinned instant");

            Assert.DoesNotThrowAsync(async () => await database.Transactions.CommitAsync(writer),
                "the writer's commit must not be fenced by the autocommit read");
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(writer);
        }

        Assert.AreEqual(0, read.GetAcquiredRangeLocks().Count, "the autocommit read took no range lock throughout");
        await coordinator.CommitOrReleaseAsync(database, read);
    }

    /// <summary>
    /// The real HTTP entry point: <c>/execute-sql-query</c> answers an autocommit SELECT under key-range
    /// sharding and leaves no transaction registered with the coordinator.
    /// </summary>
    [Test]
    public async Task ExecuteSqlQueryOverHttp_ReadsEveryRowAndRegistersNothing()
    {
        (string dbname, _, CommandExecutor executor) = await SetupAsync();
        HttpTransactionCoordinator coordinator = new(executor);
        PreparedStatementRegistry registry = new(Options);

        ExecuteSQLController controller = new(executor, coordinator, registry, logger, Options)
        {
            ControllerContext = Context(new { databaseName = dbname, sql = "SELECT token FROM tokens WHERE slot >= 1" })
        };

        JsonResult result = await controller.ExecuteSQLQuery();
        ExecuteSQLQueryResponse response = (ExecuteSQLQueryResponse)result.Value!;

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo("ok"), response.Message);
            Assert.That(response.Total, Is.EqualTo(Slots));
            Assert.That(coordinator.ActiveCount, Is.Zero, "the autocommit read registered no transaction");
        });
    }

    private static ControllerContext Context(object body)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(body, CamelCase));
        DefaultHttpContext http = new();
        http.Request.Body = new MemoryStream(bytes);
        http.Request.ContentLength = bytes.Length;
        http.Request.IsHttps = true;
        http.Response.Body = new MemoryStream();
        return new ControllerContext { HttpContext = http };
    }
}
