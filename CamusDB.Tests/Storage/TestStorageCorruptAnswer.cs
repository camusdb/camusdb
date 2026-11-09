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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

using Grpc.Core;
using Microsoft.Extensions.Logging;
using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.System;
using Kommander.Time;

using CamusDB.App.Controllers;
using CamusDB.App.Grpc;
using CamusDB.App.Models;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Config;
using CamusDB.Core.Config.Models;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Storage;

/// <summary>
/// A node with a damaged store answers <c>StorageCorrupt</c>. CamusDB must raise that as a
/// non-retryable error on every path, because most of its read code decodes any answer other than
/// <c>Get</c> as an absent key, and its scan paths used to translate a Kahuna failure into a retry.
/// A damaged key must never read as a missing row or as a transient failure.
/// </summary>
[NonParallelizable]
[TestFixture]
public sealed class TestStorageCorruptAnswer
{
    private static readonly ILoggerFactory LoggerFactory =
        Microsoft.Extensions.Logging.LoggerFactory.Create(builder => builder.AddFilter("Camus", LogLevel.Warning));

    [Test]
    public void GetRetry_RaisesTheDamage_AfterOneAttempt()
    {
        int calls = 0;

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await KahunaRetryPolicy.RetryOnMustRetry(
            () =>
            {
                calls++;
                return Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((KeyValueResponseType.StorageCorrupt, null));
            },
            CancellationToken.None))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.StorageCorrupted));
        Assert.That(calls, Is.EqualTo(1), "a damaged store gives the same answer again, so a retry only delays the error");
    }

    [Test]
    public void StatefulGetRetry_RaisesTheDamage_AfterOneAttempt()
    {
        int calls = 0;

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await KahunaRetryPolicy.RetryOnMustRetry(
            _ =>
            {
                calls++;
                return ValueTask.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((KeyValueResponseType.StorageCorrupt, null));
            },
            0,
            CancellationToken.None))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.StorageCorrupted));
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void TypeOnlyRetry_RaisesTheDamage_AfterOneAttempt()
    {
        int calls = 0;

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await KahunaRetryPolicy.RetryOnMustRetry(
            () =>
            {
                calls++;
                return Task.FromResult(KeyValueResponseType.StorageCorrupt);
            },
            CancellationToken.None))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.StorageCorrupted));
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public void SetRetry_RaisesTheDamage_AfterOneAttempt()
    {
        int calls = 0;

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await KahunaRetryPolicy.RetryOnMustRetry(
            () =>
            {
                calls++;
                return Task.FromResult((KeyValueResponseType.StorageCorrupt, 0L, HLCTimestamp.Zero));
            },
            CancellationToken.None))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.StorageCorrupted));
        Assert.That(calls, Is.EqualTo(1));
    }

    [Test]
    public async Task GetRetry_KeepsEveryOtherAnswer()
    {
        (KeyValueResponseType absent, _) = await KahunaRetryPolicy.RetryOnMustRetry(
            () => Task.FromResult<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>((KeyValueResponseType.DoesNotExist, null)),
            CancellationToken.None);

        Assert.That(absent, Is.EqualTo(KeyValueResponseType.DoesNotExist));
    }

    [TestCase(KeyValueResponseType.Get)]
    [TestCase(KeyValueResponseType.DoesNotExist)]
    [TestCase(KeyValueResponseType.MustRetry)]
    [TestCase(KeyValueResponseType.Errored)]
    public void TheCheck_PassesEveryOtherAnswer(KeyValueResponseType type)
    {
        Assert.DoesNotThrow(() => KahunaRetryPolicy.ThrowIfStorageCorrupt(type));
    }

    [Test]
    public void AScanOfADamagedStore_IsNotRetryable()
    {
        CamusDBException ex = KvScanFailure.Describe(
            new KahunaServerException("page refused", KeyValueResponseType.StorageCorrupt), "row scan of table t");

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.StorageCorrupted));
        Assert.That(ex.Message, Does.Contain("row scan of table t"));
    }

    [Test]
    public void AScanThatTimedOut_StaysRetryable()
    {
        CamusDBException ex = KvScanFailure.Describe(
            new KahunaServerException("page refused", KeyValueResponseType.MustRetry), "row scan of table t");

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry));
    }

    [Test]
    public async Task ATranslatedScan_RaisesTheDamage()
    {
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            await foreach (int _ in KvScanFailure.Translate(ThrowsDamage(), "row scan of table t"))
            {
            }
        })!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.StorageCorrupted));
        await Task.CompletedTask;

        static async IAsyncEnumerable<int> ThrowsDamage()
        {
            await Task.Yield();
            throw new KahunaServerException("page refused", KeyValueResponseType.StorageCorrupt);
#pragma warning disable CS0162 // The iterator needs a yield to compile as one.
            yield return 0;
#pragma warning restore CS0162
        }
    }

    [Test]
    public void TheErrorCode_MapsToItsTransportStatus()
    {
        Assert.That(CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.StorageCorrupted), Is.EqualTo(500));
        Assert.That(GrpcErrorMapper.GetGrpcStatus(CamusDBErrorCodes.StorageCorrupted), Is.EqualTo(StatusCode.DataLoss));
    }

    /// <summary>
    /// Holds every point read in the engine to the rule: the answer goes through a retry overload, which
    /// raises the damage, or through <see cref="KahunaRetryPolicy.ThrowIfStorageCorrupt"/>. A new read that
    /// does neither decodes a damaged key as a missing key, and nothing else would catch that.
    /// </summary>
    [Test]
    public void EveryPointRead_RaisesTheDamage()
    {
        string root = Path.Combine(FindRepositoryRoot(), "CamusDB.Core");
        Regex read = new(@"\.LocateAndTryGetValue\(", RegexOptions.Compiled);
        Regex funnel = new(@"RetryOnMustRetry|InvokeOrTransient|InvokeOrUnanswered", RegexOptions.Compiled);
        Regex statementEnd = new(@"[;{}]\s*$", RegexOptions.Compiled);
        List<string> unguarded = new();
        int reads = 0;

        foreach (string path in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
        {
            if (path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}") ||
                path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}"))
                continue;

            string[] lines = File.ReadAllLines(path);

            for (int i = 0; i < lines.Length; i++)
            {
                if (!read.IsMatch(lines[i]))
                    continue;

                reads++;

                int start = i;
                while (start > 0 && !statementEnd.IsMatch(lines[start - 1]))
                    start--;

                if (funnel.IsMatch(string.Join('\n', lines[start..(i + 1)])))
                    continue;

                int end = Math.Min(lines.Length, i + 25);
                if (!string.Join('\n', lines[i..end]).Contains(nameof(KahunaRetryPolicy.ThrowIfStorageCorrupt)))
                    unguarded.Add($"{Path.GetRelativePath(root, path)}:{i + 1}");
            }
        }

        Assert.That(reads, Is.GreaterThan(30), "the scan found too few reads, so its pattern no longer matches the code");
        Assert.That(unguarded, Is.Empty, "a point read decodes StorageCorrupt as an absent key");
    }

    [Test]
    public async Task AStoreFault_ClearsReadiness_AndIsNamed()
    {
        string dataPath = NewDataPath();

        try
        {
            await using Node node = await Node.StartAsync(dataPath);

            ClusterHealthResponse healthy = ClusterController.BuildHealth(node.Kahuna.Raft, node.Kahuna.Kahuna);
            Assert.That(healthy.Ready, Is.True);
            Assert.That(healthy.StorageFault, Is.Null);
            Assert.That(healthy.PartitionStorageFaults, Is.Null);

            FaultedKahuna faulted = new(node.Kahuna.Kahuna, new KahunaStorageHealth("Corruption: bad block in 000042.sst", null));
            ClusterHealthResponse failed = ClusterController.BuildHealth(node.Kahuna.Raft, faulted);
            Assert.That(failed.Ready, Is.False);
            Assert.That(failed.StorageFault, Does.Contain("000042.sst"));

            FaultedKahuna replaced = new(node.Kahuna.Kahuna, new KahunaStorageHealth(null, "The store was found empty."));
            Assert.That(ClusterController.BuildHealth(node.Kahuna.Raft, replaced).Ready, Is.False);

            ThrowingKahuna unreadable = new(node.Kahuna.Kahuna);
            ClusterHealthResponse closed = ClusterController.BuildHealth(node.Kahuna.Raft, unreadable);
            Assert.That(closed.Ready, Is.False, "a node that cannot read its storage health must not report ready");
            Assert.That(closed.StorageFault, Is.Not.Null);
        }
        finally
        {
            TryDelete(dataPath);
        }
    }

    [Test]
    public void TheLostEntriesPolicy_IsReadAndValidated()
    {
        ConfigDefinition config = new ConfigReader().Read("kahuna:\n  raft_lost_acknowledged_entries_policy: accept_and_log");
        Assert.That(config.Kahuna!.RaftLostAcknowledgedEntriesPolicy, Is.EqualTo("accept_and_log"));

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("kahuna:\n  raft_lost_acknowledged_entries_policy: ignore"))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("'kahuna.raft_lost_acknowledged_entries_policy'"));
    }

    [TestCase("accept_and_log", Kommander.LostAcknowledgedEntriesPolicy.AcceptAndLog)]
    [TestCase("quarantine", Kommander.LostAcknowledgedEntriesPolicy.Quarantine)]
    public void TheLostEntriesPolicy_ReachesTheNode(string value, Kommander.LostAcknowledgedEntriesPolicy expected)
    {
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/camusdb-policy-absent", new KahunaOptionsConfig { RaftLostAcknowledgedEntriesPolicy = value }, CamusDBOptions.Default);

        Assert.That(options.RaftLostAcknowledgedEntriesPolicy, Is.EqualTo(expected));
    }

    [Test]
    public void TheLostEntriesPolicy_DefaultsToKommandersQuarantine()
    {
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/camusdb-policy-absent", new KahunaOptionsConfig(), CamusDBOptions.Default);

        Assert.That(options.RaftLostAcknowledgedEntriesPolicy, Is.Null);
    }

    [Test]
    public void TheSnapshotReceiveCap_IsReadAndReachesTheNode()
    {
        ConfigDefinition config = new ConfigReader().Read("kahuna:\n  raft_snapshot_max_pending_bytes: 8589934592");
        Assert.That(config.Kahuna!.RaftSnapshotMaxPendingBytes, Is.EqualTo(8589934592L));

        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/camusdb-policy-absent", config.Kahuna, CamusDBOptions.Default);

        Assert.That(options.RaftSnapshotMaxPendingBytes, Is.EqualTo(8589934592L));
    }

    [Test]
    public void TheSnapshotStepTimeout_IsReadAndReachesTheNode()
    {
        ConfigDefinition config = new ConfigReader().Read("kahuna:\n  raft_snapshot_transfer_step_timeout_ms: 600000");
        Assert.That(config.Kahuna!.RaftSnapshotTransferStepTimeoutMs, Is.EqualTo(600000));

        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/camusdb-policy-absent", config.Kahuna, CamusDBOptions.Default);

        Assert.That(options.RaftSnapshotTransferStepTimeout, Is.EqualTo(TimeSpan.FromMinutes(10)));
    }

    [Test]
    public void TheSnapshotStepTimeout_MustBePositive()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read("kahuna:\n  raft_snapshot_transfer_step_timeout_ms: 0"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
    }

    [Test]
    public void TheSnapshotReceiveCap_DefaultsToKahunas()
    {
        EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(
            "/tmp/camusdb-policy-absent", new KahunaOptionsConfig(), CamusDBOptions.Default);

        Assert.That(options.RaftSnapshotMaxPendingBytes, Is.Null);
    }

    [TestCase("0")]
    [TestCase("-1")]
    public void TheSnapshotReceiveCap_MustBePositive(string value)
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => new ConfigReader().Read($"kahuna:\n  raft_snapshot_max_pending_bytes: {value}"))!;

        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidConfig));
        Assert.That(ex.Message, Does.Contain("'kahuna.raft_snapshot_max_pending_bytes'"));
    }

    /// <summary>
    /// The whole path, with real damage: rows in a RocksDB SST, one byte flipped on disk, and a SQL read
    /// that reaches it. The statement must fail with the non-retryable code, not return the rows around the
    /// damage, and the node must then report the damage in its readiness probe.
    ///
    /// <para>The two cases reach the damage by different paths, and each has its own translation. A short
    /// string stays inside its row, so the row scan meets the damaged block. A long string is stored
    /// out of line, so the batched read of its cell does. The expected text pins which path ran.</para>
    /// </summary>
    /// <summary>
    /// A partition with no leader answers a schema read with <c>MustRetry</c>. This happens after a store
    /// fail-stops at RF 1, and for a moment in any election. The open must fail with the retryable code:
    /// read as an absent key, the answer gives the open database an empty schema, and every statement
    /// then reports that a table with intact rows does not exist. Once a leader returns, the same node
    /// must open the database, so the failed load must not stay cached.
    /// </summary>
    [Test]
    public async Task AnOpenWithoutALeader_FailsAsRetryable_AndSucceedsOnceALeaderReturns()
    {
        string dataPath = NewDataPath();
        string dbname = "noleader" + Guid.NewGuid().ToString("n");

        try
        {
            await using (Node first = await Node.StartAsync(dataPath))
            {
                DatabaseDescriptor database = await first.Executor.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));

                KvTransaction ddl = await database.Transactions.BeginAsync();
                await first.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                    txnState: ddl, database: dbname,
                    sql: "CREATE TABLE blobs (id OID PRIMARY KEY NOT NULL, body STRING NOT NULL)",
                    parameters: null));
                await database.Transactions.CommitAsync(ddl);

                KvTransaction txn = await database.Transactions.BeginAsync();
                await first.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                    txnState: txn, database: dbname,
                    sql: "INSERT INTO blobs (id, body) VALUES (GEN_ID(), \"intact\")",
                    parameters: null));
                await database.Transactions.CommitAsync(txn);
                await first.Kahuna.FlushAsync();
            }

            await using Node second = await Node.StartAsync(dataPath);

            IRaft raft = second.Kahuna.Raft;
            List<int> partitions = raft.GetPartitionMap()
                .Where(range => range.State != RaftPartitionState.Removed && raft.HostsPartition(range.PartitionId))
                .Select(range => range.PartitionId)
                .ToList();

            foreach (int partition in partitions)
            {
                raft.SetCandidacyWithheld(partition, true);
                await raft.StepDownAsync(partition, CancellationToken.None);
            }

            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await second.Executor.OpenDatabase(dbname))!;
            Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.TransactionMustRetry), ex.Message);

            foreach (int partition in partitions)
                raft.SetCandidacyWithheld(partition, false);

            foreach (int partition in partitions)
            {
                using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(60));
                while (!await raft.AmILeader(partition, timeout.Token))
                    await Task.Delay(100, timeout.Token);
            }

            DatabaseDescriptor reopened = await second.Executor.OpenDatabase(dbname);
            Assert.That(await CountBlobs(second.Executor, reopened, dbname), Is.EqualTo(1));
        }
        finally
        {
            TryDelete(dataPath);
        }
    }

    [TestCase(1500, 256, "row scan")]
    [TestCase(256, 2048, "batch read")]
    public async Task ADamagedSst_FailsTheSelect_AndTheNodeReportsIt(int rows, int valueBytes, string expectedPath)
    {
        string dataPath = NewDataPath();
        string dbname = "corrupt" + Guid.NewGuid().ToString("n");

        try
        {
            await using (Node first = await Node.StartAsync(dataPath))
            {
                DatabaseDescriptor database = await first.Executor.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));

                KvTransaction ddl = await database.Transactions.BeginAsync();
                await first.Executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                    txnState: ddl, database: dbname,
                    sql: "CREATE TABLE blobs (id OID PRIMARY KEY NOT NULL, body STRING NOT NULL)",
                    parameters: null));
                await database.Transactions.CommitAsync(ddl);

                Random random = new(1729);

                for (int i = 0; i < rows; i++)
                {
                    byte[] bytes = new byte[valueBytes / 2];
                    random.NextBytes(bytes);

                    KvTransaction txn = await database.Transactions.BeginAsync();
                    await first.Executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                        txnState: txn, database: dbname,
                        sql: $"INSERT INTO blobs (id, body) VALUES (GEN_ID(), \"{Convert.ToHexString(bytes)}\")",
                        parameters: null));
                    await database.Transactions.CommitAsync(txn);
                }

                Assert.That(await CountBlobs(first.Executor, database, dbname), Is.EqualTo(rows));
                await first.Kahuna.FlushAsync();
            }

            // A clean close leaves the rows in RocksDB's log. The next open writes them to an SST, so a
            // boot and a close put them where a flipped byte reaches them.
            await using (Node flush = await Node.StartAsync(dataPath))
                await flush.Kahuna.FlushAsync();

            string store = Path.Combine(dataPath, "kv", EmbeddedKahunaOptionsBuilder.CurrentStorageRevision);
            FlipByteAtMiddle(LargestSst(store));

            await using Node second = await Node.StartAsync(dataPath);

            DatabaseDescriptor reopened = await second.Executor.OpenDatabase(dbname);

            CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
                await CountBlobs(second.Executor, reopened, dbname))!;
            Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.StorageCorrupted), ex.Message);
            Assert.That(ex.Message, Does.Contain(expectedPath));

            ClusterHealthResponse health = ClusterController.BuildHealth(second.Kahuna.Raft, second.Kahuna.Kahuna);
            Assert.That(health.Ready, Is.False);
            Assert.That(health.StorageFault, Does.Contain("Corruption:"));
        }
        finally
        {
            TryDelete(dataPath);
        }
    }

    private static async Task<int> CountBlobs(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();

        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(
            txnState: txn, database: dbname, sql: "SELECT id, body FROM blobs", parameters: null));

        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        return rows.Count;
    }

    private static string LargestSst(string store)
    {
        FileInfo? largest = new DirectoryInfo(store)
            .EnumerateFiles("*.sst", SearchOption.AllDirectories)
            .OrderByDescending(file => file.Length)
            .FirstOrDefault();

        Assert.That(largest, Is.Not.Null, $"no SST under {store}, so the test would damage nothing");
        return largest!.FullName;
    }

    private static void FlipByteAtMiddle(string path)
    {
        using FileStream stream = new(path, FileMode.Open, FileAccess.ReadWrite);

        long offset = stream.Length / 2;
        stream.Position = offset;
        int value = stream.ReadByte();

        stream.Position = offset;
        stream.WriteByte((byte)(value ^ 0xFF));
        stream.Flush(flushToDisk: true);
    }

    private static string NewDataPath()
    {
        string dataPath = Path.Combine(Path.GetTempPath(), "camusdb-corrupt-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(dataPath);
        return dataPath;
    }

    private static void TryDelete(string path)
    {
        try { Directory.Delete(path, recursive: true); } catch { }
    }

    private static string FindRepositoryRoot()
    {
        DirectoryInfo? dir = new(TestContext.CurrentContext.TestDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "CamusDB.sln")))
            dir = dir.Parent;

        Assert.That(dir, Is.Not.Null, "could not locate the repository root");
        return dir!.FullName;
    }

    /// <summary>Test-only: a node that reports the given store health and forwards everything else.</summary>
    private sealed class FaultedKahuna(IKahuna inner, KahunaStorageHealth health) : DelegatingKahuna(inner)
    {
        public override KahunaStorageHealth GetStorageHealth() => health;
    }

    /// <summary>Test-only: a node whose store health cannot be read at all.</summary>
    private sealed class ThrowingKahuna(IKahuna inner) : DelegatingKahuna(inner)
    {
        public override KahunaStorageHealth GetStorageHealth() => throw new InvalidOperationException("store health is unreadable");
    }

    /// <summary>One standalone RocksDB node, its database registry and its executor, closed in reverse order.</summary>
    private sealed class Node : IAsyncDisposable
    {
        private readonly DatabaseRegistry registry;

        private Node(EmbeddedKahuna kahuna, DatabaseRegistry registry, CommandExecutor executor)
        {
            Kahuna = kahuna;
            this.registry = registry;
            Executor = executor;
        }

        public EmbeddedKahuna Kahuna { get; }

        public CommandExecutor Executor { get; }

        public static async Task<Node> StartAsync(string dataPath)
        {
            EmbeddedKahunaOptions options = EmbeddedKahunaOptionsBuilder.BuildStandaloneRocksDb(dataPath, new KahunaOptionsConfig(), CamusDBOptions.Default);

            EmbeddedKahuna node = new(options, LoggerFactory);
            await node.StartAsync(CancellationToken.None);
            await node.WaitForLeaderAsync("warmup", CancellationToken.None);
            await node.FlushAsync();

            DatabaseRegistry registry = await DatabaseRegistry.OpenAsync(node, CamusDBOptions.Default);

            ILogger<ICamusDB> logger = LoggerFactory.CreateLogger<ICamusDB>();
            CommandExecutor executor = new(
                new CommandValidator(CamusDBOptions.Default), new CatalogsManager(logger), logger, CamusDBOptions.Default,
                sharedNode: node, registry: registry, isClusterMode: false);

            return new Node(node, registry, executor);
        }

        public async ValueTask DisposeAsync()
        {
            await Executor.DisposeAsync();
            await registry.DisposeAsync();
            await Kahuna.DisposeAsync();
        }
    }
}
