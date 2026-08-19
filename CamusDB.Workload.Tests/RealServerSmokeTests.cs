/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using CamusDB.Workload.Client;
using CamusDB.Workload.Metrics;
using CamusDB.Workload.Operations;
using CamusDB.Workload.Scheduling;
using CamusDB.Workload.Workload;
using NUnit.Framework;

namespace CamusDB.Workload.Tests;

/// <summary>
/// End-to-end smoke against a real standalone server. Explicit and non-parallelizable so the normal
/// suite never starts an extra server: point it at an already-running standalone node via
/// <c>CAMUS_WORKLOAD_TEST_ENDPOINT</c> (gRPC port) and optionally <c>CAMUS_WORKLOAD_TEST_DATABASE</c>.
/// It seeds a tiny dataset, drives a short closed-loop mixed workload over gRPC, observes both reads
/// and committed optimistic writes, then reopens the database and verifies the persisted version
/// totals equal the committed write count.
/// </summary>
[TestFixture]
[Explicit("Requires a running standalone CamusDB server; set CAMUS_WORKLOAD_TEST_ENDPOINT.")]
[NonParallelizable]
public sealed class RealServerSmokeTests
{
    [Test]
    public async Task MixedWorkloadCommitsAndReconciles()
    {
        string? endpoint = Environment.GetEnvironmentVariable("CAMUS_WORKLOAD_TEST_ENDPOINT");
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            Assert.Ignore("Set CAMUS_WORKLOAD_TEST_ENDPOINT (gRPC endpoint) to run the real-server smoke test.");
            return;
        }

        string database = Environment.GetEnvironmentVariable("CAMUS_WORKLOAD_TEST_DATABASE")
                          ?? "workload_smoke";
        const string protocol = "grpc";
        const ulong seed = 1847;
        const long rows = 2_000;
        const int payload = 64;
        const int writesPerTx = 1;

        var dataset = new Dataset(seed, rows, payload);
        var ct = CancellationToken.None;

        // Setup (outside measurement) and capture the version baseline for delta reconciliation.
        long baselineVersionSum;
        await using (CamusConnection setup = await ConnectionSet.OpenSingleAsync(endpoint, database, protocol, ConnectionSettings.Default, ct))
        {
            await dataset.EnsureSchemaAsync(setup, ct);
            await dataset.SeedAsync(setup, batchSize: 250, ct);
            baselineVersionSum = await Reconciliation.ReadVersionSumAsync(setup, ct);
        }

        // Short mixed run.
        await using ConnectionSet connections = await ConnectionSet.OpenAsync(endpoint, database, protocol, connections: 4, ConnectionSettings.Default, ct);
        var writeOperation = new WriteOperation(connections, dataset, writesPerTx);
        var dispatcher = new OperationDispatcher(
            new ReadOperation(connections, dataset),
            writeOperation);
        var scheduler = new ClosedLoopScheduler(dispatcher, writesPerTx);
        WorkerState[] workers = WorkerState.Build(seed, workers: 8, readPercent: 60, rows);

        (RunMetrics metrics, _) = await scheduler.RunAsync(
            workers, warmup: TimeSpan.FromSeconds(1), measure: TimeSpan.FromSeconds(5), ct);

        Assert.That(metrics.CompletedRead, Is.GreaterThan(0), "expected committed reads");
        Assert.That(metrics.CompletedWrite, Is.GreaterThan(0), "expected committed optimistic writes");
        Assert.That(metrics.Conflicts, Is.EqualTo(0), "baseline must be conflict-free");

        // Reopen and reconcile persisted effect.
        await using CamusConnection verify = await ConnectionSet.OpenSingleAsync(endpoint, database, protocol, ConnectionSettings.Default, ct);
        ReconciliationResult reconciliation = await Reconciliation.VerifyAsync(
            verify, metrics, baselineVersionSum, writeOperation.CommittedRows,
            writeOperation.IndeterminateTxns, writesPerTx, expectFaults: false, rows, ct);

        Assert.That(reconciliation.Passed, Is.True,
            "persisted version totals and accounting must reconcile: " + string.Join("; ", reconciliation.Failures));
    }
}
