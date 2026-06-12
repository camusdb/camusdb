/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading;
using NUnit.Framework;

namespace CamusDB.Tests.Cluster;

/// <summary>
/// Assembly-wide one-time setup for the in-process cluster tests. Raises the thread pool minimum
/// thread count before any cluster fixture starts.
///
/// <para>
/// Every cluster test spins up multiple <c>EmbeddedKahuna</c> nodes, each running a Raft group per
/// partition plus Kahuna/CamusDB actors, and many of those fire continuation callbacks via
/// <c>Task.Run</c>. On a cold thread pool the injector only adds ~1 worker/second, so the burst of
/// runnable tasks at cluster bring-up and during a DDL 2PC queues for many seconds. When a follower's
/// schema-applied callback is delayed past the 30s <c>SchemaAckWaitTimeout</c>, the leader's
/// <c>WaitForSchemaAcksAsync</c> times out and <c>CreateTable</c> fails — the dominant cluster-test
/// flake (worse with more partitions = more Raft groups = more starvation pressure). Mirrors
/// <c>CamusDB.Tests.GlobalTestSetup</c>, which the unit-test assembly already has, but with a higher
/// floor because a 3-node in-process cluster behaves like three mini-servers (the real server sets
/// <c>SetMinThreads(1024, 512)</c> in <c>Program.cs</c>).
/// </para>
/// </summary>
[SetUpFixture]
public sealed class ClusterTestSetup
{
    [OneTimeSetUp]
    public void Configure()
    {
        ThreadPool.GetMinThreads(out int workerMin, out int completionMin);
        ThreadPool.SetMinThreads(Math.Max(workerMin, 256), Math.Max(completionMin, 256));
    }
}
