/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading;
using NUnit.Framework;

namespace CamusDB.Tests;

/// <summary>
/// Assembly-wide one-time setup. Raises the thread pool minimum thread count so that
/// in-process Raft cluster tests — which run many concurrent actors and fire callbacks
/// via Task.Run — do not hit thread pool starvation when the full test suite runs
/// sequentially. Without this, the pool injector rate (~1 thread/second) means 10+
/// queued Tasks can take 10+ seconds to start, causing spurious timeouts in cluster tests.
/// </summary>
[SetUpFixture]
public sealed class GlobalTestSetup
{
    [OneTimeSetUp]
    public void Configure()
    {
        ThreadPool.GetMinThreads(out int workerMin, out int completionMin);
        int target = Math.Max(workerMin, 32);
        ThreadPool.SetMinThreads(target, completionMin);
    }
}
