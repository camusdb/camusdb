
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.Catalogs;

/// <summary>
/// The "no replicated write under the schema lock" assertions must ask whether the <em>current
/// flow</em> holds the lock. Asking whether anyone does killed a Debug node under concurrent DDL:
/// a proposer that had correctly released the lock persisted its checkpoint while another
/// operation's delta was being applied under it.
/// </summary>
[TestFixture]
public class TestSchemaLockFlow
{
    [Test]
    public async Task TheHolderSeesItsOwnHold_FromAcquireToRelease()
    {
        using Schema schema = new();

        Assert.That(schema.IsHeldByCurrentFlow, Is.False);

        await schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            Assert.That(schema.IsHeldByCurrentFlow, Is.True);

            // Across a real suspension, and on whatever thread the continuation lands.
            await Task.Delay(10).ConfigureAwait(false);
            Assert.That(schema.IsHeldByCurrentFlow, Is.True);

            // A callee is the case the assertions exist for.
            Assert.That(await ReadFromCalleeAsync(schema).ConfigureAwait(false), Is.True);
        }
        finally
        {
            schema.ReleaseLock();
        }

        Assert.That(schema.IsHeldByCurrentFlow, Is.False);
        Assert.That(schema.LockDepth, Is.EqualTo(0));
    }

    [Test]
    public async Task AnotherFlowHoldingTheLock_IsNotAHoldOfThisFlow()
    {
        using Schema schema = new();

        TaskCompletionSource held = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        Task other = Task.Run(async () =>
        {
            await schema.AcquireLockAsync().ConfigureAwait(false);
            try
            {
                held.SetResult();
                await release.Task.ConfigureAwait(false);
            }
            finally
            {
                schema.ReleaseLock();
            }
        });

        await held.Task.ConfigureAwait(false);

        // Somebody holds it — and that is all the process-wide counter can say.
        Assert.That(schema.LockDepth, Is.EqualTo(1));
        Assert.That(schema.IsHeldByCurrentFlow, Is.False);

        release.SetResult();
        await other.ConfigureAwait(false);
    }

    [Test]
    public async Task AFlowWaitingForTheLock_DoesNotHoldItYet()
    {
        using Schema schema = new();

        await schema.AcquireLockAsync().ConfigureAwait(false);

        bool? seenWhileWaiting = null;
        Task waiter = Task.Run(async () =>
        {
            Task acquire = schema.AcquireLockAsync();
            seenWhileWaiting = schema.IsHeldByCurrentFlow;

            await acquire.ConfigureAwait(false);
            try
            {
                Assert.That(schema.IsHeldByCurrentFlow, Is.True);
            }
            finally
            {
                schema.ReleaseLock();
            }
        });

        await Task.Delay(50).ConfigureAwait(false);
        schema.ReleaseLock();
        await waiter.ConfigureAwait(false);

        Assert.That(seenWhileWaiting, Is.False);
        Assert.That(schema.LockDepth, Is.EqualTo(0));
    }

    [Test]
    public async Task WorkForkedUnderTheLock_SeesTheReleaseToo()
    {
        using Schema schema = new();

        TaskCompletionSource released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<(bool During, bool After)> forked;

        await schema.AcquireLockAsync().ConfigureAwait(false);
        try
        {
            forked = Task.Run(async () =>
            {
                bool during = schema.IsHeldByCurrentFlow;
                await released.Task.ConfigureAwait(false);
                return (during, schema.IsHeldByCurrentFlow);
            });

            await Task.Delay(20).ConfigureAwait(false);
        }
        finally
        {
            schema.ReleaseLock();
        }

        released.SetResult();
        (bool during, bool after) = await forked.ConfigureAwait(false);

        Assert.That(during, Is.True);
        Assert.That(after, Is.False);
    }

    private static async Task<bool> ReadFromCalleeAsync(Schema schema)
    {
        await Task.Yield();
        return schema.IsHeldByCurrentFlow;
    }
}
