/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;

namespace CamusDB.Tests.Catalogs;

/// <summary>
/// The DDL gate on its own: a DDL that names a held table waits for the hold, a DDL on another table
/// enters at once, an exclusive entry waits for every hold, and a job's hold of the gate is restored to
/// what its caller expects.
/// </summary>
[TestFixture]
public sealed class TestSchemaDdlGate
{
    private static readonly TimeSpan Soon = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan NotYet = TimeSpan.FromMilliseconds(300);

    private static async Task<bool> CompletesWithin(Task task, TimeSpan within)
        => ReferenceEquals(await Task.WhenAny(task, Task.Delay(within)), task);

    [Test]
    public async Task EnterForAnotherTableProceedsWhileATableIsHeld()
    {
        using SchemaDdlGate gate = new("db");

        await gate.Semaphore.WaitAsync();
        using SchemaDdlGate.StagedTableHold hold = gate.HoldTable("t1", "accounts", "index 'acct_balance'");
        gate.Semaphore.Release();

        Assert.That(gate.IsTableHeld("accounts"), Is.True);

        Task entered = gate.EnterAsync("holds");
        Assert.That(await CompletesWithin(entered, Soon), Is.True, "a DDL on another table enters while the hold lasts");
        Assert.That(gate.Semaphore.CurrentCount, Is.EqualTo(0), "it holds the semaphore");
        gate.Exit();
    }

    [Test]
    public async Task EnterForTheHeldTableWaitsUntilTheHoldEnds()
    {
        using SchemaDdlGate gate = new("db");

        await gate.Semaphore.WaitAsync();
        SchemaDdlGate.StagedTableHold hold = gate.HoldTable("t1", "accounts", "column 'tier'");
        gate.Semaphore.Release();

        // Names match the way the schema matches them: without regard to case.
        Task entered = gate.EnterAsync("ACCOUNTS");
        Assert.That(await CompletesWithin(entered, NotYet), Is.False, "a DDL on the held table waits");
        Assert.That(gate.Semaphore.CurrentCount, Is.EqualTo(1), "and it does not hold the semaphore while it waits");

        hold.Dispose();

        Assert.That(await CompletesWithin(entered, Soon), Is.True, "it enters once the hold ends");
        Assert.That(gate.IsTableHeld("accounts"), Is.False);
        gate.Exit();
    }

    [Test]
    public async Task EnterForSeveralTablesWaitsWhenAnyIsHeld()
    {
        using SchemaDdlGate gate = new("db");

        await gate.Semaphore.WaitAsync();
        SchemaDdlGate.StagedTableHold hold = gate.HoldTable("t2", "new_name", "index 'ix'");
        gate.Semaphore.Release();

        Task rename = gate.EnterAsync(["old_name", "new_name"]);
        Assert.That(await CompletesWithin(rename, NotYet), Is.False, "a rename onto a held name waits");

        hold.Dispose();
        Assert.That(await CompletesWithin(rename, Soon), Is.True);
        gate.Exit();
    }

    [Test]
    public async Task EnterExclusiveWaitsForEveryHold()
    {
        using SchemaDdlGate gate = new("db");

        await gate.Semaphore.WaitAsync();
        SchemaDdlGate.StagedTableHold first = gate.HoldTable("t1", "accounts", "index 'a'");
        SchemaDdlGate.StagedTableHold second = gate.HoldTable("t2", "holds", "column 'b'");
        gate.Semaphore.Release();

        Task exclusive = gate.EnterExclusiveAsync();
        Assert.That(await CompletesWithin(exclusive, NotYet), Is.False);

        first.Dispose();
        Assert.That(await CompletesWithin(exclusive, NotYet), Is.False, "one hold left is still a hold");

        second.Dispose();
        Assert.That(await CompletesWithin(exclusive, Soon), Is.True);
        Assert.That(gate.HeldTableCount, Is.EqualTo(0));
        gate.Exit();
    }

    [Test]
    public void HoldTableRequiresTheSemaphore()
    {
        using SchemaDdlGate gate = new("db");

        CamusDBException ex = Assert.Throws<CamusDBException>(() => gate.HoldTable("t1", "accounts", "index 'a'"))!;
        Assert.That(ex.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInternalOperation));
        Assert.That(gate.IsTableHeld("accounts"), Is.False);
    }

    [Test]
    public async Task HoldRefusesATableAnotherChangeHolds()
    {
        using SchemaDdlGate gate = new("db");

        await gate.Semaphore.WaitAsync();
        using SchemaDdlGate.StagedTableHold hold = gate.HoldTable("t1", "accounts", "index 'a'");

        CamusDBException ex = Assert.Throws<CamusDBException>(() => gate.HoldTable("t1", "Accounts", "column 'b'"))!;
        Assert.That(ex.Message, Does.Contain("index 'a'"));
        gate.Semaphore.Release();
    }

    [Test]
    public async Task DisposingTheGateWakesEveryWaiter()
    {
        SchemaDdlGate gate = new("db");

        await gate.Semaphore.WaitAsync();
        SchemaDdlGate.StagedTableHold hold = gate.HoldTable("t1", "accounts", "index 'a'");
        gate.Semaphore.Release();

        Task completion = hold.Completion;
        gate.Dispose();

        Assert.That(await CompletesWithin(completion, Soon), Is.True, "a database that closes ends its holds");
    }

    [Test]
    public async Task AHoldOfTheGateIsRestoredToWhatTheCallerHad()
    {
        using SchemaDdlGate gate = new("db");

        // The caller entered the gate and hands the job over: the job may release and re-take it, and
        // must leave it held.
        await gate.Semaphore.WaitAsync();
        SchemaDdlGateHold heldByCaller = new(gate, heldByCaller: true);
        Assert.That(heldByCaller.IsHeld, Is.True);

        heldByCaller.Release();
        Assert.That(gate.Semaphore.CurrentCount, Is.EqualTo(1));
        heldByCaller.Release();
        Assert.That(gate.Semaphore.CurrentCount, Is.EqualTo(1), "a second release is a no-op");

        await heldByCaller.RestoreAsync();
        Assert.That(heldByCaller.IsHeld, Is.True);
        Assert.That(gate.Semaphore.CurrentCount, Is.EqualTo(0), "restored held");
        gate.Semaphore.Release();

        // A caller that held nothing, such as the resume, gets it back released.
        SchemaDdlGateHold heldByNobody = new(gate, heldByCaller: false);
        await heldByNobody.AcquireAsync();
        await heldByNobody.AcquireAsync();
        Assert.That(gate.Semaphore.CurrentCount, Is.EqualTo(0), "a second acquire is a no-op");

        await heldByNobody.RestoreAsync();
        Assert.That(heldByNobody.IsHeld, Is.False);
        Assert.That(gate.Semaphore.CurrentCount, Is.EqualTo(1), "restored released");
    }
}
