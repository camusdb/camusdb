
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// A point read registers with the Kahuna coordinator when the transaction folds every read, and
/// otherwise only for a key the transaction already wrote: that is the read whose answer must be the
/// transaction's own staging, which the coordinator verifies and a leader change can silently drop.
/// </summary>
[TestFixture]
public sealed class TestKvTransactionReadRegistration
{
    private static KvTransaction Pessimistic() => new(new HLCTimestamp(1, 10, 0), "tx-pessimistic");

    [Test]
    public void PessimisticReadOfAnUntouchedKey_IsUnregistered()
    {
        KvTransaction tx = Pessimistic();

        Assert.That(tx.FoldReads, Is.False);
        Assert.That(tx.ReadRegistrationKey("t/row/1"), Is.EqualTo(""));
        Assert.That(tx.ReadRegistrationKey(new[] { "t/row/1", "t/row/2" }), Is.EqualTo(""));
    }

    [Test]
    public void PessimisticReadOfAKeyTheTransactionWrote_RegistersUnderTheCoordinatorKey()
    {
        KvTransaction tx = Pessimistic();
        tx.TrackModified("t/row/1", KeyValueDurability.Persistent);

        Assert.That(tx.HasModified("t/row/1"), Is.True);
        Assert.That(tx.HasModified("t/row/2"), Is.False);
        Assert.That(tx.ReadRegistrationKey("t/row/1"), Is.EqualTo(tx.CoordinatorKey));
        Assert.That(tx.ReadRegistrationKey("t/row/2"), Is.EqualTo(""));
    }

    [Test]
    public void BatchRead_RegistersWhenAnyKeyWasWritten()
    {
        KvTransaction tx = Pessimistic();
        tx.TrackModifiedRange(new[] { "t/row/3" }, KeyValueDurability.Persistent);

        Assert.That(tx.ReadRegistrationKey(new[] { "t/row/1", "t/row/2" }), Is.EqualTo(""));
        Assert.That(tx.ReadRegistrationKey(new[] { "t/row/1", "t/row/3" }), Is.EqualTo(tx.CoordinatorKey));
    }

    [Test]
    public void OptimisticTransaction_RegistersEveryRead()
    {
        KvTransaction tx = new(new HLCTimestamp(1, 11, 0), "tx-optimistic", locking: KeyValueTransactionLocking.Optimistic);

        Assert.That(tx.FoldReads, Is.True);
        Assert.That(tx.ReadRegistrationKey("t/row/1"), Is.EqualTo(tx.CoordinatorKey));
        Assert.That(tx.ReadRegistrationKey(new[] { "t/row/1" }), Is.EqualTo(tx.CoordinatorKey));
    }
}
