/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.Transactions;

/// <summary>
/// The tracking identity (<see cref="KvTransaction.ClientId"/>) must be minted by the local clock
/// for every tracked transaction, never taken from the Kahuna session id.
///
/// <para>The wire transaction handle carries only the identity's (physical, counter) pair — no node
/// component — and the HTTP/gRPC coordinator keys its tracking map on that pair. A Kahuna session
/// id is minted by the session's coordinator partition leader, which in a cluster is another node's
/// clock for most sessions. When session ids were used as tracking identities, a remotely-minted
/// session id could equal a locally-minted identity from the same millisecond, and the tracking map
/// silently bound two live transactions to one key. A statement or finalize for one transaction
/// then resolved the other: a transfer's write staged into a colliding autocommit read (which
/// committed it as a one-legged transaction), and a transfer's commit landed on a colliding empty
/// transaction and reported success while its own staged writes were never committed. One local
/// monotonic clock never repeats a (physical, counter) pair, which makes the key collision-free.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class TestTransactionTrackingIdentity
{
    private static async Task<(EmbeddedKahuna node, KvTransactionsManager mgr)> CreateAsync(
        string tag, CamusDBOptions? options = null, bool withLocalMinter = true)
    {
        EmbeddedKahuna node = new();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderAsync($"{tag}/warmup", CancellationToken.None);

        if (!withLocalMinter)
            return (node, new KvTransactionsManager(node.Kahuna, options ?? CamusDBOptions.Default));

        HLCTimestampMinter mint = new(node);
        return (node, new KvTransactionsManager(node.Kahuna, options ?? CamusDBOptions.Default, mint.Mint));
    }

    /// <summary>Supplies the local-HLC minter used for every tracked identity.</summary>
    private sealed class HLCTimestampMinter(EmbeddedKahuna node)
    {
        public Kommander.Time.HLCTimestamp Mint(Kommander.Time.HLCTimestamp? _) =>
            node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId());
    }

    [Test]
    public async Task EagerBegin_TrackingIdentityIsLocallyMinted_NotTheSessionId()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("trackid-eager");
        await using EmbeddedKahuna _ = node;

        KvTransaction tx = await mgr.BeginAsync();

        Assert.Multiple(() =>
        {
            Assert.That(tx.TransactionId.IsNull(), Is.False, "eager begin must open a Kahuna session");
            Assert.That(tx.ClientId.IsNull(), Is.False, "the tracking identity must exist");
            Assert.That(tx.ClientId, Is.Not.EqualTo(tx.TransactionId),
                "the tracking identity must be a separate local mint, not the session id — a session id " +
                "can come from another node's clock and collide with local identities on the node-less " +
                "(physical, counter) wire pair");
        });

        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task PromotedReadOnly_TrackingIdentityIsLocallyMinted()
    {
        CamusDBOptions options = CamusDBOptions.Default with { KeyRangeShardingEnabled = true };
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("trackid-promoted", options);
        await using EmbeddedKahuna _ = node;

        KvTransaction tx = await mgr.BeginReadOnlyAsync(promote: true);

        Assert.Multiple(() =>
        {
            Assert.That(tx.TransactionId.IsNull(), Is.False, "a promoted read has a real session");
            Assert.That(tx.ClientId.IsNull(), Is.False);
            Assert.That(tx.ClientId, Is.Not.EqualTo(tx.TransactionId));
        });

        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task SerializableReadOnly_TrackingIdentityIsLocallyMinted()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("trackid-serro");
        await using EmbeddedKahuna _ = node;

        KvTransaction tx = await mgr.BeginAsync(
            CamusIsolationLevel.Serializable, CamusTransactionMode.ReadOnly);

        Assert.Multiple(() =>
        {
            Assert.That(tx.TransactionId.IsNull(), Is.False, "the RO snapshot keeps its Kahuna handle");
            Assert.That(tx.ClientId.IsNull(), Is.False);
            Assert.That(tx.ClientId, Is.Not.EqualTo(tx.TransactionId));
        });

        await mgr.RollbackAsync(tx);
    }

    [Test]
    public async Task ConcurrentBegins_MintDistinctTrackingIdentities()
    {
        (EmbeddedKahuna node, KvTransactionsManager mgr) = await CreateAsync("trackid-distinct");
        await using EmbeddedKahuna _ = node;

        KvTransaction a = await mgr.BeginAsync();
        KvTransaction b = await mgr.BeginAsync(deferStart: true);
        KvTransaction c = await mgr.BeginAsync();

        Assert.Multiple(() =>
        {
            Assert.That(a.ClientId, Is.Not.EqualTo(b.ClientId));
            Assert.That(b.ClientId, Is.Not.EqualTo(c.ClientId));
            Assert.That(a.ClientId, Is.Not.EqualTo(c.ClientId));
        });

        await mgr.RollbackAsync(a);
        await mgr.RollbackAsync(b);
        await mgr.RollbackAsync(c);
    }

    [Test]
    public async Task WithoutLocalMinter_FallsBackToTheSessionId()
    {
        // Fixtures that construct the manager without a local minter have exactly one clock (the
        // embedded node's), so the session id cannot collide with anything and remains a valid key.
        (EmbeddedKahuna node, KvTransactionsManager mgr) =
            await CreateAsync("trackid-fallback", withLocalMinter: false);
        await using EmbeddedKahuna _ = node;

        KvTransaction tx = await mgr.BeginAsync();

        Assert.That(tx.ClientId, Is.EqualTo(tx.TransactionId));

        await mgr.RollbackAsync(tx);
    }
}
