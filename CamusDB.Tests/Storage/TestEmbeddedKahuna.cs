
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Storage;

/// <summary>
/// T2.1 — EmbeddedKahuna wrapper.
///
/// Verifies that the wrapper correctly:
///   1. Constructs and starts with default (in-memory) options.
///   2. Exposes a usable IKahuna after StartAsync + WaitForLeaderAsync.
///   3. Performs a Set/Get round-trip through the exposed IKahuna.
///   4. Disposes cleanly via IAsyncDisposable.
/// </summary>
[TestFixture]
public sealed class TestEmbeddedKahuna
{
    [Test]
    public async Task ConstructStartDisposeWithDefaultOptions()
    {
        await using EmbeddedKahuna kahuna = new();

        await kahuna.StartAsync(CancellationToken.None);

        Assert.IsNotNull(kahuna.Kahuna);
        Assert.IsNotNull(kahuna.Raft);
    }

    [Test]
    public async Task KahunaIsUsableAfterStart()
    {
        await using EmbeddedKahuna kahuna = new();

        await kahuna.StartAsync(CancellationToken.None);
        await kahuna.WaitForLeaderAsync("test/warmup", CancellationToken.None);

        const string key = "test/k1";
        byte[] value = Encoding.UTF8.GetBytes("hello-camusdb");

        (KeyValueResponseType setType, _, _) = await kahuna.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero,
            key,
            value,
            null,
            -1,
            KeyValueFlags.Set,
            0,
            KeyValueDurability.Persistent,
            CancellationToken.None
        );

        Assert.AreEqual(KeyValueResponseType.Set, setType);

        (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await kahuna.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero,
            key,
            -1,
            KeyValueDurability.Persistent,
            CancellationToken.None
        );

        Assert.AreEqual(KeyValueResponseType.Get, getType);
        Assert.IsNotNull(entry);
        Assert.AreEqual(value, entry!.Value);
    }

    [Test]
    public async Task MultipleSetGetRoundTrips()
    {
        await using EmbeddedKahuna kahuna = new();

        await kahuna.StartAsync(CancellationToken.None);
        await kahuna.WaitForLeaderAsync("t1/warmup", CancellationToken.None);

        for (int i = 0; i < 5; i++)
        {
            string key = $"t1/row/{i}";
            byte[] value = Encoding.UTF8.GetBytes($"value-{i}");

            (KeyValueResponseType setType, _, _) = await kahuna.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero,
                key,
                value,
                null,
                -1,
                KeyValueFlags.Set,
                0,
                KeyValueDurability.Persistent,
                CancellationToken.None
            );

            Assert.AreEqual(KeyValueResponseType.Set, setType, $"Set failed for key {key}");
        }

        for (int i = 0; i < 5; i++)
        {
            string key = $"t1/row/{i}";
            byte[] expected = Encoding.UTF8.GetBytes($"value-{i}");

            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await kahuna.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero,
                key,
                -1,
                KeyValueDurability.Persistent,
                CancellationToken.None
            );

            Assert.AreEqual(KeyValueResponseType.Get, getType, $"Get failed for key {key}");
            Assert.IsNotNull(entry);
            Assert.AreEqual(expected, entry!.Value, $"Value mismatch for key {key}");
        }
    }

    [Test]
    public async Task WaitForLeaderReturnsNonEmptyLeader()
    {
        await using EmbeddedKahuna kahuna = new();

        await kahuna.StartAsync(CancellationToken.None);
        string leader = await kahuna.WaitForLeaderAsync("any/key", CancellationToken.None);

        Assert.IsFalse(string.IsNullOrEmpty(leader), "Leader node name must not be empty");
    }
}
