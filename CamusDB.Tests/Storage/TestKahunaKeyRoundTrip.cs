
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Storage;

/// <summary>
/// T0.5 — End-to-end key safety spike.
///
/// Verifies two properties:
///   1. Prefix scans via LocateAndGetByBucket return keys in ascending ordinal order
///      when those keys were inserted out-of-order using KeyEncoder.Encode.
///   2. The codec's separator bytes (U+0000, U+0001, U+FFFF) that appear in
///      string-valued encoded keys survive a round-trip through Kahuna's in-memory
///      storage intact (point-read returns the exact key that was inserted).
/// </summary>
[TestFixture]
public sealed class TestKahunaKeyRoundTrip
{
    private static EmbeddedKahunaNode CreateNode() =>
        new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        });

    private static async Task SetKey(IKahuna kahuna, string key, string value)
    {
        (KeyValueResponseType response, _, _) = await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero,
            key,
            Encoding.UTF8.GetBytes(value),
            null,
            -1,
            KeyValueFlags.Set,
            0,
            KeyValueDurability.Persistent,
            CancellationToken.None
        );

        Assert.AreEqual(KeyValueResponseType.Set, response, $"Set failed for key {Readable(key)}");
    }

    // ----- Tests -------------------------------------------------------

    [Test]
    public async Task IntegerKeysScanInAscendingOrder()
    {
        await using EmbeddedKahunaNode node = CreateNode();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderForKeyAsync("t0/r/warmup", CancellationToken.None);

        // Prefix WITHOUT trailing slash: InversePrefixedStaticHash("t0/r/{key}", '/')
        // hashes "t0/r", and GetByBucket with prefix "t0/r" uses SimpleHash("t0/r") — same actor.
        const string bucketPrefix = "t0/r";
        const string keyPrefix = "t0/r/";

        // Ascending sample set covering negatives, zero, and positives.
        long[] ascendingLongs = { long.MinValue, -1000L, -1L, 0L, 1L, 1000L, long.MaxValue };

        // Insert in descending order to prove the scan is not just insertion order.
        for (int i = ascendingLongs.Length - 1; i >= 0; i--)
        {
            CompositeColumnValue cv = new(new[] { new ColumnValue(ColumnType.Integer64, ascendingLongs[i]) });
            await SetKey(node.Kahuna, keyPrefix + KeyEncoder.Encode(cv), $"row-{i}");
        }

        KeyValueGetByBucketResult result = await node.Kahuna.LocateAndGetByBucket(
            HLCTimestamp.Zero,
            bucketPrefix,
            KeyValueDurability.Persistent,
            CancellationToken.None
        );

        Assert.AreEqual(KeyValueResponseType.Get, result.Type);
        Assert.AreEqual(ascendingLongs.Length, result.Items.Count, "All inserted keys must appear in scan");

        List<string> returned = result.Items.Select(item => item.Item1).ToList();

        // Adjacent pairs must be strictly ascending.
        for (int i = 0; i + 1 < returned.Count; i++)
        {
            Assert.That(
                string.CompareOrdinal(returned[i], returned[i + 1]),
                Is.LessThan(0),
                $"Scan not sorted at [{i}] vs [{i + 1}]: {Readable(returned[i])} >= {Readable(returned[i + 1])}"
            );
        }

        // Semantic check: position i must correspond to ascendingLongs[i].
        for (int i = 0; i < ascendingLongs.Length; i++)
        {
            CompositeColumnValue cv = new(new[] { new ColumnValue(ColumnType.Integer64, ascendingLongs[i]) });
            string expected = keyPrefix + KeyEncoder.Encode(cv);
            Assert.AreEqual(expected, returned[i], $"Wrong key at scan position {i} for long={ascendingLongs[i]}");
        }
    }

    [Test]
    public async Task FloatKeysScanInAscendingOrder()
    {
        await using EmbeddedKahunaNode node = CreateNode();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderForKeyAsync("t0/f/warmup", CancellationToken.None);

        const string bucketPrefix = "t0/f";
        const string keyPrefix = "t0/f/";

        double[] ascendingDoubles = { double.MinValue, -1e9, -1.5, 0.0, 1.5, 1e9, double.MaxValue };

        // Insert shuffled.
        int[] insertOrder = { 4, 6, 1, 0, 3, 2, 5 };
        foreach (int idx in insertOrder)
        {
            CompositeColumnValue cv = new(new[] { new ColumnValue(ColumnType.Float64, ascendingDoubles[idx]) });
            await SetKey(node.Kahuna, keyPrefix + KeyEncoder.Encode(cv), $"frow-{idx}");
        }

        KeyValueGetByBucketResult result = await node.Kahuna.LocateAndGetByBucket(
            HLCTimestamp.Zero,
            bucketPrefix,
            KeyValueDurability.Persistent,
            CancellationToken.None
        );

        Assert.AreEqual(KeyValueResponseType.Get, result.Type);
        Assert.AreEqual(ascendingDoubles.Length, result.Items.Count);

        List<string> returned = result.Items.Select(item => item.Item1).ToList();

        for (int i = 0; i + 1 < returned.Count; i++)
        {
            Assert.That(
                string.CompareOrdinal(returned[i], returned[i + 1]),
                Is.LessThan(0),
                $"Float scan not sorted at [{i}] vs [{i + 1}]"
            );
        }
    }

    [Test]
    public async Task StringControlCharsInKeySurviveRoundTrip()
    {
        await using EmbeddedKahunaNode node = CreateNode();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderForKeyAsync("t0/s/warmup", CancellationToken.None);

        const string bucketPrefix = "t0/s";
        const string keyPrefix = "t0/s/";

        // These strings produce encoded keys whose bodies are pure ASCII hex ending in the
        // terminator (U+0000 U+0001). An embedded NUL becomes the hex body "0000" — it is content,
        // not the terminator, so it round-trips without escaping.
        string[] strings =
        {
            "",
            "hello",
            "a" + (char)0x0000,    // embedded NUL — encodes as hex "0061""0000", no escaping
            "z"
        };

        Dictionary<string, string> storedKeys = new();

        foreach (string s in strings)
        {
            CompositeColumnValue cv = new(new[] { new ColumnValue(ColumnType.String, s) });
            string encodedKey = KeyEncoder.Encode(cv);
            string fullKey = keyPrefix + encodedKey;
            await SetKey(node.Kahuna, fullKey, s);
            storedKeys[fullKey] = s;
        }

        // Point-read every key — if any control char were mangled the key would not be found.
        foreach (KeyValuePair<string, string> kvp in storedKeys)
        {
            (KeyValueResponseType response, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero,
                kvp.Key,
                -1,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                CancellationToken.None
            );

            Assert.AreEqual(
                KeyValueResponseType.Get,
                response,
                $"Point-read failed for string '{kvp.Value}', key {Readable(kvp.Key)}"
            );
            Assert.IsNotNull(entry, $"Null entry for key {Readable(kvp.Key)}");
        }

        // Prefix scan — all keys must come back and must contain the terminator bytes.
        KeyValueGetByBucketResult result = await node.Kahuna.LocateAndGetByBucket(
            HLCTimestamp.Zero,
            bucketPrefix,
            KeyValueDurability.Persistent,
            CancellationToken.None
        );

        Assert.AreEqual(KeyValueResponseType.Get, result.Type);
        Assert.AreEqual(strings.Length, result.Items.Count, "All string keys must appear in scan");

        char termLead = (char)0x0000;
        char termTail = (char)0x0001;

        foreach ((string returnedKey, _) in result.Items)
        {
            Assert.IsTrue(
                storedKeys.ContainsKey(returnedKey),
                $"Scan returned unexpected key: {Readable(returnedKey)}"
            );
            // Every present-encoded string ends with U+0000 U+0001.
            Assert.IsTrue(
                returnedKey.Contains(termLead) && returnedKey.Contains(termTail),
                $"Terminator bytes missing from key: {Readable(returnedKey)}"
            );
        }

        // The encoded String body is pure ASCII hex (C3b): the key for "a\0" must contain no
        // non-ASCII code unit — the embedded NUL is encoded as the hex digits "0000", not as a raw
        // U+0000 in content, so its UTF-8 byte order matches its UTF-16-ordinal order everywhere.
        string nulKey = KeyEncoder.Encode(
            new CompositeColumnValue(new[] { new ColumnValue(ColumnType.String, "a" + (char)0x0000) })
        );
        foreach (char c in nulKey)
            Assert.IsTrue(c == (char)0x0000 || c == (char)0x0001 || c < (char)0x0080,
                $"Encoded String key must be ASCII (plus the U+0000/U+0001 terminator): U+{(int)c:X4} in {Readable(nulKey)}");
    }

    [Test]
    public async Task MixedTypeCompositeKeysScanInCorrectOrder()
    {
        await using EmbeddedKahunaNode node = CreateNode();
        await node.StartAsync(CancellationToken.None);
        await node.WaitForLeaderForKeyAsync("t1/r/warmup", CancellationToken.None);

        const string bucketPrefix = "t1/r";
        const string keyPrefix = "t1/r/";

        // Pairs (Integer64, String) in ascending CompareTo order.
        (long l, string s)[] ascendingPairs =
        {
            (long.MinValue, ""),
            (-1L,           "a"),
            (0L,            ""),
            (0L,            "b"),
            (1L,            ""),
            (long.MaxValue, "z"),
        };

        List<CompositeColumnValue> ascendingValues = ascendingPairs
            .Select(p => new CompositeColumnValue(new[]
            {
                new ColumnValue(ColumnType.Integer64, p.l),
                new ColumnValue(ColumnType.String, p.s)
            }))
            .ToList();

        // Insert in a shuffled order.
        int[] insertOrder = { 3, 0, 5, 1, 4, 2 };
        foreach (int idx in insertOrder)
        {
            string fullKey = keyPrefix + KeyEncoder.Encode(ascendingValues[idx]);
            await SetKey(node.Kahuna, fullKey, $"row-{idx}");
        }

        KeyValueGetByBucketResult result = await node.Kahuna.LocateAndGetByBucket(
            HLCTimestamp.Zero,
            bucketPrefix,
            KeyValueDurability.Persistent,
            CancellationToken.None
        );

        Assert.AreEqual(KeyValueResponseType.Get, result.Type);
        Assert.AreEqual(ascendingValues.Count, result.Items.Count);

        List<string> returned = result.Items.Select(item => item.Item1).ToList();

        // Strictly ascending.
        for (int i = 0; i + 1 < returned.Count; i++)
        {
            Assert.That(
                string.CompareOrdinal(returned[i], returned[i + 1]),
                Is.LessThan(0),
                $"Composite scan not sorted at [{i}] vs [{i + 1}]"
            );
        }

        // Semantic correctness: position i matches ascendingValues[i].
        for (int i = 0; i < ascendingValues.Count; i++)
        {
            string expected = keyPrefix + KeyEncoder.Encode(ascendingValues[i]);
            Assert.AreEqual(expected, returned[i], $"Wrong composite key at scan position {i}");
        }
    }

    // Formats a string for display, escaping non-printable code units.
    private static string Readable(string key)
    {
        StringBuilder sb = new();
        foreach (char c in key)
        {
            if (c < 0x0020 || c == 0xFFFF)
                sb.Append($"\\u{(int)c:X4}");
            else
                sb.Append(c);
        }
        return sb.ToString();
    }
}
