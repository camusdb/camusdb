
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using NUnit.Framework;

using CamusDB.Core.Storage.Kv;

namespace CamusDB.Tests.Storage;

/// <summary>
/// Unit tests for the one-byte value envelope that prefixes every row and index entry stored
/// in Kahuna. The envelope must distinguish three states — a live value, a tombstone, and a
/// genuine miss — which Kahuna's own DoesNotExist response cannot express on its own.
/// </summary>
[TestFixture]
public sealed class TestBranchKvCodec
{
    [Test]
    public void EncodeValue_ThenDecode_RoundTripsPayload()
    {
        byte[] payload = [0x10, 0x20, 0x30, 0x40, 0x50];

        byte[] encoded = BranchKvCodec.EncodeValue(payload);
        BranchKvValue result = BranchKvCodec.Decode(encoded);

        Assert.AreEqual(BranchKvKind.Value, result.Kind);
        Assert.IsTrue(result.HasPayload);
        Assert.AreEqual(payload, result.Payload.ToArray());
    }

    [Test]
    public void EncodeValue_PrependsKindByte_WithoutMutatingPayloadBytes()
    {
        // First payload byte equal to the Tombstone marker must survive — the codec must read
        // the kind from the prefix only, never from payload content.
        byte[] payload = [(byte)BranchKvKind.Tombstone, 0x02, 0x02];

        byte[] encoded = BranchKvCodec.EncodeValue(payload);

        Assert.AreEqual((byte)BranchKvKind.Value, encoded[0]);
        Assert.AreEqual(payload.Length + 1, encoded.Length);

        BranchKvValue result = BranchKvCodec.Decode(encoded);
        Assert.AreEqual(BranchKvKind.Value, result.Kind);
        Assert.AreEqual(payload, result.Payload.ToArray());
    }

    [Test]
    public void EncodeTombstone_DecodesAsTombstone_WithNoPayload()
    {
        byte[] encoded = BranchKvCodec.EncodeTombstone();

        Assert.AreEqual(1, encoded.Length);
        Assert.AreEqual((byte)BranchKvKind.Tombstone, encoded[0]);

        BranchKvValue result = BranchKvCodec.Decode(encoded);
        Assert.AreEqual(BranchKvKind.Tombstone, result.Kind);
        Assert.IsFalse(result.HasPayload);
    }

    [Test]
    public void Decode_Null_IsTreatedAsMiss()
    {
        // A null Kahuna value (DoesNotExist) decodes to a null payload — the caller's miss signal.
        BranchKvValue result = BranchKvCodec.Decode(null);

        Assert.AreEqual(BranchKvKind.Value, result.Kind);
        Assert.IsFalse(result.HasPayload);
    }

    [Test]
    public void Decode_Empty_IsTreatedAsMiss()
    {
        BranchKvValue result = BranchKvCodec.Decode([]);

        Assert.AreEqual(BranchKvKind.Value, result.Kind);
        Assert.IsFalse(result.HasPayload);
    }

    [Test]
    public void EncodeValue_EmptyPayload_DecodesToNullPayload()
    {
        // A Value envelope wrapping a zero-length payload carries only the kind byte; the decoder
        // reports a null payload, which the store treats as a miss. Real rows/index values are
        // never empty, so this only guards the degenerate boundary.
        byte[] encoded = BranchKvCodec.EncodeValue([]);

        Assert.AreEqual(1, encoded.Length);

        BranchKvValue result = BranchKvCodec.Decode(encoded);
        Assert.AreEqual(BranchKvKind.Value, result.Kind);
        Assert.IsFalse(result.HasPayload);
    }

    [Test]
    public void EncodeValue_LargePayload_RoundTrips()
    {
        byte[] payload = Enumerable.Range(0, 4096).Select(i => (byte)(i % 256)).ToArray();

        BranchKvValue result = BranchKvCodec.Decode(BranchKvCodec.EncodeValue(payload));

        Assert.AreEqual(BranchKvKind.Value, result.Kind);
        Assert.AreEqual(payload, result.Payload.ToArray());
    }
}
