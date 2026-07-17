/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Kind marker stored as the first byte of every CamusDB value in Kahuna.
/// Distinguishes a live value from a tombstone and from a genuine Kahuna DoesNotExist miss —
/// a three-way distinction that Kahuna's own response type cannot express (DoesNotExist vs.
/// tombstone look identical at the Kahuna layer).
/// </summary>
public enum BranchKvKind : byte
{
    /// <summary>Live value; remaining bytes are the serialized payload.</summary>
    Value = 0x01,

    /// <summary>
    /// Logical delete or suppressed inherited entry. No payload follows.
    /// Physical deletes (<c>LocateAndTryDeleteKeyValue</c>) are reserved for DROP TABLE /
    /// DROP DATABASE lifecycle cleanup.
    /// </summary>
    Tombstone = 0x02,
}

/// <summary>
/// Result of decoding a stored Kahuna value's envelope. <see cref="Payload"/> is a
/// <see cref="ReadOnlyMemory{T}"/> slice of the caller-owned storage array (the kind byte peeled off
/// with no copy); it is only meaningful when <see cref="HasPayload"/> is true. The three storable
/// states map as: value present → <c>Kind == Value, HasPayload == true</c>; tombstone →
/// <c>Kind == Tombstone, HasPayload == false</c>; miss (Kahuna DoesNotExist or empty) →
/// <c>Kind == Value, HasPayload == false</c>. A separate <see cref="HasPayload"/> flag is required
/// because a zero-length payload and a miss both surface as an empty <see cref="ReadOnlyMemory{T}"/>.
/// </summary>
public readonly record struct BranchKvValue(BranchKvKind Kind, ReadOnlyMemory<byte> Payload, bool HasPayload)
{
    /// <summary>A Kahuna miss: no envelope, no payload. Distinct from a tombstone.</summary>
    public static BranchKvValue Miss => new(BranchKvKind.Value, default, false);
}

/// <summary>
/// Encodes and decodes the one-byte CamusDB envelope that prefixes every row and index
/// entry stored in Kahuna. The envelope allows read paths to distinguish three states:
/// <list type="bullet">
///   <item>Value — a payload exists at this key.</item>
///   <item>Tombstone — the key was logically deleted or an inherited entry was suppressed.</item>
///   <item>No-envelope / null — Kahuna DoesNotExist; the key was never written.</item>
/// </list>
/// Wire format: <c>[0] kind-byte, [1..N] payload (absent for Tombstone)</c>.
/// </summary>
public static class BranchKvCodec
{
    /// <summary>Prepends the Value marker to <paramref name="payload"/>.</summary>
    public static byte[] EncodeValue(byte[] payload)
    {
        byte[] result = new byte[1 + payload.Length];
        result[0] = (byte)BranchKvKind.Value;
        payload.CopyTo(result, 1);
        return result;
    }

    /// <summary>Returns a single-byte Tombstone record.</summary>
    public static byte[] EncodeTombstone() => [(byte)BranchKvKind.Tombstone];

    /// <summary>
    /// Composes the storage value for a secondary-index entry — the referenced row id as its 24
    /// lowercase-hex ASCII bytes, prefixed with the <see cref="BranchKvKind.Value"/> marker — directly
    /// into one 25-byte array. Byte-identical to
    /// <c>EncodeValue(Encoding.UTF8.GetBytes(rowId.ToString()))</c> but without the intermediate 24-char
    /// string and separate UTF-8 array those two calls would allocate.
    /// </summary>
    public static byte[] EncodeIndexRowId(ObjectIdValue rowId)
    {
        byte[] result = new byte[1 + 24];
        result[0] = (byte)BranchKvKind.Value;
        ObjectId.WriteHexAscii(result.AsSpan(1), rowId.a, rowId.b, rowId.c);
        return result;
    }

    /// <summary>
    /// Peels the kind byte from <paramref name="data"/> and returns the remaining payload as a
    /// zero-copy <see cref="ReadOnlyMemory{T}"/> slice of the same array (<c>data.AsMemory(1)</c>) —
    /// envelope removal no longer allocates a payload-sized copy. The caller must keep
    /// <paramref name="data"/> (the Kahuna-owned <c>ReadOnlyKeyValueEntry.Value</c>) alive for as long
    /// as it references the returned <see cref="BranchKvValue.Payload"/>; because CamusDB writes always
    /// allocate fresh arrays and never mutate stored buffers in place, the slice stays valid.
    /// Returns <see cref="BranchKvValue.Miss"/> when <paramref name="data"/> is null or empty.
    /// Throws <see cref="CamusDBException"/> with <see cref="CamusDBErrorCodes.SystemSpaceCorrupt"/>
    /// when the kind byte is unrecognized. CamusDB targets fresh databases only (no
    /// migration path), so an unrecognized byte means storage corruption or an unversioned
    /// legacy write, not a forwards-compatibility gap. A loud failure is preferable to
    /// silently stripping the wrong byte and producing garbage payloads.
    /// </summary>
    public static BranchKvValue Decode(byte[]? data)
    {
        if (data is null || data.Length == 0)
            return BranchKvValue.Miss;

        BranchKvKind kind = (BranchKvKind)data[0];
        if (kind != BranchKvKind.Value && kind != BranchKvKind.Tombstone)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Unrecognized BranchKvKind byte 0x{data[0]:X2} — possible storage corruption or non-enveloped legacy write");

        bool hasPayload = data.Length > 1;
        return new BranchKvValue(kind, hasPayload ? data.AsMemory(1) : default, hasPayload);
    }
}
