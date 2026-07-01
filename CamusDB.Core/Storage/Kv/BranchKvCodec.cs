/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
    /// Peels the kind byte from <paramref name="data"/> and returns the remaining payload.
    /// Returns <c>(Value, null)</c> when <paramref name="data"/> is null or empty — the
    /// caller checks <c>payload == null</c> to detect a miss.
    /// Throws <see cref="CamusDBException"/> with <see cref="CamusDBErrorCodes.SystemSpaceCorrupt"/>
    /// when the kind byte is unrecognized. CamusDB targets fresh databases only (no
    /// migration path), so an unrecognized byte means storage corruption or an unversioned
    /// legacy write, not a forwards-compatibility gap. A loud failure is preferable to
    /// silently stripping the wrong byte and producing garbage payloads.
    /// </summary>
    public static (BranchKvKind kind, byte[]? payload) Decode(byte[]? data)
    {
        if (data is null || data.Length == 0)
            return (BranchKvKind.Value, null);

        BranchKvKind kind = (BranchKvKind)data[0];
        if (kind != BranchKvKind.Value && kind != BranchKvKind.Tombstone)
            throw new CamusDBException(
                CamusDBErrorCodes.SystemSpaceCorrupt,
                $"Unrecognized BranchKvKind byte 0x{data[0]:X2} — possible storage corruption or non-enveloped legacy write");

        return (kind, data.Length > 1 ? data[1..] : null);
    }
}
