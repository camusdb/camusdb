/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;

namespace CamusDB.Workload.Util;

/// <summary>
/// Produces the workload's row ids as deterministic 24-character lowercase-hex strings, the form
/// CamusDB's <c>OID</c>/<c>Id</c> column type expects (12 bytes = 24 hex). Ids are a pure function of
/// <c>(seed, rowIndex)</c> so the driver never has to read ids back from an unordered scan to know
/// which rows exist — every worker can reconstruct the id of any row it owns. The high 4 bytes encode
/// a per-seed salt (so different seeds occupy disjoint id space); the low 8 bytes encode the row
/// index, guaranteeing uniqueness across the whole generated set.
/// </summary>
public static class RowIdFactory
{
    public static string ForRow(ulong seed, long rowIndex)
    {
        Span<byte> bytes = stackalloc byte[12];
        BinaryPrimitives.WriteUInt32BigEndian(bytes, SaltFor(seed));
        BinaryPrimitives.WriteInt64BigEndian(bytes[4..], rowIndex);
        return Convert.ToHexStringLower(bytes);
    }

    /// <summary>
    /// The per-seed salt that occupies the high 4 bytes of every id this seed produces.
    /// </summary>
    public static uint SaltFor(ulong seed) => (uint)(new DeterministicRandom(seed).NextUInt64() >> 32);

    /// <summary>
    /// Recovers the row index from an id read back off a scan, so a full-table scan can be joined to
    /// the driver's per-row bookkeeping without a side lookup table. Returns false — rather than a
    /// wrong index — for anything this seed did not produce: a wrong length, a non-hex character, a
    /// foreign salt, or a negative index. A caller that scans a table it believes it owns uses that
    /// false to say "this row is not mine", which is itself worth reporting.
    /// </summary>
    public static bool TryRowIndex(ulong seed, string? id, out long rowIndex)
    {
        rowIndex = -1;
        if (id is null || id.Length != 24)
            return false;

        Span<byte> bytes = stackalloc byte[12];
        for (int i = 0; i < 12; i++)
        {
            int hi = HexValue(id[i * 2]);
            int lo = HexValue(id[i * 2 + 1]);
            if (hi < 0 || lo < 0)
                return false;
            bytes[i] = (byte)((hi << 4) | lo);
        }

        if (BinaryPrimitives.ReadUInt32BigEndian(bytes) != SaltFor(seed))
            return false;

        long decoded = BinaryPrimitives.ReadInt64BigEndian(bytes[4..]);
        if (decoded < 0)
            return false;

        rowIndex = decoded;
        return true;
    }

    /// <summary>The value of one hex digit, or -1 for anything that is not one.</summary>
    private static int HexValue(char c) => c switch
    {
        >= '0' and <= '9' => c - '0',
        >= 'a' and <= 'f' => c - 'a' + 10,
        >= 'A' and <= 'F' => c - 'A' + 10,
        _ => -1,
    };
}
