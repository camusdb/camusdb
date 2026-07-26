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
        uint salt = (uint)(new DeterministicRandom(seed).NextUInt64() >> 32);
        BinaryPrimitives.WriteUInt32BigEndian(bytes, salt);
        BinaryPrimitives.WriteInt64BigEndian(bytes[4..], rowIndex);
        return Convert.ToHexStringLower(bytes);
    }
}
