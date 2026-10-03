/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Layout and paging constants shared by the per-table KV access classes
/// (<see cref="KvTableStore"/> and its collaborators). They live in one place because a change to
/// any of them must be applied to every path at once: the key layout, the lock bounds and the raw
/// scan bounds all have to agree, or a scan and the lock that fences it stop covering the same keys.
/// </summary>
internal static class KvStoreConstants
{
    /// <summary>
    /// Width of a row id in a KV key. <c>ObjectIdValue</c> renders as exactly 24 lowercase hex
    /// characters, and every non-unique index key ends with one, so index scans slice the row id off
    /// by this fixed length rather than by searching for a separator.
    /// </summary>
    internal const int RowIdHexLength = 24;

    /// <summary>Page size requested from Kahuna for every range scan this store issues.</summary>
    internal const int DefaultPageSize = 512;

    /// <summary>
    /// Page size of a foreign-key existence probe. The probe stops at the first live entry, so a full
    /// page would transfer up to <see cref="DefaultPageSize"/> keys to answer one yes-or-no question
    /// for every parent that has many children. A small page keeps that cost flat; entries the page
    /// skips (the transaction's own deletes, tombstones on a branch) only cost another round trip.
    /// </summary>
    internal const int ForeignKeyProbePageSize = 16;

    /// <summary>
    /// How many foreign-key rendezvous locks one statement acquires at the same time. Each lock is one
    /// round trip to the parent's partition leader, and Kahuna has no batched shared-lock call, so a
    /// statement that references many parents would otherwise wait for them one after another. The
    /// bound keeps one large statement from flooding the parent partition.
    /// </summary>
    internal const int ForeignKeyLockConcurrency = 16;

    /// <summary>
    /// How many parent-side probes of a child index one statement runs at the same time. Each probe is
    /// one round trip to the child's partition, so a DELETE of many referenced parents would otherwise
    /// wait for them one after another: measured, 100 sequential probes added about 60% to a 100-row
    /// DELETE. The bound keeps one large statement from flooding the child partition.
    /// </summary>
    internal const int ForeignKeyProbeConcurrency = 16;

    /// <summary>
    /// Upper-bound sentinel appended to the encoded last value for non-unique index keys.
    /// Non-unique stored key = "{encodedValue}{rowId24}" where rowId24 is exactly 24 lowercase
    /// hex chars (code points 0x0030-0x0066). The sentinel U+FFFF is the highest BMP code point
    /// and exceeds every character KeyEncoder can emit:
    /// <list type="bullet">
    ///   <item>Integer64 / Float64 / Bool: uppercase hex digits 0x0030-0x0046</item>
    ///   <item>String / Id: ordered ASCII U+0002-U+007F excluding '/', plus the field
    ///     terminator pair U+0000 U+0001 (all far below U+FFFF)</item>
    ///   <item>NULL marker: 0x0030 ('0'); Present marker: 0x0031 ('1')</item>
    /// </list>
    /// If KeyEncoder ever emits a character greater than or equal to U+FFFF (surrogates are illegal
    /// in C# strings; a future supplementary-plane encoding would need two code units) this sentinel
    /// would under-cover the upper bound, letting phantom inserts escape the range lock.
    /// </summary>
    internal const char IndexKeySentinel = '￿';
}
