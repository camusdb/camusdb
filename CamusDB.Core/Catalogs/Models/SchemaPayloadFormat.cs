/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// How the bytes in <see cref="SchemaChangeLogEntry.Payload"/> are encoded. The value is not part
/// of the replicated entry: it is recovered from the entry's own byte frame when the entry is
/// decoded, and it tells <see cref="SchemaChangeLogEntry.GetPayload{T}"/> which JSON reader to use.
///
/// The format exists because payload bytes written before the framed entry format was introduced
/// are still sitting in Raft logs and WALs. Sniffing the payload cannot separate the two reliably —
/// a UTF-16 JSON document also begins with <c>0x7B</c> — so the format travels with the entry.
/// </summary>
public enum SchemaPayloadFormat
{
    /// <summary>UTF-8 JSON. Every payload built by this build.</summary>
    Utf8 = 0,

    /// <summary>UTF-16 JSON, written before the framed entry format existed.</summary>
    Utf16Legacy = 1
}
