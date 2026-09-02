
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

using Kommander.Time;
using CamusDB.Core.Catalogs.Replication;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// A single schema-change <i>delta</i> — the unit of replication, ordering, and idempotency
/// for distributed DDL. Each entry is serialized, replicated through Kommander/Raft on the
/// database's schema-log partition, and applied in version order by every node so all nodes
/// converge on the same schema. See the architecture documentation.
///
/// An entry is only valid to apply when the node is currently at <see cref="FromVersion"/>;
/// applying it advances the schema to <see cref="ToVersion"/> (always <c>FromVersion + 1</c>).
/// Deterministic object IDs are baked into <see cref="Payload"/> by the proposer and reused
/// verbatim on every node — IDs are never regenerated during apply.
/// </summary>
public sealed class SchemaChangeLogEntry
{
    /// <summary>Hybrid logical clock stamp, taken from the originating DDL transaction.</summary>
    public HLCTimestamp Ts { get; set; }

    /// <summary>Database this delta belongs to. Entries for other databases are ignored on apply.</summary>
    public string Database { get; set; } = "";

    /// <summary>Schema version this delta expects to apply onto (the chain's previous link).</summary>
    public long FromVersion { get; set; }

    /// <summary>Resulting schema version after applying this delta. Always <see cref="FromVersion"/> + 1.</summary>
    public long ToVersion { get; set; }

    /// <summary>The kind of change carried by this delta.</summary>
    public SchemaOp Op { get; set; }

    /// <summary>
    /// Op-specific serialized payload (e.g. <c>SchemaCreateTablePayload</c>,
    /// <c>SchemaAlterColumnPayload</c>, <c>SchemaElementStatePayload</c>), including the
    /// deterministic IDs assigned once by the proposer.
    /// </summary>
    public byte[] Payload { get; set; } = [];

    /// <summary>
    /// How <see cref="Payload"/> is encoded. Never replicated — the frame the entry arrived in
    /// says which form its bytes are, and <see cref="SchemaChangeLogEntryCodec.Decode"/> stamps it
    /// here. A freshly built entry is UTF-8, because every builder encodes its payload through
    /// <see cref="SchemaChangeLogEntryCodec.EncodePayload{T}"/>; an entry recovered from bytes
    /// written before the framed format carries <see cref="SchemaPayloadFormat.Utf16Legacy"/>
    /// instead, and only <see cref="GetPayload{T}"/> ever looks at the difference.
    /// </summary>
    [JsonIgnore]
    public SchemaPayloadFormat PayloadFormat { get; set; } = SchemaPayloadFormat.Utf8;

    /// <summary>
    /// Memoized deserialized form of <see cref="Payload"/>. A single apply touches the payload
    /// several times (idempotency check, delta apply, table-name resolution, checkpoint persist),
    /// and each used to pay a full deserialization; the cache makes those after the first free.
    /// Private field, so serialization (which walks public properties only) never sees it.
    /// </summary>
    private object? decodedPayload;

    /// <summary>
    /// Returns the deserialized payload, decoding <see cref="Payload"/> at most once per instance.
    /// Safe to share the cached object across readers: <see cref="Payload"/> is immutable after the
    /// entry is decoded, and no apply/idempotency path mutates the payload object (appliers copy
    /// its fields into schema-owned objects). A benign race may decode twice; last write wins.
    /// </summary>
    public T GetPayload<T>() where T : new()
    {
        if (decodedPayload is T cached)
            return cached;

        T payload = SchemaChangeLogEntryCodec.DecodePayload<T>(Payload, PayloadFormat, Op);

        decodedPayload = payload;
        return payload;
    }
}
