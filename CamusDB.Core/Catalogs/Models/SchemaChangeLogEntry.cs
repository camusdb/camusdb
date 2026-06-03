
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kommander.Time;

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// A single schema-change <i>delta</i> — the unit of replication, ordering, and idempotency
/// for distributed DDL. Each entry is serialized, replicated through Kommander/Raft on the
/// database's schema-log partition, and applied in version order by every node so all nodes
/// converge on the same schema. See <c>docs/distributed-schema-architecture.md</c> §3.
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
}
