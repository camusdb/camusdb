
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// The kind of change carried by a <see cref="SchemaChangeLogEntry"/>. The numeric values
/// are persisted/serialized, so they must remain stable. <c>CatalogsManager.ApplySchemaDelta</c>
/// dispatches on this enum.
/// </summary>
public enum SchemaOp
{
    /// <summary>Create a new table (payload: <c>SchemaCreateTablePayload</c>).</summary>
    CreateTable = 0,

    /// <summary>Remove a table (payload: <c>SchemaDropTablePayload</c>).</summary>
    DropTable = 1,

    /// <summary>Add a column to a table (payload: <c>SchemaAlterColumnPayload</c>).</summary>
    AddColumn = 2,

    /// <summary>Remove a column from a table (payload: <c>SchemaAlterColumnPayload</c>).</summary>
    DropColumn = 3,

    /// <summary>
    /// Add or rebuild an index (payload: <c>SchemaIndexPayload</c>). The proposer runs the
    /// full backfill locally first (shared KV means the index data is immediately visible to
    /// all nodes), then replicates the completed <c>TableIndexSchema</c> so every node
    /// updates its in-memory <c>TableSchema.Indexes</c> and evicts its cached
    /// <c>TableDescriptor</c>. Does not bump <c>TableSchema.Version</c>.
    /// </summary>
    AddIndex = 4,

    /// <summary>
    /// Remove an index (payload: <c>SchemaIndexPayload</c>). Idempotent on apply.
    /// Does not bump <c>TableSchema.Version</c>.
    /// </summary>
    DropIndex = 5,

    /// <summary>
    /// Advance a column/index across one adjacent online-schema state
    /// (payload: <c>SchemaElementStatePayload</c>). The building block of staged
    /// add/drop sequences — see <see cref="SchemaElementState"/>.
    /// </summary>
    SetElementState = 6
}
