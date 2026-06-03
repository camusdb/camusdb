
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
    /// Reserved for replicated index creation. Index DDL currently flows through
    /// <c>SystemSchema</c> rather than this log — see architecture doc §7.3 / §8.
    /// </summary>
    AddIndex = 4,

    /// <summary>Reserved for replicated index removal (see <see cref="AddIndex"/>).</summary>
    DropIndex = 5,

    /// <summary>
    /// Advance a column/index across one adjacent online-schema state
    /// (payload: <c>SchemaElementStatePayload</c>). The building block of staged
    /// add/drop sequences — see <see cref="SchemaElementState"/>.
    /// </summary>
    SetElementState = 6
}
