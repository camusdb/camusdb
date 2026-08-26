/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// What an <see cref="OrphanTableRecord"/> is protecting. Numeric values are persisted, so they must
/// stay stable, and a record written before this discriminator existed deserializes as
/// <see cref="DroppedTable"/> — which is what those records are.
/// </summary>
public enum OrphanKind
{
    /// <summary>
    /// A relation that was dropped. It is absent from the live schema, and its retained rows live
    /// under its own preserved id. Recovery reattaches that id under a new name.
    /// </summary>
    DroppedTable = 0,

    /// <summary>
    /// One physical contents generation that a <b>still-live</b> relation stopped reading — what a
    /// <c>TRUNCATE</c> leaves behind.
    ///
    /// <para>The relation itself is untouched: same id, same name, same schema, and on a first
    /// truncate the retired key-space is named by that very id. Nothing about the live relation can
    /// therefore be used to decide this record's fate; only
    /// <see cref="OrphanTableRecord.RelinkTargetId"/> can.</para>
    /// </summary>
    RetiredContents = 1,
}
