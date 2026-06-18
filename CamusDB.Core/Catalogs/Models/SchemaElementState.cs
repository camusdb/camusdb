/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// Online-schema-change visibility state of a single element (a column or an index),
/// following the CockroachDB/Yugabyte staged-rollout model. Adding rolls forward
/// <c>Absent → DeleteOnly → WriteOnly → Public</c>; dropping rolls back the same chain.
/// Staging an element through intermediate states (rather than flipping it instantly)
/// is what lets concurrent DML on other nodes avoid lost writes and half-built structures.
/// See the architecture documentation and <see cref="SchemaElementStateRules"/>.
///
/// Numeric values are persisted/serialized and must remain stable. Legacy elements with no
/// stored state default to <see cref="Public"/>.
/// </summary>
public enum SchemaElementState
{
    /// <summary>Not part of the schema. Invisible to reads and writes.</summary>
    Absent = 0,

    /// <summary>Visible only to delete operations. Not readable, not writable by insert/update.</summary>
    DeleteOnly = 1,

    /// <summary>Written by DML (insert/update/delete) but not yet visible to user reads.</summary>
    WriteOnly = 2,

    /// <summary>Fully live: readable and writable.</summary>
    Public = 3
}
