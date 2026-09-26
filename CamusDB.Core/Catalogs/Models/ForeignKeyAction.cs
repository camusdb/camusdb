/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// What a foreign key does when a referenced parent row is deleted, or its referenced key is
/// updated, while child rows still point at it (<c>ON DELETE</c> / <c>ON UPDATE</c>).
///
/// <para>Only <see cref="NoAction"/> and <see cref="Restrict"/> are enforced today; both refuse the
/// change. The other members are persisted so that a later release can honour them without a schema
/// migration, and the DDL path refuses them until it does.</para>
///
/// <para>Persisted as an integer, like every enum in <c>MetaJsonContext</c>: append members only and
/// never renumber.</para>
/// </summary>
public enum ForeignKeyAction
{
    /// <summary>
    /// Refuse the change if child rows still reference the parent when the check runs. The default,
    /// as in PostgreSQL. Checks run at the end of the statement, so this behaves like
    /// <see cref="Restrict"/> until deferred checking exists.
    /// </summary>
    NoAction = 0,

    /// <summary>Refuse the change if child rows reference the parent.</summary>
    Restrict = 1,

    /// <summary>Delete (or re-key) the child rows along with the parent. Not enforced yet.</summary>
    Cascade = 2,

    /// <summary>Set the referencing columns of the child rows to NULL. Not enforced yet.</summary>
    SetNull = 3,

    /// <summary>Set the referencing columns of the child rows to their defaults. Not enforced yet.</summary>
    SetDefault = 4
}
