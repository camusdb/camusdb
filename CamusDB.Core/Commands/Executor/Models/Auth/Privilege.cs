/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// A set of privileges a grant confers, as an independent bitmask. Stored on a <see cref="GrantRecord"/>
/// and unioned/subtracted by <c>GRANT</c>/<c>REVOKE</c>.
///
/// <para><see cref="All"/> is the OR of the concrete bits known <b>at compile time</b>, deliberately
/// frozen: a grant of <c>ALL PRIVILEGES</c> is expanded to this mask at grant time and stored, so
/// adding a new privilege bit later never silently widens an existing grant. A server-wide superuser
/// is modeled as a separate explicit user attribute (enforcement phase), not as an <see cref="All"/>
/// grant.</para>
/// </summary>
[Flags]
public enum Privilege
{
    None = 0,

    /// <summary><c>SELECT</c> — read rows.</summary>
    Select = 1 << 0,

    /// <summary><c>INSERT</c> — add rows.</summary>
    Insert = 1 << 1,

    /// <summary><c>UPDATE</c> — modify rows.</summary>
    Update = 1 << 2,

    /// <summary><c>DELETE</c> — remove rows.</summary>
    Delete = 1 << 3,

    /// <summary><c>CREATE TABLE</c> — create tables in the scoped database.</summary>
    CreateTable = 1 << 4,

    /// <summary><c>DROP</c> — drop tables in scope.</summary>
    Drop = 1 << 5,

    /// <summary><c>ALTER</c> — alter table schema in scope.</summary>
    Alter = 1 << 6,

    /// <summary><c>INDEX</c> — create/drop indexes in scope.</summary>
    Index = 1 << 7,

    /// <summary><c>CREATE</c> — database-level create (e.g. create the scoped database's objects).</summary>
    Create = 1 << 8,

    /// <summary>
    /// The union of every concrete privilege above. Frozen at compile time — see the type remarks for
    /// why this must not become a "future-inclusive" sentinel.
    /// </summary>
    All = Select | Insert | Update | Delete | CreateTable | Drop | Alter | Index | Create,
}
