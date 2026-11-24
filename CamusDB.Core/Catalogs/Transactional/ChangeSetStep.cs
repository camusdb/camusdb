/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Transactional;

/// <summary>
/// The points of <see cref="ChangeSetCommitter"/> at which a fault test can stop, fail or disturb the driver
/// of a change set. Each one follows a durable effect and precedes the next, so together they cover every
/// state in which a crash can leave a change set. Production code never reads them.
/// </summary>
internal enum ChangeSetStep
{
    /// <summary>The decision key, the new layouts and the purge records are staged in the transaction. Nothing is announced.</summary>
    Staged,

    /// <summary>Every live node applied the pending entry. The KV commit request has not left the node.</summary>
    Announced,

    /// <summary>The KV commit landed. The publish is not proposed.</summary>
    RowsCommitted,

    /// <summary>Every live node applied the publish. The work after it has not run, and the DDL lock is held.</summary>
    Published,

    /// <summary>A rollback or a refused commit: the KV transaction ended. The abort is not proposed.</summary>
    Aborting,

    /// <summary>The change set is no longer pending. The rollback work has not run, and the DDL lock is held.</summary>
    Aborted,
}
