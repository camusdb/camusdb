/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs;

/// <summary>
/// What asked the schema-change coordinator to resume the persisted jobs of a database. Every
/// trigger drives the same jobs the same way; the trigger decides only how a resume treats an empty
/// job list and how loud it is about it.
///
/// <para>A job can stay unresumed when the only trigger is the leader election: the callback fires
/// before this node's database is open, or before the record written by the previous driver is
/// visible to this node's reads, and neither case fires it again. So a resume is also run when a
/// database opens on a node that already leads, and on a periodic sweep for as long as a node leads.
/// Whatever the order of open, election and record visibility, one of the three reaches the job.</para>
/// </summary>
public enum CoordinatorResumeTrigger
{
    /// <summary>
    /// This node won the schema leadership. The record of a job may not be visible yet on a node that
    /// is still applying the committed log, so an empty list is re-read for a short while.
    /// </summary>
    LeaderElected,

    /// <summary>The database opened on a node that is the schema leader already.</summary>
    DatabaseOpened,

    /// <summary>
    /// The periodic sweep of a schema leader. It reads the list once and says nothing when it is empty.
    /// </summary>
    Sweep,

    /// <summary>
    /// A statement waiting for the outcome of a job it handed over found that this node leads again
    /// and nobody drives the job.
    /// </summary>
    HandoverWait,
}
