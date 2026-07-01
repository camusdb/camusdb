
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// Ticket for <c>CREATE DATABASE</c> and <c>CREATE DATABASE … BRANCH FROM source</c>.
/// When <see cref="BranchFrom"/> is non-null the engine creates a branch database whose
/// schema metadata is copied from the source and whose registry entry carries the full
/// ancestry chain.  When it is null a root (non-branched) database is created.
/// </summary>
public readonly struct CreateDatabaseTicket
{
    public string DatabaseName { get; }

    public bool IfNotExists { get; }

    /// <summary>
    /// Name of the source database to branch from, or <c>null</c> for a root database.
    /// </summary>
    public string? BranchFrom { get; }

    public CreateDatabaseTicket(string name, bool ifNotExists, string? branchFrom = null)
    {
        DatabaseName = name;
        IfNotExists = ifNotExists;
        BranchFrom = branchFrom;
    }
}
