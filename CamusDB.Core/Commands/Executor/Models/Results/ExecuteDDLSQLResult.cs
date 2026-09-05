
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Results;

/// <summary>
/// What one DDL statement did. <see cref="Database"/> is nullable for the reason spelled out on
/// <see cref="ExecuteNonSQLResult"/>: a server-level statement is answered before any database is
/// opened and reports no descriptor, and declaring the field non-nullable hid that null from the
/// compiler at every commit site.
/// </summary>
public readonly struct ExecuteDDLSQLResult
{
    /// <summary>
    /// The database the statement ran against, or null when it never opened one — user and grant
    /// administration, <c>DROP</c>/<c>RENAME DATABASE</c>, and the cluster settings.
    /// </summary>
    public DatabaseDescriptor? Database { get; }

    public bool Success { get; }

    /// <summary>
    /// Rows written by a DDL statement that also loads data — <c>CREATE TABLE … AS SELECT</c> and
    /// <c>CREATE MATERIALIZED VIEW</c>. Zero for every other DDL statement, which writes no rows at
    /// all, and for either of those created <c>WITH NO DATA</c> or skipped by <c>IF NOT EXISTS</c>.
    /// </summary>
    public int ModifiedRows { get; }

    /// <summary>
    /// A human-readable note about an outcome that succeeded but may not be what the caller intended
    /// — today, a time-travel copy that read no rows, which can mean the requested history was already
    /// reclaimed. Null when there is nothing to report.
    ///
    /// <para>It exists because the engine's log is not visible to a remote client: a warning that only
    /// reaches the server log leaves an HTTP or gRPC caller staring at a successful response for an
    /// empty table, which is exactly the silent failure the warning is meant to prevent.</para>
    /// </summary>
    public string? Warning { get; }

    public ExecuteDDLSQLResult(DatabaseDescriptor? database, bool success, int modifiedRows = 0, string? warning = null)
    {
        Database = database;
        Success = success;
        ModifiedRows = modifiedRows;
        Warning = warning;
    }
}
