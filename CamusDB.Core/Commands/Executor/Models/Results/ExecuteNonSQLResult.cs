
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Results;

/// <summary>
/// What one no-rows statement did. <see cref="Database"/> is nullable on purpose, and the reason is
/// the trap this type used to hide: a server-level statement (user and grant administration, the
/// database lifecycle, cluster settings) is answered before any database is opened and returns the
/// <c>default</c> value of this struct, whose <c>Database</c> is null. Declared non-nullable, that
/// null was invisible to the compiler, so a transport could hand it straight to a commit and
/// dereference it — which is exactly how every account statement over the batched gRPC route came
/// back as "Internal server error". A transport must keep such a statement off the transaction path
/// altogether (see <c>StatementScope.IsDatabaseScopedMutation</c>) and commit through
/// <c>CommitOrReleaseAsync</c>, which accepts the null rather than faulting on it.
/// </summary>
public readonly struct ExecuteNonSQLResult
{
    /// <summary>
    /// The database the statement ran against, or null when it never opened one. See the type
    /// summary: null is a normal outcome here, not a defect.
    /// </summary>
    public DatabaseDescriptor? Database { get; }

    public TableDescriptor Table { get; }

    public int ModifiedRows { get; }

    /// <summary>
    /// A human-readable note about an outcome that succeeded but may not be what the caller intended
    /// — today, a time-travel copy that read no rows, which can mean the requested history was already
    /// reclaimed. Null when there is nothing to report. See
    /// <see cref="ExecuteDDLSQLResult.Warning"/> for why this is carried in the result rather than
    /// only logged.
    /// </summary>
    public string? Warning { get; }

    public ExecuteNonSQLResult(DatabaseDescriptor? database, TableDescriptor table, int modifiedRows, string? warning = null)
    {
        Database = database;
        Table = table;
        ModifiedRows = modifiedRows;
        Warning = warning;
    }
}
