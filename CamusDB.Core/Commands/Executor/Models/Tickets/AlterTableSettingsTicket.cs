
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// <c>ALTER TABLE t SET (key = value, ...)</c> — sets table storage parameters. Currently the only
/// recognized key is <c>sql_stats_automatic_collection_enabled</c> (per-table auto-analyze opt-out).
/// The change is version-neutral: it rides the table blob without bumping <c>TableSchema.Version</c>
/// (settings do not affect row encoding), mirroring how index/constraint changes are persisted.
/// </summary>
public readonly struct AlterTableSettingsTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    /// <summary>Setting key → value (as written, e.g. "true"/"false"). Never empty.</summary>
    public IReadOnlyDictionary<string, string> Settings { get; }

    public AlterTableSettingsTicket(string databaseName, string tableName, IReadOnlyDictionary<string, string> settings)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        Settings = settings;
    }
}
