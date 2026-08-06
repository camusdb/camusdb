
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models.Tickets;

/// <summary>
/// <c>ALTER TABLE t SET (key = value, ...)</c> — sets table storage parameters (per-table auto-analyze
/// opt-out and the row-level TTL configuration; see <c>Catalogs.Models.TableSettings</c> for the
/// recognized keys). The change is version-neutral: it rides the table blob without bumping
/// <c>TableSchema.Version</c> (settings do not affect row encoding), mirroring how index/constraint
/// changes are persisted.
/// </summary>
public readonly struct AlterTableSettingsTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    /// <summary>Setting key → value, canonicalized. Never empty.</summary>
    public IReadOnlyDictionary<string, string> Settings { get; }

    public AlterTableSettingsTicket(string databaseName, string tableName, IReadOnlyDictionary<string, string> settings)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        Settings = settings;
    }
}

/// <summary>
/// <c>ALTER TABLE t RESET (key, ...)</c> — removes table storage parameters, restoring each to its
/// engine default.
///
/// <para>The counterpart to <see cref="AlterTableSettingsTicket"/>, which can only add or overwrite. A
/// parameter set once could otherwise never be unset, only assigned a value that happens to match the
/// current default — which silently stops tracking that default if it ever changes. <c>RESET (ttl)</c>
/// expands to every TTL parameter, so turning TTL off never leaves orphaned tuning behind a cleared
/// expiration column.</para>
///
/// <para>Resetting a key the table never set is a no-op, not an error: the requested end state (the key
/// is absent) already holds, and a script that resets defensively should not fail.</para>
/// </summary>
public readonly struct AlterTableResetSettingsTicket
{
    public string DatabaseName { get; }

    public string TableName { get; }

    /// <summary>Canonical, group-expanded keys to remove. Never empty.</summary>
    public IReadOnlySet<string> Keys { get; }

    public AlterTableResetSettingsTicket(string databaseName, string tableName, IReadOnlySet<string> keys)
    {
        DatabaseName = databaseName;
        TableName = tableName;
        Keys = keys;
    }
}
