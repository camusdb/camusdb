
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Catalogs.Meta;

/// <summary>
/// One mutually consistent copy of a database's persisted schema, read by
/// <see cref="SchemaLoader.LoadSnapshotAsync"/> in a single transaction. It exists so the read
/// and the install are separate steps: the KV reads run without the schema lock, and the caller
/// installs the finished snapshot under the lock. Holding the lock across the reads is forbidden —
/// the schema apply pipeline yields on that lock, and stalling it stalls the partition.
/// </summary>
internal sealed class SchemaSnapshot
{
    /// <summary>
    /// True when the version key exists. False means a fresh database that never persisted
    /// schema; the maps below are then empty and <see cref="SchemaVersion"/> is 0.
    /// </summary>
    public bool HasPersistedSchema;

    /// <summary>The persisted schema version the snapshot was read at.</summary>
    public long SchemaVersion;

    /// <summary>Tables keyed by name, case-insensitive, matching <see cref="Schema.Tables"/>.</summary>
    public Dictionary<string, TableSchema> Tables = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Views keyed by name, case-insensitive, matching <see cref="Schema.Views"/>.</summary>
    public Dictionary<string, ViewSchema> Views = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>The legacy system blob, or null when the key is absent.</summary>
    public SystemSchema? System;
}
