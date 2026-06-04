
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

using System.Text.Json.Serialization;
using Kommander.Time;

/// <summary>
/// Represents the current version of the table schema.
/// </summary>
public sealed class TableSchema
{
    /// <summary>
    /// Unique identifier of the table. It remains immutable throughout the life of the table.
    /// </summary>
    public string? Id { get; set; }

    /// <summary>
    /// Column-layout version, used for row MVCC decoding. Incremented only on column
    /// adds, drops, and state transitions — operations that change how stored bytes are
    /// interpreted. Index DDL deliberately does NOT bump this: indexes are not part of
    /// the row encoding, so changing them requires no re-decoding of existing rows.
    /// Index changes ride the table blob (via <c>PersistSchemaTableAsync</c> /
    /// <c>TableSchema.Indexes</c>) but stay invisible to other cluster nodes until B2
    /// routes them through <c>SchemaChangeLogEntry</c>.
    /// </summary>
    public int Version { get; set; }

    /// <summary>
    /// The name of the table. It can be changed.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// The list of columns that make up the table
    /// </summary>
    public List<TableColumnSchema>? Columns { get; set; }

    /// <summary>
    /// Index definitions replicated alongside this table schema. Each entry carries an
    /// immutable <c>Id</c> and <c>ColumnIds</c>; column names are resolved at table-open
    /// time. Null for tables that have not yet been migrated from the legacy
    /// <c>SystemSchema.Indexes</c> storage; <c>LoadMetaAsync</c> populates this in-memory
    /// on load and B1 persists it on the next DDL write.
    /// </summary>
    public List<TableIndexSchema>? Indexes { get; set; }

    /// <summary>
    /// A list of all the previous versions of the table schema.
    /// </summary>
    public List<TableSchemaHistory>? SchemaHistory { get; set; }

    [JsonIgnore]
    public Func<HLCTimestamp, int, ValueTask<TableSchemaHistory?>>? SchemaHistoryLoader { get; set; }

    public TableSchemaHistory GetSchemaHistory(int version)
    {
        if (version == Version && Columns is not null)
            return new() { Version = Version, Columns = Columns };

        if (SchemaHistory is not null)
        {
            foreach (TableSchemaHistory history in SchemaHistory)
            {
                if (history.Version == version)
                    return history;
            }
        }

        if (SchemaHistoryLoader is not null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                $"Schema history for table '{Name}' version {version} requires asynchronous loading"
            );

        throw new CamusDBException(
            CamusDBErrorCodes.SystemSpaceCorrupt,
            $"Missing schema history for table '{Name}' version {version}"
        );
    }

    public async ValueTask<TableSchemaHistory> GetSchemaHistoryAsync(HLCTimestamp txId, int version)
    {
        if (version == Version && Columns is not null)
            return new() { Version = Version, Columns = Columns };

        if (SchemaHistory is not null)
        {
            foreach (TableSchemaHistory history in SchemaHistory)
            {
                if (history.Version == version)
                    return history;
            }
        }

        TableSchemaHistory? loaded = SchemaHistoryLoader is not null
            ? await SchemaHistoryLoader(txId, version).ConfigureAwait(false)
            : null;

        if (loaded is not null)
        {
            SchemaHistory ??= [];
            SchemaHistory.Add(loaded);
            SchemaHistory.Sort(static (a, b) => a.Version.CompareTo(b.Version));
            return loaded;
        }

        throw new CamusDBException(
            CamusDBErrorCodes.SystemSpaceCorrupt,
            $"Missing schema history for table '{Name}' version {version}"
        );
    }
}
