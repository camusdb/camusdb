
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Statistics.Models;
using Kommander.Time;

namespace CamusDB.Core.Catalogs;

[JsonSerializable(typeof(long))]
[JsonSerializable(typeof(byte[]))]
[JsonSerializable(typeof(List<ColumnValue>))]
[JsonSerializable(typeof(IReadOnlyList<ColumnValue>))]
[JsonSerializable(typeof(ColumnValue))]
[JsonSerializable(typeof(SchemaElementState))]
[JsonSerializable(typeof(SchemaElementKind))]
[JsonSerializable(typeof(TableColumnSchema))]
[JsonSerializable(typeof(TableSchema))]
[JsonSerializable(typeof(TableSchemaHistory))]
[JsonSerializable(typeof(Dictionary<string, TableSchema>))]
[JsonSerializable(typeof(TableIndexSchema))]
[JsonSerializable(typeof(SchemaCheckpoint))]
[JsonSerializable(typeof(SchemaChangeLogEntry))]
[JsonSerializable(typeof(SchemaCreateTablePayload))]
[JsonSerializable(typeof(SchemaAlterColumnPayload))]
[JsonSerializable(typeof(SchemaColumnPayload))]
[JsonSerializable(typeof(SchemaDropTablePayload))]
[JsonSerializable(typeof(SchemaIndexPayload))]
[JsonSerializable(typeof(SchemaElementStatePayload))]
[JsonSerializable(typeof(SchemaRenameKind))]
[JsonSerializable(typeof(SchemaRenamePayload))]
[JsonSerializable(typeof(PersistedCoordinatorJob))]
[JsonSerializable(typeof(SystemSchema))]
[JsonSerializable(typeof(DatabaseTableObject))]
[JsonSerializable(typeof(DatabaseIndexObject))]
[JsonSerializable(typeof(Dictionary<string, DatabaseTableObject>))]
[JsonSerializable(typeof(Dictionary<string, DatabaseIndexObject>))]
[JsonSerializable(typeof(HLCTimestamp))]
[JsonSerializable(typeof(DatabaseRegistryEntry))]
[JsonSerializable(typeof(TableStatistics))]
[JsonSerializable(typeof(ScalarBound))]
[JsonSerializable(typeof(ColumnMinMax))]
[JsonSerializable(typeof(Dictionary<string, ColumnMinMax>))]
internal sealed partial class MetaJsonContext : JsonSerializerContext;
