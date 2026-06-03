
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using Kommander.Time;

namespace CamusDB.Core.Catalogs;

[JsonSerializable(typeof(long))]
[JsonSerializable(typeof(ColumnValue))]
[JsonSerializable(typeof(SchemaElementState))]
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
[JsonSerializable(typeof(SystemSchema))]
[JsonSerializable(typeof(DatabaseTableObject))]
[JsonSerializable(typeof(DatabaseIndexObject))]
[JsonSerializable(typeof(Dictionary<string, DatabaseTableObject>))]
[JsonSerializable(typeof(Dictionary<string, DatabaseIndexObject>))]
[JsonSerializable(typeof(HLCTimestamp))]
internal sealed partial class MetaJsonContext : JsonSerializerContext;
