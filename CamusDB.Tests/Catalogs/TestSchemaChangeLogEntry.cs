
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System.Collections.Generic;
using System.Linq;
using System.Text;

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer;
using Kommander.Time;

namespace CamusDB.Tests.Catalogs;

[TestFixture]
public sealed class TestSchemaChangeLogEntry
{
    private static SchemaChangeLogEntry Entry<T>(SchemaOp op, T payload)
    {
        return new()
        {
            Ts = new HLCTimestamp(1, 10, 2),
            Database = "db",
            FromVersion = 1,
            ToVersion = 2,
            Op = op,
            Payload = Serializator.Serialize(payload)
        };
    }

    private static SchemaCreateTablePayload CreateTablePayload()
    {
        return new()
        {
            TableName = "robots",
            Columns =
            [
                new()
                {
                    Name = "id",
                    Type = ColumnType.Id,
                    NotNull = true
                },
                new()
                {
                    Name = "name",
                    Type = ColumnType.String,
                    DefaultValue = new(ColumnType.String, "t-800")
                }
            ]
        };
    }

    [Test]
    public void ApplySchemaDelta_CreateTable_AddsTableAndInitialHistory()
    {
        Schema schema = new();

        TableSchema? table = CatalogsManager.ApplySchemaDelta(
            schema,
            Entry(SchemaOp.CreateTable, CreateTablePayload())
        );

        Assert.NotNull(table);
        Assert.True(schema.Tables.ContainsKey("robots"));
        Assert.AreEqual("robots", table!.Name);
        Assert.AreEqual(0, table.Version);
        Assert.AreEqual(2, table.Columns!.Count);
        Assert.True(table.Columns.TrueForAll(column => column.State == SchemaElementState.Public));
        Assert.AreEqual(1, table.SchemaHistory!.Count);
        Assert.AreEqual(0, table.SchemaHistory[0].Version);
        Assert.AreSame(table.Columns, table.SchemaHistory[0].Columns);
        Assert.AreEqual(2, schema.SchemaVersion);
    }

    [Test]
    public void ApplySchemaDelta_AddColumn_AppendsColumnAndHistory()
    {
        Schema schema = new();
        CatalogsManager.ApplySchemaDelta(schema, Entry(SchemaOp.CreateTable, CreateTablePayload()));

        TableSchema? table = CatalogsManager.ApplySchemaDelta(
            schema,
            Entry(
                SchemaOp.AddColumn,
                new SchemaAlterColumnPayload
                {
                    TableName = "robots",
                    Column = new()
                    {
                        Name = "enabled",
                        Type = ColumnType.Bool,
                        NotNull = true
                    }
                }
            )
        );

        Assert.NotNull(table);
        Assert.AreEqual(1, table!.Version);
        Assert.AreEqual(3, table.Columns!.Count);
        Assert.AreEqual("enabled", table.Columns[2].Name);
        Assert.AreEqual(SchemaElementState.Public, table.Columns[2].State);
        Assert.AreEqual(2, table.SchemaHistory!.Count);
        Assert.AreEqual(1, table.SchemaHistory[1].Version);
        Assert.AreEqual(2, schema.SchemaVersion);
    }

    [Test]
    public void ApplySchemaDelta_DropColumn_RemovesColumnAndAppendsHistory()
    {
        Schema schema = new();
        CatalogsManager.ApplySchemaDelta(schema, Entry(SchemaOp.CreateTable, CreateTablePayload()));

        TableSchema? table = CatalogsManager.ApplySchemaDelta(
            schema,
            Entry(
                SchemaOp.DropColumn,
                new SchemaAlterColumnPayload
                {
                    TableName = "robots",
                    Column = new()
                    {
                        Name = "name",
                        Type = ColumnType.String
                    }
                }
            )
        );

        Assert.NotNull(table);
        Assert.AreEqual(1, table!.Version);
        Assert.AreEqual(1, table.Columns!.Count);
        Assert.AreEqual("id", table.Columns[0].Name);
        Assert.AreEqual(2, table.SchemaHistory!.Count);
        Assert.AreEqual(2, schema.SchemaVersion);
    }

    [Test]
    public void ApplySchemaDelta_DropTable_RemovesTable()
    {
        Schema schema = new();
        CatalogsManager.ApplySchemaDelta(schema, Entry(SchemaOp.CreateTable, CreateTablePayload()));

        TableSchema? table = CatalogsManager.ApplySchemaDelta(
            schema,
            Entry(SchemaOp.DropTable, new SchemaDropTablePayload { TableName = "robots" })
        );

        Assert.NotNull(table);
        Assert.False(schema.Tables.ContainsKey("robots"));
        Assert.AreEqual(2, schema.SchemaVersion);
    }

    [Test]
    public void ApplySchemaDelta_AddIndex_AppendsIndexToTableSchema()
    {
        Schema schema = new();
        CatalogsManager.ApplySchemaDelta(schema, Entry(SchemaOp.CreateTable, CreateTablePayload()));

        SchemaChangeLogEntry entry = Entry(
            SchemaOp.AddIndex,
            new SchemaIndexPayload
            {
                TableName = "robots",
                IndexName = "name_idx",
                Index = new("name_idx", ["name"], IndexType.Multi)
            }
        );

        TableSchema? table = CatalogsManager.ApplySchemaDelta(schema, entry);

        Assert.NotNull(table);
        Assert.True(schema.Tables.ContainsKey("robots"));
        Assert.AreEqual(0, schema.Tables["robots"].Version);  // index DDL does not bump Version
        Assert.NotNull(schema.Tables["robots"].Indexes);
        Assert.AreEqual(1, schema.Tables["robots"].Indexes!.Count);
        Assert.AreEqual("name_idx", schema.Tables["robots"].Indexes![0].Name);
        Assert.AreEqual(2, schema.SchemaVersion);
    }

    [Test]
    public void ApplySchemaDelta_DropIndex_RemovesIndexFromTableSchema()
    {
        Schema schema = new();
        CatalogsManager.ApplySchemaDelta(schema, Entry(SchemaOp.CreateTable, CreateTablePayload()));
        CatalogsManager.ApplySchemaDelta(schema, Entry(SchemaOp.AddIndex, new SchemaIndexPayload
        {
            TableName = "robots",
            IndexName = "name_idx",
            Index = new("name_idx", ["name"], IndexType.Multi)
        }));

        TableSchema? table = CatalogsManager.ApplySchemaDelta(
            schema,
            Entry(SchemaOp.DropIndex, new SchemaIndexPayload { TableName = "robots", IndexName = "name_idx" })
        );

        Assert.NotNull(table);
        Assert.True(schema.Tables.ContainsKey("robots"));
        Assert.AreEqual(0, schema.Tables["robots"].Version);  // index DDL does not bump Version
        Assert.AreEqual(0, schema.Tables["robots"].Indexes?.Count ?? 0);
        Assert.AreEqual(2, schema.SchemaVersion);
    }

    [Test]
    public void ApplySchemaDelta_SetElementState_AdvancesThroughOnlineColumnStates()
    {
        Schema schema = new();
        TableSchema table = CatalogsManager.ApplySchemaDelta(schema, Entry(SchemaOp.CreateTable, CreateTablePayload()))!;

        // Seed a column already staged in DeleteOnly (as the coordinator will once it
        // drives staged adds); this test exercises the SetElementState advance machinery.
        table.Columns!.Add(new("enabled-col", "enabled", ColumnType.Bool, false, null, SchemaElementState.DeleteOnly));

        Assert.AreEqual(SchemaElementState.DeleteOnly, table.Columns!.Single(column => column.Name == "enabled").State);

        TableSchema? writeOnly = CatalogsManager.ApplySchemaDelta(
            schema,
            Entry(
                SchemaOp.SetElementState,
                new SchemaElementStatePayload
                {
                    TableName = "robots",
                    ElementName = "enabled",
                    State = SchemaElementState.WriteOnly
                }
            )
        );

        Assert.NotNull(writeOnly);
        Assert.AreEqual(SchemaElementState.WriteOnly, table.Columns!.Single(column => column.Name == "enabled").State);

        CatalogsManager.ApplySchemaDelta(
            schema,
            Entry(
                SchemaOp.SetElementState,
                new SchemaElementStatePayload
                {
                    TableName = "robots",
                    ElementName = "enabled",
                    State = SchemaElementState.Public
                }
            )
        );

        Assert.AreEqual(SchemaElementState.Public, table.Columns!.Single(column => column.Name == "enabled").State);
        Assert.AreEqual(2, table.Version);
        Assert.AreEqual(3, table.SchemaHistory!.Count);
    }

    [Test]
    public void ApplySchemaDelta_SetElementState_DoesNotSkipState()
    {
        Schema schema = new();
        CatalogsManager.ApplySchemaDelta(schema, Entry(SchemaOp.CreateTable, CreateTablePayload()));

        TableSchema table = schema.Tables["robots"];
        table.Columns!.Add(new("enabled-col", "enabled", ColumnType.Bool, false, null, SchemaElementState.DeleteOnly));

        CamusDBException? ex = Assert.Throws<CamusDBException>(() => CatalogsManager.ApplySchemaDelta(
            schema,
            Entry(
                SchemaOp.SetElementState,
                new SchemaElementStatePayload
                {
                    TableName = "robots",
                    ElementName = "enabled",
                    State = SchemaElementState.Public
                }
            )
        ));

        Assert.NotNull(ex);
        Assert.That(ex!.Message, Does.Contain("Invalid state transition"));
        Assert.AreEqual(SchemaElementState.DeleteOnly, table.Columns.Single(column => column.Name == "enabled").State);
    }

    [Test]
    public void ApplySchemaDelta_SetElementState_SameStateDoesNotBumpTableVersionOrHistory()
    {
        Schema schema = new();
        CatalogsManager.ApplySchemaDelta(schema, Entry(SchemaOp.CreateTable, CreateTablePayload()));

        TableSchema table = schema.Tables["robots"];
        int versionBefore = table.Version;
        int historyCountBefore = table.SchemaHistory!.Count;

        TableSchema? applied = CatalogsManager.ApplySchemaDelta(
            schema,
            Entry(
                SchemaOp.SetElementState,
                new SchemaElementStatePayload
                {
                    TableName = "robots",
                    ElementName = "name",
                    State = SchemaElementState.Public
                }
            )
        );

        Assert.AreSame(table, applied);
        Assert.AreEqual(versionBefore, table.Version);
        Assert.AreEqual(historyCountBefore, table.SchemaHistory!.Count);
        Assert.AreEqual(2, schema.SchemaVersion);
    }

    [Test]
    public void ApplySchemaDelta_SetElementState_DeleteOnlyToAbsentRemovesColumn()
    {
        Schema schema = new();
        CatalogsManager.ApplySchemaDelta(schema, Entry(SchemaOp.CreateTable, CreateTablePayload()));

        TableSchema table = schema.Tables["robots"];
        table.Columns!.Add(new("enabled-col", "enabled", ColumnType.Bool, false, null, SchemaElementState.DeleteOnly));

        CatalogsManager.ApplySchemaDelta(
            schema,
            Entry(
                SchemaOp.SetElementState,
                new SchemaElementStatePayload
                {
                    TableName = "robots",
                    ElementName = "enabled",
                    State = SchemaElementState.Absent
                }
            )
        );

        Assert.False(table.Columns.Any(column => column.Name == "enabled"));
        Assert.AreEqual(1, table.Version);
        Assert.AreEqual(2, table.SchemaHistory!.Count);
    }

    [Test]
    public void SchemaChangeLogEntry_RoundTripsEveryPayloadShape()
    {
        SchemaChangeLogEntry[] entries =
        [
            Entry(SchemaOp.CreateTable, CreateTablePayload()),
            Entry(SchemaOp.DropTable, new SchemaDropTablePayload { TableName = "robots" }),
            Entry(
                SchemaOp.AddColumn,
                new SchemaAlterColumnPayload
                {
                    TableName = "robots",
                    Column = new() { Name = "enabled", Type = ColumnType.Bool }
                }
            ),
            Entry(
                SchemaOp.DropColumn,
                new SchemaAlterColumnPayload
                {
                    TableName = "robots",
                    Column = new() { Name = "enabled", Type = ColumnType.Bool }
                }
            ),
            Entry(
                SchemaOp.AddIndex,
                new SchemaIndexPayload
                {
                    TableName = "robots",
                    IndexName = "name_idx",
                    Index = new("name_idx", ["name"], IndexType.Multi)
                }
            ),
            Entry(
                SchemaOp.DropIndex,
                new SchemaIndexPayload
                {
                    TableName = "robots",
                    IndexName = "name_idx"
                }
            ),
            Entry(
                SchemaOp.SetElementState,
                new SchemaElementStatePayload
                {
                    TableName = "robots",
                    ElementName = "name",
                    State = SchemaElementState.Public
                }
            )
        ];

        foreach (SchemaChangeLogEntry entry in entries)
        {
            byte[] bytes = Serializator.Serialize(entry);
            SchemaChangeLogEntry roundTrip = Serializator.Unserialize<SchemaChangeLogEntry>(bytes);

            Assert.AreEqual(entry.Ts, roundTrip.Ts);
            Assert.AreEqual(entry.Database, roundTrip.Database);
            Assert.AreEqual(entry.FromVersion, roundTrip.FromVersion);
            Assert.AreEqual(entry.ToVersion, roundTrip.ToVersion);
            Assert.AreEqual(entry.Op, roundTrip.Op);
            Assert.Greater(roundTrip.Payload.Length, 0);
        }
    }

    [Test]
    public void SchemaCheckpoint_RoundTripsSchemaVersionAndTables()
    {
        SchemaCheckpoint checkpoint = new()
        {
            SchemaVersion = 7,
            Tables = new()
            {
                ["robots"] = new()
                {
                    Id = "table-id",
                    Name = "robots",
                    Version = 3,
                    Columns = [new("id-col", "id", ColumnType.Id, true, null)],
                    SchemaHistory = [new() { Version = 3, Columns = [new("id-col", "id", ColumnType.Id, true, null)] }]
                }
            }
        };

        SchemaCheckpoint roundTrip = CatalogsManager.LoadSchemaCheckpoint(Serializator.Serialize(checkpoint));

        Assert.AreEqual(7, roundTrip.SchemaVersion);
        Assert.True(roundTrip.Tables.ContainsKey("robots"));
        Assert.AreEqual(3, roundTrip.Tables["robots"].Version);
    }

    [Test]
    public void SchemaCheckpoint_LoadsLegacyRawTableDictionaryWithMaxTableVersion()
    {
        Dictionary<string, TableSchema> legacyTables = new()
        {
            ["robots"] = new()
            {
                Id = "robots-id",
                Name = "robots",
                Version = 4,
                Columns = [new("id-col", "id", ColumnType.Id, true, null)],
                SchemaHistory = [new() { Version = 4, Columns = [new("id-col", "id", ColumnType.Id, true, null)] }]
            },
            ["parts"] = new()
            {
                Id = "parts-id",
                Name = "parts",
                Version = 2,
                Columns = [new("id-col", "id", ColumnType.Id, true, null)],
                SchemaHistory = [new() { Version = 2, Columns = [new("id-col", "id", ColumnType.Id, true, null)] }]
            }
        };

        SchemaCheckpoint checkpoint = CatalogsManager.LoadSchemaCheckpoint(Serializator.Serialize(legacyTables));

        Assert.AreEqual(4, checkpoint.SchemaVersion);
        Assert.AreEqual(2, checkpoint.Tables.Count);
        Assert.True(checkpoint.Tables.ContainsKey("robots"));
        Assert.True(checkpoint.Tables.ContainsKey("parts"));
    }

    [Test]
    public void MetaJsonSerializer_UsesUtf8AndSourceGeneratedContext()
    {
        SchemaCheckpoint checkpoint = new()
        {
            SchemaVersion = 3,
            Tables = new()
            {
                ["robots"] = new()
                {
                    Id = "table-id",
                    Name = "robots",
                    Version = 1,
                    Columns =
                    [
                        new("id-col", "id", ColumnType.Id, true, null),
                        new("name-col", "name", ColumnType.String, false, new(ColumnType.String, "robot"))
                    ],
                    SchemaHistory =
                    [
                        new()
                        {
                            Version = 0,
                            Columns = [new("id-col", "id", ColumnType.Id, true, null)]
                        }
                    ]
                }
            }
        };

        byte[] utf8 = MetaJsonSerializer.Serialize(checkpoint, MetaJsonContext.Default.SchemaCheckpoint);
        byte[] utf16 = Serializator.Serialize(checkpoint);
        SchemaCheckpoint roundTrip = MetaJsonSerializer.Deserialize(utf8, MetaJsonContext.Default.SchemaCheckpoint);

        Assert.AreEqual(3, roundTrip.SchemaVersion);
        Assert.True(roundTrip.Tables.ContainsKey("robots"));
        Assert.Less(utf8.Length, utf16.Length);
        Assert.AreEqual(SchemaElementState.Public, roundTrip.Tables["robots"].Columns![0].State);
        Assert.AreEqual((byte)'{', utf8[0], "New meta writes should be UTF-8 JSON, not UTF-16 with interleaved zero bytes");
        Assert.DoesNotThrow(() => Encoding.UTF8.GetString(utf8));
    }
}
