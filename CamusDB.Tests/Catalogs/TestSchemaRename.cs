/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Serializer;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

using Kommander.Time;
using Microsoft.Extensions.Logging;
using Nito.AsyncEx;
using NUnit.Framework;

namespace CamusDB.Tests.Catalogs;

/// <summary>
/// G3 — draining the old name across the two-version window.
/// Tests cover: apply behavior, error cases, G3a mid-ladder rejection,
/// idempotency/re-delivery, old-name reuse after convergence, payload
/// round-trip, and descriptor eviction via SchemaReplicator.
/// </summary>
[TestFixture]
public sealed class TestSchemaRename
{
    // ─── helpers ─────────────────────────────────────────────────────────────

    private static readonly ILoggerFactory LoggerFactory =
        Microsoft.Extensions.Logging.LoggerFactory.Create(b => b.AddFilter("Camus", LogLevel.Warning));

    private static SchemaChangeLogEntry Entry<T>(SchemaOp op, T payload, long from = 1, long to = 2)
    {
        return new()
        {
            Ts = new HLCTimestamp(1, 10, 2),
            Database = "db",
            FromVersion = from,
            ToVersion = to,
            Op = op,
            Payload = Serializator.Serialize(payload)
        };
    }

    private static Schema SchemaWithTable(string tableName = "robots")
    {
        Schema schema = new();
        CatalogsManager.ApplySchemaDelta(schema, Entry(
            SchemaOp.CreateTable,
            new SchemaCreateTablePayload
            {
                TableId = "robots-id",
                TableName = tableName,
                Columns =
                [
                    new() { Id = "id-col",   Name = "id",   Type = ColumnType.Id,     NotNull = true  },
                    new() { Id = "name-col", Name = "name", Type = ColumnType.String, NotNull = false }
                ]
            }
        ));
        return schema;
    }

    private static Schema SchemaWithTableAndIndex(string tableName = "robots")
    {
        Schema schema = SchemaWithTable(tableName);
        CatalogsManager.ApplySchemaDelta(schema, Entry(
            SchemaOp.AddIndex,
            new SchemaIndexPayload
            {
                TableName = tableName,
                IndexName = "name_idx",
                Index = new TableIndexSchema(
                    id: "idx-1",
                    name: "name_idx",
                    columnIds: ["name-col"],
                    type: IndexType.Multi,
                    state: SchemaElementState.Public
                )
            },
            from: 2, to: 3
        ));
        return schema;
    }

    private static SchemaChangeLogEntry RenameTableEntry(string oldName, string newName, long from = 2, long to = 3)
    {
        return Entry(
            SchemaOp.RenameTable,
            new SchemaRenamePayload { TableName = oldName, Kind = SchemaRenameKind.Table, NewName = newName },
            from, to
        );
    }

    private static SchemaChangeLogEntry RenameColumnEntry(
        string tableName, string oldCol, string newCol, long from = 2, long to = 3)
    {
        return Entry(
            SchemaOp.RenameColumn,
            new SchemaRenamePayload
            {
                TableName = tableName,
                Kind = SchemaRenameKind.Column,
                ElementName = oldCol,
                NewName = newCol
            },
            from, to
        );
    }

    private static SchemaChangeLogEntry RenameIndexEntry(
        string tableName, string oldIdx, string newIdx, long from = 3, long to = 4)
    {
        return Entry(
            SchemaOp.RenameIndex,
            new SchemaRenamePayload
            {
                TableName = tableName,
                Kind = SchemaRenameKind.Index,
                ElementName = oldIdx,
                NewName = newIdx
            },
            from, to
        );
    }

    // ─── RenameTable — happy path ─────────────────────────────────────────────

    [Test]
    public void ApplySchemaDelta_RenameTable_NewNamePresentOldNameAbsent()
    {
        Schema schema = SchemaWithTable();

        CatalogsManager.ApplySchemaDelta(schema, RenameTableEntry("robots", "machines"));

        Assert.True(schema.Tables.ContainsKey("machines"));
        Assert.False(schema.Tables.ContainsKey("robots"));
        Assert.AreEqual(3, schema.SchemaVersion);
    }

    [Test]
    public void ApplySchemaDelta_RenameTable_MutatesNameOnSameInstance()
    {
        Schema schema = SchemaWithTable();
        TableSchema original = schema.Tables["robots"];

        TableSchema? returned = CatalogsManager.ApplySchemaDelta(schema, RenameTableEntry("robots", "machines"));

        // Same object instance preserved (pin/visibility closures still track it).
        Assert.AreSame(original, returned);
        Assert.AreSame(original, schema.Tables["machines"]);
        Assert.AreEqual("machines", original.Name);
    }

    [Test]
    public void ApplySchemaDelta_RenameTable_DoesNotBumpTableSchemaVersion()
    {
        Schema schema = SchemaWithTable();
        int versionBefore = schema.Tables["robots"].Version;

        CatalogsManager.ApplySchemaDelta(schema, RenameTableEntry("robots", "machines"));

        Assert.AreEqual(versionBefore, schema.Tables["machines"].Version);
    }

    [Test]
    public void ApplySchemaDelta_RenameTable_OldNameFreeForReuseAfterApply()
    {
        // After a rename A→B, the old name A is absent from the live schema.
        // A subsequent CreateTable for A must succeed — the uniqueness gate runs
        // against the post-rename schema (two-version invariant proof at the unit level).
        Schema schema = SchemaWithTable("robots");

        CatalogsManager.ApplySchemaDelta(schema, RenameTableEntry("robots", "machines"));

        // CreateTable "robots" must succeed — old name is free.
        Assert.DoesNotThrow(() => CatalogsManager.ApplySchemaDelta(schema, Entry(
            SchemaOp.CreateTable,
            new SchemaCreateTablePayload
            {
                TableName = "robots",
                Columns = [new() { Name = "id", Type = ColumnType.Id, NotNull = true }]
            },
            from: 3, to: 4
        )));

        Assert.True(schema.Tables.ContainsKey("robots"));
        Assert.True(schema.Tables.ContainsKey("machines"));
    }

    // ─── RenameTable — idempotency/re-delivery ────────────────────────────────

    [Test]
    public async Task ApplyAsync_RenameTable_RedeliveredEntryIsNoOp()
    {
        // After robots→machines is applied, the old name is gone from schema.Tables.
        // A raw second call to ApplySchemaDelta would hit TableDoesntExist("robots").
        // SchemaReplicator.ApplyAsync must detect via WasSchemaDeltaApplied/WasRenamed
        // (new name present AND old name absent) and skip reapplication — this is the
        // precise regression the predicate exists to prevent.
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);

        byte[] create = Serializator.Serialize(new SchemaChangeLogEntry
        {
            Database = db,
            FromVersion = 0,
            ToVersion = 1,
            Op = SchemaOp.CreateTable,
            Payload = Serializator.Serialize(new SchemaCreateTablePayload
            {
                TableId = "robots-id",
                TableName = "robots",
                Columns = [new() { Id = "id-col", Name = "id", Type = ColumnType.Id, NotNull = true }]
            })
        });
        await replicator.ApplyAsync(database, partitionId, create);

        byte[] rename = Serializator.Serialize(new SchemaChangeLogEntry
        {
            Database = db,
            FromVersion = 1,
            ToVersion = 2,
            Op = SchemaOp.RenameTable,
            Payload = Serializator.Serialize(new SchemaRenamePayload
            {
                TableName = "robots",
                Kind = SchemaRenameKind.Table,
                NewName = "machines"
            })
        });

        // First delivery — rename applies.
        await replicator.ApplyAsync(database, partitionId, rename);
        Assert.True(database.Schema.Tables.ContainsKey("machines"));
        Assert.False(database.Schema.Tables.ContainsKey("robots"));
        Assert.AreEqual(2, database.Schema.SchemaVersion);

        // Second delivery of the same bytes — must not throw and must leave state stable.
        Assert.DoesNotThrowAsync(() => replicator.ApplyAsync(database, partitionId, rename));
        Assert.True(database.Schema.Tables.ContainsKey("machines"));
        Assert.False(database.Schema.Tables.ContainsKey("robots"));
        Assert.AreEqual(2, database.Schema.SchemaVersion);
    }

    [Test]
    public async Task ApplyAsync_RenameColumn_RedeliveredEntryIsNoOp()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);

        byte[] create = Serializator.Serialize(new SchemaChangeLogEntry
        {
            Database = db,
            FromVersion = 0,
            ToVersion = 1,
            Op = SchemaOp.CreateTable,
            Payload = Serializator.Serialize(new SchemaCreateTablePayload
            {
                TableId = "robots-id",
                TableName = "robots",
                Columns =
                [
                    new() { Id = "id-col",   Name = "id",   Type = ColumnType.Id,     NotNull = true },
                    new() { Id = "name-col", Name = "name", Type = ColumnType.String, NotNull = false }
                ]
            })
        });
        await replicator.ApplyAsync(database, partitionId, create);

        byte[] rename = Serializator.Serialize(new SchemaChangeLogEntry
        {
            Database = db,
            FromVersion = 1,
            ToVersion = 2,
            Op = SchemaOp.RenameColumn,
            Payload = Serializator.Serialize(new SchemaRenamePayload
            {
                TableName = "robots",
                Kind = SchemaRenameKind.Column,
                ElementName = "name",
                NewName = "label"
            })
        });

        await replicator.ApplyAsync(database, partitionId, rename);
        Assert.True(database.Schema.Tables["robots"].Columns!.Any(c => c.Name == "label"));

        Assert.DoesNotThrowAsync(() => replicator.ApplyAsync(database, partitionId, rename));
        Assert.True(database.Schema.Tables["robots"].Columns!.Any(c => c.Name == "label"));
        Assert.False(database.Schema.Tables["robots"].Columns!.Any(c => c.Name == "name"));
        Assert.AreEqual(2, database.Schema.SchemaVersion);
    }

    [Test]
    public async Task ApplyAsync_RenameIndex_RedeliveredEntryIsNoOp()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);

        byte[] create = Serializator.Serialize(new SchemaChangeLogEntry
        {
            Database = db, FromVersion = 0, ToVersion = 1,
            Op = SchemaOp.CreateTable,
            Payload = Serializator.Serialize(new SchemaCreateTablePayload
            {
                TableId = "robots-id", TableName = "robots",
                Columns = [new() { Id = "id-col", Name = "id", Type = ColumnType.Id, NotNull = true }]
            })
        });
        byte[] addIdx = Serializator.Serialize(new SchemaChangeLogEntry
        {
            Database = db, FromVersion = 1, ToVersion = 2,
            Op = SchemaOp.AddIndex,
            Payload = Serializator.Serialize(new SchemaIndexPayload
            {
                TableName = "robots", IndexName = "name_idx",
                Index = new TableIndexSchema("idx-1", "name_idx", ["id-col"], IndexType.Multi, SchemaElementState.Public)
            })
        });
        byte[] rename = Serializator.Serialize(new SchemaChangeLogEntry
        {
            Database = db, FromVersion = 2, ToVersion = 3,
            Op = SchemaOp.RenameIndex,
            Payload = Serializator.Serialize(new SchemaRenamePayload
            {
                TableName = "robots", Kind = SchemaRenameKind.Index,
                ElementName = "name_idx", NewName = "label_idx"
            })
        });

        await replicator.ApplyAsync(database, partitionId, create);
        await replicator.ApplyAsync(database, partitionId, addIdx);
        await replicator.ApplyAsync(database, partitionId, rename);

        Assert.True(database.Schema.Tables["robots"].Indexes!.Any(ix => ix.Name == "label_idx"));

        Assert.DoesNotThrowAsync(() => replicator.ApplyAsync(database, partitionId, rename));
        Assert.True(database.Schema.Tables["robots"].Indexes!.Any(ix => ix.Name == "label_idx"));
        Assert.False(database.Schema.Tables["robots"].Indexes!.Any(ix => ix.Name == "name_idx"));
        Assert.AreEqual(3, database.Schema.SchemaVersion);
    }

    // ─── RenameTable — error cases ────────────────────────────────────────────

    [Test]
    public void ApplySchemaDelta_RenameTable_TableNotFound_Throws()
    {
        Schema schema = SchemaWithTable();

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            CatalogsManager.ApplySchemaDelta(schema, RenameTableEntry("nonexistent", "machines")));

        Assert.NotNull(ex);
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.TableDoesntExist));
        Assert.That(ex.Message, Does.Contain("nonexistent"));
    }

    [Test]
    public void ApplySchemaDelta_RenameTable_NewNameAlreadyTaken_Throws()
    {
        Schema schema = SchemaWithTable("robots");
        // Add a second table under the target name.
        CatalogsManager.ApplySchemaDelta(schema, Entry(
            SchemaOp.CreateTable,
            new SchemaCreateTablePayload
            {
                TableName = "machines",
                Columns = [new() { Name = "id", Type = ColumnType.Id, NotNull = true }]
            },
            from: 2, to: 3
        ));

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            CatalogsManager.ApplySchemaDelta(schema, RenameTableEntry("robots", "machines", from: 3, to: 4)));

        Assert.NotNull(ex);
        Assert.That(ex!.Code, Is.EqualTo(CamusDBErrorCodes.TableAlreadyExists));
    }

    // ─── RenameTable — G3a mid-ladder child elements ─────────────────────────

    [Test]
    public void ApplySchemaDelta_RenameTable_ChildColumnMidLadder_Throws()
    {
        Schema schema = SchemaWithTable();
        schema.Tables["robots"].Columns!.Add(
            new TableColumnSchema("new-col", "extra", ColumnType.String, false, null, SchemaElementState.WriteOnly));

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            CatalogsManager.ApplySchemaDelta(schema, RenameTableEntry("robots", "machines")));

        Assert.NotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        Assert.That(ex.Message, Does.Contain("extra"));
        Assert.That(ex.Message, Does.Contain("WriteOnly"));
        // Table must be unchanged.
        Assert.True(schema.Tables.ContainsKey("robots"));
        Assert.False(schema.Tables.ContainsKey("machines"));
    }

    [Test]
    public void ApplySchemaDelta_RenameTable_ChildIndexMidLadder_Throws()
    {
        Schema schema = SchemaWithTableAndIndex();
        schema.Tables["robots"].Indexes![0] = new TableIndexSchema(
            id: "idx-1",
            name: "name_idx",
            columnIds: ["name-col"],
            type: IndexType.Multi,
            state: SchemaElementState.DeleteOnly
        );

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            CatalogsManager.ApplySchemaDelta(schema, RenameTableEntry("robots", "machines", from: 3, to: 4)));

        Assert.NotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        Assert.That(ex.Message, Does.Contain("name_idx"));
        Assert.That(ex.Message, Does.Contain("DeleteOnly"));
        Assert.True(schema.Tables.ContainsKey("robots"));
        Assert.False(schema.Tables.ContainsKey("machines"));
    }

    [Test]
    [TestCase(SchemaElementState.DeleteOnly)]
    [TestCase(SchemaElementState.WriteOnly)]
    public void ApplySchemaDelta_RenameTable_AllMidLadderStatesBlocked(SchemaElementState midState)
    {
        Schema schema = SchemaWithTable();
        schema.Tables["robots"].Columns!.Add(
            new TableColumnSchema("extra-col", "extra", ColumnType.String, false, null, midState));

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            CatalogsManager.ApplySchemaDelta(schema, RenameTableEntry("robots", "machines")));

        Assert.NotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    [Test]
    public void ApplySchemaDelta_RenameTable_ChildInPublicState_Succeeds()
    {
        Schema schema = SchemaWithTableAndIndex();
        // All elements are Public → rename must succeed.
        Assert.DoesNotThrow(() =>
            CatalogsManager.ApplySchemaDelta(schema, RenameTableEntry("robots", "machines", from: 3, to: 4)));

        Assert.True(schema.Tables.ContainsKey("machines"));
        Assert.False(schema.Tables.ContainsKey("robots"));
    }

    // ─── RenameColumn — happy path ────────────────────────────────────────────

    [Test]
    public void ApplySchemaDelta_RenameColumn_NewNamePresentOldNameAbsent()
    {
        Schema schema = SchemaWithTable();

        CatalogsManager.ApplySchemaDelta(schema, RenameColumnEntry("robots", "name", "label"));

        TableSchema table = schema.Tables["robots"];
        Assert.True(table.Columns!.Any(c => c.Name == "label"));
        Assert.False(table.Columns!.Any(c => c.Name == "name"));
    }

    [Test]
    public void ApplySchemaDelta_RenameColumn_PreservesImmutableId()
    {
        Schema schema = SchemaWithTable();
        string originalId = schema.Tables["robots"].Columns!.First(c => c.Name == "name").Id;

        CatalogsManager.ApplySchemaDelta(schema, RenameColumnEntry("robots", "name", "label"));

        string renamedId = schema.Tables["robots"].Columns!.First(c => c.Name == "label").Id;
        Assert.AreEqual(originalId, renamedId);
    }

    [Test]
    public void ApplySchemaDelta_RenameColumn_BumpsVersionAndAppendsHistory()
    {
        Schema schema = SchemaWithTable();
        int versionBefore = schema.Tables["robots"].Version;
        int historyBefore = schema.Tables["robots"].SchemaHistory!.Count;

        CatalogsManager.ApplySchemaDelta(schema, RenameColumnEntry("robots", "name", "label"));

        TableSchema table = schema.Tables["robots"];
        Assert.AreEqual(versionBefore + 1, table.Version);
        Assert.AreEqual(historyBefore + 1, table.SchemaHistory!.Count);
        Assert.AreEqual(table.Version, table.SchemaHistory.Last().Version);
        Assert.True(table.SchemaHistory.Last().Columns!.Any(c => c.Name == "label"),
            "History snapshot must contain the new column name");
        Assert.False(table.SchemaHistory.Last().Columns!.Any(c => c.Name == "name"),
            "History snapshot must not contain the old column name");
    }

    [Test]
    public void ApplySchemaDelta_RenameColumn_PreservesTypeAndNullabilityAndDefault()
    {
        Schema schema = new();
        CatalogsManager.ApplySchemaDelta(schema, Entry(
            SchemaOp.CreateTable,
            new SchemaCreateTablePayload
            {
                TableName = "robots",
                Columns =
                [
                    new() { Id = "id-col",  Name = "id",   Type = ColumnType.Id,     NotNull = true  },
                    new() { Id = "str-col", Name = "label", Type = ColumnType.String, NotNull = false,
                            DefaultValue = new ColumnValue(ColumnType.String, "anon") }
                ]
            }
        ));

        CatalogsManager.ApplySchemaDelta(schema, RenameColumnEntry("robots", "label", "display_name"));

        TableColumnSchema renamed = schema.Tables["robots"].Columns!.First(c => c.Name == "display_name");
        Assert.AreEqual(ColumnType.String, renamed.Type);
        Assert.False(renamed.NotNull);
        Assert.NotNull(renamed.DefaultValue);
        Assert.AreEqual("anon", renamed.DefaultValue!.StrValue);
    }

    // ─── RenameColumn — error cases ───────────────────────────────────────────

    [Test]
    public void ApplySchemaDelta_RenameColumn_OldNameNotFound_Throws()
    {
        Schema schema = SchemaWithTable();

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            CatalogsManager.ApplySchemaDelta(schema, RenameColumnEntry("robots", "nonexistent", "label")));

        Assert.NotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.UnknownColumn, ex!.Code);
    }

    [Test]
    public void ApplySchemaDelta_RenameColumn_NewNameAlreadyTaken_Throws()
    {
        Schema schema = SchemaWithTable();

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            CatalogsManager.ApplySchemaDelta(schema, RenameColumnEntry("robots", "name", "id")));

        Assert.NotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.DuplicateColumn, ex!.Code);
    }

    [Test]
    public void ApplySchemaDelta_RenameColumn_SelfRename_ThrowsDuplicateColumn()
    {
        // At the apply level a self-rename (NewName == ElementName) surfaces as
        // DuplicateColumn: FindIndex locates the column by old name, then
        // Any(c.Name == NewName) matches that same column — so the "already exists"
        // guard fires before the replacement. This is a quirk, not the intended error;
        // the correct "same name" rejection belongs in RenameValidator (wired in G4).
        // This test pins the current apply-layer behaviour so the G4 validator wiring
        // doesn't accidentally become the only guard — the apply path still rejects it.
        Schema schema = SchemaWithTable();

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            CatalogsManager.ApplySchemaDelta(schema, RenameColumnEntry("robots", "name", "name")));

        Assert.NotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.DuplicateColumn, ex!.Code);
        // Column must be unchanged.
        Assert.True(schema.Tables["robots"].Columns!.Any(c => c.Name == "name"));
    }

    // ─── RenameColumn — G3a mid-ladder ────────────────────────────────────────

    [Test]
    [TestCase(SchemaElementState.DeleteOnly)]
    [TestCase(SchemaElementState.WriteOnly)]
    public void ApplySchemaDelta_RenameColumn_MidLadderState_Throws(SchemaElementState midState)
    {
        Schema schema = SchemaWithTable();
        int idx = schema.Tables["robots"].Columns!.FindIndex(c => c.Name == "name");
        TableColumnSchema col = schema.Tables["robots"].Columns![idx];
        schema.Tables["robots"].Columns![idx] = new TableColumnSchema(
            col.Id, col.Name, col.Type, col.NotNull, col.DefaultValue, midState);

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            CatalogsManager.ApplySchemaDelta(schema, RenameColumnEntry("robots", "name", "label")));

        Assert.NotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        Assert.That(ex.Message, Does.Contain(midState.ToString()));
        // Column unchanged.
        Assert.True(schema.Tables["robots"].Columns!.Any(c => c.Name == "name"));
        Assert.False(schema.Tables["robots"].Columns!.Any(c => c.Name == "label"));
    }

    [Test]
    public void ApplySchemaDelta_RenameColumn_PublicState_Succeeds()
    {
        Schema schema = SchemaWithTable();
        Assert.DoesNotThrow(() =>
            CatalogsManager.ApplySchemaDelta(schema, RenameColumnEntry("robots", "name", "label")));

        Assert.True(schema.Tables["robots"].Columns!.Any(c => c.Name == "label"));
    }

    // ─── RenameIndex — happy path ─────────────────────────────────────────────

    [Test]
    public void ApplySchemaDelta_RenameIndex_NewNamePresentOldNameAbsent()
    {
        Schema schema = SchemaWithTableAndIndex();

        CatalogsManager.ApplySchemaDelta(schema, RenameIndexEntry("robots", "name_idx", "label_idx"));

        TableSchema table = schema.Tables["robots"];
        Assert.True(table.Indexes!.Any(ix => ix.Name == "label_idx"));
        Assert.False(table.Indexes!.Any(ix => ix.Name == "name_idx"));
    }

    [Test]
    public void ApplySchemaDelta_RenameIndex_PreservesImmutableFields()
    {
        Schema schema = SchemaWithTableAndIndex();
        TableIndexSchema before = schema.Tables["robots"].Indexes!.First(ix => ix.Name == "name_idx");

        CatalogsManager.ApplySchemaDelta(schema, RenameIndexEntry("robots", "name_idx", "label_idx"));

        TableIndexSchema after = schema.Tables["robots"].Indexes!.First(ix => ix.Name == "label_idx");
        Assert.AreEqual(before.Id, after.Id);
        Assert.AreEqual(before.ColumnIds, after.ColumnIds);
        Assert.AreEqual(before.Type, after.Type);
        Assert.AreEqual(before.State, after.State);
        Assert.AreEqual(before.StartOffset, after.StartOffset);
    }

    [Test]
    public void ApplySchemaDelta_RenameIndex_DoesNotBumpTableSchemaVersion()
    {
        Schema schema = SchemaWithTableAndIndex();
        int versionBefore = schema.Tables["robots"].Version;

        CatalogsManager.ApplySchemaDelta(schema, RenameIndexEntry("robots", "name_idx", "label_idx"));

        Assert.AreEqual(versionBefore, schema.Tables["robots"].Version);
    }

    // ─── RenameIndex — error cases ────────────────────────────────────────────

    [Test]
    public void ApplySchemaDelta_RenameIndex_OldNameNotFound_Throws()
    {
        Schema schema = SchemaWithTableAndIndex();

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            CatalogsManager.ApplySchemaDelta(schema, RenameIndexEntry("robots", "nonexistent_idx", "label_idx")));

        Assert.NotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    [Test]
    public void ApplySchemaDelta_RenameIndex_NewNameAlreadyTaken_Throws()
    {
        Schema schema = SchemaWithTableAndIndex();
        // Add a second index under the target name.
        CatalogsManager.ApplySchemaDelta(schema, Entry(
            SchemaOp.AddIndex,
            new SchemaIndexPayload
            {
                TableName = "robots",
                IndexName = "label_idx",
                Index = new TableIndexSchema(
                    id: "idx-2", name: "label_idx",
                    columnIds: ["id-col"], type: IndexType.Unique,
                    state: SchemaElementState.Public)
            },
            from: 3, to: 4
        ));

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            CatalogsManager.ApplySchemaDelta(schema, RenameIndexEntry("robots", "name_idx", "label_idx", from: 4, to: 5)));

        Assert.NotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
    }

    // ─── RenameIndex — G3a mid-ladder ─────────────────────────────────────────

    [Test]
    [TestCase(SchemaElementState.DeleteOnly)]
    [TestCase(SchemaElementState.WriteOnly)]
    public void ApplySchemaDelta_RenameIndex_MidLadderState_Throws(SchemaElementState midState)
    {
        Schema schema = SchemaWithTableAndIndex();
        // Replace the index with a mid-ladder copy.
        TableIndexSchema midLadder = new(
            id: "idx-1", name: "name_idx",
            columnIds: ["name-col"], type: IndexType.Multi,
            state: midState);
        schema.Tables["robots"].Indexes![0] = midLadder;

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            CatalogsManager.ApplySchemaDelta(schema, RenameIndexEntry("robots", "name_idx", "label_idx")));

        Assert.NotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        Assert.That(ex.Message, Does.Contain(midState.ToString()));
        Assert.True(schema.Tables["robots"].Indexes!.Any(ix => ix.Name == "name_idx"));
        Assert.False(schema.Tables["robots"].Indexes!.Any(ix => ix.Name == "label_idx"));
    }

    // ─── Payload round-trip ───────────────────────────────────────────────────

    [Test]
    public void SchemaRenamePayload_AllThreeKinds_RoundTripThroughSerializator()
    {
        SchemaChangeLogEntry[] entries =
        [
            RenameTableEntry("robots", "machines"),
            RenameColumnEntry("robots", "name", "label"),
            RenameIndexEntry("robots", "name_idx", "label_idx")
        ];

        foreach (SchemaChangeLogEntry entry in entries)
        {
            byte[] bytes = Serializator.Serialize(entry);
            SchemaChangeLogEntry rt = Serializator.Unserialize<SchemaChangeLogEntry>(bytes);

            Assert.AreEqual(entry.Op, rt.Op);
            Assert.AreEqual(entry.Database, rt.Database);
            Assert.AreEqual(entry.FromVersion, rt.FromVersion);
            Assert.AreEqual(entry.ToVersion, rt.ToVersion);
            Assert.Greater(rt.Payload.Length, 0);

            SchemaRenamePayload p = Serializator.Unserialize<SchemaRenamePayload>(rt.Payload);
            SchemaRenamePayload orig = Serializator.Unserialize<SchemaRenamePayload>(entry.Payload);
            Assert.AreEqual(orig.Kind, p.Kind);
            Assert.AreEqual(orig.TableName, p.TableName);
            Assert.AreEqual(orig.ElementName, p.ElementName);
            Assert.AreEqual(orig.NewName, p.NewName);
        }
    }

    // ─── Descriptor eviction via SchemaReplicator ─────────────────────────────

    [Test]
    public async Task ApplyAsync_RenameTable_EvictsOldDescriptorAndRekeysSchema()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);

        // 1. Apply CreateTable "robots".
        byte[] create = Serializator.Serialize(new SchemaChangeLogEntry
        {
            Database = db,
            FromVersion = 0,
            ToVersion = 1,
            Op = SchemaOp.CreateTable,
            Payload = Serializator.Serialize(new SchemaCreateTablePayload
            {
                TableId = "robots-id",
                TableName = "robots",
                Columns =
                [
                    new() { Id = "id-col", Name = "id", Type = ColumnType.Id, NotNull = true }
                ]
            })
        });
        await replicator.ApplyAsync(database, partitionId, create);

        // 2. Seed a cached descriptor under the old name.
        TableSchema tableSchema = database.Schema.Tables["robots"];
        database.TableDescriptors["robots"] = new AsyncLazy<TableDescriptor>(() =>
            Task.FromResult(new TableDescriptor(
                tableSchema.Id!, tableSchema.Name!, tableSchema,
                new KvTableStore(kahuna.Kahuna, database.Id, tableSchema.Id!))));

        Assert.AreEqual(1, database.TableDescriptors.Count);
        Assert.True(database.TableDescriptors.ContainsKey("robots"));

        // 3. Apply RenameTable "robots" → "machines".
        byte[] rename = Serializator.Serialize(new SchemaChangeLogEntry
        {
            Database = db,
            FromVersion = 1,
            ToVersion = 2,
            Op = SchemaOp.RenameTable,
            Payload = Serializator.Serialize(new SchemaRenamePayload
            {
                TableName = "robots",
                Kind = SchemaRenameKind.Table,
                NewName = "machines"
            })
        });
        await replicator.ApplyAsync(database, partitionId, rename);

        // 4. Descriptor under old name is evicted; schema re-keyed to new name.
        Assert.False(database.TableDescriptors.ContainsKey("robots"),
            "Stale descriptor under old name must be evicted on rename");
        Assert.True(database.Schema.Tables.ContainsKey("machines"),
            "Schema must be re-keyed to new name after rename");
        Assert.False(database.Schema.Tables.ContainsKey("robots"),
            "Old name must be absent from schema after rename");
    }

    [Test]
    public async Task ApplyAsync_RenameColumn_EvictsDescriptor()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);

        byte[] create = Serializator.Serialize(new SchemaChangeLogEntry
        {
            Database = db,
            FromVersion = 0,
            ToVersion = 1,
            Op = SchemaOp.CreateTable,
            Payload = Serializator.Serialize(new SchemaCreateTablePayload
            {
                TableId = "robots-id",
                TableName = "robots",
                Columns =
                [
                    new() { Id = "id-col",   Name = "id",   Type = ColumnType.Id,     NotNull = true },
                    new() { Id = "name-col", Name = "name", Type = ColumnType.String, NotNull = false }
                ]
            })
        });
        await replicator.ApplyAsync(database, partitionId, create);

        TableSchema tableSchema = database.Schema.Tables["robots"];
        database.TableDescriptors["robots"] = new AsyncLazy<TableDescriptor>(() =>
            Task.FromResult(new TableDescriptor(
                tableSchema.Id!, tableSchema.Name!, tableSchema,
                new KvTableStore(kahuna.Kahuna, database.Id, tableSchema.Id!))));

        byte[] rename = Serializator.Serialize(new SchemaChangeLogEntry
        {
            Database = db,
            FromVersion = 1,
            ToVersion = 2,
            Op = SchemaOp.RenameColumn,
            Payload = Serializator.Serialize(new SchemaRenamePayload
            {
                TableName = "robots",
                Kind = SchemaRenameKind.Column,
                ElementName = "name",
                NewName = "label"
            })
        });
        await replicator.ApplyAsync(database, partitionId, rename);

        Assert.False(database.TableDescriptors.ContainsKey("robots"),
            "Descriptor must be evicted on column rename so next open resolves new column name");
        Assert.True(database.Schema.Tables["robots"].Columns!.Any(c => c.Name == "label"));
        Assert.False(database.Schema.Tables["robots"].Columns!.Any(c => c.Name == "name"));
    }

    [Test]
    public async Task ApplyAsync_RenameIndex_EvictsDescriptor()
    {
        await using EmbeddedKahuna kahuna = new();
        await kahuna.StartAsync(CancellationToken.None);

        string db = NextSchemaLogDatabaseName(kahuna);
        DatabaseDescriptor database = CreateDescriptor(db, kahuna);
        SchemaReplicator replicator = CreateReplicator();
        int partitionId = database.Kahuna.SchemaLogPartition(db);

        byte[] create = Serializator.Serialize(new SchemaChangeLogEntry
        {
            Database = db,
            FromVersion = 0,
            ToVersion = 1,
            Op = SchemaOp.CreateTable,
            Payload = Serializator.Serialize(new SchemaCreateTablePayload
            {
                TableId = "robots-id",
                TableName = "robots",
                Columns = [new() { Id = "id-col", Name = "id", Type = ColumnType.Id, NotNull = true }]
            })
        });
        await replicator.ApplyAsync(database, partitionId, create);

        byte[] addIdx = Serializator.Serialize(new SchemaChangeLogEntry
        {
            Database = db,
            FromVersion = 1,
            ToVersion = 2,
            Op = SchemaOp.AddIndex,
            Payload = Serializator.Serialize(new SchemaIndexPayload
            {
                TableName = "robots",
                IndexName = "name_idx",
                Index = new TableIndexSchema(
                    id: "idx-1", name: "name_idx",
                    columnIds: ["id-col"], type: IndexType.Multi,
                    state: SchemaElementState.Public)
            })
        });
        await replicator.ApplyAsync(database, partitionId, addIdx);

        TableSchema tableSchema = database.Schema.Tables["robots"];
        database.TableDescriptors["robots"] = new AsyncLazy<TableDescriptor>(() =>
            Task.FromResult(new TableDescriptor(
                tableSchema.Id!, tableSchema.Name!, tableSchema,
                new KvTableStore(kahuna.Kahuna, database.Id, tableSchema.Id!))));

        byte[] rename = Serializator.Serialize(new SchemaChangeLogEntry
        {
            Database = db,
            FromVersion = 2,
            ToVersion = 3,
            Op = SchemaOp.RenameIndex,
            Payload = Serializator.Serialize(new SchemaRenamePayload
            {
                TableName = "robots",
                Kind = SchemaRenameKind.Index,
                ElementName = "name_idx",
                NewName = "label_idx"
            })
        });
        await replicator.ApplyAsync(database, partitionId, rename);

        Assert.False(database.TableDescriptors.ContainsKey("robots"),
            "Descriptor must be evicted on index rename so next open resolves new index name");
        Assert.True(database.Schema.Tables["robots"].Indexes!.Any(ix => ix.Name == "label_idx"));
        Assert.False(database.Schema.Tables["robots"].Indexes!.Any(ix => ix.Name == "name_idx"));
    }

    // ─── Fixtures ─────────────────────────────────────────────────────────────

    private static SchemaReplicator CreateReplicator()
    {
        ILogger<ICamusDB> logger = LoggerFactory.CreateLogger<ICamusDB>();
        return new(new CatalogsManager(logger), logger);
    }

    private static DatabaseDescriptor CreateDescriptor(string db, EmbeddedKahuna kahuna)
    {
        return new(
            id: db,
            name: db,
            kahuna: kahuna,
            transactions: new KvTransactionsManager(kahuna.Kahuna),
            tableDescriptors: new ConcurrentDictionary<string, AsyncLazy<TableDescriptor>>()
        );
    }

    private static string NextSchemaLogDatabaseName(EmbeddedKahuna kahuna)
    {
        for (int i = 0; i < 100; i++)
        {
            string db = $"db_{Guid.NewGuid():N}";
            try
            {
                _ = kahuna.SchemaLogPartition(db);
                return db;
            }
            catch (CamusDBException)
            {
            }
        }
        throw new AssertionException("Could not generate a valid schema-log database name");
    }
}
