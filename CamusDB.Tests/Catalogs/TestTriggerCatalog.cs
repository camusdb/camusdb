/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using NUnit.Framework;

using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Replication;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.Catalogs;

/// <summary>
/// The trigger catalog without an engine: the apply of <see cref="SchemaOp.CreateTrigger"/> and
/// <see cref="SchemaOp.DropTrigger"/>, its idempotence under re-delivery, both copies of the
/// already-applied check, the two field-by-field copiers of <see cref="TableSchema"/>, and the write
/// fence. A field or arm missed in any of these loses triggers without an error.
/// </summary>
[TestFixture]
public sealed class TestTriggerCatalog
{
    private const string TableId = "T1";

    private static Schema SchemaWithTable(RelationKind kind = RelationKind.Table)
    {
        Schema schema = new();
        schema.Tables["t"] = new TableSchema
        {
            Id = TableId,
            Name = "t",
            Kind = kind,
            Columns = [new TableColumnSchema("c-id", "id", ColumnType.Integer64, notNull: true, defaultValue: null)],
        };

        // A trigger names an installed extension, so the extension is installed first.
        schema.ExtensionCatalog.Publish(
            new Dictionary<string, ExtensionDefinition>(StringComparer.OrdinalIgnoreCase)
            {
                ["ext"] = new() { Name = "ext", Version = "1.0.0", Sha256 = new string('a', 64), InstalledAt = new HLCTimestamp(1, 1, 1) },
            },
            1);
        return schema;
    }

    private static TriggerDefinition Trigger(string name = "audit", string type = "Ns.Audit") => new()
    {
        Name = name,
        Timing = TriggerFireTiming.After,
        Events = TriggerEventSet.Insert,
        TypeName = type,
        ExtensionName = "ext",
        CreatedAt = new HLCTimestamp(1, 2, 3),
    };

    private static SchemaChangeLogEntry Entry<T>(Schema schema, SchemaOp op, T payload) => new()
    {
        Ts = new HLCTimestamp(1, 1, 1),
        Database = "db",
        FromVersion = schema.SchemaVersion,
        ToVersion = schema.SchemaVersion + 1,
        Op = op,
        Payload = SchemaChangeLogEntryCodec.EncodePayload(payload),
    };

    private static SchemaChangeLogEntry CreateEntry(Schema schema, TriggerDefinition trigger) =>
        Entry(schema, SchemaOp.CreateTrigger, new SchemaCreateTriggerPayload { TableName = "t", TableId = TableId, Trigger = trigger });

    private static SchemaChangeLogEntry DropEntry(Schema schema, string name) =>
        Entry(schema, SchemaOp.DropTrigger, new SchemaDropTriggerPayload { TableName = "t", TableId = TableId, TriggerName = name });

    /// <summary>The private copy in <see cref="SchemaReplicator"/>, which its duplicate check uses.</summary>
    private static bool ReplicatorWasApplied(Schema schema, SchemaChangeLogEntry entry) =>
        (bool)typeof(SchemaReplicator)
            .GetMethod("WasSchemaDeltaApplied", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, [schema, entry])!;

    [Test]
    public void Create_AddsTheRecord_AndIsIdempotent()
    {
        Schema schema = SchemaWithTable();
        SchemaChangeLogEntry create = CreateEntry(schema, Trigger());

        Assert.IsFalse(SchemaDeltaApplier.WasSchemaDeltaApplied(schema, create));
        Assert.IsFalse(ReplicatorWasApplied(schema, create));

        SchemaDeltaApplier.ApplySchemaDelta(schema, create);

        TriggerDefinition stored = schema.Tables["t"].Triggers!.Single();
        Assert.AreEqual("audit", stored.Name);
        Assert.AreEqual("ext", stored.ExtensionName, "the payload round-trips through the JSON codec");
        Assert.IsTrue(SchemaDeltaApplier.WasSchemaDeltaApplied(schema, create));
        Assert.IsTrue(ReplicatorWasApplied(schema, create));

        // A re-delivered entry finds its own trigger and changes nothing.
        SchemaDeltaApplier.ApplySchemaDelta(schema, CreateEntry(schema, Trigger()));
        Assert.AreEqual(1, schema.Tables["t"].Triggers!.Count);
    }

    [Test]
    public void Create_SameNameDifferentTrigger_IsRefused()
    {
        Schema schema = SchemaWithTable();
        SchemaDeltaApplier.ApplySchemaDelta(schema, CreateEntry(schema, Trigger()));

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => SchemaDeltaApplier.ApplySchemaDelta(schema, CreateEntry(schema, Trigger(name: "AUDIT", type: "Ns.Other"))))!;
        Assert.AreEqual(CamusDBErrorCodes.TriggerAlreadyExists, ex.Code);
    }

    [Test]
    public void Create_OnAReplacedTable_IsRefused()
    {
        Schema schema = SchemaWithTable();
        schema.Tables["t"].Id = "T2";

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => SchemaDeltaApplier.ApplySchemaDelta(schema, CreateEntry(schema, Trigger())))!;
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, ex.Code);
    }

    [Test]
    public void Create_OnAMaterializedView_IsRefused()
    {
        Schema schema = SchemaWithTable(RelationKind.MaterializedView);

        CamusDBException ex = Assert.Throws<CamusDBException>(
            () => SchemaDeltaApplier.ApplySchemaDelta(schema, CreateEntry(schema, Trigger())))!;
        Assert.AreEqual(CamusDBErrorCodes.TriggerTargetInvalid, ex.Code);
    }

    [Test]
    public void Drop_RemovesTheRecord_AndIsIdempotent()
    {
        Schema schema = SchemaWithTable();
        SchemaDeltaApplier.ApplySchemaDelta(schema, CreateEntry(schema, Trigger("a")));
        SchemaDeltaApplier.ApplySchemaDelta(schema, CreateEntry(schema, Trigger("b")));
        System.Collections.Generic.List<TriggerDefinition> before = schema.Tables["t"].Triggers!;

        SchemaChangeLogEntry drop = DropEntry(schema, "A");
        Assert.IsFalse(SchemaDeltaApplier.WasSchemaDeltaApplied(schema, drop));
        Assert.IsFalse(ReplicatorWasApplied(schema, drop));

        SchemaDeltaApplier.ApplySchemaDelta(schema, drop);

        Assert.AreEqual("b", schema.Tables["t"].Triggers!.Single().Name);
        Assert.AreEqual(2, before.Count, "the list is replaced, never edited in place");
        Assert.IsTrue(SchemaDeltaApplier.WasSchemaDeltaApplied(schema, drop));
        Assert.IsTrue(ReplicatorWasApplied(schema, drop));

        SchemaDeltaApplier.ApplySchemaDelta(schema, DropEntry(schema, "a"));
        SchemaDeltaApplier.ApplySchemaDelta(schema, DropEntry(schema, "b"));
        Assert.IsNull(schema.Tables["t"].Triggers, "an empty list is stored as null");
    }

    [Test]
    public void Copiers_KeepTheTriggers()
    {
        Schema schema = SchemaWithTable();
        SchemaDeltaApplier.ApplySchemaDelta(schema, CreateEntry(schema, Trigger()));
        TableSchema table = schema.Tables["t"];

        Assert.AreEqual("audit", SchemaReplicator.CloneTable(table).Triggers!.Single().Name, "the dry-run clone");
        Assert.AreEqual("audit", SchemaMetaStore.WithoutHistory(table).Triggers!.Single().Name, "the persisted projection");
    }

    [Test]
    public void CreateFencesTheTable_DropFencesNothing()
    {
        Schema schema = SchemaWithTable();

        WriteShapeTargets create = WriteShapeDeltaRules.Resolve(schema, CreateEntry(schema, Trigger()));
        Assert.AreSame(schema.Tables["t"], create.Tables!.Single(), "a write planned before the trigger did not fire it");

        WriteShapeTargets drop = WriteShapeDeltaRules.Resolve(schema, DropEntry(schema, "audit"));
        Assert.IsNull(drop.Tables);
        Assert.IsFalse(drop.UseAppliedTable);
    }

    [Test]
    public void TableSchema_RoundTripsTheTriggerListThroughJson()
    {
        Schema schema = SchemaWithTable();
        SchemaDeltaApplier.ApplySchemaDelta(schema, CreateEntry(schema, Trigger()));

        string json = System.Text.Json.JsonSerializer.Serialize(schema.Tables["t"], MetaJsonContext.Default.TableSchema);
        TableSchema read = System.Text.Json.JsonSerializer.Deserialize(json, MetaJsonContext.Default.TableSchema)!;

        TriggerDefinition trigger = read.Triggers!.Single();
        Assert.AreEqual(TriggerEventSet.Insert, trigger.Events);
        Assert.AreEqual(TriggerFireTiming.After, trigger.Timing);
        Assert.AreEqual(new HLCTimestamp(1, 2, 3), trigger.CreatedAt);
        StringAssert.DoesNotContain("Binding", json, "the per-node binding is never persisted");
    }
}
