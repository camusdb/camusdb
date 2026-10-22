/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using NUnit.Framework;
using Kommander.Time;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Replication;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.Catalogs;

/// <summary>
/// The installed-extension catalog without an engine: the apply of
/// <see cref="SchemaOp.CreateExtension"/>, <see cref="SchemaOp.UpdateExtension"/> and
/// <see cref="SchemaOp.DropExtension"/>, its idempotence under re-delivery, both copies of the
/// already-applied check, the rules it checks in log order, the clone the dry-run validator applies
/// to, and the stamp the caches compare.
/// </summary>
[TestFixture]
public sealed class TestExtensionCatalog
{
    private static ExtensionDefinition Record(string name = "geo", string version = "1.0.0", int installed = 10, int updated = 10, string[]? requires = null, params string[] functions) => new()
    {
        Name = name,
        Version = version,
        Sha256 = new string('a', 64),
        Requires = [.. requires ?? []],
        Members = [.. Array.ConvertAll(functions, f => new ExtensionMemberDefinition { Kind = ExtensionMemberKind.Function, Name = f })],
        InstalledAt = new HLCTimestamp(installed, 0, 0),
        UpdatedAt = new HLCTimestamp(updated, 0, 0),
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

    private static SchemaChangeLogEntry Create(Schema schema, ExtensionDefinition record) =>
        Entry(schema, SchemaOp.CreateExtension, new SchemaExtensionPayload { Extension = record });

    private static SchemaChangeLogEntry Update(Schema schema, ExtensionDefinition record, string from) =>
        Entry(schema, SchemaOp.UpdateExtension, new SchemaExtensionPayload { Extension = record, FromVersion = from });

    private static SchemaChangeLogEntry Drop(Schema schema, ExtensionDefinition record) =>
        Entry(schema, SchemaOp.DropExtension, new SchemaDropExtensionPayload { ExtensionName = record.Name, Version = record.Version, InstalledAt = record.InstalledAt });

    /// <summary>The private copy in <see cref="SchemaReplicator"/>, which its duplicate check uses.</summary>
    private static bool ReplicatorWasApplied(Schema schema, SchemaChangeLogEntry entry) =>
        (bool)typeof(SchemaReplicator)
            .GetMethod("WasSchemaDeltaApplied", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, [schema, entry])!;

    private static void AssertApplied(Schema schema, SchemaChangeLogEntry entry, bool expected)
    {
        Assert.AreEqual(expected, SchemaDeltaApplier.WasSchemaDeltaApplied(schema, entry), "SchemaDeltaApplier");
        Assert.AreEqual(expected, ReplicatorWasApplied(schema, entry), "SchemaReplicator");
    }

    [Test]
    public void CreateUpdateDrop_AreIdempotent_AndMoveTheStamp()
    {
        Schema schema = new();
        Assert.AreEqual(0, schema.ExtensionCatalog.Stamp, "nothing installed");

        SchemaChangeLogEntry create = Create(schema, Record(functions: "distance"));
        AssertApplied(schema, create, false);
        SchemaDeltaApplier.ApplySchemaDelta(schema, create);
        AssertApplied(schema, create, true);
        Assert.AreEqual("1.0.0", schema.Extensions["GEO"].Version, "names are case-insensitive");
        Assert.AreEqual(create.ToVersion, schema.ExtensionCatalog.Stamp);

        // A re-delivered create finds its own record and changes nothing.
        IReadOnlyDictionary<string, ExtensionDefinition> published = schema.Extensions;
        SchemaDeltaApplier.ApplySchemaDelta(schema, Create(schema, Record(functions: "distance")));
        Assert.AreSame(published, schema.Extensions);

        SchemaChangeLogEntry update = Update(schema, Record(version: "2.0.0", updated: 20, functions: "distance"), "1.0.0");
        AssertApplied(schema, update, false);
        SchemaDeltaApplier.ApplySchemaDelta(schema, update);
        AssertApplied(schema, update, true);
        Assert.AreEqual("2.0.0", schema.Extensions["geo"].Version);
        Assert.AreEqual(update.ToVersion, schema.ExtensionCatalog.Stamp);

        // A re-delivered update, or one proposed against a version that is gone, is a no-op.
        SchemaDeltaApplier.ApplySchemaDelta(schema, Update(schema, Record(version: "2.0.0", updated: 20, functions: "distance"), "1.0.0"));
        SchemaChangeLogEntry stale = Update(schema, Record(version: "3.0.0", updated: 30), "1.0.0");
        AssertApplied(schema, stale, true);
        SchemaDeltaApplier.ApplySchemaDelta(schema, stale);
        Assert.AreEqual("2.0.0", schema.Extensions["geo"].Version);

        SchemaChangeLogEntry drop = Drop(schema, schema.Extensions["geo"]);
        AssertApplied(schema, drop, false);
        SchemaDeltaApplier.ApplySchemaDelta(schema, drop);
        AssertApplied(schema, drop, true);
        Assert.IsEmpty(schema.Extensions);
        Assert.AreEqual(0, schema.ExtensionCatalog.Stamp, "nothing installed again");

        // A re-delivered drop finds nothing to remove; it also leaves a later re-install alone.
        SchemaDeltaApplier.ApplySchemaDelta(schema, Create(schema, Record(installed: 40, updated: 40)));
        SchemaDeltaApplier.ApplySchemaDelta(schema, drop);
        Assert.AreEqual(new HLCTimestamp(40, 0, 0), schema.Extensions["geo"].InstalledAt);
    }

    [Test]
    public void Rules_AreCheckedInLogOrder()
    {
        Schema schema = new();

        CamusDBException missing = Assert.Throws<CamusDBException>(() =>
            SchemaDeltaApplier.ApplySchemaDelta(schema, Create(schema, Record(name: "routes", requires: ["geo"]))))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionRequirementMissing, missing.Code);

        SchemaDeltaApplier.ApplySchemaDelta(schema, Create(schema, Record(functions: "distance")));
        SchemaDeltaApplier.ApplySchemaDelta(schema, Create(schema, Record(name: "routes", requires: ["GEO"], functions: "route")));

        CamusDBException conflict = Assert.Throws<CamusDBException>(() =>
            SchemaDeltaApplier.ApplySchemaDelta(schema, Create(schema, Record(name: "maps", functions: "DISTANCE"))))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionMemberConflict, conflict.Code);

        CamusDBException again = Assert.Throws<CamusDBException>(() =>
            SchemaDeltaApplier.ApplySchemaDelta(schema, Create(schema, Record(version: "9.0.0", installed: 99))))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionAlreadyInstalled, again.Code);

        CamusDBException required = Assert.Throws<CamusDBException>(() =>
            SchemaDeltaApplier.ApplySchemaDelta(schema, Drop(schema, schema.Extensions["geo"])))!;
        Assert.AreEqual(CamusDBErrorCodes.ExtensionHasDependents, required.Code);
        StringAssert.Contains("routes", required.Message);
    }

    [Test]
    public void Clone_SharesTheSnapshot_AndADryRunNeverReachesTheLiveCatalog()
    {
        Schema schema = new();
        SchemaDeltaApplier.ApplySchemaDelta(schema, Create(schema, Record()));

        Schema clone = SchemaReplicator.CloneSchema(schema);
        Assert.AreSame(schema.Extensions, clone.Extensions);
        Assert.AreEqual(schema.ExtensionCatalog.Stamp, clone.ExtensionCatalog.Stamp);

        SchemaDeltaApplier.ApplySchemaDelta(clone, Drop(clone, clone.Extensions["geo"]));
        Assert.IsEmpty(clone.Extensions);
        Assert.IsTrue(schema.Extensions.ContainsKey("geo"), "the live catalog is untouched");
    }

    [Test]
    public void Record_RoundTripsThroughTheMetaCodec()
    {
        ExtensionDefinition record = Record(requires: ["units"], functions: "distance");
        record.Members.Add(new ExtensionMemberDefinition { Kind = ExtensionMemberKind.Trigger, Name = "Geo.Audit" });

        ExtensionDefinition copy = MetaJsonSerializer.Deserialize(
            MetaJsonSerializer.Serialize(record, MetaJsonContext.Default.ExtensionDefinition), MetaJsonContext.Default.ExtensionDefinition);

        Assert.AreEqual(record.Name, copy.Name);
        Assert.AreEqual(record.Version, copy.Version);
        Assert.AreEqual(record.Sha256, copy.Sha256);
        CollectionAssert.AreEqual(record.Requires, copy.Requires);
        Assert.IsTrue(copy.HasMember(ExtensionMemberKind.Function, "DISTANCE"));
        Assert.IsTrue(copy.HasMember(ExtensionMemberKind.Trigger, "Geo.Audit"));
        Assert.IsFalse(copy.HasMember(ExtensionMemberKind.Trigger, "geo.audit"), "trigger type names compare exactly");
        Assert.AreEqual(record.InstalledAt, copy.InstalledAt);
        Assert.AreEqual(record.UpdatedAt, copy.UpdatedAt);
    }
}
