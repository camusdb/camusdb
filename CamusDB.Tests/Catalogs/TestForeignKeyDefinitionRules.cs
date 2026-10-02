/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.Catalogs.Replication;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Tests.Catalogs;

/// <summary>
/// The rules a CREATE TABLE delta's foreign keys must pass when the delta is applied, on a bare schema.
/// They run in log order on every node, so they are what orders a constraint against a concurrent drop
/// or change of its parent — the proposer's own checks only produce the message.
/// </summary>
[TestFixture]
public sealed class TestForeignKeyDefinitionRules
{
    private const string ParentId = "P1";
    private const string ParentIdColumn = "pc-id";
    private const string ParentNameColumn = "pc-name";
    private const string ParentPk = "pi-pk";
    private const string ParentUnique = "pi-name";

    private const string ChildId = "C1";
    private const string ChildCityColumn = "cc-city";
    private const string ChildIndex = "ci-city";

    [Test]
    public void ValidConstraintIsApplied()
    {
        Schema schema = SchemaWithParent();

        TableSchema child = Apply(schema, Payload(Constraint()))!;

        Assert.AreEqual(1, child.ForeignKeys!.Count);
        Assert.IsTrue(schema.ForeignKeys.ChildPlansOf(ChildId).Single().IsEnforced);
    }

    /// <summary>
    /// The delta was built while the parent existed; another node dropped the parent first. The apply must
    /// refuse, rather than publish a constraint that references nothing.
    /// </summary>
    [Test]
    public void ParentDroppedBeforeTheApplyIsRefused()
    {
        Schema schema = SchemaWithParent();
        schema.Tables.Remove("cities");

        AssertApplyRefused(schema, Payload(Constraint()), CamusDBErrorCodes.TableDoesntExist);
    }

    [Test]
    public void ReferencedIndexThatIsNotPublicIsRefused()
    {
        Schema schema = SchemaWithParent(uniqueState: SchemaElementState.WriteOnly);

        AssertApplyRefused(schema, Payload(Constraint()), CamusDBErrorCodes.InvalidForeignKeyDefinition);
    }

    [Test]
    public void ReferencedIndexThatIsNotUniqueIsRefused()
    {
        Schema schema = SchemaWithParent(uniqueType: IndexType.Multi);

        AssertApplyRefused(schema, Payload(Constraint()), CamusDBErrorCodes.InvalidForeignKeyDefinition);
    }

    [Test]
    public void ParentWithRowLevelTtlIsRefused()
    {
        Schema schema = SchemaWithParent();
        schema.Tables["cities"].Settings = new Dictionary<string, string> { [TableSettings.TtlExpirationExpressionKey] = "id" };

        AssertApplyRefused(schema, Payload(Constraint()), CamusDBErrorCodes.FeatureNotSupported);
    }

    [Test]
    public void MaterializedViewAsParentIsRefused()
    {
        Schema schema = SchemaWithParent();
        schema.Tables["cities"].Kind = RelationKind.MaterializedView;

        AssertApplyRefused(schema, Payload(Constraint()), CamusDBErrorCodes.InvalidForeignKeyDefinition);
    }

    [Test]
    public void TypeMismatchIsRefused()
    {
        Schema schema = SchemaWithParent();

        AssertApplyRefused(schema, Payload(Constraint(referencedColumnIds: [ParentIdColumn], referencedIndexId: ParentPk)), CamusDBErrorCodes.InvalidForeignKeyDefinition);
    }

    [Test]
    public void NameTakenByACheckConstraintIsRefused()
    {
        Schema schema = SchemaWithParent();
        SchemaCreateTablePayload payload = Payload(Constraint());
        payload.CheckConstraints = [new CheckConstraintSchema { Name = "weather_city_fkey", Expression = "id > 0", ReferencedColumns = ["id"] }];

        AssertApplyRefused(schema, payload, CamusDBErrorCodes.InvalidInput);
    }

    [Test]
    public void UnsupportedActionIsRefused()
    {
        Schema schema = SchemaWithParent();

        AssertApplyRefused(schema, Payload(Constraint(onDelete: ForeignKeyAction.Cascade)), CamusDBErrorCodes.FeatureNotSupported);
    }

    [Test]
    public void BackingIndexThatDoesNotLeadWithTheColumnsIsRefused()
    {
        Schema schema = SchemaWithParent();
        SchemaCreateTablePayload payload = Payload(Constraint());
        payload.Indexes =
        [
            new TableIndexSchema("ci-pk", "~pk", ["cc-id"], IndexType.Unique, SchemaElementState.Public),
            new TableIndexSchema(ChildIndex, "weather_city", ["cc-id", ChildCityColumn], IndexType.Multi, SchemaElementState.Public),
        ];

        AssertApplyRefused(schema, payload, CamusDBErrorCodes.InvalidInternalOperation);
    }

    [Test]
    public void OwnedIndexWithoutItsConstraintIsRefused()
    {
        Schema schema = SchemaWithParent();
        SchemaCreateTablePayload payload = Payload(Constraint());
        payload.Indexes =
        [
            .. payload.Indexes!,
            new TableIndexSchema("ci-orphan", "~fk_gone", [ChildCityColumn], IndexType.Multi, SchemaElementState.Public, ownerConstraintId: "no-such-constraint"),
        ];

        AssertApplyRefused(schema, payload, CamusDBErrorCodes.InvalidInternalOperation);
    }

    [Test]
    public void SelfReferenceResolvesAgainstTheTableBeingCreated()
    {
        Schema schema = new();
        SchemaCreateTablePayload payload = Payload(Constraint(
            referencedTableId: ChildId, referencedColumnIds: ["cc-id"], referencedIndexId: "ci-pk"));
        payload.Columns = [.. payload.Columns.Select(c => c.Name == "city" ? Column(ChildCityColumn, "city", ColumnType.Integer64) : c)];

        TableSchema child = Apply(schema, payload)!;

        Assert.IsTrue(schema.ForeignKeys.ChildPlansOf(ChildId).Single().IsSelfReference);
        Assert.AreEqual(1, child.ForeignKeys!.Count);
    }

    // ── The DDL guards at apply time ────────────────────────────────────────
    //
    // These run in the apply, in log order on every node. They are what stops a DROP or TRUNCATE of a
    // parent that a child created by another node has referenced since the statement was validated.

    [Test]
    public void DropOfAReferencedParentIsRefusedAtApply()
    {
        Schema schema = SchemaWithParent();
        Apply(schema, Payload(Constraint()));

        CamusDBException exception = Assert.Throws<CamusDBException>(() => ApplyOp(schema, SchemaOp.DropTable,
            new SchemaDropTablePayload { TableName = "cities" }))!;

        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, exception.Code);
        Assert.IsTrue(schema.Tables.ContainsKey("cities"));

        // The child goes freely, and then the parent can go.
        ApplyOp(schema, SchemaOp.DropTable, new SchemaDropTablePayload { TableName = "weather" });
        ApplyOp(schema, SchemaOp.DropTable, new SchemaDropTablePayload { TableName = "cities" });
        Assert.IsFalse(schema.Tables.ContainsKey("cities"));
    }

    [Test]
    public void TruncateOfAReferencedParentIsRefusedAtApply()
    {
        Schema schema = SchemaWithParent();
        Apply(schema, Payload(Constraint()));

        CamusDBException exception = Assert.Throws<CamusDBException>(() => ApplyOp(schema, SchemaOp.TruncateTable,
            new SchemaTruncateTablePayload { TableId = ParentId, TableName = "cities", ExpectedStorageId = ParentId, NewStorageId = "P2" }))!;

        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, exception.Code);
        Assert.AreEqual(ParentId, schema.Tables["cities"].EffectiveStorageId, "A refused truncate must not swap the contents");
    }

    [Test]
    public void DropOfAReferencedOrReferencingColumnIsRefusedAtApply()
    {
        Schema schema = SchemaWithParent();
        Apply(schema, Payload(Constraint()));
        long parentVersion = schema.Tables["cities"].Version;

        foreach ((string table, string column) in new[] { ("cities", "name"), ("weather", "city") })
        {
            CamusDBException exception = Assert.Throws<CamusDBException>(() => ApplyOp(schema, SchemaOp.DropColumn,
                new SchemaAlterColumnPayload { TableName = table, Column = new SchemaColumnPayload { Name = column } }))!;

            Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, exception.Code, $"{table}.{column}");
        }

        Assert.AreEqual(parentVersion, schema.Tables["cities"].Version, "A refused drop must not bump the version");
    }

    [Test]
    public void RowLevelTtlOnAReferencedTableIsRefusedAtApply()
    {
        Schema schema = SchemaWithParent();
        Apply(schema, Payload(Constraint()));

        SchemaSetTableSettingsPayload payload = new() { TableName = "cities" };
        payload.Settings[TableSettings.TtlExpirationExpressionKey] = "id";

        CamusDBException exception = Assert.Throws<CamusDBException>(() => ApplyOp(schema, SchemaOp.SetTableSettings, payload))!;

        Assert.AreEqual(CamusDBErrorCodes.FeatureNotSupported, exception.Code);
        Assert.IsNull(schema.Tables["cities"].Settings);
    }

    // ── The foreign-key state ladder ────────────────────────────────────────

    [Test]
    public void WriteOnlyGoesPublic()
    {
        Schema schema = SchemaWithParent();
        Apply(schema, Payload(Constraint(state: SchemaElementState.WriteOnly)));

        SchemaChangeLogEntry entry = StateEntry(SchemaElementState.Public);
        Assert.IsFalse(SchemaDeltaApplier.WasSchemaDeltaApplied(schema, entry));

        long tableVersion = schema.Tables["weather"].Version;
        SchemaDeltaApplier.ApplySchemaDelta(schema, entry);

        Assert.AreEqual(SchemaElementState.Public, schema.Tables["weather"].ForeignKeys!.Single().State);
        Assert.AreEqual(tableVersion, schema.Tables["weather"].Version, "A constraint is not part of the row encoding");
        Assert.IsTrue(SchemaDeltaApplier.WasSchemaDeltaApplied(schema, entry));
    }

    [Test]
    public void PublicCannotGoBackToWriteOnly()
    {
        Schema schema = SchemaWithParent();
        Apply(schema, Payload(Constraint()));

        CamusDBException exception = Assert.Throws<CamusDBException>(() =>
            SchemaDeltaApplier.ApplySchemaDelta(schema, StateEntry(SchemaElementState.WriteOnly)))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, exception.Code);
    }

    [Test]
    public void AbsentRemovesTheConstraintAndReleasesItsIndex()
    {
        Schema schema = SchemaWithParent();
        SchemaCreateTablePayload payload = Payload(Constraint());
        payload.Indexes =
        [
            new TableIndexSchema("ci-pk", "~pk", ["cc-id"], IndexType.Unique, SchemaElementState.Public),
            new TableIndexSchema(ChildIndex, "~fk_weather_city_fkey", [ChildCityColumn], IndexType.Multi, SchemaElementState.Public, ownerConstraintId: "fk-1"),
        ];
        Apply(schema, payload);

        SchemaChangeLogEntry entry = StateEntry(SchemaElementState.Absent);
        SchemaDeltaApplier.ApplySchemaDelta(schema, entry);

        TableSchema weather = schema.Tables["weather"];
        Assert.IsNull(weather.ForeignKeys);
        Assert.IsNull(weather.Indexes!.Single(i => i.KvId == ChildIndex).OwnerConstraintId);
        Assert.IsTrue(schema.ForeignKeys.IsEmpty);
        Assert.IsTrue(SchemaDeltaApplier.WasSchemaDeltaApplied(schema, entry));
    }

    [Test]
    public void CoordinatorPathForAForeignKeyIsOneStep()
    {
        Assert.AreEqual(new[] { SchemaElementState.Public },
            CamusDB.Core.Catalogs.SchemaChangeCoordinator.ComputePathFor(SchemaElementKind.ForeignKey, SchemaElementState.WriteOnly, SchemaElementState.Public));
        Assert.AreEqual(new[] { SchemaElementState.Absent },
            CamusDB.Core.Catalogs.SchemaChangeCoordinator.ComputePathFor(SchemaElementKind.ForeignKey, SchemaElementState.Public, SchemaElementState.Absent));
        Assert.IsEmpty(
            CamusDB.Core.Catalogs.SchemaChangeCoordinator.ComputePathFor(SchemaElementKind.ForeignKey, SchemaElementState.Absent, SchemaElementState.Public),
            "A constraint that is gone is never re-added by a job");
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static void AssertApplyRefused(Schema schema, SchemaCreateTablePayload payload, string expectedCode)
    {
        CamusDBException exception = Assert.Throws<CamusDBException>(() => Apply(schema, payload))!;
        Assert.AreEqual(expectedCode, exception.Code, exception.Message);
        Assert.IsFalse(schema.Tables.ContainsKey("weather"));
    }

    private static TableSchema? Apply(Schema schema, SchemaCreateTablePayload payload) =>
        SchemaDeltaApplier.ApplySchemaDelta(schema, new SchemaChangeLogEntry
        {
            Ts = new HLCTimestamp(1, 1, 1),
            Database = "db",
            FromVersion = schema.SchemaVersion,
            ToVersion = schema.SchemaVersion + 1,
            Op = SchemaOp.CreateTable,
            Payload = SchemaChangeLogEntryCodec.EncodePayload(payload),
        });

    private static TableSchema? ApplyOp<T>(Schema schema, SchemaOp op, T payload) =>
        SchemaDeltaApplier.ApplySchemaDelta(schema, new SchemaChangeLogEntry
        {
            Ts = new HLCTimestamp(1, 1, 1),
            Database = "db",
            FromVersion = schema.SchemaVersion,
            ToVersion = schema.SchemaVersion + 1,
            Op = op,
            Payload = SchemaChangeLogEntryCodec.EncodePayload(payload),
        });

    private static SchemaChangeLogEntry StateEntry(SchemaElementState state) => new()
    {
        Ts = new HLCTimestamp(1, 1, 1),
        Database = "db",
        FromVersion = 1,
        ToVersion = 2,
        Op = SchemaOp.SetElementState,
        Payload = SchemaChangeLogEntryCodec.EncodePayload(new SchemaElementStatePayload
        {
            TableName = "weather",
            ElementName = "weather_city_fkey",
            ElementKind = SchemaElementKind.ForeignKey,
            State = state,
        }),
    };

    private static Schema SchemaWithParent(
        IndexType uniqueType = IndexType.Unique,
        SchemaElementState uniqueState = SchemaElementState.Public)
    {
        Schema schema = new();
        schema.Tables["cities"] = new TableSchema
        {
            Id = ParentId,
            Name = "cities",
            Columns =
            [
                new TableColumnSchema(ParentIdColumn, "id", ColumnType.Integer64, notNull: true, defaultValue: null),
                new TableColumnSchema(ParentNameColumn, "name", ColumnType.String, notNull: true, defaultValue: null),
            ],
            Indexes =
            [
                new TableIndexSchema(ParentPk, "~pk", [ParentIdColumn], IndexType.Unique, SchemaElementState.Public),
                new TableIndexSchema(ParentUnique, "cities_name", [ParentNameColumn], uniqueType, uniqueState),
            ],
        };
        return schema;
    }

    private static ForeignKeySchema Constraint(
        string referencedTableId = ParentId,
        string[]? referencedColumnIds = null,
        string referencedIndexId = ParentUnique,
        ForeignKeyAction onDelete = ForeignKeyAction.NoAction,
        SchemaElementState state = SchemaElementState.Public) => new(
            "fk-1",
            "weather_city_fkey",
            [ChildCityColumn],
            referencedTableId,
            referencedColumnIds ?? [ParentNameColumn],
            referencedIndexId,
            ChildIndex,
            onDelete,
            ForeignKeyAction.NoAction,
            ForeignKeyMatch.Simple,
            state);

    private static SchemaCreateTablePayload Payload(ForeignKeySchema constraint) => new()
    {
        TableId = ChildId,
        TableName = "weather",
        Columns = [Column("cc-id", "id", ColumnType.Integer64), Column(ChildCityColumn, "city", ColumnType.String)],
        Indexes =
        [
            new TableIndexSchema("ci-pk", "~pk", ["cc-id"], IndexType.Unique, SchemaElementState.Public),
            new TableIndexSchema(ChildIndex, "weather_city", [ChildCityColumn], IndexType.Multi, SchemaElementState.Public),
        ],
        ForeignKeys = [constraint],
    };

    private static SchemaColumnPayload Column(string id, string name, ColumnType type) => new()
    {
        Id = id,
        Name = name,
        Type = type,
        NotNull = name == "id",
    };
}
