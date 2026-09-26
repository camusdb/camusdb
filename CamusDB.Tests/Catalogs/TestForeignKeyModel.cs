/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json.Serialization;

using CamusDB.App.Grpc;
using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Apply;
using CamusDB.Core.Catalogs.Meta;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

using Grpc.Core;
using Kommander.Time;
using NUnit.Framework;

namespace CamusDB.Tests.Catalogs;

/// <summary>
/// The persisted foreign-key model and the in-memory graph built from it, with no enforcement: JSON
/// round trips, the copy sites that must carry the new fields, graph resolution in both directions,
/// and the error-code mappings.
/// </summary>
[TestFixture]
public sealed class TestForeignKeyModel
{
    private static TableColumnSchema Column(string id, string name, ColumnType type = ColumnType.Integer64) =>
        new(id: id, name: name, type: type, notNull: false, defaultValue: null);

    private static TableIndexSchema Index(string id, string name, IndexType type, params string[] columnIds) =>
        new(id, name, columnIds, type, SchemaElementState.Public);

    private static ForeignKeySchema Constraint(
        string id,
        string name,
        string[] columnIds,
        string parentId,
        string[] parentColumnIds,
        string parentIndexId,
        string backingIndexId,
        SchemaElementState state = SchemaElementState.Public) =>
        new(id, name, columnIds, parentId, parentColumnIds, parentIndexId, backingIndexId,
            ForeignKeyAction.NoAction, ForeignKeyAction.Restrict, ForeignKeyMatch.Simple, state);

    /// <summary>cities(name) ← weather(city), the single-column shape of the PostgreSQL example.</summary>
    private static (TableSchema parent, TableSchema child) CitiesAndWeather()
    {
        TableSchema cities = new()
        {
            Id = "P",
            Name = "cities",
            Columns = [Column("p-id", "id"), Column("p-name", "name", ColumnType.String)],
            Indexes = [Index("p-pk", "~pk", IndexType.Unique, "p-id"), Index("p-uk", "cities_name", IndexType.Unique, "p-name")]
        };

        TableSchema weather = new()
        {
            Id = "C",
            Name = "weather",
            Columns = [Column("c-id", "id"), Column("c-city", "city", ColumnType.String)],
            Indexes = [Index("c-pk", "~pk", IndexType.Unique, "c-id"), Index("c-fk", "~fk_weather_city_fkey", IndexType.Multi, "c-city")],
            ForeignKeys = [Constraint("fk1", "weather_city_fkey", ["c-city"], "P", ["p-name"], "p-uk", "c-fk")]
        };

        return (cities, weather);
    }

    // ── Persistence ─────────────────────────────────────────────────────────

    [Test]
    public void TableWithoutForeignKeysRoundTripsAsNull()
    {
        (TableSchema cities, _) = CitiesAndWeather();

        TableSchema loaded = RoundTrip(cities);

        Assert.IsNull(loaded.ForeignKeys, "A table with no foreign keys must load with a null list");
    }

    [Test]
    public void ForeignKeyRoundTripsWithEveryField()
    {
        ForeignKeySchema original = new(
            "fk-id", "orders_customer_fkey", ["c1", "c2"], "parent-id", ["p2", "p1"], "parent-index", "backing-index",
            ForeignKeyAction.Restrict, ForeignKeyAction.SetDefault, ForeignKeyMatch.Full, SchemaElementState.WriteOnly);

        TableSchema table = new() { Id = "T", Name = "orders", Columns = [], ForeignKeys = [original] };

        ForeignKeySchema loaded = RoundTrip(table).ForeignKeys!.Single();

        AssertAllPropertiesEqual(original, loaded);
    }

    [Test]
    public void IndexOwnerRoundTripsWithEveryPersistedField()
    {
        TableIndexSchema original = new(
            "ix-id", "~fk_orders_customer_fkey", ["c1", "c2"], IndexType.Multi, SchemaElementState.WriteOnly,
            startOffset: "0011", columnDirections: [OrderType.Ascending, OrderType.Descending],
            includeColumnIds: ["c3"], comment: "auto", ownerConstraintId: "fk-id");

        byte[] bytes = MetaJsonSerializer.Serialize(original, MetaJsonContext.Default.TableIndexSchema);
        TableIndexSchema loaded = MetaJsonSerializer.Deserialize(bytes, MetaJsonContext.Default.TableIndexSchema);

        // Columns and IncludeColumns are the in-memory names; the persisted form carries ids only.
        AssertAllPropertiesEqual(original, loaded, exclude: [nameof(TableIndexSchema.Columns), nameof(TableIndexSchema.IncludeColumns)]);
    }

    [Test]
    public void WithStateCarriesEveryOtherField()
    {
        (_, TableSchema weather) = CitiesAndWeather();
        ForeignKeySchema before = weather.ForeignKeys![0].WithState(SchemaElementState.WriteOnly);

        ForeignKeySchema after = before.WithState(SchemaElementState.Public);

        Assert.AreEqual(SchemaElementState.Public, after.State);
        AssertAllPropertiesEqual(before, after, exclude: [nameof(ForeignKeySchema.State), nameof(ForeignKeySchema.IsEnforced)]);
    }

    // ── Copy sites ──────────────────────────────────────────────────────────

    /// <summary>
    /// Every persisted property of <see cref="TableSchema"/> must reach the meta key. A property that
    /// <c>WithoutHistory</c> forgets looks correct in memory for the life of the process and is lost on
    /// the next restart, so this test covers every property, present and future.
    /// </summary>
    [Test]
    public void WithoutHistoryCopiesEveryPersistedProperty()
    {
        TableSchema source = FullyPopulatedTable();

        TableSchema copy = SchemaMetaStore.WithoutHistory(source);

        AssertAllPropertiesEqual(source, copy, exclude:
        [
            // History lives in its own per-version keys, which is the point of this projection.
            nameof(TableSchema.SchemaHistory)
        ]);
    }

    /// <summary>
    /// A schema delta is dry-run against a <c>CloneTable</c> copy, and a truncate retires one. A property
    /// the clone forgets is silently missing from both.
    /// </summary>
    [Test]
    public void CloneTableCopiesEveryProperty()
    {
        TableSchema source = FullyPopulatedTable();

        TableSchema copy = SchemaReplicator.CloneTable(source);

        // The clone deep-copies the history, so its entries are compared by value below.
        AssertAllPropertiesEqual(source, copy, exclude: [nameof(TableSchema.SchemaHistory)]);
        Assert.AreEqual(
            source.SchemaHistory!.Select(h => h.Version),
            copy.SchemaHistory!.Select(h => h.Version),
            "TableSchema.SchemaHistory was not copied");
        Assert.AreNotSame(source.ForeignKeys, copy.ForeignKeys, "The clone must not share the live list");
    }

    [Test]
    public void IndexRebuildsKeepTheOwnerConstraint()
    {
        Schema schema = new();
        (TableSchema cities, TableSchema weather) = CitiesAndWeather();
        weather.Indexes![1] = new TableIndexSchema("c-fk", "~fk_weather_city_fkey", ["c-city"], IndexType.Multi,
            SchemaElementState.WriteOnly, ownerConstraintId: "fk1");
        schema.Tables.Add(cities.Name!, cities);
        schema.Tables.Add(weather.Name!, weather);

        ElementStateApplier.ApplyElementState(schema, new SchemaElementStatePayload
        {
            TableName = "weather", ElementName = "~fk_weather_city_fkey", State = SchemaElementState.Public, ElementKind = SchemaElementKind.Index
        });
        Assert.AreEqual("fk1", weather.Indexes[1].OwnerConstraintId, "A state change must keep the owner");

        TableDeltaApplier.ApplySetComment(schema, new SchemaSetCommentPayload
        {
            TableName = "weather", Target = CommentTarget.Index, ElementName = "~fk_weather_city_fkey", Comment = "backs a foreign key"
        });
        Assert.AreEqual("fk1", weather.Indexes[1].OwnerConstraintId, "A comment must keep the owner");

        IndexDeltaApplier.ApplyRenameIndex(schema, new SchemaRenamePayload
        {
            TableName = "weather", Kind = SchemaRenameKind.Index, ElementName = "~fk_weather_city_fkey", NewName = "weather_city_idx"
        });
        Assert.AreEqual("fk1", weather.Indexes[1].OwnerConstraintId, "A rename must keep the owner");
    }

    // ── Graph ───────────────────────────────────────────────────────────────

    [Test]
    public void EmptyDatabaseSharesTheEmptyGraph()
    {
        (TableSchema cities, _) = CitiesAndWeather();

        ForeignKeyGraph graph = ForeignKeyGraph.Build([cities]);

        Assert.AreSame(ForeignKeyGraph.Empty, graph);
        Assert.IsTrue(graph.IsEmpty);
        Assert.IsEmpty(graph.ChildPlansOf("P"));
        Assert.IsEmpty(graph.ParentPlansOf("P"));
    }

    [Test]
    public void SingleColumnConstraintResolvesInBothDirections()
    {
        (TableSchema cities, TableSchema weather) = CitiesAndWeather();

        ForeignKeyGraph graph = ForeignKeyGraph.Build([cities, weather]);

        Assert.IsFalse(graph.IsEmpty);
        ForeignKeyPlan plan = graph.ChildPlansOf("C").Single();
        Assert.AreSame(plan, graph.ParentPlansOf("P").Single());
        Assert.IsEmpty(graph.ChildPlansOf("P"));
        Assert.IsEmpty(graph.ParentPlansOf("C"));

        Assert.IsTrue(plan.IsResolved);
        Assert.IsTrue(plan.IsEnforced);
        Assert.IsFalse(plan.IsSelfReference);
        Assert.AreEqual(new[] { "city" }, plan.ChildColumnNames);
        Assert.AreEqual(new[] { "name" }, plan.ParentColumnNames);
        Assert.AreEqual(new[] { 0 }, plan.ParentKeyOrder);
        Assert.AreEqual(new[] { 0 }, plan.BackingKeyOrder);
        Assert.IsEmpty(graph.Unresolved);
    }

    /// <summary>
    /// The constraint lists (a, b), the parent index is on (y, x) and the backing index leads with
    /// (b, a) and has a third column. The key orders must map each index position to the constraint
    /// position that supplies it.
    /// </summary>
    [Test]
    public void CompositeConstraintMapsIndexPositionsToConstraintPositions()
    {
        TableSchema parent = new()
        {
            Id = "P", Name = "parent",
            Columns = [Column("px", "x"), Column("py", "y")],
            Indexes = [new TableIndexSchema("p-uk", "parent_yx", ["py", "px"], IndexType.Unique, SchemaElementState.Public,
                columnDirections: [OrderType.Descending, OrderType.Ascending])]
        };

        TableSchema child = new()
        {
            Id = "C", Name = "child",
            Columns = [Column("ca", "a"), Column("cb", "b"), Column("cz", "z")],
            Indexes = [Index("c-ix", "child_baz", IndexType.Multi, "cb", "ca", "cz")],
            ForeignKeys = [Constraint("fk", "child_ab_fkey", ["ca", "cb"], "P", ["px", "py"], "p-uk", "c-ix")]
        };

        ForeignKeyPlan plan = ForeignKeyGraph.Build([parent, child]).ChildPlansOf("C").Single();

        Assert.IsTrue(plan.IsResolved, plan.UnresolvedReason);
        Assert.AreEqual(new[] { "a", "b" }, plan.ChildColumnNames);
        Assert.AreEqual(new[] { "x", "y" }, plan.ParentColumnNames);
        // Parent index position 0 is y, supplied by constraint position 1 (b); position 1 is x, from a.
        Assert.AreEqual(new[] { 1, 0 }, plan.ParentKeyOrder);
        Assert.AreEqual(new[] { OrderType.Descending, OrderType.Ascending }, plan.ParentKeyDirections);
        // Backing index leads with b then a.
        Assert.AreEqual(new[] { 1, 0 }, plan.BackingKeyOrder);
    }

    [Test]
    public void SelfReferenceAppearsOnceInEachDirectionOfTheSameTable()
    {
        TableSchema employees = new()
        {
            Id = "E", Name = "employees",
            Columns = [Column("e-id", "id"), Column("e-mgr", "manager_id")],
            Indexes = [Index("e-pk", "~pk", IndexType.Unique, "e-id"), Index("e-mgr-ix", "employees_manager", IndexType.Multi, "e-mgr")],
            ForeignKeys = [Constraint("fk", "employees_manager_id_fkey", ["e-mgr"], "E", ["e-id"], "e-pk", "e-mgr-ix")]
        };

        ForeignKeyGraph graph = ForeignKeyGraph.Build([employees]);

        ForeignKeyPlan plan = graph.ChildPlansOf("E").Single();
        Assert.AreSame(plan, graph.ParentPlansOf("E").Single());
        Assert.IsTrue(plan.IsSelfReference);
        Assert.IsTrue(plan.IsEnforced);
    }

    [Test]
    public void WriteOnlyConstraintIsEnforcedAndStaysInTheGraph()
    {
        (TableSchema cities, TableSchema weather) = CitiesAndWeather();
        weather.ForeignKeys![0] = weather.ForeignKeys[0].WithState(SchemaElementState.WriteOnly);

        ForeignKeyPlan plan = ForeignKeyGraph.Build([cities, weather]).ChildPlansOf("C").Single();

        Assert.IsTrue(plan.IsEnforced, "WriteOnly is enforced on both sides; only validation is pending");
    }

    [TestCase("missing parent table")]
    [TestCase("missing child column")]
    [TestCase("missing parent column")]
    [TestCase("missing parent index")]
    [TestCase("parent index not unique")]
    [TestCase("parent index on other columns")]
    [TestCase("missing backing index")]
    [TestCase("backing index on other columns")]
    public void UnresolvedConstraintIsNotEnforcedAndDoesNotThrow(string defect)
    {
        (TableSchema cities, TableSchema weather) = CitiesAndWeather();
        List<TableSchema> tables = [cities, weather];

        switch (defect)
        {
            case "missing parent table":
                tables.Remove(cities);
                break;
            case "missing child column":
                weather.Columns!.RemoveAt(1);
                break;
            case "missing parent column":
                cities.Columns!.RemoveAt(1);
                break;
            case "missing parent index":
                cities.Indexes!.RemoveAt(1);
                break;
            case "parent index not unique":
                cities.Indexes![1] = Index("p-uk", "cities_name", IndexType.Multi, "p-name");
                break;
            case "parent index on other columns":
                cities.Indexes![1] = Index("p-uk", "cities_name", IndexType.Unique, "p-id");
                break;
            case "missing backing index":
                weather.Indexes!.RemoveAt(1);
                break;
            case "backing index on other columns":
                weather.Indexes![1] = Index("c-fk", "~fk_weather_city_fkey", IndexType.Multi, "c-id", "c-city");
                break;
        }

        ForeignKeyGraph graph = null!;
        Assert.DoesNotThrow(() => graph = ForeignKeyGraph.Build(tables));

        ForeignKeyPlan plan = graph.ChildPlansOf("C").Single();
        Assert.IsFalse(plan.IsResolved);
        Assert.IsFalse(plan.IsEnforced);
        Assert.IsNotNull(plan.UnresolvedReason);
        Assert.AreEqual(1, graph.Unresolved.Count);
        Assert.That(graph.Unresolved[0], Does.Contain("weather_city_fkey"));

        // Still indexed under the parent id, so a DDL guard keeps seeing the reference.
        Assert.AreSame(plan, graph.ParentPlansOf("P").Single());
    }

    [Test]
    public void SchemaRebuildsTheGraphLazilyAndOnDemand()
    {
        Schema schema = new();
        (TableSchema cities, TableSchema weather) = CitiesAndWeather();
        schema.Tables.Add(cities.Name!, cities);

        Assert.IsTrue(schema.ForeignKeys.IsEmpty, "A schema that was never rebuilt builds its graph on first read");

        schema.Tables.Add(weather.Name!, weather);
        Assert.IsTrue(schema.ForeignKeys.IsEmpty, "The graph is a published snapshot, not a live view");

        schema.RebuildForeignKeyGraph();
        Assert.AreEqual(1, schema.ForeignKeys.ParentPlansOf("P").Length);
    }

    // ── Error codes ─────────────────────────────────────────────────────────

    [TestCase(CamusDBErrorCodes.ForeignKeyViolation, "CADB0304", 409, StatusCode.FailedPrecondition)]
    [TestCase(CamusDBErrorCodes.ForeignKeyRestrictDelete, "CADB0305", 409, StatusCode.FailedPrecondition)]
    [TestCase(CamusDBErrorCodes.ForeignKeyRestrictUpdate, "CADB0306", 409, StatusCode.FailedPrecondition)]
    [TestCase(CamusDBErrorCodes.InvalidForeignKeyDefinition, "CADB0415", 400, StatusCode.InvalidArgument)]
    [TestCase(CamusDBErrorCodes.ForeignKeyCycle, "CADB0416", 400, StatusCode.InvalidArgument)]
    [TestCase(CamusDBErrorCodes.DependentObjectsExist, "CADB0530", 409, StatusCode.FailedPrecondition)]
    public void ErrorCodeMapsToItsDocumentedStatus(string code, string value, int http, StatusCode grpc)
    {
        Assert.AreEqual(value, code);
        Assert.AreEqual(http, CamusDBErrorCodes.GetHttpStatus(code));
        Assert.AreEqual(grpc, GrpcErrorMapper.GetGrpcStatus(code));
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private static TableSchema RoundTrip(TableSchema table)
    {
        byte[] bytes = MetaJsonSerializer.Serialize(SchemaMetaStore.WithoutHistory(table), MetaJsonContext.Default.TableSchema);
        return MetaJsonSerializer.Deserialize(bytes, MetaJsonContext.Default.TableSchema);
    }

    /// <summary>
    /// A table with every settable property set to a value that differs from its default. A property
    /// with a type this method does not know fails the calling test, which is the prompt to add it here
    /// and to check the copy sites.
    /// </summary>
    private static TableSchema FullyPopulatedTable()
    {
        (_, TableSchema weather) = CitiesAndWeather();
        TableSchema table = new();

        foreach (PropertyInfo property in SettableProperties(typeof(TableSchema)))
        {
            object value = property.Name switch
            {
                nameof(TableSchema.Id) => "T",
                nameof(TableSchema.Name) => "weather",
                nameof(TableSchema.StorageId) => "S",
                nameof(TableSchema.Comment) => "a comment",
                nameof(TableSchema.Version) => 3,
                nameof(TableSchema.ContentsGeneration) => 4L,
                nameof(TableSchema.MetadataGeneration) => 5L,
                nameof(TableSchema.IsPopulated) => true,
                nameof(TableSchema.Kind) => RelationKind.MaterializedView,
                nameof(TableSchema.ContentsValidFrom) => new HLCTimestamp(1, 10, 1),
                nameof(TableSchema.RefreshedAt) => new HLCTimestamp(1, 20, 2),
                nameof(TableSchema.Columns) => weather.Columns!,
                nameof(TableSchema.Indexes) => weather.Indexes!,
                nameof(TableSchema.ForeignKeys) => weather.ForeignKeys!,
                nameof(TableSchema.CheckConstraints) => new List<CheckConstraintSchema> { new() { Name = "c", Expression = "id > 0", ReferencedColumns = ["id"] } },
                nameof(TableSchema.Settings) => new Dictionary<string, string> { ["k"] = "v" },
                nameof(TableSchema.ViewDefinition) => new ViewDefinition(),
                nameof(TableSchema.SchemaHistory) => new List<TableSchemaHistory> { new() { Version = 1, Columns = weather.Columns } },
                _ => throw new AssertionException(
                    $"TableSchema.{property.Name} has no sample value. Add one here, and check that " +
                    "WithoutHistory and CloneTable copy it.")
            };

            property.SetValue(table, value);
        }

        return table;
    }

    private static IEnumerable<PropertyInfo> SettableProperties(Type type) =>
        type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && p.SetMethod!.IsPublic && p.GetCustomAttribute<JsonIgnoreAttribute>() is null);

    private static void AssertAllPropertiesEqual(object expected, object actual, string[]? exclude = null)
    {
        foreach (PropertyInfo property in expected.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.GetIndexParameters().Length > 0 || (exclude?.Contains(property.Name) ?? false))
                continue;

            if (property.GetCustomAttribute<JsonIgnoreAttribute>() is not null)
                continue;

            object? a = property.GetValue(expected);
            object? b = property.GetValue(actual);

            if (a is IEnumerable ea && a is not string && b is IEnumerable eb)
                CollectionAssert.AreEqual(ea, eb, $"{expected.GetType().Name}.{property.Name} was not copied");
            else
                Assert.AreEqual(a, b, $"{expected.GetType().Name}.{property.Name} was not copied");
        }
    }
}
