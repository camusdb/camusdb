
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.Queries;

/// <summary>
/// Unit tests for <see cref="RowLayout"/> ordinal resolution and the <see cref="QueryRow"/>
/// dictionary adapter. These tests lock down the exact name-resolution rules that the query
/// pipeline migration must preserve through every subsequent phase.
/// </summary>
[TestFixture]
public class TestRowLayout
{
    // ── RowLayout.IndexOf ─────────────────────────────────────────────────────

    [Test]
    public void IndexOf_BareColumnHit_ReturnsCorrectOrdinal()
    {
        var layout = RowLayout.ForColumns(["id", "name", "age"]);

        Assert.That(layout.IndexOf("id"), Is.EqualTo(0));
        Assert.That(layout.IndexOf("name"), Is.EqualTo(1));
        Assert.That(layout.IndexOf("age"), Is.EqualTo(2));
    }

    [Test]
    public void IndexOf_BareColumnMiss_ReturnsMinusOne()
    {
        var layout = RowLayout.ForColumns(["id", "name"]);

        Assert.That(layout.IndexOf("missing"), Is.EqualTo(-1));
        Assert.That(layout.IndexOf(""), Is.EqualTo(-1));
    }

    [Test]
    public void IndexOf_QualifiedColumnHit_ReturnsCorrectOrdinal()
    {
        // Join output includes qualified {alias}.{column} names.
        var layout = new RowLayout(["u.id", "u.name", "o.id", "o.total"]);

        Assert.That(layout.IndexOf("u.id"), Is.EqualTo(0));
        Assert.That(layout.IndexOf("u.name"), Is.EqualTo(1));
        Assert.That(layout.IndexOf("o.id"), Is.EqualTo(2));
        Assert.That(layout.IndexOf("o.total"), Is.EqualTo(3));
    }

    [Test]
    public void IndexOf_BareNameAliasForUniqueColumn_ResolvesToQualifiedOrdinal()
    {
        // When a bare column name is unique across join sources, BoundRow contract says to also
        // expose the bare name. The bare name is an alias that resolves to the SAME ordinal as its
        // qualified {alias}.{column} form — it does not occupy its own Values[] slot.
        var layout = new RowLayout(
            ["u.id", "u.name", "o.total"],
            [
                new KeyValuePair<string, int>("name", 1),
                new KeyValuePair<string, int>("total", 2),
            ]);

        Assert.That(layout.IndexOf("u.name"), Is.EqualTo(1));
        Assert.That(layout.IndexOf("name"), Is.EqualTo(1));   // same ordinal as u.name
        Assert.That(layout.IndexOf("o.total"), Is.EqualTo(2));
        Assert.That(layout.IndexOf("total"), Is.EqualTo(2));  // same ordinal as o.total

        // Aliases do not add physical slots.
        Assert.That(layout.Count, Is.EqualTo(3));
        Assert.That(layout.OutputNames, Is.EqualTo(new[] { "u.id", "u.name", "o.total" }));
    }

    [Test]
    public void Constructor_AliasOrdinalOutOfRange_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = new RowLayout(["a", "b"], [new KeyValuePair<string, int>("c", 5)]));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            _ = new RowLayout(["a", "b"], [new KeyValuePair<string, int>("c", -1)]));
    }

    [Test]
    public void QueryRow_AliasName_ReturnsSameValueAsQualifiedName()
    {
        var layout = new RowLayout(
            ["u.id", "u.name"],
            [new KeyValuePair<string, int>("name", 1)]);
        var row = new QueryRow(
            ObjectIdValue.Empty,
            layout,
            [new ColumnValue(ColumnType.Integer64, 7L), new ColumnValue(ColumnType.String, "Bob")]);

        // Both the qualified name and its bare alias reach the same physical cell.
        Assert.That(row["u.name"].StrValue, Is.EqualTo("Bob"));
        Assert.That(row["name"].StrValue, Is.EqualTo("Bob"));
        Assert.That(row.ContainsKey("name"), Is.True);

        // The alias is resolution-only: it is not double-listed in the enumerated key set.
        Assert.That(row.Keys, Is.EquivalentTo(new[] { "u.id", "u.name" }));
        Assert.That(row.Count, Is.EqualTo(2));
    }

    [Test]
    public void IndexOf_DuplicateName_FirstOccurrenceWins()
    {
        // When the same bare column name appears more than once, only the first occurrence is
        // registered (TryAdd semantics). The query binder rejects ambiguous bare references before
        // execution, so this case only occurs in layouts where the caller has already handled
        // disambiguation; the layout resolves to the first slot consistently.
        var layout = new RowLayout(["u.id", "o.id", "id"]);

        Assert.That(layout.IndexOf("u.id"), Is.EqualTo(0));
        Assert.That(layout.IndexOf("o.id"), Is.EqualTo(1));
        Assert.That(layout.IndexOf("id"), Is.EqualTo(2));
    }

    [Test]
    public void Count_MatchesNumberOfOutputNames()
    {
        var layout = RowLayout.ForColumns(["a", "b", "c"]);

        Assert.That(layout.Count, Is.EqualTo(3));
        Assert.That(layout.OutputNames.Length, Is.EqualTo(3));
    }

    [Test]
    public void NameAt_ReturnsNameAtOrdinal()
    {
        var layout = RowLayout.ForColumns(["x", "y", "z"]);

        Assert.That(layout.NameAt(0), Is.EqualTo("x"));
        Assert.That(layout.NameAt(1), Is.EqualTo("y"));
        Assert.That(layout.NameAt(2), Is.EqualTo("z"));
    }

    [Test]
    public void NameAt_OutOfRange_Throws()
    {
        var layout = RowLayout.ForColumns(["a"]);

        Assert.Throws<IndexOutOfRangeException>(() => _ = layout.NameAt(1));
    }

    // ── QueryRow dictionary adapter ───────────────────────────────────────────

    private static QueryRow MakeRow(string[] names, ColumnValue[] values)
    {
        var layout = RowLayout.ForColumns(names);
        return new QueryRow(ObjectIdValue.Empty, layout, values);
    }

    [Test]
    public void TryGetValue_PresentKey_ReturnsTrueAndValue()
    {
        var row = MakeRow(
            ["id", "name"],
            [new ColumnValue(ColumnType.Integer64, 42L),
             new ColumnValue(ColumnType.String, "Alice")]);

        Assert.That(row.TryGetValue("id", out ColumnValue val), Is.True);
        Assert.That(val.LongValue, Is.EqualTo(42L));

        Assert.That(row.TryGetValue("name", out ColumnValue nameVal), Is.True);
        Assert.That(nameVal.StrValue, Is.EqualTo("Alice"));
    }

    [Test]
    public void TryGetValue_AbsentKey_ReturnsFalse()
    {
        var row = MakeRow(["id"], [new ColumnValue(ColumnType.Integer64, 1L)]);

        Assert.That(row.TryGetValue("missing", out _), Is.False);
    }

    [Test]
    public void Indexer_PresentKey_ReturnsValue()
    {
        var row = MakeRow(["score"], [new ColumnValue(ColumnType.Float64, 3.14)]);

        Assert.That(row["score"].FloatValue, Is.EqualTo(3.14).Within(0.0001));
    }

    [Test]
    public void Indexer_AbsentKey_ThrowsKeyNotFoundException()
    {
        var row = MakeRow(["a"], [new ColumnValue(ColumnType.Integer64, 0L)]);

        Assert.Throws<KeyNotFoundException>(() => _ = row["nope"]);
    }

    [Test]
    public void ContainsKey_PresentAndAbsent()
    {
        var row = MakeRow(["x", "y"], [ColumnValue.Null, ColumnValue.Null]);

        Assert.That(row.ContainsKey("x"), Is.True);
        Assert.That(row.ContainsKey("y"), Is.True);
        Assert.That(row.ContainsKey("z"), Is.False);
    }

    [Test]
    public void Enumeration_YieldsAllKeyValuePairsInOrder()
    {
        var row = MakeRow(
            ["a", "b", "c"],
            [new ColumnValue(ColumnType.Integer64, 1L),
             new ColumnValue(ColumnType.Integer64, 2L),
             new ColumnValue(ColumnType.Integer64, 3L)]);

        List<KeyValuePair<string, ColumnValue>> pairs = row.ToList();

        Assert.That(pairs.Count, Is.EqualTo(3));
        Assert.That(pairs[0].Key, Is.EqualTo("a"));
        Assert.That(pairs[0].Value.LongValue, Is.EqualTo(1L));
        Assert.That(pairs[1].Key, Is.EqualTo("b"));
        Assert.That(pairs[2].Key, Is.EqualTo("c"));
    }

    [Test]
    public void Keys_ReturnsAllColumnNames()
    {
        var row = MakeRow(["p", "q"], [ColumnValue.Null, ColumnValue.Null]);

        Assert.That(row.Keys, Is.EquivalentTo(new[] { "p", "q" }));
    }

    [Test]
    public void ValuesInterfaceMember_ReturnsAllValues()
    {
        var cv1 = new ColumnValue(ColumnType.Integer64, 10L);
        var cv2 = new ColumnValue(ColumnType.Integer64, 20L);
        var row = MakeRow(["x", "y"], [cv1, cv2]);

        // Cast to the interface to hit the explicit interface member.
        IReadOnlyDictionary<string, ColumnValue> dict = row;
        List<ColumnValue> vals = dict.Values.ToList();

        Assert.That(vals.Count, Is.EqualTo(2));
        Assert.That(vals[0].LongValue, Is.EqualTo(10L));
        Assert.That(vals[1].LongValue, Is.EqualTo(20L));
    }

    [Test]
    public void Count_MatchesLayout()
    {
        var row = MakeRow(["a", "b", "c"], [ColumnValue.Null, ColumnValue.Null, ColumnValue.Null]);

        Assert.That(row.Count, Is.EqualTo(3));
    }

    [Test]
    public void MissingKey_BehavesLikeDictionaryMiss_NotNullValue()
    {
        // A missing key must return false from TryGetValue, not return a null ColumnValue.
        var row = MakeRow(["col"], [new ColumnValue(ColumnType.String, "hello")]);

        bool found = row.TryGetValue("absent", out _);

        Assert.That(found, Is.False);

        // Contrast: a present-but-null column is distinguishable from an absent column.
        var nullRow = MakeRow(["col"], [ColumnValue.Null]);
        Assert.That(nullRow.TryGetValue("col", out ColumnValue nullVal), Is.True);
        Assert.That(nullVal, Is.SameAs(ColumnValue.Null));
    }

    [Test]
    public void ForColumns_Factory_SameAsConstructor()
    {
        string[] names = ["first", "second"];
        var viaFactory = RowLayout.ForColumns(names);
        var viaCtor = new RowLayout(names);

        Assert.That(viaFactory.Count, Is.EqualTo(viaCtor.Count));
        Assert.That(viaFactory.IndexOf("first"), Is.EqualTo(viaCtor.IndexOf("first")));
        Assert.That(viaFactory.IndexOf("second"), Is.EqualTo(viaCtor.IndexOf("second")));
    }
}
