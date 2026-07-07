
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

[TestFixture]
public sealed class TestQueryRowMerger
{
    [Test]
    public void QualifyRow_AddsAliasPrefixToBareColumnNames()
    {
        Dictionary<string, ColumnValue> row = new()
        {
            { "email", new(ColumnType.String, "a@example.com") },
            { "id", new(ColumnType.Id, "abc") },
        };

        Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(row, "u");

        Assert.AreEqual("a@example.com", qualified["u.email"].StrValue);
        Assert.AreEqual("abc", qualified["u.id"].StrValue);
    }

    [Test]
    public void QualifyRow_PreservesAlreadyQualifiedKeys()
    {
        Dictionary<string, ColumnValue> row = new()
        {
            { "u.email", new(ColumnType.String, "a@example.com") },
        };

        Dictionary<string, ColumnValue> qualified = QueryRowMerger.QualifyRow(row, "u");

        Assert.AreEqual("a@example.com", qualified["u.email"].StrValue);
    }

    [Test]
    public void MergeRows_QualifiesRightSideAndPreservesLeftQualifiedKeys()
    {
        Dictionary<string, ColumnValue> left = new()
        {
            { "u.email", new(ColumnType.String, "a@example.com") },
            { "u.id", new(ColumnType.Id, "abc") },
        };

        Dictionary<string, ColumnValue> right = new()
        {
            { "title", new(ColumnType.String, "Post A") },
            { "user_id", new(ColumnType.Id, "abc") },
        };

        Dictionary<string, ColumnValue> merged = QueryRowMerger.MergeRows(left, right, "p");

        Assert.AreEqual("a@example.com", merged["u.email"].StrValue);
        Assert.AreEqual("Post A", merged["p.title"].StrValue);
        Assert.AreEqual("abc", merged["p.user_id"].StrValue);
        Assert.AreEqual(4, merged.Count);
    }

    [Test]
    public void MergeRows_ThrowsOnColumnCollision()
    {
        Dictionary<string, ColumnValue> left = new()
        {
            { "u.id", new(ColumnType.Id, "left-id") },
        };

        Dictionary<string, ColumnValue> right = new()
        {
            { "u.id", new(ColumnType.Id, "right-id") },
        };

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            QueryRowMerger.MergeRows(left, right, "p"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInternalOperation, ex!.Code);
    }

    [Test]
    public void BuildJoinLayout_ProducesQualifiedPhysicalSlotsAndUniqueBaresAsAliases()
    {
        Dictionary<string, ColumnValue> left = new()
        {
            { "u.id",    new(ColumnType.Id, "abc") },
            { "u.email", new(ColumnType.String, "a@example.com") },
        };

        Dictionary<string, ColumnValue> right = new()
        {
            { "title",   new(ColumnType.String, "Post A") },
            { "user_id", new(ColumnType.Id, "abc") },
        };

        RowLayout layout = QueryRowMerger.BuildJoinLayout(left, right, "p");

        // Physical slots: two left (already qualified) + two right (qualified with "p")
        Assert.AreEqual(4, layout.Count);
        Assert.That(layout.OutputNames, Has.Member("u.id"));
        Assert.That(layout.OutputNames, Has.Member("u.email"));
        Assert.That(layout.OutputNames, Has.Member("p.title"));
        Assert.That(layout.OutputNames, Has.Member("p.user_id"));

        // Bare alias: "id" is unique (only u.id) → resolves; "email" unique → resolves.
        // "title" and "user_id" are also unique.
        Assert.That(layout.IndexOf("id"),      Is.GreaterThanOrEqualTo(0));
        Assert.That(layout.IndexOf("email"),   Is.GreaterThanOrEqualTo(0));
        Assert.That(layout.IndexOf("title"),   Is.GreaterThanOrEqualTo(0));
        Assert.That(layout.IndexOf("user_id"), Is.GreaterThanOrEqualTo(0));
    }

    [Test]
    public void BuildJoinLayout_AmbiguousBareNames_NotAddedAsAliases()
    {
        // Both left and right have a column called "id" after qualification — bare "id"
        // must not be added as an alias since it's ambiguous.
        Dictionary<string, ColumnValue> left  = new() { { "u.id", new(ColumnType.Id, "left")  } };
        Dictionary<string, ColumnValue> right = new() { { "id",   new(ColumnType.Id, "right") } };

        RowLayout layout = QueryRowMerger.BuildJoinLayout(left, right, "p");

        // Physical slots: "u.id" and "p.id"
        Assert.AreEqual(2, layout.Count);
        // Bare "id" is ambiguous — IndexOf should not resolve it to either slot.
        Assert.That(layout.IndexOf("id"), Is.LessThan(0));
        // But qualified names resolve fine.
        Assert.That(layout.IndexOf("u.id"), Is.GreaterThanOrEqualTo(0));
        Assert.That(layout.IndexOf("p.id"), Is.GreaterThanOrEqualTo(0));
    }

    [Test]
    public void MergeRowsAsQueryRow_HappyPath_ValuesMatchLayoutOrdinals()
    {
        Dictionary<string, ColumnValue> left = new()
        {
            { "u.id",    new(ColumnType.Id, "abc") },
            { "u.email", new(ColumnType.String, "a@example.com") },
        };

        Dictionary<string, ColumnValue> right = new()
        {
            { "title",   new(ColumnType.String, "Post A") },
            { "user_id", new(ColumnType.Id, "abc") },
        };

        RowLayout layout = QueryRowMerger.BuildJoinLayout(left, right, "p");
        QueryRow merged  = QueryRowMerger.MergeRowsAsQueryRow(left, right, "p", layout);

        Assert.AreEqual(4, merged.Values.Length);
        Assert.AreEqual("abc",          merged["u.id"].StrValue);
        Assert.AreEqual("a@example.com", merged["u.email"].StrValue);
        Assert.AreEqual("Post A",        merged["p.title"].StrValue);
        Assert.AreEqual("abc",           merged["p.user_id"].StrValue);
        // Bare aliases also resolve.
        Assert.AreEqual("abc",           merged["id"].StrValue);
        Assert.AreEqual("Post A",        merged["title"].StrValue);
    }

    /// <summary>
    /// Verifies that MergeRowsAsQueryRow throws when a later row pair has an extra key not
    /// present in the layout built from the first pair. Without this guard the value would be
    /// silently dropped, producing a hard-to-diagnose wrong result downstream.
    /// </summary>
    [Test]
    public void MergeRowsAsQueryRow_ExtraKeyOnLaterPair_ThrowsDivergenceException()
    {
        Dictionary<string, ColumnValue> left1  = new() { { "u.id", new(ColumnType.Id, "abc") } };
        Dictionary<string, ColumnValue> right1 = new() { { "title", new(ColumnType.String, "Post A") } };

        RowLayout layout = QueryRowMerger.BuildJoinLayout(left1, right1, "p");

        // Second pair has an extra right key not in the layout.
        Dictionary<string, ColumnValue> left2  = new() { { "u.id", new(ColumnType.Id, "xyz") } };
        Dictionary<string, ColumnValue> right2 = new()
        {
            { "title",  new(ColumnType.String, "Post B") },
            { "author", new(ColumnType.String, "Alice") },   // extra — not in layout
        };

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            QueryRowMerger.MergeRowsAsQueryRow(left2, right2, "p", layout));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInternalOperation, ex!.Code);
        StringAssert.Contains("diverged", ex.Message);
    }

    /// <summary>
    /// Verifies that MergeRowsAsQueryRow throws when a later row pair is missing a key that
    /// the layout expects, which would otherwise leave a slot as a null ColumnValue and cause
    /// a deferred NRE or silent wrong result when the row is read or compared downstream.
    /// </summary>
    [Test]
    public void MergeRowsAsQueryRow_MissingKeyOnLaterPair_ThrowsDivergenceException()
    {
        Dictionary<string, ColumnValue> left1  = new() { { "u.id", new(ColumnType.Id, "abc") } };
        Dictionary<string, ColumnValue> right1 = new() { { "title", new(ColumnType.String, "Post A") } };

        RowLayout layout = QueryRowMerger.BuildJoinLayout(left1, right1, "p");

        // Second pair is missing the right-side "title" key entirely.
        Dictionary<string, ColumnValue> left2  = new() { { "u.id", new(ColumnType.Id, "xyz") } };
        Dictionary<string, ColumnValue> right2 = new();  // empty — missing "title"

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            QueryRowMerger.MergeRowsAsQueryRow(left2, right2, "p", layout));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInternalOperation, ex!.Code);
        StringAssert.Contains("diverged", ex.Message);
    }

    /// <summary>
    /// Verifies the write-once guard: when a later pair carries a key that resolves to an
    /// already-filled slot (here an already-qualified right key colliding with a left slot),
    /// MergeRowsAsQueryRow throws rather than silently overwriting the left value and leaving a
    /// different slot null — which a plain placement counter would fail to detect.
    /// </summary>
    [Test]
    public void MergeRowsAsQueryRow_KeyCollidesWithFilledSlot_ThrowsDivergenceException()
    {
        Dictionary<string, ColumnValue> left1  = new() { { "u.id", new(ColumnType.Id, "abc") } };
        Dictionary<string, ColumnValue> right1 = new() { { "title", new(ColumnType.String, "Post A") } };

        RowLayout layout = QueryRowMerger.BuildJoinLayout(left1, right1, "p");

        // Second pair: the right row carries an already-qualified "u.id" that collides with the
        // left slot at ordinal 0, while "p.title" is left unfilled.
        Dictionary<string, ColumnValue> left2  = new() { { "u.id", new(ColumnType.Id, "xyz") } };
        Dictionary<string, ColumnValue> right2 = new() { { "u.id", new(ColumnType.Id, "collide") } };

        CamusDBException? ex = Assert.Throws<CamusDBException>(() =>
            QueryRowMerger.MergeRowsAsQueryRow(left2, right2, "p", layout));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInternalOperation, ex!.Code);
        StringAssert.Contains("diverged", ex.Message);
    }
}
