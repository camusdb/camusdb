
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
using NUnit.Framework;

namespace CamusDB.Tests.CommandsExecutor;

[TestFixture]
[NonParallelizable]
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
}
