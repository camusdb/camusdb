
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Collections.Generic;

using CamusDB.Core.Catalogs.Meta;

namespace CamusDB.Tests.Catalogs;

/// <summary>
/// Pins the shape of every catalog metadata key, and the routing invariant they all share.
///
/// <para>The invariant matters more than any single shape: Kahuna routes a point write by the
/// substring before the last <c>'/'</c>, and matches a bucket scan against that same substring. A
/// meta key whose last-slash prefix is not <c>{dbId}/meta</c> lands in a different partition, and
/// the single scan that the load path and the database purge both perform can no longer reach it.
/// A key written there is not lost, it is invisible — which is far harder to notice than a
/// crash.</para>
/// </summary>
[TestFixture]
public sealed class TestMetaKeys
{
    private const string DbId = "db1";
    private const string TableId = "A0";
    private const string ViewId = "V7";

    /// <summary>
    /// Every key family, as (description, key). A new family must be added here, which is the point:
    /// the routing test below then covers it without further work.
    /// </summary>
    private static IEnumerable<(string Description, string Key)> AllKeys()
    {
        yield return ("system", MetaKeys.SystemKey(DbId));
        yield return ("version", MetaKeys.VersionKey(DbId));
        yield return ("table", MetaKeys.TableKey(DbId, TableId));
        yield return ("history", MetaKeys.HistoryKey(DbId, TableId, 3));
        yield return ("view", MetaKeys.ViewKey(DbId, ViewId));
        yield return ("coordinator", MetaKeys.CoordinatorKey(DbId, TableId, "idx_name"));
        yield return ("refresh job", MetaKeys.RefreshJobKey(DbId, TableId));
        yield return ("orphan", MetaKeys.OrphanKey(DbId, TableId));
        yield return ("keyspace catalog", MetaKeys.KeyspaceCatalogKey(DbId, TableId));
    }

    [Test]
    public void EveryMetaKeyRoutesToTheSharedBucket()
    {
        string bucket = MetaKeys.MetaBucketPrefix(DbId);

        foreach ((string description, string key) in AllKeys())
        {
            int lastSlash = key.LastIndexOf('/');

            Assert.Greater(lastSlash, 0, $"the {description} key has no '/' at all: '{key}'");
            Assert.AreEqual(
                bucket,
                key[..lastSlash],
                $"the {description} key '{key}' does not route to the shared '{bucket}' bucket; " +
                "a sub-field separator must be ':' and never '/'"
            );
        }
    }

    [Test]
    public void NoMetaKeySeparatesASubFieldWithASlash()
    {
        string bucket = MetaKeys.MetaBucketPrefix(DbId);

        foreach ((string description, string key) in AllKeys())
        {
            string suffix = key[bucket.Length..];

            Assert.AreEqual(
                1,
                suffix.Split('/').Length - 1,
                $"the {description} key '{key}' holds more than one '/' after the bucket; " +
                "that splits the family across partitions"
            );
        }
    }

    [Test]
    public void EveryPrefixIsAPrefixOfItsKey()
    {
        Assert.That(MetaKeys.TableKey(DbId, TableId), Does.StartWith(MetaKeys.TableKeyPrefix(DbId)));
        Assert.That(MetaKeys.HistoryKey(DbId, TableId, 3), Does.StartWith(MetaKeys.HistoryKeyPrefix(DbId, TableId)));
        Assert.That(MetaKeys.ViewKey(DbId, ViewId), Does.StartWith(MetaKeys.ViewKeyPrefix(DbId)));
        Assert.That(MetaKeys.CoordinatorKey(DbId, TableId, "e"), Does.StartWith(MetaKeys.CoordinatorKeyPrefix(DbId)));
        Assert.That(MetaKeys.RefreshJobKey(DbId, TableId), Does.StartWith(MetaKeys.RefreshJobKeyPrefix(DbId)));
        Assert.That(MetaKeys.OrphanKey(DbId, TableId), Does.StartWith(MetaKeys.OrphanKeyPrefix(DbId)));
        Assert.That(MetaKeys.KeyspaceCatalogKey(DbId, TableId), Does.StartWith(MetaKeys.KeyspaceCatalogKeyPrefix(DbId)));
    }

    [Test]
    public void KeyShapesAreExact()
    {
        Assert.AreEqual("db1/meta", MetaKeys.MetaBucketPrefix(DbId));
        Assert.AreEqual("db1/meta/system", MetaKeys.SystemKey(DbId));
        Assert.AreEqual("db1/meta/version", MetaKeys.VersionKey(DbId));
        Assert.AreEqual("db1/meta/table:A0", MetaKeys.TableKey(DbId, TableId));
        Assert.AreEqual("db1/meta/history:A0:3", MetaKeys.HistoryKey(DbId, TableId, 3));
        Assert.AreEqual("db1/meta/view:V7", MetaKeys.ViewKey(DbId, ViewId));
        Assert.AreEqual("db1/meta/mvrefresh:A0", MetaKeys.RefreshJobKey(DbId, TableId));
        Assert.AreEqual("db1/meta/orphan:A0", MetaKeys.OrphanKey(DbId, TableId));
        Assert.AreEqual("db1/meta/keyspace:A0", MetaKeys.KeyspaceCatalogKey(DbId, TableId));
    }

    /// <summary>
    /// The coordinator key joins the table id and the element name with '~'. Neither part can
    /// contain that character, so the two fields stay unambiguous even when an element name holds
    /// a ':' or a '-'.
    /// </summary>
    [Test]
    public void CoordinatorKeyJoinsTableIdAndElementNameWithATilde()
    {
        Assert.AreEqual("db1/meta/coordinator:A0~idx_name", MetaKeys.CoordinatorKey(DbId, TableId, "idx_name"));
    }

    /// <summary>
    /// The history key keeps a trailing ':' on its prefix so a scan for one table's history cannot
    /// also match a table whose id merely starts with the same characters.
    /// </summary>
    [Test]
    public void HistoryPrefixDoesNotMatchATableIdThatStartsWithTheSameCharacters()
    {
        string prefixForA = MetaKeys.HistoryKeyPrefix(DbId, "A");

        Assert.That(MetaKeys.HistoryKey(DbId, "A0", 1), Does.Not.StartWith(prefixForA));
        Assert.That(MetaKeys.HistoryKey(DbId, "A", 1), Does.StartWith(prefixForA));
    }

    [Test]
    public void PrefixUpperBoundIncrementsTheLastCodeUnit()
    {
        Assert.AreEqual("db1/meta/tablf", MetaKeys.PrefixUpperBound("db1/meta/table"));
    }

    /// <summary>
    /// The bound is exclusive: every key that starts with the prefix must sort below it, and the
    /// first key outside the family must not.
    /// </summary>
    [Test]
    public void PrefixUpperBoundExcludesTheWholeFamilyAndNothingElse()
    {
        string prefix = MetaKeys.TableKeyPrefix(DbId);
        string bound = MetaKeys.PrefixUpperBound(prefix)!;

        Assert.Less(string.CompareOrdinal(MetaKeys.TableKey(DbId, "A0"), bound), 0);
        Assert.Less(string.CompareOrdinal(MetaKeys.TableKey(DbId, "zzzzzzzz"), bound), 0);
        Assert.Greater(string.CompareOrdinal(MetaKeys.VersionKey(DbId), bound), 0);
    }

    [Test]
    public void PrefixUpperBoundReturnsNullWhenNoBoundExists()
    {
        Assert.IsNull(MetaKeys.PrefixUpperBound(new string(char.MaxValue, 3)));
    }

    /// <summary>
    /// Two databases must never share a key, whatever their ids look like.
    /// </summary>
    [Test]
    public void KeysOfDifferentDatabasesNeverCollide()
    {
        Assert.AreNotEqual(MetaKeys.TableKey("db1", TableId), MetaKeys.TableKey("db2", TableId));
        Assert.AreNotEqual(MetaKeys.MetaBucketPrefix("db1"), MetaKeys.MetaBucketPrefix("db2"));
    }
}
