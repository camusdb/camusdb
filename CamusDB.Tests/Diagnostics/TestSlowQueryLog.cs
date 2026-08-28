/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Diagnostics;

namespace CamusDB.Tests.Diagnostics;

/// <summary>
/// Unit coverage for the bounded ring behind <c>SHOW SLOW QUERIES</c>.
///
/// <para>The ring is what makes the log safe to leave on, so the properties tested here are the
/// ones an operator relies on and cannot check from the outside: it never grows past its capacity,
/// it truncates the text it stores, and a burst of concurrent slow statements neither loses an entry
/// nor produces a torn one.</para>
/// </summary>
[TestFixture]
internal sealed class TestSlowQueryLog
{
    private static SlowQueryEntry Record(SlowQueryLog log, string sql, double durationMs = 10)
        => log.Record(
            DateTime.UtcNow, durationMs, database: "db", user: null, kind: "select", sql: sql,
            rowsReturned: 1, rowsRead: 2, fullScan: false, spilled: false,
            outcome: SlowQueryOutcome.Completed, errorCode: null);

    [Test]
    public void EmptyLogReportsNothing()
    {
        SlowQueryLog log = new(capacity: 4, maxSqlLength: 64);

        Assert.IsEmpty(log.Snapshot());
        Assert.AreEqual(0, log.TotalRecorded);
        Assert.AreEqual(4, log.Capacity);
    }

    [Test]
    public void SnapshotIsNewestFirst()
    {
        SlowQueryLog log = new(capacity: 8, maxSqlLength: 64);

        Record(log, "first");
        Record(log, "second");
        Record(log, "third");

        IReadOnlyList<SlowQueryEntry> entries = log.Snapshot();

        Assert.AreEqual(new[] { "third", "second", "first" }, entries.Select(entry => entry.Sql).ToArray());
        Assert.AreEqual(new[] { 3L, 2L, 1L }, entries.Select(entry => entry.Sequence).ToArray());
    }

    /// <summary>
    /// The bound is the whole point of the ring, so it is asserted on the count and on which entries
    /// survived — a ring that kept the count but dropped the newest entries would be useless.
    /// </summary>
    [Test]
    public void RingOverwritesTheOldestEntryAtCapacity()
    {
        SlowQueryLog log = new(capacity: 3, maxSqlLength: 64);

        for (int i = 1; i <= 7; i++)
            Record(log, $"statement {i}");

        IReadOnlyList<SlowQueryEntry> entries = log.Snapshot();

        Assert.AreEqual(3, entries.Count);
        Assert.AreEqual(new[] { "statement 7", "statement 6", "statement 5" }, entries.Select(entry => entry.Sql).ToArray());
        Assert.AreEqual(7, log.TotalRecorded);
    }

    /// <summary>
    /// The sequence keeps counting past the ring's capacity. A reader compares it between two
    /// readings to learn how many entries were overwritten in between, which is the only signal that
    /// the ring is too small for the threshold in force.
    /// </summary>
    [Test]
    public void SequenceKeepsCountingPastCapacity()
    {
        SlowQueryLog log = new(capacity: 2, maxSqlLength: 64);

        for (int i = 0; i < 5; i++)
            Record(log, "statement");

        Assert.AreEqual(5, log.Snapshot()[0].Sequence);
        Assert.AreEqual(5, log.TotalRecorded);
    }

    [Test]
    public void LongSqlIsTruncatedAndSaysSo()
    {
        SlowQueryLog log = new(capacity: 2, maxSqlLength: 10);

        SlowQueryEntry entry = Record(log, new string('x', 40));

        Assert.AreEqual(10, entry.Sql.Length);
        Assert.IsTrue(entry.SqlTruncated);
    }

    [Test]
    public void SqlThatFitsIsNotMarkedTruncated()
    {
        SlowQueryLog log = new(capacity: 2, maxSqlLength: 10);

        SlowQueryEntry entry = Record(log, "SELECT 1");

        Assert.AreEqual("SELECT 1", entry.Sql);
        Assert.IsFalse(entry.SqlTruncated);
    }

    /// <summary>
    /// A runtime change to the truncation length applies to the next entry. The ring's capacity does
    /// not follow, which is why the two settings carry different mutability classifications.
    /// </summary>
    [Test]
    public void TruncationLengthFollowsAPublishedSnapshotButCapacityDoesNot()
    {
        SlowQueryLog log = new(capacity: 3, maxSqlLength: 4);

        log.ApplyOptions(CamusDBOptions.Default with { SlowQueryLogMaxSqlLength = 20, SlowQueryLogMaxEntries = 500 });

        SlowQueryEntry entry = Record(log, "SELECT * FROM robots");

        Assert.AreEqual("SELECT * FROM robots", entry.Sql);
        Assert.AreEqual(3, log.Capacity);
    }

    [Test]
    public void ClearDropsTheEntriesButNotTheNumbering()
    {
        SlowQueryLog log = new(capacity: 4, maxSqlLength: 64);

        Record(log, "first");
        Record(log, "second");
        log.Clear();

        Assert.IsEmpty(log.Snapshot());

        SlowQueryEntry next = Record(log, "third");

        Assert.AreEqual(3, next.Sequence);
        Assert.AreEqual(1, log.Snapshot().Count);
    }

    /// <summary>
    /// Concurrent writers are the normal case: a node is recording slow statements precisely when
    /// many are running at once. The ring must not lose a write, hand two writers the same sequence
    /// number, or let a reader observe a half-built entry.
    /// </summary>
    [Test]
    public void ConcurrentWritersNeitherLoseNorTearEntries()
    {
        const int writers = 8;
        const int perWriter = 250;

        SlowQueryLog log = new(capacity: 64, maxSqlLength: 64);

        Parallel.For(0, writers, _ =>
        {
            for (int i = 0; i < perWriter; i++)
                Record(log, "SELECT 1");
        });

        Assert.AreEqual(writers * perWriter, log.TotalRecorded);

        IReadOnlyList<SlowQueryEntry> entries = log.Snapshot();

        Assert.AreEqual(64, entries.Count);
        Assert.AreEqual(64, entries.Select(entry => entry.Sequence).Distinct().Count());

        foreach (SlowQueryEntry entry in entries)
        {
            Assert.AreEqual("SELECT 1", entry.Sql);
            Assert.AreEqual("db", entry.Database);
        }
    }

    /// <summary>
    /// A reader taking a snapshot while writers are running must get a well-formed list rather than
    /// a null slot or a duplicated entry.
    /// </summary>
    [Test]
    public void SnapshotTakenDuringWritesIsWellFormed()
    {
        SlowQueryLog log = new(capacity: 16, maxSqlLength: 64);

        using System.Threading.CancellationTokenSource stop = new();

        Task writer = Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
                Record(log, "SELECT 1");
        });

        for (int i = 0; i < 200; i++)
        {
            IReadOnlyList<SlowQueryEntry> entries = log.Snapshot();

            Assert.LessOrEqual(entries.Count, 16);
            Assert.AreEqual(entries.Count, entries.Select(entry => entry.Sequence).Distinct().Count());
            CollectionAssert.AllItemsAreNotNull(entries);
        }

        stop.Cancel();
        writer.Wait(TimeSpan.FromSeconds(10));
    }
}
