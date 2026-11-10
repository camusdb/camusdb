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

using Kahuna.Shared.KeyValue;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>column IN (...)</c> on the leading column of a unique index of several columns, the shape of
/// a composite primary key. Each list value is a seek over the entries that share that leading
/// value, as on a non-unique index. Before this rule the planner refused every unique index of more
/// than one column, so the statement read the whole table, and a Serializable read-write
/// transaction share-locked the whole row range against every writer.
///
/// <para>The lock tests state the contract of the seek locks: a list value is fenced, present or
/// absent; a key between two list values is not. A list longer than the escalation threshold takes
/// one lock over the span of its values instead, which fences the keys between them and nothing
/// outside.</para>
/// </summary>
[TestFixture(KeyValueTransactionLocking.Pessimistic)]
[TestFixture(KeyValueTransactionLocking.Optimistic)]
[NonParallelizable]
internal sealed class TestInListOnCompositeUniqueIndex : BaseTest
{
    private const int EscalationThreshold = 8;

    private readonly KeyValueTransactionLocking locking;

    public TestInListOnCompositeUniqueIndex(KeyValueTransactionLocking locking) => this.locking = locking;

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults)
        => defaults with { DefaultTransactionLocking = locking, LockEscalationThreshold = EscalationThreshold };

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string sql, bool ddl = false)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            if (ddl)
                await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
            else
                await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null));
            await db.Transactions.CommitAsync(tx);
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Select(CommandExecutor executor, DatabaseDescriptor db, string sql, KvTransaction? tx = null)
    {
        KvTransaction own = tx ?? await db.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(own, db.Name, sql, null));
            return await cursor.ToListAsync();
        }
        finally
        {
            if (tx is null)
                await db.Transactions.RollbackIfNotCompletedAsync(own);
        }
    }

    private static async Task<string> PlanOf(CommandExecutor executor, DatabaseDescriptor db, string sql, KvTransaction? tx = null) =>
        string.Join(" | ", (await Select(executor, db, "EXPLAIN " + sql, tx)).Select(r => r.Row["node"].StrValue + " " + r.Row["detail"].StrValue));

    private static IEnumerable<string> Pairs(List<QueryResultRow> rows) =>
        rows.Select(r => r.Row["k"].StrValue + ":" + r.Row["tag"].LongValue).OrderBy(s => s, StringComparer.Ordinal);

    /// <summary>Keys <c>k00</c> to <c>k39</c>, each with tags 1 and 2: 80 rows.</summary>
    private async Task<(DatabaseDescriptor db, CommandExecutor executor)> CreateVals()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await Exec(executor, db,
            "CREATE TABLE vals (k STRING(36) NOT NULL, tag INT64 NOT NULL, value STRING, PRIMARY KEY (k, tag))", ddl: true);

        List<string> tuples = new();
        for (int i = 0; i < 40; i++)
        {
            tuples.Add($"('k{i:D2}', 1, 'v{i}-1')");
            tuples.Add($"('k{i:D2}', 2, 'v{i}-2')");
        }
        await Exec(executor, db, "INSERT INTO vals (k, tag, value) VALUES " + string.Join(", ", tuples));
        return (db, executor);
    }

    /// <summary>True when the statement commits; false when a lock of another transaction refuses it.</summary>
    private static async Task<bool> TryWrite(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction writer = await db.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(writer, db.Name, sql, null));
            await db.Transactions.CommitAsync(writer);
            return true;
        }
        catch (CamusDBException)
        {
            return false;
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(writer);
        }
    }

    // ── Plans ────────────────────────────────────────────────────────────────

    [TestCase("SELECT k, tag, value FROM vals WHERE k IN ('k03', 'k17')")]
    [TestCase("SELECT k, tag, value FROM vals WHERE k IN ('k03')")]
    [TestCase("SELECT k, tag, value FROM vals WHERE k = 'k03' OR k = 'k17'")]
    [TestCase("SELECT k, tag, value FROM vals WHERE k IN ('k03', 'k17') AND tag = 2")]
    public async Task LeadingColumnListSeeksThePrimaryKey(string sql)
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateVals();

        string plan = await PlanOf(executor, db, sql);
        Assert.That(plan, Does.Contain("index-in-list").And.Contain(CamusDBConstants.PrimaryKeyInternalName), plan);
        Assert.That(plan, Does.Not.Contain("table-scan"), plan);

        await Select(executor, db, "ANALYZE TABLE vals");

        plan = await PlanOf(executor, db, sql);
        Assert.That(plan, Does.Contain("index-in-list").And.Contain(CamusDBConstants.PrimaryKeyInternalName), "after ANALYZE: " + plan);
    }

    // ── Results ──────────────────────────────────────────────────────────────

    [Test]
    public async Task ListReturnsEveryRowOfEachValue()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateVals();

        // 'zz' and 'k0' match nothing: 'k0' is a strict prefix of stored keys and must not match them.
        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT k, tag, value FROM vals WHERE k IN ('k17', 'k03', 'zz', 'k0', 'k17')");

        Assert.That(Pairs(rows), Is.EqualTo(new[] { "k03:1", "k03:2", "k17:1", "k17:2" }));
        Assert.That(rows.Single(r => r.Row["k"].StrValue == "k17" && r.Row["tag"].LongValue == 2).Row["value"].StrValue, Is.EqualTo("v17-2"));
    }

    [Test]
    public async Task ListWithAFilterOnTheNextKeyColumn()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateVals();

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT k, tag FROM vals WHERE k IN ('k03', 'k17') AND tag = 2");
        Assert.That(Pairs(rows), Is.EqualTo(new[] { "k03:2", "k17:2" }));

        rows = await Select(executor, db, "SELECT k, tag FROM vals WHERE k = 'k03' OR k = 'k17'");
        Assert.That(Pairs(rows), Is.EqualTo(new[] { "k03:1", "k03:2", "k17:1", "k17:2" }));
    }

    [Test]
    public async Task ListLongerThanTheEscalationThreshold()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateVals();

        // Without column statistics each list value is priced at a tenth of the table, so a list
        // this long would read the whole table.
        await Select(executor, db, "ANALYZE TABLE vals");

        string list = string.Join(", ", Enumerable.Range(10, 2 * EscalationThreshold).Select(i => $"'k{i:D2}'"));
        KvTransaction tx = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            Assert.That(await PlanOf(executor, db, $"SELECT k, tag FROM vals WHERE k IN ({list})", tx), Does.Contain("index-in-list"));
            List<QueryResultRow> rows = await Select(executor, db, $"SELECT k, tag FROM vals WHERE k IN ({list})", tx);
            Assert.That(rows, Has.Count.EqualTo(4 * EscalationThreshold));
            Assert.That(rows.Select(r => r.Row["k"].StrValue).Distinct().Count(), Is.EqualTo(2 * EscalationThreshold));
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task UpdateAndDeleteByList()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateVals();

        await Exec(executor, db, "UPDATE vals SET value = 'changed' WHERE k IN ('k03', 'k17')");
        List<QueryResultRow> changed = await Select(executor, db, "SELECT k, tag FROM vals WHERE value = 'changed'");
        Assert.That(Pairs(changed), Is.EqualTo(new[] { "k03:1", "k03:2", "k17:1", "k17:2" }));

        await Exec(executor, db, "DELETE FROM vals WHERE k IN ('k03', 'k17')");
        Assert.That(await Select(executor, db, "SELECT k FROM vals WHERE k IN ('k03', 'k17')"), Is.Empty);
        Assert.That(await Select(executor, db, "SELECT k FROM vals"), Has.Count.EqualTo(76));
    }

    [Test]
    public async Task JoinLeafListSeeksThePrimaryKey()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateVals();
        await Exec(executor, db, "CREATE TABLE items (id STRING(36) NOT NULL PRIMARY KEY, owner STRING)", ddl: true);
        await Exec(executor, db, "INSERT INTO items (id, owner) VALUES ('k03', 'u1'), ('k17', 'u1'), ('k20', 'u2')");

        List<QueryResultRow> rows = await Select(executor, db,
            "SELECT v.k AS k, v.tag AS tag FROM items i JOIN vals v ON v.k = i.id WHERE v.k IN ('k03', 'k17', 'k20') AND i.owner = 'u1'");

        Assert.That(Pairs(rows), Is.EqualTo(new[] { "k03:1", "k03:2", "k17:1", "k17:2" }));
    }

    /// <summary>
    /// A unique index has no entry for a row with NULL in a key column. A seek on the leading column
    /// of <c>UNIQUE (a, b)</c> with a nullable <c>b</c> would lose the row <c>(a, NULL)</c>, so the
    /// planner must not use that index for the list.
    /// </summary>
    [Test]
    public async Task UniqueIndexWithANullableLaterColumnIsNotUsed()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await Exec(executor, db, "CREATE TABLE pairs (id INT64 PRIMARY KEY, a STRING NOT NULL, b INT64)", ddl: true);
        await Exec(executor, db, "CREATE UNIQUE INDEX pairs_ab ON pairs (a, b)", ddl: true);
        await Exec(executor, db, "INSERT INTO pairs (id, a, b) VALUES (1, 'x', 1), (2, 'x', NULL), (3, 'y', NULL), (4, 'z', 4)");

        const string sql = "SELECT id FROM pairs WHERE a IN ('x', 'y')";
        Assert.That(await PlanOf(executor, db, sql), Does.Not.Contain("pairs_ab"));

        List<QueryResultRow> rows = await Select(executor, db, sql);
        Assert.That(rows.Select(r => r.Row["id"].LongValue), Is.EquivalentTo(new long[] { 1, 2, 3 }));
    }

    // ── Locks ────────────────────────────────────────────────────────────────

    [Test]
    public async Task SerializableListReadFencesItsValuesAndNothingBetween()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateVals();
        const string read = "SELECT k, tag FROM vals WHERE k IN ('k03', 'k17', 'absent')";

        KvTransaction reader = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            Assert.That(await PlanOf(executor, db, read, reader), Does.Contain("index-in-list"));
            Assert.That(Pairs(await Select(executor, db, read, reader)), Is.EqualTo(new[] { "k03:1", "k03:2", "k17:1", "k17:2" }));

            // Keys between the list values, and outside them, stay writable.
            Assert.That(await TryWrite(executor, db, "INSERT INTO vals (k, tag, value) VALUES ('k10', 3, 'new')"), Is.True,
                "an insert between two list values must not conflict with the read");
            Assert.That(await TryWrite(executor, db, "UPDATE vals SET value = 'new' WHERE k = 'k10' AND tag = 1"), Is.True,
                "an update of a row between two list values must not conflict with the read");
            Assert.That(await TryWrite(executor, db, "INSERT INTO vals (k, tag, value) VALUES ('zz', 1, 'new')"), Is.True,
                "an insert after the last list value must not conflict with the read");

            // The list values are fenced: a phantom, a present row, and a value that matched nothing.
            Assert.That(await TryWrite(executor, db, "INSERT INTO vals (k, tag, value) VALUES ('k03', 9, 'phantom')"), Is.False,
                "a new row of a list value is a phantom");
            Assert.That(await TryWrite(executor, db, "UPDATE vals SET value = 'new' WHERE k = 'k17' AND tag = 1"), Is.False,
                "a row the read returned is locked");
            Assert.That(await TryWrite(executor, db, "INSERT INTO vals (k, tag, value) VALUES ('absent', 1, 'phantom')"), Is.False,
                "a list value with no rows is fenced too");

            Assert.That(Pairs(await Select(executor, db, read, reader)), Is.EqualTo(new[] { "k03:1", "k03:2", "k17:1", "k17:2" }), "the read repeats");
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(reader);
        }
    }

    [Test]
    public async Task LongListTakesOneSpanLock()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await CreateVals();

        // Without column statistics each list value is priced at a tenth of the table, so a list
        // this long would read the whole table.
        await Select(executor, db, "ANALYZE TABLE vals");

        // Two stored keys and absent values between them: more values than the threshold, but few
        // rows. The row count must stay under the threshold, or the row locks of the read escalate
        // to the whole row range and every insert conflicts, which is not the lock under test.
        List<string> values = new() { "'k10'", "'k30'" };
        for (int i = 0; i < EscalationThreshold + 1; i++)
            values.Add($"'k{11 + i:D2}x'");
        string read = $"SELECT k, tag FROM vals WHERE k IN ({string.Join(", ", values)})";

        KvTransaction reader = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            Assert.That(await PlanOf(executor, db, read, reader), Does.Contain("index-in-list"));
            Assert.That(Pairs(await Select(executor, db, read, reader)), Is.EqualTo(new[] { "k10:1", "k10:2", "k30:1", "k30:2" }));

            Assert.That(await TryWrite(executor, db, "INSERT INTO vals (k, tag, value) VALUES ('k12x', 9, 'phantom')"), Is.False,
                "a new row of a list value is a phantom");
            Assert.That(await TryWrite(executor, db, "INSERT INTO vals (k, tag, value) VALUES ('k30', 9, 'phantom')"), Is.False,
                "a new row of the last list value is a phantom: the span covers every entry of that value");
            Assert.That(await TryWrite(executor, db, "INSERT INTO vals (k, tag, value) VALUES ('k05', 9, 'new')"), Is.True,
                "an insert before the first list value must not conflict with the read");
            Assert.That(await TryWrite(executor, db, "INSERT INTO vals (k, tag, value) VALUES ('k39', 9, 'new')"), Is.True,
                "an insert after the last list value must not conflict with the read");

            Assert.That(Pairs(await Select(executor, db, read, reader)), Is.EqualTo(new[] { "k10:1", "k10:2", "k30:1", "k30:2" }), "the read repeats");
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(reader);
        }
    }

    /// <summary>
    /// The same contract on a non-unique index: the seek locks are shared by every index an IN list
    /// seeks by range, so a list on a secondary index does not fence the keys between its values.
    /// </summary>
    [Test]
    public async Task NonUniqueIndexListFencesItsValuesAndNothingBetween()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await Exec(executor, db, "CREATE TABLE robots (id INT64 PRIMARY KEY, year INT64 NOT NULL, name STRING)", ddl: true);
        await Exec(executor, db, "CREATE INDEX robots_year ON robots (year)", ddl: true);
        await Exec(executor, db, "INSERT INTO robots (id, year, name) VALUES (1, 2000, 'a'), (2, 2010, 'b'), (3, 2020, 'c')");

        const string read = "SELECT id FROM robots WHERE year IN (2000, 2020)";

        KvTransaction reader = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        try
        {
            Assert.That(await PlanOf(executor, db, read, reader), Does.Contain("index-in-list").And.Contain("robots_year"));
            Assert.That((await Select(executor, db, read, reader)).Select(r => r.Row["id"].LongValue), Is.EquivalentTo(new long[] { 1, 3 }));

            Assert.That(await TryWrite(executor, db, "INSERT INTO robots (id, year, name) VALUES (10, 2010, 'between')"), Is.True,
                "an insert between two list values must not conflict with the read");
            Assert.That(await TryWrite(executor, db, "INSERT INTO robots (id, year, name) VALUES (11, 2020, 'phantom')"), Is.False,
                "a new row of a list value is a phantom");

            Assert.That((await Select(executor, db, read, reader)).Select(r => r.Row["id"].LongValue), Is.EquivalentTo(new long[] { 1, 3 }), "the read repeats");
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(reader);
        }
    }
}
