
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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Acceptance tests for the native UUID column type: DDL (uuid/guid keywords), literal coercion,
/// unique-index enforcement (which exercises the order-preserving key encoding), the gen_uuid_v4 /
/// gen_uuid_v7 generators, CAST, parameters, and SHOW COLUMNS reporting.
/// </summary>
internal sealed class TestUuidType : SharedNodeBaseTest
{
    private const string SampleUuid = "550e8400-e29b-41d4-a716-446655440000";

    private async Task<(string dbname, DatabaseDescriptor db, CommandExecutor executor)> SetupTable(string ddl)
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, ddl, null));
        return (dbname, db, executor);
    }

    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task ExecInsert(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, parameters));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecSelect(
        CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, db.Name, sql, parameters));
        return await cursor.ToListAsync();
    }

    private static async Task<CamusDBException> AssertInsertThrows(
        CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db.Name, sql, null)))!;
        await db.Transactions.RollbackIfNotCompletedAsync(tx);
        return ex;
    }

    // Version nibble of the canonical string ("xxxxxxxx-xxxx-Vxxx-...") is at index 14.
    private static char VersionNibble(string canonical) => canonical[14];

    [Test]
    [NonParallelizable]
    public async Task Uuid_CreateInsertSelect_RoundTrips()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id uuid NOT NULL, PRIMARY KEY (id))");

        await ExecInsert(executor, db, $"INSERT INTO t (id) VALUES ('{SampleUuid}')");

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT id FROM t");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Uuid, rows[0].Row["id"].Type);
        Assert.AreEqual(SampleUuid, rows[0].Row["id"].UuidValue);
        Assert.AreEqual(Guid.Parse(SampleUuid), rows[0].Row["id"].ToGuid());
    }

    [Test]
    [NonParallelizable]
    public async Task Uuid_GuidKeyword_Works()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id guid NOT NULL, PRIMARY KEY (id))");

        await ExecInsert(executor, db, $"INSERT INTO t (id) VALUES ('{SampleUuid}')");

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT id FROM t");
        Assert.AreEqual(ColumnType.Uuid, rows[0].Row["id"].Type);
        Assert.AreEqual(SampleUuid, rows[0].Row["id"].UuidValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Uuid_InsertUnhyphenated_Coerces()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id uuid NOT NULL, PRIMARY KEY (id))");

        await ExecInsert(executor, db, "INSERT INTO t (id) VALUES ('550e8400e29b41d4a716446655440000')");

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT id FROM t");
        Assert.AreEqual(SampleUuid, rows[0].Row["id"].UuidValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Uuid_InvalidLiteral_Rejected()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id uuid NOT NULL, PRIMARY KEY (id))");

        CamusDBException ex = await AssertInsertThrows(executor, db,
            "INSERT INTO t (id) VALUES ('not-a-uuid')");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    // Inserting the same UUID primary key twice must be rejected — this exercises the
    // order-preserving key encoding end to end (encode on insert, match on the unique probe).
    [Test]
    [NonParallelizable]
    public async Task Uuid_PrimaryKey_DuplicateRejected()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id uuid NOT NULL, PRIMARY KEY (id))");

        await ExecInsert(executor, db, $"INSERT INTO t (id) VALUES ('{SampleUuid}')");

        CamusDBException ex = await AssertInsertThrows(executor, db,
            $"INSERT INTO t (id) VALUES ('{SampleUuid}')");
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task Uuid_UniqueSecondaryIndex_DuplicateRejected()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id uuid NOT NULL, ref uuid, PRIMARY KEY (id))");
        await ExecDDL(executor, db, "CREATE UNIQUE INDEX ix_ref ON t (ref)");

        await ExecInsert(executor, db,
            $"INSERT INTO t (id, ref) VALUES ('{Guid.NewGuid():D}', '{SampleUuid}')");

        CamusDBException ex = await AssertInsertThrows(executor, db,
            $"INSERT INTO t (id, ref) VALUES ('{Guid.NewGuid():D}', '{SampleUuid}')");
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task GenUuidV4_WellFormedAndDistinct()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id uuid NOT NULL, tag int64, PRIMARY KEY (id))");

        await ExecInsert(executor, db, "INSERT INTO t (id, tag) VALUES (gen_uuid_v4(), 1)");
        await ExecInsert(executor, db, "INSERT INTO t (id, tag) VALUES (gen_uuid_v4(), 2)");

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT id FROM t");
        Assert.AreEqual(2, rows.Count);

        List<string> ids = rows.Select(r => r.Row["id"].UuidValue!).ToList();
        Assert.AreEqual(2, ids.Distinct().Count(), "gen_uuid_v4 values must be distinct");
        foreach (string id in ids)
            Assert.AreEqual('4', VersionNibble(id), $"gen_uuid_v4 must produce a version-4 UUID: {id}");
    }

    // v7 is time-ordered: the 48-bit millisecond prefix must be non-decreasing across inserts, and
    // every value must carry version 7. (Full-value monotonicity is not asserted because the random
    // tail can reorder two values generated within the same millisecond.)
    [Test]
    [NonParallelizable]
    public async Task GenUuidV7_TimeOrderedAndWellFormed()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id uuid NOT NULL, seq int64, PRIMARY KEY (id))");

        for (int i = 0; i < 8; i++)
            await ExecInsert(executor, db, $"INSERT INTO t (id, seq) VALUES (gen_uuid_v7(), {i})");

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT id, seq FROM t ORDER BY seq ASC");
        Assert.AreEqual(8, rows.Count);

        long previousMs = long.MinValue;
        foreach (QueryResultRow row in rows)
        {
            ColumnValue id = row.Row["id"];
            Assert.AreEqual('7', VersionNibble(id.UuidValue!), $"gen_uuid_v7 must produce a version-7 UUID: {id.UuidValue}");

            long ms = (long)((ulong)id.UuidHigh >> 16); // top 48 bits = unix-ms timestamp
            Assert.That(ms, Is.GreaterThanOrEqualTo(previousMs), "v7 millisecond prefix must be non-decreasing in insertion order");
            previousMs = ms;
        }
    }

    [Test]
    [NonParallelizable]
    public async Task Uuid_CastStringInWhere_Matches()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id uuid NOT NULL, label string, PRIMARY KEY (id))");

        await ExecInsert(executor, db, $"INSERT INTO t (id, label) VALUES ('{SampleUuid}', \"hit\")");
        await ExecInsert(executor, db, $"INSERT INTO t (id, label) VALUES ('{Guid.NewGuid():D}', \"miss\")");

        List<QueryResultRow> rows = await ExecSelect(executor, db,
            $"SELECT label FROM t WHERE id = CAST('{SampleUuid}' AS uuid)");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("hit", rows[0].Row["label"].StrValue);
    }

    // Bare string equality on a uuid PRIMARY KEY (unique) — the constant is coerced in the index
    // selector so the unique-lookup key is built in the column's type, not as a String.
    [Test]
    [NonParallelizable]
    public async Task Uuid_BareStringEquality_PrimaryKey_Matches()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id uuid NOT NULL, label string, PRIMARY KEY (id))");

        await ExecInsert(executor, db, $"INSERT INTO t (id, label) VALUES ('{SampleUuid}', \"hit\")");
        await ExecInsert(executor, db, $"INSERT INTO t (id, label) VALUES ('{Guid.NewGuid():D}', \"miss\")");

        List<QueryResultRow> rows = await ExecSelect(executor, db,
            $"SELECT label FROM t WHERE id = '{SampleUuid}'");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("hit", rows[0].Row["label"].StrValue);
    }

    // Bare string equality on a NON-unique uuid secondary index — exercises the fixed-width
    // successor upper bound ([v, v+1)) in the index selector.
    [Test]
    [NonParallelizable]
    public async Task Uuid_BareStringEquality_NonUniqueIndex_Matches()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id uuid NOT NULL, ref uuid, label string, PRIMARY KEY (id))");
        await ExecDDL(executor, db, "CREATE INDEX ix_ref ON t (ref)");

        await ExecInsert(executor, db, $"INSERT INTO t (id, ref, label) VALUES ('{Guid.NewGuid():D}', '{SampleUuid}', \"hit\")");
        await ExecInsert(executor, db, $"INSERT INTO t (id, ref, label) VALUES ('{Guid.NewGuid():D}', '{SampleUuid}', \"hit2\")");
        await ExecInsert(executor, db, $"INSERT INTO t (id, ref, label) VALUES ('{Guid.NewGuid():D}', '{Guid.NewGuid():D}', \"miss\")");

        List<QueryResultRow> rows = await ExecSelect(executor, db,
            $"SELECT label FROM t WHERE ref = '{SampleUuid}'");

        Assert.AreEqual(2, rows.Count);
        CollectionAssert.AreEquivalent(new[] { "hit", "hit2" }, rows.Select(r => r.Row["label"].StrValue));
    }

    // Bare string range comparison on a non-indexed uuid column — exercises the filter-eval coercion
    // (a full scan, no index), and confirms the ordering matches canonical big-endian order.
    [Test]
    [NonParallelizable]
    public async Task Uuid_BareStringRange_FullScan_Filters()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id oid NOT NULL, u uuid, PRIMARY KEY (id))");

        const string small = "00000000-0000-0000-0000-000000000001";
        const string large = "ffffffff-ffff-ffff-ffff-ffffffffffff";
        const string threshold = "80000000-0000-0000-0000-000000000000";

        await ExecInsert(executor, db, $"INSERT INTO t (id, u) VALUES (gen_id(), '{small}')");
        await ExecInsert(executor, db, $"INSERT INTO t (id, u) VALUES (gen_id(), '{large}')");

        List<QueryResultRow> above = await ExecSelect(executor, db, $"SELECT u FROM t WHERE u > '{threshold}'");
        Assert.AreEqual(1, above.Count);
        Assert.AreEqual(large, above[0].Row["u"].UuidValue);

        List<QueryResultRow> below = await ExecSelect(executor, db, $"SELECT u FROM t WHERE u < '{threshold}'");
        Assert.AreEqual(1, below.Count);
        Assert.AreEqual(small, below[0].Row["u"].UuidValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Uuid_Parameter_Matches()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id uuid NOT NULL, label string, PRIMARY KEY (id))");

        await ExecInsert(executor, db, $"INSERT INTO t (id, label) VALUES ('{SampleUuid}', \"hit\")");

        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@p"] = ColumnValue.FromUuidString(SampleUuid)
        };
        List<QueryResultRow> rows = await ExecSelect(executor, db,
            "SELECT label FROM t WHERE id = @p", parameters);

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("hit", rows[0].Row["label"].StrValue);
    }

    // A literal uuid DEFAULT must survive schema persistence (the source-generated JSON round-trip of
    // the new UuidHigh backing field) and be applied to rows that omit the column.
    [Test]
    [NonParallelizable]
    public async Task Uuid_LiteralDefault_AppliedAndPersisted()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            $"CREATE TABLE t (id oid NOT NULL, u uuid DEFAULT('{SampleUuid}'), PRIMARY KEY (id))");

        await ExecInsert(executor, db, $"INSERT INTO t (id) VALUES (gen_id())");

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT u FROM t");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(ColumnType.Uuid, rows[0].Row["u"].Type);
        Assert.AreEqual(SampleUuid, rows[0].Row["u"].UuidValue);

        // SHOW COLUMNS renders the stored default (goes through the SchemaQuerier default-display path).
        List<QueryResultRow> cols = await ExecSelect(executor, db, "SHOW COLUMNS FROM t");
        QueryResultRow uRow = cols.Single(r => r.Row["Field"].StrValue == "u");
        Assert.AreEqual(SampleUuid, uRow.Row["Default"].StrValue);
    }

    // The requested feature: a volatile function default on a uuid PK, evaluated per inserted row.
    [Test]
    [NonParallelizable]
    public async Task Uuid_FunctionDefault_GeneratesDistinctValuePerRow()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE x1 (id uuid primary key default(gen_uuid_v7()), name string(20) not null)");

        await ExecInsert(executor, db, "INSERT INTO x1 (name) VALUES (\"a\")");
        await ExecInsert(executor, db, "INSERT INTO x1 (name) VALUES (\"b\")");
        await ExecInsert(executor, db, "INSERT INTO x1 (name) VALUES (\"c\")");

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT id, name FROM x1");
        Assert.AreEqual(3, rows.Count);

        List<string> ids = rows.Select(r => r.Row["id"].UuidValue!).ToList();
        Assert.AreEqual(3, ids.Distinct().Count(), "each defaulted row must get a distinct generated UUID");
        foreach (string id in ids)
        {
            Assert.IsNotNull(id);
            Assert.AreEqual('7', VersionNibble(id), $"default must produce a version-7 UUID: {id}");
        }
    }

    // A multi-row INSERT must also get distinct per-row values from the function default.
    [Test]
    [NonParallelizable]
    public async Task Uuid_FunctionDefault_DistinctAcrossMultiRowInsert()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE x1 (id uuid primary key default(gen_uuid_v7()), name string(20) not null)");

        await ExecInsert(executor, db, "INSERT INTO x1 (name) VALUES (\"a\"), (\"b\"), (\"c\"), (\"d\")");

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT id FROM x1");
        Assert.AreEqual(4, rows.Count);
        Assert.AreEqual(4, rows.Select(r => r.Row["id"].UuidValue).Distinct().Count());
    }

    // An explicitly supplied value must override the function default.
    [Test]
    [NonParallelizable]
    public async Task Uuid_FunctionDefault_ExplicitValueOverrides()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE x1 (id uuid primary key default(gen_uuid_v7()), name string(20) not null)");

        await ExecInsert(executor, db, $"INSERT INTO x1 (id, name) VALUES ('{SampleUuid}', \"a\")");

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT id FROM x1");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(SampleUuid, rows[0].Row["id"].UuidValue);
    }

    // A function whose return type does not match the column type is rejected at CREATE.
    [Test]
    [NonParallelizable]
    public async Task Uuid_FunctionDefault_TypeMismatch_Rejected()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        KvTransaction tx = await db.Transactions.BeginAsync();

        // gen_id() returns Id, not Uuid.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
                "CREATE TABLE bad (id uuid primary key default(gen_id()), name string(20) not null)", null)))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    // A volatile default that is not a bare zero-argument call is rejected (scope guard).
    [Test]
    [NonParallelizable]
    public async Task Uuid_FunctionDefault_UnknownFunction_Rejected()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        KvTransaction tx = await db.Transactions.BeginAsync();

        // An unregistered function name in a DEFAULT must be rejected rather than silently accepted.
        Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
                "CREATE TABLE bad (id uuid primary key default(no_such_fn()), name string(20) not null)", null)));
    }

    [Test]
    [NonParallelizable]
    public async Task Uuid_FunctionDefault_ShowColumns_RendersCall()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE x1 (id uuid primary key default(gen_uuid_v7()), name string(20) not null)");

        List<QueryResultRow> cols = await ExecSelect(executor, db, "SHOW COLUMNS FROM x1");
        QueryResultRow idRow = cols.Single(r => r.Row["Field"].StrValue == "id");
        Assert.AreEqual("gen_uuid_v7()", idRow.Row["Default"].StrValue);
    }

    // gen_id() as a per-row default on an oid column (proves the mechanism is not uuid-specific).
    [Test]
    [NonParallelizable]
    public async Task GenId_FunctionDefault_OnOidColumn_DistinctPerRow()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE x2 (id oid primary key default(gen_id()), name string(20) not null)");

        await ExecInsert(executor, db, "INSERT INTO x2 (name) VALUES (\"a\")");
        await ExecInsert(executor, db, "INSERT INTO x2 (name) VALUES (\"b\")");

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SELECT id FROM x2");
        Assert.AreEqual(2, rows.Count);
        Assert.AreEqual(2, rows.Select(r => r.Row["id"].StrValue).Distinct().Count());
    }

    [Test]
    [NonParallelizable]
    public async Task ShowColumns_ReportsUuidType()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await SetupTable(
            "CREATE TABLE t (id uuid NOT NULL, PRIMARY KEY (id))");

        List<QueryResultRow> rows = await ExecSelect(executor, db, "SHOW COLUMNS FROM t");
        QueryResultRow idRow = rows.Single(r => r.Row["Field"].StrValue == "id");
        Assert.AreEqual("UUID", idRow.Row["Type"].StrValue);
    }
}
