/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Guards the batched ancestry resolution on a branch database.
///
/// <para>
/// A branch resolves a row that was never written into its own namespace by walking its ancestry.
/// The batch read now carries the unanswered input positions through the levels and issues one
/// bounded batch per level, and the branch batch writers resolve every unique-index write flag the
/// same way. Both must produce exactly what the per-row walk produced: nearest ancestor wins, a
/// tombstone at one level permanently suppresses a value at an older one, input order and repeated
/// ids survive, and a unique key that already exists in an ancestor is still a duplicate.
/// </para>
/// </summary>
// Serial: boots an embedded Kahuna node per test, like the sibling branch fixtures.
[NonParallelizable]
internal sealed class TestBranchBatchedAncestryReads : BaseTest
{
    private static string NewName() => "db_" + Guid.NewGuid().ToString("n");

    private async Task<(string dbName, DatabaseDescriptor db, CommandExecutor executor)>
        CreateRootWithTable(string tableSql, CamusDBOptions? options = null)
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase(options ?? Options);
        TrackDatabase(dbName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: dbName, sql: tableSql, parameters: null));

        return (dbName, db, executor);
    }

    private async Task<DatabaseDescriptor> Branch(CommandExecutor executor, string parent, string branchName)
    {
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: parent));
        TrackDatabase(branchName, executor);
        return branch;
    }

    private static async Task NonQuery(string dbName, DatabaseDescriptor db, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> Select(string dbName, DatabaseDescriptor db, CommandExecutor executor, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbName, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    // ── The batch read itself, driven directly against a branch store ─────────

    /// <summary>
    /// Every id resolved by one batch call must equal what a per-row <c>GetRow</c> resolves, including
    /// ids that live only in an ancestor, ids overridden in the branch, an id deleted in the branch, a
    /// repeated id, and an id that exists nowhere.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchGetRowsBatch_MatchesPerRowGetRow_AcrossAncestryLevels()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE items (id OBJECT_ID PRIMARY KEY, label STRING)");

        for (int i = 0; i < 12; i++)
            await NonQuery(rootName, rootDb, executor,
                $"INSERT INTO items (id, label) VALUES (gen_id(), \"root-{i}\")");

        // The KV row id is the store's own id, not the user's primary-key column, so it is read back
        // from a query rather than assumed.
        Dictionary<string, ObjectIdValue> rowIdByLabel = (await Select(rootName, rootDb, executor, "SELECT label FROM items"))
            .ToDictionary(r => r.Row["label"].StrValue!, r => r.RowId);

        Assert.AreEqual(12, rowIdByLabel.Count, "every inserted row must be readable from the root");

        string midName = NewName();
        DatabaseDescriptor midDb = await Branch(executor, rootName, midName);

        // Middle level overrides one inherited row and deletes another.
        await NonQuery(midName, midDb, executor, "UPDATE items SET label = \"mid-3\" WHERE label = \"root-3\"");
        await NonQuery(midName, midDb, executor, "DELETE FROM items WHERE label = \"root-5\"");

        string leafName = NewName();
        DatabaseDescriptor leafDb = await Branch(executor, midName, leafName);

        // Leaf level overrides one more and adds a row of its own.
        await NonQuery(leafName, leafDb, executor, "UPDATE items SET label = \"leaf-7\" WHERE label = \"root-7\"");
        await NonQuery(leafName, leafDb, executor, "INSERT INTO items (id, label) VALUES (gen_id(), \"leaf-new\")");

        ObjectIdValue leafOnlyRowId = (await Select(leafName, leafDb, executor, "SELECT label FROM items"))
            .Single(r => r.Row["label"].StrValue == "leaf-new").RowId;

        TableDescriptor leafTable = await executor.OpenTable(new OpenTableTicket(leafName, "items"));

        // Shuffled, with a repeat, a leaf-local row, a deleted row and an id that exists nowhere.
        List<ObjectIdValue> probe =
        [
            rowIdByLabel["root-7"],
            rowIdByLabel["root-0"],
            rowIdByLabel["root-5"],
            leafOnlyRowId,
            rowIdByLabel["root-3"],
            rowIdByLabel["root-0"],
            ObjectIdGenerator.Generate(),
            rowIdByLabel["root-11"],
        ];

        KvTransaction tx = await leafDb.Transactions.BeginAsync();

        try
        {
            ReadOnlyMemory<byte>?[] batched = await leafTable.Store.GetRowsBatch(tx, probe, CancellationToken.None);

            Assert.AreEqual(probe.Count, batched.Length, "one result per input position");

            for (int i = 0; i < probe.Count; i++)
            {
                ReadOnlyMemory<byte>? expected = await leafTable.Store.GetRow(tx, probe[i], CancellationToken.None);

                if (expected is null)
                {
                    Assert.IsNull(batched[i], $"position {i} must be absent, as the per-row read reports");
                    continue;
                }

                Assert.IsNotNull(batched[i], $"position {i} must resolve, as the per-row read does");
                Assert.AreEqual(expected.Value.ToArray(), batched[i]!.Value.ToArray(),
                    $"position {i} must decode to the same bytes as the per-row read");
            }

            // The specific ancestry outcomes, stated rather than only implied by the oracle.
            Assert.IsNull(batched[2], "a row deleted in the middle branch must stay deleted in the leaf");
            Assert.IsNotNull(batched[1], "a row that lives only in the root must resolve through two levels");
            Assert.AreEqual(batched[1]!.Value.ToArray(), batched[5]!.Value.ToArray(),
                "a repeated id must resolve identically at both of its positions");
            Assert.IsNull(batched[6], "an id that exists at no level must resolve to null");
        }
        finally
        {
            await leafDb.Transactions.CommitAsync(tx);
        }
    }

    /// <summary>
    /// A tombstone at one level must suppress a live value at an older level, even when the batch
    /// resolves several levels at once. This is the invariant the per-level batching could silently
    /// break by carrying an already-answered position into the next level.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchGetRowsBatch_TombstoneAtOneLevelSuppressesOlderValue()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE notes (id OBJECT_ID PRIMARY KEY, body STRING)");

        await NonQuery(rootName, rootDb, executor, "INSERT INTO notes (id, body) VALUES (gen_id(), \"root-body\")");
        await NonQuery(rootName, rootDb, executor, "INSERT INTO notes (id, body) VALUES (gen_id(), \"kept\")");

        Dictionary<string, ObjectIdValue> rowIdByBody = (await Select(rootName, rootDb, executor, "SELECT body FROM notes"))
            .ToDictionary(r => r.Row["body"].StrValue!, r => r.RowId);

        string midName = NewName();
        DatabaseDescriptor midDb = await Branch(executor, rootName, midName);
        await NonQuery(midName, midDb, executor, "DELETE FROM notes WHERE body = \"root-body\"");

        string leafName = NewName();
        DatabaseDescriptor leafDb = await Branch(executor, midName, leafName);

        TableDescriptor leafTable = await executor.OpenTable(new OpenTableTicket(leafName, "notes"));

        KvTransaction tx = await leafDb.Transactions.BeginAsync();

        try
        {
            ReadOnlyMemory<byte>?[] batched = await leafTable.Store.GetRowsBatch(
                tx, [rowIdByBody["root-body"], rowIdByBody["kept"]], CancellationToken.None);

            Assert.IsNull(batched[0], "the middle level's tombstone must win over the root's value");
            Assert.IsNotNull(batched[1], "the untouched row must still resolve from the root");
        }
        finally
        {
            await leafDb.Transactions.CommitAsync(tx);
        }
    }

    // ── The real caller: an index scan on a branch pages through inherited rows ──

    /// <summary>
    /// An index scan on a branch merges the ancestors' index levels, so a fetch page is full of rows
    /// that miss level 0 by construction — the shape that made the per-row ancestry walk expensive.
    /// The result must not depend on the page size.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchIndexScan_ReturnsInheritedRows_AtEveryPageSize()
    {
        List<string>? reference = null;

        foreach (int pageSize in new[] { 1, 2, 5, 64 })
        {
            (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
                await CreateRootWithTable(
                    "CREATE TABLE robots (id OBJECT_ID PRIMARY KEY, year INT64, name STRING)",
                    Options with { IndexScanFetchBatchSize = pageSize });

            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                txnState: null!, database: rootName, sql: "CREATE INDEX year_idx ON robots (year)", parameters: null));

            for (int i = 0; i < 20; i++)
                await NonQuery(rootName, rootDb, executor,
                    $"INSERT INTO robots (id, year, name) VALUES (gen_id(), {1900 + i}, \"root-{i}\")");

            string branchName = NewName();
            DatabaseDescriptor branchDb = await Branch(executor, rootName, branchName);

            // One inherited row overridden in the branch, one deleted, one new row added.
            await NonQuery(branchName, branchDb, executor, "UPDATE robots SET name = \"branch-4\" WHERE name = \"root-4\"");
            await NonQuery(branchName, branchDb, executor, "DELETE FROM robots WHERE name = \"root-9\"");
            await NonQuery(branchName, branchDb, executor,
                "INSERT INTO robots (id, year, name) VALUES (gen_id(), 1975, \"branch-new\")");

            List<QueryResultRow> rows = await Select(branchName, branchDb, executor,
                "SELECT year, name FROM robots WHERE year > 1901 ORDER BY year, name");

            List<string> rendered = rows
                .Select(r => r.Row["year"].LongValue + ":" + r.Row["name"].StrValue)
                .ToList();

            if (reference is null)
            {
                Assert.IsTrue(rendered.Count > 5, "the fixture must return enough rows to span several pages");
                Assert.IsTrue(rendered.Any(v => v.EndsWith("branch-4", StringComparison.Ordinal)),
                    "the branch-local override must win over the inherited row");
                Assert.IsFalse(rendered.Any(v => v.EndsWith("root-9", StringComparison.Ordinal)),
                    "the row deleted in the branch must not come back through ancestry");
                reference = rendered;
            }
            else
            {
                Assert.AreEqual(reference, rendered, $"page size {pageSize} changed the branch scan result");
            }
        }
    }

    // ── The write path: unique-index flags resolved one batch per ancestry level ──

    /// <summary>
    /// A batch insert on a branch must still see a unique key that exists only in an ancestor and
    /// reject it, and must still accept a key whose ancestor entry was tombstoned in the branch.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchBatchInsert_UniqueKeyInheritedFromAncestor_IsRejected()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable(
                "CREATE TABLE accounts (id OBJECT_ID PRIMARY KEY, code STRING UNIQUE, note STRING)");

        for (int i = 0; i < 8; i++)
            await NonQuery(rootName, rootDb, executor,
                $"INSERT INTO accounts (id, code, note) VALUES (gen_id(), \"code-{i}\", \"root\")");

        string branchName = NewName();
        DatabaseDescriptor branchDb = await Branch(executor, rootName, branchName);

        // Fresh codes insert fine, one statement, several rows — one batch on the branch.
        await NonQuery(branchName, branchDb, executor,
            "INSERT INTO accounts (id, code, note) VALUES " +
            "(gen_id(), \"code-100\", \"branch\"), (gen_id(), \"code-101\", \"branch\"), (gen_id(), \"code-102\", \"branch\")");

        Assert.AreEqual(11, (await Select(branchName, branchDb, executor, "SELECT id FROM accounts")).Count);

        // A code that only exists in the ancestor must still collide.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await NonQuery(
            branchName, branchDb, executor,
            "INSERT INTO accounts (id, code, note) VALUES " +
            "(gen_id(), \"code-200\", \"branch\"), (gen_id(), \"code-3\", \"branch\")"))!;

        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code,
            "an inherited unique key must be detected through the batched ancestry resolution");
    }

    /// <summary>
    /// Deleting an inherited row writes a tombstone over its unique index entry; re-inserting the same
    /// code must then succeed (tombstone-replace), and the batch resolution must reach that decision
    /// for every row of a multi-row statement.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchBatchInsert_TombstonedInheritedUniqueKey_IsReusable()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable(
                "CREATE TABLE badges (id OBJECT_ID PRIMARY KEY, code STRING UNIQUE, owner STRING)");

        for (int i = 0; i < 6; i++)
            await NonQuery(rootName, rootDb, executor,
                $"INSERT INTO badges (id, code, owner) VALUES (gen_id(), \"badge-{i}\", \"root\")");

        string branchName = NewName();
        DatabaseDescriptor branchDb = await Branch(executor, rootName, branchName);

        await NonQuery(branchName, branchDb, executor, "DELETE FROM badges WHERE code = \"badge-1\"");
        await NonQuery(branchName, branchDb, executor, "DELETE FROM badges WHERE code = \"badge-2\"");

        // Both freed codes plus a brand new one, in one batch.
        await NonQuery(branchName, branchDb, executor,
            "INSERT INTO badges (id, code, owner) VALUES " +
            "(gen_id(), \"badge-1\", \"branch\"), (gen_id(), \"badge-2\", \"branch\"), (gen_id(), \"badge-9\", \"branch\")");

        List<QueryResultRow> rows = await Select(branchName, branchDb, executor,
            "SELECT code, owner FROM badges ORDER BY code");

        Assert.AreEqual(7, rows.Count, "four inherited rows plus the three re-inserted ones");

        Dictionary<string, string> byCode = rows.ToDictionary(
            r => r.Row["code"].StrValue!, r => r.Row["owner"].StrValue!);

        Assert.AreEqual("branch", byCode["badge-1"], "the tombstoned slot must now hold the branch's row");
        Assert.AreEqual("branch", byCode["badge-2"], "the tombstoned slot must now hold the branch's row");
        Assert.AreEqual("root", byCode["badge-0"], "an untouched inherited row must be unchanged");
    }

    /// <summary>
    /// A multi-row UPDATE on a branch resolves the new unique keys through the same batched path.
    /// Moving several rows onto free codes must succeed; moving one onto an inherited code must fail.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchBatchUpdate_ResolvesInheritedUniqueKeys()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable(
                "CREATE TABLE seats (id OBJECT_ID PRIMARY KEY, code STRING UNIQUE, row_no INT64)");

        for (int i = 0; i < 6; i++)
            await NonQuery(rootName, rootDb, executor,
                $"INSERT INTO seats (id, code, row_no) VALUES (gen_id(), \"seat-{i}\", {i})");

        string branchName = NewName();
        DatabaseDescriptor branchDb = await Branch(executor, rootName, branchName);

        // Rewrites three inherited rows onto codes nobody holds — one batch, three unique resolutions.
        await NonQuery(branchName, branchDb, executor,
            "UPDATE seats SET code = CONCAT(\"moved-\", CAST(row_no AS STRING)) WHERE row_no < 3");

        List<QueryResultRow> moved = await Select(branchName, branchDb, executor,
            "SELECT code FROM seats WHERE row_no < 3 ORDER BY code");

        Assert.AreEqual(new List<string?> { "moved-0", "moved-1", "moved-2" }, moved.Select(r => r.Row["code"].StrValue).ToList());

        // The root still holds its own codes: the branch's rewrite must not have touched it.
        List<QueryResultRow> rootRows = await Select(rootName, rootDb, executor,
            "SELECT code FROM seats WHERE row_no < 3 ORDER BY code");

        Assert.AreEqual(new List<string?> { "seat-0", "seat-1", "seat-2" }, rootRows.Select(r => r.Row["code"].StrValue).ToList());

        // Colliding with an inherited code must still be rejected.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await NonQuery(
            branchName, branchDb, executor,
            "UPDATE seats SET code = \"seat-5\" WHERE row_no = 4"))!;

        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code);
    }
}
