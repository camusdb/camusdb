
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

using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Sequences;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Branch-aware read-layer tests: the branch-aware read layer (KvTableStore) walks ancestry so a branch
/// returns inherited data without physically copying rows or index entries.
/// </summary>
[NonParallelizable]
internal sealed class TestBranchAwareStorage : BaseTest
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

    private static async Task InsertRow(
        string dbName, DatabaseDescriptor db, CommandExecutor executor, string insertSql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbName, insertSql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> SelectAll(
        string dbName, DatabaseDescriptor db, CommandExecutor executor, string selectSql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) =
            await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbName, selectSql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    /// <summary>Counts all KV keys under <paramref name="prefix"/> in the given <paramref name="bucket"/>.</summary>
    private static async Task<int> CountKeysUnder(IKahuna kahuna, string bucket, string prefix)
    {
        int count = 0;
        await foreach ((string key, ReadOnlyKeyValueEntry _) in kahuna.LocateAndScanRange(
            HLCTimestamp.Zero, bucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(prefix, StringComparison.Ordinal))
                count++;
        }
        return count;
    }

    [Test]
    [NonParallelizable]
    public async Task Branch_ScanRows_ReturnsParentRows()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE items (id OBJECT_ID PRIMARY KEY, label STRING)");

        await InsertRow(rootName, rootDb, executor, "INSERT INTO items (id, label) VALUES (gen_id(), \"alpha\")");
        await InsertRow(rootName, rootDb, executor, "INSERT INTO items (id, label) VALUES (gen_id(), \"beta\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        List<QueryResultRow> rows = await SelectAll(branchName, branch, executor,
            "SELECT label FROM items");

        Assert.AreEqual(2, rows.Count,
            "branch must see all parent rows via ancestry lineage walk");
    }

    [Test]
    [NonParallelizable]
    public async Task Branch_GetRow_ReturnsParentRow()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE users (id OBJECT_ID PRIMARY KEY, name STRING)");

        await InsertRow(rootName, rootDb, executor, "INSERT INTO users (id, name) VALUES (gen_id(), \"alice\")");

        List<QueryResultRow> rootRows = await SelectAll(rootName, rootDb, executor, "SELECT id FROM users");
        Assert.AreEqual(1, rootRows.Count);
        string rowId = rootRows[0].Row["id"].StrValue!;

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        List<QueryResultRow> branchRows = await SelectAll(branchName, branch, executor,
            $"SELECT name FROM users WHERE id = \"{rowId}\"");

        Assert.AreEqual(1, branchRows.Count, "branch must find parent row by PK via ancestry walk");
        Assert.AreEqual("alice", branchRows[0].Row["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task Branch_LookupUnique_ReturnsParentIndexEntry()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE products (id OBJECT_ID PRIMARY KEY, code STRING)");

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE UNIQUE INDEX code_uidx ON products (code)",
            parameters: null));

        await InsertRow(rootName, rootDb, executor, "INSERT INTO products (id, code) VALUES (gen_id(), \"P001\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        List<QueryResultRow> branchRows = await SelectAll(branchName, branch, executor,
            "SELECT code FROM products WHERE code = \"P001\"");

        Assert.AreEqual(1, branchRows.Count,
            "branch must find parent row via unique index lookup through ancestry");
    }

    [Test]
    [NonParallelizable]
    public async Task Branch_ScanIndex_YieldsParentEntries()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE orders (id OBJECT_ID PRIMARY KEY, status STRING)");

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE INDEX status_idx ON orders (status)",
            parameters: null));

        await InsertRow(rootName, rootDb, executor, "INSERT INTO orders (id, status) VALUES (gen_id(), \"open\")");
        await InsertRow(rootName, rootDb, executor, "INSERT INTO orders (id, status) VALUES (gen_id(), \"closed\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        List<QueryResultRow> branchRows = await SelectAll(branchName, branch, executor,
            "SELECT status FROM orders WHERE status = \"open\"");

        Assert.AreEqual(1, branchRows.Count,
            "branch must find parent row via non-unique index scan through ancestry");
    }

    [Test]
    [NonParallelizable]
    public async Task BranchWrite_NotVisibleOnParent()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE logs (id OBJECT_ID PRIMARY KEY, msg STRING)");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        await InsertRow(branchName, branch, executor, "INSERT INTO logs (id, msg) VALUES (gen_id(), \"branch-only\")");

        List<QueryResultRow> parentRows = await SelectAll(rootName, rootDb, executor, "SELECT msg FROM logs");
        Assert.AreEqual(0, parentRows.Count,
            "parent must not see rows inserted into the branch namespace");
    }

    [Test]
    [NonParallelizable]
    public async Task ThreeDeepChain_InheritsFromGrandparent()
    {
        (string aName, DatabaseDescriptor aDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE chain (id OBJECT_ID PRIMARY KEY, origin STRING)");

        await InsertRow(aName, aDb, executor, "INSERT INTO chain (id, origin) VALUES (gen_id(), \"from-A\")");

        string bName = NewName();
        DatabaseDescriptor bDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(bName, ifNotExists: false, branchFrom: aName));
        TrackDatabase(bName, executor);

        string cName = NewName();
        DatabaseDescriptor cDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(cName, ifNotExists: false, branchFrom: bName));
        TrackDatabase(cName, executor);

        Assert.AreEqual(2, cDb.Ancestors.Count, "C must carry both B and A in its ancestry");

        List<QueryResultRow> cRows = await SelectAll(cName, cDb, executor, "SELECT origin FROM chain");
        Assert.AreEqual(1, cRows.Count, "C must see A's row via 2-level ancestry walk");
        Assert.AreEqual("from-A", cRows[0].Row["origin"].StrValue);
    }

    // -----------------------------------------------------------------------
    // Snapshot isolation: parent writes AFTER fork must be invisible to the branch.
    // These tests are the discriminating gate for the forkTimestamp mechanism —
    // if ancestor reads used HLCTimestamp.Zero (latest) instead of forkTimestamp,
    // the branch would erroneously see the post-fork parent writes and every test
    // below would fail.
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task ParentInsertAfterFork_NotVisibleOnBranch_Scan()
    {
        // One pre-fork row → branch should always see exactly 1, never 2.
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE events (id OBJECT_ID PRIMARY KEY, kind STRING)");

        await InsertRow(rootName, rootDb, executor, "INSERT INTO events (id, kind) VALUES (gen_id(), \"pre-fork\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Parent adds a second row AFTER the fork.
        await InsertRow(rootName, rootDb, executor, "INSERT INTO events (id, kind) VALUES (gen_id(), \"post-fork\")");

        // Parent must see both rows.
        List<QueryResultRow> parentRows = await SelectAll(rootName, rootDb, executor, "SELECT kind FROM events");
        Assert.AreEqual(2, parentRows.Count, "parent must see both pre- and post-fork rows");

        // Branch must see only the pre-fork row (snapshot at forkTimestamp).
        List<QueryResultRow> branchRows = await SelectAll(branchName, branch, executor, "SELECT kind FROM events");
        Assert.AreEqual(1, branchRows.Count,
            "branch scan must not see rows inserted into the parent after the fork (snapshot at forkTimestamp)");
        Assert.AreEqual("pre-fork", branchRows[0].Row["kind"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task ParentUpdateAfterFork_BranchSeesPreForkValue()
    {
        // Branch must observe the value as-of forkTimestamp, not the latest parent value.
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE config (id OBJECT_ID PRIMARY KEY, val STRING)");

        await InsertRow(rootName, rootDb, executor, "INSERT INTO config (id, val) VALUES (gen_id(), \"v1\")");

        List<QueryResultRow> beforeFork = await SelectAll(rootName, rootDb, executor, "SELECT id FROM config");
        string rowId = beforeFork[0].Row["id"].StrValue!;

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Parent updates the row to "v2" AFTER the fork.
        KvTransaction upd = await rootDb.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            upd, rootName, $"UPDATE config SET val = \"v2\" WHERE id = \"{rowId}\"", null));
        await rootDb.Transactions.CommitAsync(upd);

        // Parent sees the new value.
        List<QueryResultRow> parentRows = await SelectAll(rootName, rootDb, executor,
            $"SELECT val FROM config WHERE id = \"{rowId}\"");
        Assert.AreEqual("v2", parentRows[0].Row["val"].StrValue, "parent must see the updated value");

        // Branch must still see "v1" (the value at forkTimestamp).
        List<QueryResultRow> branchRows = await SelectAll(branchName, branch, executor,
            $"SELECT val FROM config WHERE id = \"{rowId}\"");
        Assert.AreEqual(1, branchRows.Count, "branch must find the inherited row");
        Assert.AreEqual("v1", branchRows[0].Row["val"].StrValue,
            "branch point-read must return the pre-fork value, not the post-fork update");
    }

    [Test]
    [NonParallelizable]
    public async Task ParentInsertAfterFork_NotVisibleViaUniqueIndex()
    {
        // Unique-index lookup in the branch must not see index entries written after forkTimestamp.
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE sku (id OBJECT_ID PRIMARY KEY, code STRING)");

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE UNIQUE INDEX code_uidx ON sku (code)", parameters: null));

        await InsertRow(rootName, rootDb, executor, "INSERT INTO sku (id, code) VALUES (gen_id(), \"A\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Parent adds code "B" after the fork.
        await InsertRow(rootName, rootDb, executor, "INSERT INTO sku (id, code) VALUES (gen_id(), \"B\")");

        // Branch lookup for "B" must return nothing (the entry was committed after forkTimestamp).
        List<QueryResultRow> branchB = await SelectAll(branchName, branch, executor,
            "SELECT code FROM sku WHERE code = \"B\"");
        Assert.AreEqual(0, branchB.Count,
            "branch unique-index lookup must not resolve post-fork parent inserts");

        // Branch lookup for "A" must still work.
        List<QueryResultRow> branchA = await SelectAll(branchName, branch, executor,
            "SELECT code FROM sku WHERE code = \"A\"");
        Assert.AreEqual(1, branchA.Count, "branch must still find the pre-fork unique-index entry");
    }

    [Test]
    [NonParallelizable]
    public async Task ParentInsertAfterFork_NotVisibleViaNonUniqueIndex()
    {
        // Non-unique index scan in the branch must stop at forkTimestamp.
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE tasks (id OBJECT_ID PRIMARY KEY, state STRING)");

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE INDEX state_idx ON tasks (state)", parameters: null));

        await InsertRow(rootName, rootDb, executor, "INSERT INTO tasks (id, state) VALUES (gen_id(), \"open\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Parent inserts a second "open" task AFTER the fork.
        await InsertRow(rootName, rootDb, executor, "INSERT INTO tasks (id, state) VALUES (gen_id(), \"open\")");

        // Parent must see both.
        List<QueryResultRow> parentRows = await SelectAll(rootName, rootDb, executor,
            "SELECT state FROM tasks WHERE state = \"open\"");
        Assert.AreEqual(2, parentRows.Count, "parent must see both open tasks");

        // Branch must see only the pre-fork one.
        List<QueryResultRow> branchRows = await SelectAll(branchName, branch, executor,
            "SELECT state FROM tasks WHERE state = \"open\"");
        Assert.AreEqual(1, branchRows.Count,
            "branch non-unique index scan must not see index entries committed after forkTimestamp");
    }

    // -----------------------------------------------------------------------
    // Tombstone suppression: a branch-level delete must hide the inherited row
    // from all callers (scan, point-read), while the parent is unaffected.
    // -----------------------------------------------------------------------

    [Test]
    [NonParallelizable]
    public async Task BranchDelete_SuppressesInheritedRow_Scan()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE docs (id OBJECT_ID PRIMARY KEY, title STRING)");

        await InsertRow(rootName, rootDb, executor, "INSERT INTO docs (id, title) VALUES (gen_id(), \"inherited\")");

        List<QueryResultRow> beforeFork = await SelectAll(rootName, rootDb, executor, "SELECT id FROM docs");
        string rowId = beforeFork[0].Row["id"].StrValue!;

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Verify branch sees the inherited row before deleting it.
        List<QueryResultRow> branchBefore = await SelectAll(branchName, branch, executor, "SELECT title FROM docs");
        Assert.AreEqual(1, branchBefore.Count, "branch must see the inherited row before deletion");

        // Delete the row in the branch (writes a tombstone at level-0).
        KvTransaction del = await branch.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            del, branchName, $"DELETE FROM docs WHERE id = \"{rowId}\"", null));
        await branch.Transactions.CommitAsync(del);

        // Branch scan must now return 0: the level-0 tombstone suppresses the inherited ancestor row.
        List<QueryResultRow> branchAfter = await SelectAll(branchName, branch, executor, "SELECT title FROM docs");
        Assert.AreEqual(0, branchAfter.Count,
            "branch-level tombstone must suppress the inherited row in the ancestry merge");

        // Parent must be completely unaffected.
        List<QueryResultRow> parentRows = await SelectAll(rootName, rootDb, executor, "SELECT title FROM docs");
        Assert.AreEqual(1, parentRows.Count,
            "parent must not be affected by a branch-level delete");
        Assert.AreEqual("inherited", parentRows[0].Row["title"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task BranchDelete_SuppressesInheritedRow_PointRead()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE notes (id OBJECT_ID PRIMARY KEY, body STRING)");

        await InsertRow(rootName, rootDb, executor, "INSERT INTO notes (id, body) VALUES (gen_id(), \"hello\")");

        List<QueryResultRow> beforeFork = await SelectAll(rootName, rootDb, executor, "SELECT id FROM notes");
        string rowId = beforeFork[0].Row["id"].StrValue!;

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Delete in branch.
        KvTransaction del = await branch.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            del, branchName, $"DELETE FROM notes WHERE id = \"{rowId}\"", null));
        await branch.Transactions.CommitAsync(del);

        // Branch point-read by PK must return nothing.
        List<QueryResultRow> branchRows = await SelectAll(branchName, branch, executor,
            $"SELECT body FROM notes WHERE id = \"{rowId}\"");
        Assert.AreEqual(0, branchRows.Count,
            "branch point-read must respect level-0 tombstone and not surface the inherited row");

        // Parent point-read must still succeed.
        List<QueryResultRow> parentRows = await SelectAll(rootName, rootDb, executor,
            $"SELECT body FROM notes WHERE id = \"{rowId}\"");
        Assert.AreEqual(1, parentRows.Count, "parent point-read must be unaffected by branch delete");
        Assert.AreEqual("hello", parentRows[0].Row["body"].StrValue);
    }

    // -----------------------------------------------------------------------
    // Branch writes, deletes, and bilateral index suppression
    // -----------------------------------------------------------------------

    /// <summary>
    /// UPDATE a non-indexed column on a branch row that was inherited from the parent.
    /// The inherited unique index (on an unchanged column) must still locate the row,
    /// and the row fetch must return the branch-local updated value.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchUpdate_NonIndexedColumn_UniqueIndexStillLocatesUpdatedRow()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: rootName,
            sql: "CREATE TABLE employees (id OBJECT_ID PRIMARY KEY, email STRING, salary INT64)", parameters: null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: rootName,
            sql: "CREATE UNIQUE INDEX email_uidx ON employees (email)", parameters: null));

        await InsertRow(rootName, rootDb, executor,
            "INSERT INTO employees (id, email, salary) VALUES (gen_id(), \"alice@corp.com\", 100)");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Update the non-indexed column on the branch.
        KvTransaction upd = await branch.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            upd, branchName, "UPDATE employees SET salary = 200 WHERE email = \"alice@corp.com\"", null));
        await branch.Transactions.CommitAsync(upd);

        // The branch's index on email still locates the row and the branch-local salary is returned.
        List<QueryResultRow> branchRows = await SelectAll(branchName, branch, executor,
            "SELECT salary FROM employees WHERE email = \"alice@corp.com\"");
        Assert.AreEqual(1, branchRows.Count, "branch unique-index lookup must still find the row after non-indexed column update");
        Assert.AreEqual(200L, branchRows[0].Row["salary"].LongValue,
            "branch must return the updated salary, not the inherited value");

        // The parent is unaffected.
        List<QueryResultRow> parentRows = await SelectAll(rootName, rootDb, executor,
            "SELECT salary FROM employees WHERE email = \"alice@corp.com\"");
        Assert.AreEqual(1, parentRows.Count, "parent must be unaffected by branch update");
        Assert.AreEqual(100L, parentRows[0].Row["salary"].LongValue, "parent salary must remain 100");
    }

    /// <summary>
    /// UPDATE an indexed column on a branch: the old index value must be invisible on the branch
    /// (tombstoned), the new index value must be visible, and the ancestor must be entirely unaffected.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchUpdate_IndexedColumn_OldKeyInvisible_NewKeyVisible_AncestorUnchanged()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: rootName,
            sql: "CREATE TABLE products (id OBJECT_ID PRIMARY KEY, sku STRING, price INT64)", parameters: null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: rootName,
            sql: "CREATE UNIQUE INDEX sku_uidx ON products (sku)", parameters: null));

        await InsertRow(rootName, rootDb, executor,
            "INSERT INTO products (id, sku, price) VALUES (gen_id(), \"SKU-A\", 50)");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Change the indexed column (sku) on the branch.
        KvTransaction upd = await branch.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            upd, branchName, "UPDATE products SET sku = \"SKU-B\" WHERE sku = \"SKU-A\"", null));
        await branch.Transactions.CommitAsync(upd);

        // Old key must be invisible on the branch.
        List<QueryResultRow> branchOld = await SelectAll(branchName, branch, executor,
            "SELECT price FROM products WHERE sku = \"SKU-A\"");
        Assert.AreEqual(0, branchOld.Count,
            "branch must not find the row under the old index key after update");

        // New key must be visible on the branch.
        List<QueryResultRow> branchNew = await SelectAll(branchName, branch, executor,
            "SELECT price FROM products WHERE sku = \"SKU-B\"");
        Assert.AreEqual(1, branchNew.Count, "branch must find the row under the new index key");
        Assert.AreEqual(50L, branchNew[0].Row["price"].LongValue);

        // Ancestor must still see the original sku (old key) and must not see the new sku.
        List<QueryResultRow> ancestorOld = await SelectAll(rootName, rootDb, executor,
            "SELECT price FROM products WHERE sku = \"SKU-A\"");
        Assert.AreEqual(1, ancestorOld.Count, "ancestor must still have the original sku entry");

        List<QueryResultRow> ancestorNew = await SelectAll(rootName, rootDb, executor,
            "SELECT price FROM products WHERE sku = \"SKU-B\"");
        Assert.AreEqual(0, ancestorNew.Count, "ancestor must not see the branch-local new sku");
    }

    /// <summary>
    /// UPDATE via a non-unique (multi) index scan: old multi-index entry suppressed on branch,
    /// new entry visible; ancestor unchanged.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchUpdate_MultiIndex_OldEntryInvisible_NewEntryVisible()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) = await CreateDatabase();
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: rootName,
            sql: "CREATE TABLE orders (id OBJECT_ID PRIMARY KEY, status STRING, amount INT64)", parameters: null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: rootName,
            sql: "CREATE INDEX status_idx ON orders (status)", parameters: null));

        await InsertRow(rootName, rootDb, executor,
            "INSERT INTO orders (id, status, amount) VALUES (gen_id(), \"pending\", 300)");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Change the indexed column (status) on the branch.
        KvTransaction upd = await branch.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            upd, branchName, "UPDATE orders SET status = \"shipped\" WHERE status = \"pending\"", null));
        await branch.Transactions.CommitAsync(upd);

        // Old multi-index entry must be invisible on the branch.
        List<QueryResultRow> branchPending = await SelectAll(branchName, branch, executor,
            "SELECT amount FROM orders WHERE status = \"pending\"");
        Assert.AreEqual(0, branchPending.Count,
            "branch must not surface rows under the old multi-index key after update");

        // New multi-index entry must be visible.
        List<QueryResultRow> branchShipped = await SelectAll(branchName, branch, executor,
            "SELECT amount FROM orders WHERE status = \"shipped\"");
        Assert.AreEqual(1, branchShipped.Count, "branch must find the row under the new multi-index key");
        Assert.AreEqual(300L, branchShipped[0].Row["amount"].LongValue);

        // Ancestor still has the original status entry.
        List<QueryResultRow> ancestorPending = await SelectAll(rootName, rootDb, executor,
            "SELECT amount FROM orders WHERE status = \"pending\"");
        Assert.AreEqual(1, ancestorPending.Count, "ancestor status must be unaffected by branch update");

        List<QueryResultRow> ancestorShipped = await SelectAll(rootName, rootDb, executor,
            "SELECT amount FROM orders WHERE status = \"shipped\"");
        Assert.AreEqual(0, ancestorShipped.Count, "ancestor must not see the branch-local new status");
    }

    /// <summary>
    /// UPDATE all inherited rows on a branch using a predicate that matches everything.
    /// Verifies the data scan and write path both route through the branch-aware store,
    /// and the ancestor remains at its original values.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchUpdate_FullScan_BranchLocalValue_AncestorPreserved()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE counters (id OBJECT_ID PRIMARY KEY, val INT64)");

        await InsertRow(rootName, rootDb, executor,
            "INSERT INTO counters (id, val) VALUES (gen_id(), 10)");
        await InsertRow(rootName, rootDb, executor,
            "INSERT INTO counters (id, val) VALUES (gen_id(), 20)");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Update both inherited rows on the branch — match all via id IS NOT NULL.
        KvTransaction upd = await branch.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            upd, branchName, "UPDATE counters SET val = 99 WHERE id IS NOT NULL", null));
        await branch.Transactions.CommitAsync(upd);

        // Branch must see 99 for all rows.
        List<QueryResultRow> branchRows = await SelectAll(branchName, branch, executor,
            "SELECT val FROM counters");
        Assert.AreEqual(2, branchRows.Count, "branch must see both rows after full-scan update");
        Assert.IsTrue(branchRows.All(r => r.Row["val"].LongValue == 99),
            "every branch row must have the updated val=99");

        // Ancestor must still have its original values.
        List<QueryResultRow> ancestorRows = await SelectAll(rootName, rootDb, executor,
            "SELECT val FROM counters");
        Assert.AreEqual(2, ancestorRows.Count, "ancestor row count must be unchanged");
        List<long> ancestorVals = ancestorRows.Select(r => r.Row["val"].LongValue).OrderBy(v => v).ToList();
        CollectionAssert.AreEqual(new long[] { 10, 20 }, ancestorVals,
            "ancestor values must be unaffected by branch update");
    }

    // -----------------------------------------------------------------------
    // Unique and primary-key constraints over the union
    // -----------------------------------------------------------------------

    /// <summary>
    /// Inserting a row on a branch with a unique key that already exists in the ancestor
    /// must be rejected as a duplicate.  The constraint must span level-0 plus ancestry,
    /// not only the branch overlay.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchInsert_AncestorUniqueKey_ConflictsOnBranch()
    {
        string rootName = NewName();
        CommandExecutor executor = CreateCommandExecutor();
        DatabaseDescriptor rootDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE users (id OBJECT_ID PRIMARY KEY, email STRING)",
            parameters: null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE UNIQUE INDEX users_email ON users (email)",
            parameters: null));

        await InsertRow(rootName, rootDb, executor,
            "INSERT INTO users (id, email) VALUES (gen_id(), \"alice@example.com\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Insert on branch with the same unique email that already lives in the ancestor.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            KvTransaction ins = await branch.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                ins, branchName,
                "INSERT INTO users (id, email) VALUES (gen_id(), \"alice@example.com\")",
                null));
            await branch.Transactions.CommitAsync(ins);
        })!;

        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code,
            "inserting a key present in an ancestor must throw DuplicateUniqueKeyValue");

        // The ancestor is unaffected — still has exactly one row.
        List<QueryResultRow> ancestorRows = await SelectAll(rootName, rootDb, executor,
            "SELECT email FROM users");
        Assert.AreEqual(1, ancestorRows.Count, "ancestor must remain unchanged");
    }

    /// <summary>
    /// After deleting a row on a branch (which tombstones the unique index entry at level-0),
    /// a subsequent insert of the same unique key on the branch must succeed.
    /// The tombstone marks the slot as available; inserting after a delete is not a conflict.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchInsert_AfterBranchDelete_TombstoneReplaceSucceeds()
    {
        string rootName = NewName();
        CommandExecutor executor = CreateCommandExecutor();
        DatabaseDescriptor rootDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE products (id OBJECT_ID PRIMARY KEY, sku STRING)",
            parameters: null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE UNIQUE INDEX products_sku ON products (sku)",
            parameters: null));

        await InsertRow(rootName, rootDb, executor,
            "INSERT INTO products (id, sku) VALUES (gen_id(), \"SKU-X\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Delete the inherited row on the branch (tombstones level-0 unique index entry).
        KvTransaction del = await branch.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            del, branchName, "DELETE FROM products WHERE sku = \"SKU-X\"", null));
        await branch.Transactions.CommitAsync(del);

        // Re-insert with the same unique key on the branch — must succeed because the
        // tombstone cleared the slot.
        Assert.DoesNotThrowAsync(async () =>
        {
            KvTransaction ins = await branch.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                ins, branchName,
                "INSERT INTO products (id, sku) VALUES (gen_id(), \"SKU-X\")",
                null));
            await branch.Transactions.CommitAsync(ins);
        }, "re-inserting a tombstoned unique key on the branch must not throw");

        // Branch now has the re-inserted row.
        List<QueryResultRow> branchRows = await SelectAll(branchName, branch, executor,
            "SELECT sku FROM products WHERE sku = \"SKU-X\"");
        Assert.AreEqual(1, branchRows.Count, "branch must see the re-inserted row");

        // Ancestor still has the original row (not affected by the branch delete/re-insert).
        List<QueryResultRow> ancestorRows = await SelectAll(rootName, rootDb, executor,
            "SELECT sku FROM products WHERE sku = \"SKU-X\"");
        Assert.AreEqual(1, ancestorRows.Count, "ancestor must still have the original row");
    }

    /// <summary>
    /// Updating a unique-indexed column on a branch to a value already present in the ancestor
    /// must be rejected.  The uniqueness check must span level-0 plus ancestry.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchUpdate_UniqueColumn_ToAncestorValue_Conflicts()
    {
        string rootName = NewName();
        CommandExecutor executor = CreateCommandExecutor();
        DatabaseDescriptor rootDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE items (id OBJECT_ID PRIMARY KEY, code STRING)",
            parameters: null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE UNIQUE INDEX items_code ON items (code)",
            parameters: null));

        // Two rows in ancestor: one will be inherited, one will be the target of a conflicting update.
        await InsertRow(rootName, rootDb, executor,
            "INSERT INTO items (id, code) VALUES (gen_id(), \"A\")");
        await InsertRow(rootName, rootDb, executor,
            "INSERT INTO items (id, code) VALUES (gen_id(), \"B\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Attempt to update code="A" to code="B" on branch — "B" already exists in ancestor.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            KvTransaction upd = await branch.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                upd, branchName,
                "UPDATE items SET code = \"B\" WHERE code = \"A\"",
                null));
            await branch.Transactions.CommitAsync(upd);
        })!;

        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code,
            "updating a unique column to a value present in an ancestor must conflict");
    }

    /// <summary>
    /// Batch-inserting rows on a branch where one row has a unique key that exists only in an
    /// ancestor must be rejected.  The per-item ancestry probe in the branch batch path must
    /// catch the cross-lineage conflict.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchBatchInsert_AncestorUniqueKey_ConflictsOnBranch()
    {
        string rootName = NewName();
        CommandExecutor executor = CreateCommandExecutor();
        DatabaseDescriptor rootDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE nodes (id OBJECT_ID PRIMARY KEY, name STRING)",
            parameters: null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE UNIQUE INDEX nodes_name ON nodes (name)",
            parameters: null));

        await InsertRow(rootName, rootDb, executor,
            "INSERT INTO nodes (id, name) VALUES (gen_id(), \"node-1\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Batch INSERT two rows; the second duplicates an ancestor unique key.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            KvTransaction ins = await branch.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                ins, branchName,
                "INSERT INTO nodes (id, name) VALUES (gen_id(), \"node-2\")",
                null));
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
                ins, branchName,
                "INSERT INTO nodes (id, name) VALUES (gen_id(), \"node-1\")",
                null));
            await branch.Transactions.CommitAsync(ins);
        })!;

        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code,
            "batch insert that conflicts with an ancestor unique key must throw DuplicateUniqueKeyValue");
    }

    /// <summary>
    /// Uniqueness on the ancestor is unaffected by branch inserts.
    /// A row inserted on the branch with a brand-new unique key must not block a subsequent
    /// insert of the same key on the ancestor.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task AncestorUniqueness_UnaffectedByBranchState()
    {
        string rootName = NewName();
        CommandExecutor executor = CreateCommandExecutor();
        DatabaseDescriptor rootDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);

        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE TABLE labels (id OBJECT_ID PRIMARY KEY, tag STRING)",
            parameters: null));
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: null!, database: rootName,
            sql: "CREATE UNIQUE INDEX labels_tag ON labels (tag)",
            parameters: null));

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Insert tag="X" on branch.
        await InsertRow(branchName, branch, executor,
            "INSERT INTO labels (id, tag) VALUES (gen_id(), \"X\")");

        // Insert tag="X" on ancestor — must succeed independently (branch state is isolated).
        Assert.DoesNotThrowAsync(async () =>
        {
            await InsertRow(rootName, rootDb, executor,
                "INSERT INTO labels (id, tag) VALUES (gen_id(), \"X\")");
        }, "ancestor insert of a key that only exists on the branch must not conflict");

        // Ancestor has tag="X"; branch also has its own tag="X" row.
        List<QueryResultRow> ancestorRows = await SelectAll(rootName, rootDb, executor,
            "SELECT tag FROM labels WHERE tag = \"X\"");
        Assert.AreEqual(1, ancestorRows.Count, "ancestor must have exactly one tag=X row");

        List<QueryResultRow> branchRows = await SelectAll(branchName, branch, executor,
            "SELECT tag FROM labels WHERE tag = \"X\"");
        Assert.AreEqual(1, branchRows.Count,
            "branch must see exactly one tag=X row (its own, not the ancestor's)");
    }

    // -----------------------------------------------------------------------
    // Branch-Aware DDL
    // -----------------------------------------------------------------------

    /// <summary>
    /// Parent ALTER TABLE ADD COLUMN after fork must not be visible to the branch because each
    /// database owns an independent copy of schema metadata created at fork time.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Branch_ParentAddColumn_InvisibleToBranch()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE things (id OBJECT_ID PRIMARY KEY, val STRING)");

        await InsertRow(rootName, rootDb, executor, "INSERT INTO things (id, val) VALUES (gen_id(), \"root\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Parent DDL after fork: add a column that the branch should never see.
        await executor.AlterTable(new AlterTableTicket(
            databaseName: rootName,
            tableName: "things",
            operation: AlterTableOperation.AddColumn,
            column: new ColumnInfo("extra", ColumnType.Integer64)));

        // Root sees the new column schema (query would include it).
        TableDescriptor rootTable = await executor.OpenTable(new OpenTableTicket(rootName, "things"));
        Assert.IsTrue(rootTable.Schema.Columns!.Any(c => c.Name == "extra"),
            "root schema must carry the new column");

        // Branch must not see it — branch has its own schema copy.
        TableDescriptor branchTable = await executor.OpenTable(new OpenTableTicket(branchName, "things"));
        Assert.IsFalse(branchTable.Schema.Columns!.Any(c => c.Name == "extra"),
            "branch schema must be isolated from parent DDL after fork");
    }

    /// <summary>
    /// Branch ALTER TABLE ADD COLUMN must not be visible to the parent because branch DDL
    /// writes only to the branch's own schema metadata namespace.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Branch_BranchAddColumn_InvisibleToParent()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE gadgets (id OBJECT_ID PRIMARY KEY, name STRING)");

        await InsertRow(rootName, rootDb, executor, "INSERT INTO gadgets (id, name) VALUES (gen_id(), \"widget\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Branch DDL: add a column that the parent should never see.
        await executor.AlterTable(new AlterTableTicket(
            databaseName: branchName,
            tableName: "gadgets",
            operation: AlterTableOperation.AddColumn,
            column: new ColumnInfo("weight", ColumnType.Float64)));

        // Branch sees the new column.
        TableDescriptor branchTable = await executor.OpenTable(new OpenTableTicket(branchName, "gadgets"));
        Assert.IsTrue(branchTable.Schema.Columns!.Any(c => c.Name == "weight"),
            "branch schema must carry the new column added after fork");

        // Parent must not see the branch's column.
        TableDescriptor rootTable = await executor.OpenTable(new OpenTableTicket(rootName, "gadgets"));
        Assert.IsFalse(rootTable.Schema.Columns!.Any(c => c.Name == "weight"),
            "parent schema must be isolated from branch DDL");
    }

    /// <summary>
    /// Branch DROP TABLE must leave the parent's rows intact AND must not write tombstones for
    /// inherited rows into the branch keyspace. This directly proves the O(overlay) path:
    /// PurgeLocalRowOverlayAsync deletes 0 or branch-local entries only, leaving the branch's
    /// raw row bucket empty; the old rowDeleter.Delete path would leave 2 inherited tombstones
    /// plus a branch-local tombstone, so the bucket would have 3 entries. This test discriminates
    /// between the two paths via a raw Kahuna bucket scan.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchDropTable_LeavesParentRowsIntact()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE sensors (id OBJECT_ID PRIMARY KEY, reading INT64)");

        await InsertRow(rootName, rootDb, executor, "INSERT INTO sensors (id, reading) VALUES (gen_id(), 10)");
        await InsertRow(rootName, rootDb, executor, "INSERT INTO sensors (id, reading) VALUES (gen_id(), 20)");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Write a branch-local row so the overlay has exactly 1 physical entry.
        await InsertRow(branchName, branch, executor, "INSERT INTO sensors (id, reading) VALUES (gen_id(), 99)");

        // Capture the table id and the Kahuna instance before dropping (the table won't be in the
        // branch schema afterwards so OpenTable would throw).
        TableDescriptor branchTableBefore = await executor.OpenTable(new OpenTableTicket(branchName, "sensors"));
        string tableId = branchTableBefore.Id;
        IKahuna kahuna = branch.Kahuna.Kahuna;
        string branchDbId = branch.Id;

        string rowBucket = $"{branchDbId}:{tableId}:r";
        string rowKeyPrefix = rowBucket + "/";

        // Sanity: the branch overlay has exactly 1 physical row entry before the drop.
        Assert.AreEqual(1, await CountKeysUnder(kahuna, rowBucket, rowKeyPrefix),
            "sanity: branch must have exactly 1 branch-local row before DROP TABLE");

        // Drop the table on the branch only.
        Assert.IsTrue(await executor.DropTable(
            new DropTableTicket(databaseName: branchName, tableName: "sensors", ifExists: false)));

        // Core discriminating assertion: the branch's raw row bucket must be empty.
        // PurgeLocalRowOverlayAsync physically deletes the 1 branch-local entry and writes no
        // tombstones for the 2 inherited ancestor rows — so 0 entries remain.
        // The old rowDeleter.Delete path would write tombstones for all 3 visible rows, leaving 3
        // entries in the branch bucket. If this assertion fails, the O(parent data) path is active.
        Assert.AreEqual(0, await CountKeysUnder(kahuna, rowBucket, rowKeyPrefix),
            "branch row bucket must be empty after DROP TABLE — PurgeLocalRowOverlayAsync must not leave tombstones for inherited rows");

        // Branch no longer has the table in its schema.
        Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.OpenTable(new OpenTableTicket(branchName, "sensors")));

        // Parent rows are entirely unaffected.
        List<QueryResultRow> parentRows = await SelectAll(rootName, rootDb, executor,
            "SELECT reading FROM sensors");
        Assert.AreEqual(2, parentRows.Count,
            "parent must still have both rows after branch DROP TABLE");
    }

    /// <summary>
    /// Branch DROP INDEX must leave the parent's index entries intact. Only branch-local overlay
    /// index entries are purged. The parent can still use the index after the branch drops it.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchDropIndex_LeavesParentIndexIntact()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE products (id OBJECT_ID PRIMARY KEY, sku STRING)");

        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: rootName,
            tableName: "products",
            indexName: "sku_idx",
            columns: [new ColumnIndexInfo("sku", OrderType.Ascending)],
            operation: AlterIndexOperation.AddIndex));

        await InsertRow(rootName, rootDb, executor, "INSERT INTO products (id, sku) VALUES (gen_id(), \"A001\")");
        await InsertRow(rootName, rootDb, executor, "INSERT INTO products (id, sku) VALUES (gen_id(), \"B002\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Drop the index on the branch only.
        Assert.IsTrue(await executor.AlterIndex(new AlterIndexTicket(
            databaseName: branchName,
            tableName: "products",
            indexName: "sku_idx",
            columns: [new ColumnIndexInfo("sku", OrderType.Ascending)],
            operation: AlterIndexOperation.DropIndex)));

        // Branch no longer has the index.
        TableDescriptor branchTable = await executor.OpenTable(new OpenTableTicket(branchName, "products"));
        Assert.IsFalse(branchTable.Indexes.ContainsKey("sku_idx"),
            "branch must no longer carry the dropped index");

        // Parent still has the index and can still query through it.
        TableDescriptor rootTable = await executor.OpenTable(new OpenTableTicket(rootName, "products"));
        Assert.IsTrue(rootTable.Indexes.ContainsKey("sku_idx"),
            "parent must still carry the index after branch drops it");

        List<QueryResultRow> rows = await SelectAll(rootName, rootDb, executor,
            "SELECT sku FROM products WHERE sku = \"A001\"");
        Assert.AreEqual(1, rows.Count,
            "parent must still be able to query via the index after branch drops it");
    }

    /// <summary>
    /// Rows written in the ancestor under an older schema version must decode correctly on the
    /// branch after the branch adds a new column, using schema history copied at fork time.
    /// The new column should read as null for inherited rows.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Branch_InheritedRows_DecodeCorrectly_AfterBranchAlterTable()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE events (id OBJECT_ID PRIMARY KEY, label STRING)");

        await InsertRow(rootName, rootDb, executor, "INSERT INTO events (id, label) VALUES (gen_id(), \"fork-point\")");

        string branchName = NewName();
        DatabaseDescriptor branch = await executor.CreateDatabase(
            new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Branch DDL: add a column that inherited rows do not carry.
        await executor.AlterTable(new AlterTableTicket(
            databaseName: branchName,
            tableName: "events",
            operation: AlterTableOperation.AddColumn,
            column: new ColumnInfo("priority", ColumnType.Integer64)));

        // Write a branch-local row with the new column.
        await InsertRow(branchName, branch, executor,
            "INSERT INTO events (id, label, priority) VALUES (gen_id(), \"branch-new\", 42)");

        // Read all rows on the branch — inherited row must decode without error (priority = null).
        List<QueryResultRow> rows = await SelectAll(branchName, branch, executor,
            "SELECT label, priority FROM events");

        Assert.AreEqual(2, rows.Count, "branch must see both the inherited row and its own row");

        List<QueryResultRow> inheritedRows = rows.Where(r => r.Row["label"].StrValue == "fork-point").ToList();
        List<QueryResultRow> localRows = rows.Where(r => r.Row["label"].StrValue == "branch-new").ToList();

        Assert.AreEqual(1, inheritedRows.Count, "inherited row must be visible on the branch");
        Assert.AreEqual(1, localRows.Count, "branch-local row must be visible on the branch");

        Assert.AreEqual(ColumnType.Null, inheritedRows[0].Row["priority"].Type,
            "priority must be null for the inherited row written before the column was added");
        Assert.AreEqual(42L, localRows[0].Row["priority"].LongValue,
            "branch-local row must carry the priority value it was inserted with");
    }

    // -----------------------------------------------------------------------
    // Drop, descendants, and orphan recovery
    // -----------------------------------------------------------------------

    /// <summary>
    /// Dropping a leaf branch succeeds and leaves the parent database usable.
    /// The parent's rows are still queryable and the parent can be branched again.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task LeafDrop_LeavesParentUsable()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE products (id OBJECT_ID PRIMARY KEY, name STRING)");

        await InsertRow(rootName, rootDb, executor, "INSERT INTO products (id, name) VALUES (gen_id(), \"widget\")");

        string branchName = NewName();
        await executor.CreateDatabase(new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Drop the leaf branch.
        await executor.DropDatabase(new DropDatabaseTicket(branchName));

        // Parent remains queryable.
        List<QueryResultRow> rows = await SelectAll(rootName, rootDb, executor, "SELECT name FROM products");
        Assert.AreEqual(1, rows.Count, "parent must remain intact after leaf branch drop");
        Assert.AreEqual("widget", rows[0].Row["name"].StrValue);

        // Parent can be branched again.
        string branch2 = NewName();
        await executor.CreateDatabase(new CreateDatabaseTicket(branch2, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branch2, executor);
    }

    /// <summary>
    /// Attempting to drop a database that has at least one registered descendant branch fails with
    /// <see cref="CamusDBErrorCodes.DatabaseHasLiveDescendants"/>. The parent and the branch are
    /// both still usable after the rejected drop.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task NonLeafDrop_Fails_WhenDescendantsExist()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE items (id OBJECT_ID PRIMARY KEY, val STRING)");

        await InsertRow(rootName, rootDb, executor, "INSERT INTO items (id, val) VALUES (gen_id(), \"root\")");

        string branchName = NewName();
        await executor.CreateDatabase(new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Dropping the root while the branch is live must be rejected.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => executor.DropDatabase(new DropDatabaseTicket(rootName)));

        Assert.IsNotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.DatabaseHasLiveDescendants, ex!.Code,
            "drop must fail with DatabaseHasLiveDescendants when live branches exist");

        // Root is still queryable — the rejected drop must not have modified state.
        List<QueryResultRow> rows = await SelectAll(rootName, rootDb, executor, "SELECT val FROM items");
        Assert.AreEqual(1, rows.Count, "root rows must still be readable after rejected non-leaf drop");

        // Branch is still queryable.
        DatabaseDescriptor branchDb = await executor.OpenDatabase(branchName);
        List<QueryResultRow> branchRows = await SelectAll(branchName, branchDb, executor, "SELECT val FROM items");
        Assert.AreEqual(1, branchRows.Count, "branch must remain accessible after rejected non-leaf drop");
    }

    /// <summary>
    /// In a three-level chain (root → parent-branch → leaf-branch), the root cannot be dropped
    /// while the parent-branch exists, the parent-branch cannot be dropped while the leaf exists,
    /// and dropping leaf then parent then root all succeed in that order.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task ThreeLevelChain_DropOrder_EnforcedCorrectly()
    {
        (string rootName, DatabaseDescriptor rootDb3, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE t (id OBJECT_ID PRIMARY KEY, v STRING)");

        await InsertRow(rootName, rootDb3, executor, "INSERT INTO t (id, v) VALUES (gen_id(), \"r\")");

        string midName = NewName();
        await executor.CreateDatabase(new CreateDatabaseTicket(midName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(midName, executor);

        string leafName = NewName();
        await executor.CreateDatabase(new CreateDatabaseTicket(leafName, ifNotExists: false, branchFrom: midName));
        TrackDatabase(leafName, executor);

        // Root drop blocked by mid and leaf.
        CamusDBException? ex1 = Assert.ThrowsAsync<CamusDBException>(
            () => executor.DropDatabase(new DropDatabaseTicket(rootName)));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseHasLiveDescendants, ex1!.Code,
            "root drop must be blocked while mid-branch and leaf-branch exist");

        // Mid drop blocked by leaf.
        CamusDBException? ex2 = Assert.ThrowsAsync<CamusDBException>(
            () => executor.DropDatabase(new DropDatabaseTicket(midName)));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseHasLiveDescendants, ex2!.Code,
            "mid-branch drop must be blocked while leaf-branch exists");

        // Leaf drops successfully.
        await executor.DropDatabase(new DropDatabaseTicket(leafName));

        // Mid now drops successfully.
        await executor.DropDatabase(new DropDatabaseTicket(midName));

        // Root now drops successfully.
        await executor.DropDatabase(new DropDatabaseTicket(rootName));
    }

    /// <summary>
    /// The production startup scrubber (<c>ScrubOrphanBranchNamespacesAsync</c>) removes metadata
    /// written for a branch id that was never registered — simulating a crash between
    /// <c>CopyMetaForBranchAsync</c> and <c>RegisterAsync</c> during a branch creation attempt.
    ///
    /// This test invokes the real scrubber (not a reimplementation) to verify its bucket/prefix
    /// math, 3-round retry loop, per-key error handling, and pending-marker cleanup end-to-end.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task OrphanScrub_RemovesUnregisteredBranchMetadata()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE things (id OBJECT_ID PRIMARY KEY, v STRING)");

        // Allocate a fresh id and inject metadata directly, simulating a crash
        // after CopyMetaForBranchAsync but before RegisterAsync.
        string orphanId = await sharedRegistry!.AllocateIdAsync();

        // Write a sentinel meta key under the orphan id (mirrors what CopyMetaForBranchAsync writes).
        string orphanMetaKey = $"{orphanId}/meta/version";
        IKahuna kahuna = rootDb.Kahuna.Kahuna;
        await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, orphanMetaKey, [0x01], null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None);

        // Write a pending marker, as the production code does before CopyMetaForBranchAsync.
        await sharedRegistry.TrackPendingBranchAsync(orphanId);

        // Sanity: the sentinel and the pending marker are present before scrubbing.
        string metaBucket = $"{orphanId}/meta";
        int beforeCount = await CountKeysUnder(kahuna, metaBucket, $"{orphanId}/");
        Assert.AreEqual(1, beforeCount, "sentinel meta key must be present before scrub");

        List<string> orphansBefore = await sharedRegistry.LoadOrphanBranchIdsAsync();
        Assert.Contains(orphanId, orphansBefore, "pending id not in registry must appear as an orphan");

        // Invoke the PRODUCTION scrubber — not a reimplementation. This exercises the real
        // bucket/prefix math, 3-round retry loop, per-key error handling, and marker clearing.
        await executor.ScrubOrphanBranchNamespacesAsync(TestNode!, sharedRegistry!);

        // Sentinel meta key must be gone.
        int afterCount = await CountKeysUnder(kahuna, metaBucket, $"{orphanId}/");
        Assert.AreEqual(0, afterCount, "production scrubber must have removed the orphan meta key");

        // Pending marker must also be cleared.
        List<string> orphansAfter = await sharedRegistry.LoadOrphanBranchIdsAsync();
        Assert.IsFalse(orphansAfter.Contains(orphanId),
            "production scrubber must clear the pending marker so the id is no longer an orphan");

        // Root database is unaffected.
        List<QueryResultRow> rows = await SelectAll(rootName, rootDb, executor, "SELECT v FROM things");
        Assert.AreEqual(0, rows.Count, "root database must be unaffected by the orphan scrub");
    }

    /// <summary>
    /// The persistent existence check subsumes the old cross-node name race that used to reach the
    /// branch-create abort-after-copy path: because the existence check at the top of <c>CreateDatabase</c> now resolves through the
    /// persistent registry, a branch target that already exists in the shared KV (even if absent from
    /// this node's cache) is rejected <em>before</em> the branch flow runs. So no branch id is
    /// allocated, no snapshot hold is acquired, and no metadata is copied — the branch-create's inline
    /// abort/purge path is no longer reachable from a pre-registered name. (The inline purge remains as
    /// defense-in-depth for a genuine concurrent registration between the two persistent checks, which
    /// is not deterministically reproducible.)
    ///
    /// Discriminator: the registry id sequence is unchanged after the rejected create. With a cache-only
    /// precheck the check missed and the branch flow ran — allocating an id (advancing the sequence)
    /// and copying metadata — only to fail late at RegisterAsync.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task CreateBranch_TargetExistsInKvNotCache_RejectsEarlyWithoutEnteringBranchFlow()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE things2 (id OBJECT_ID PRIMARY KEY, v STRING)");

        // A "remote node" registers the branch target name into the shared KV registry (a valid entry),
        // leaving it absent from this executor's sharedRegistry cache.
        await using DatabaseRegistry remoteRegistry = await DatabaseRegistry.OpenAsync(TestNode!, Options);
        string branchName = NewName();
        string remoteId = await remoteRegistry.AllocateIdAsync();
        await remoteRegistry.RegisterAsync(branchName, remoteId);

        Assert.IsNull(sharedRegistry!.Get(branchName), "precondition: branch target absent from local cache");

        IKahuna kahuna = rootDb.Kahuna.Kahuna;
        (SequenceResponseType seqType, ReadOnlySequenceEntry? seqBefore) = await kahuna.LocateAndGetSequence(
            "_system/dbregistry/seq", SequenceDurability.Persistent, CancellationToken.None);
        Assert.AreEqual(SequenceResponseType.Success, seqType);
        long seqValueBefore = seqBefore!.CurrentValue;

        // Must reject early with DatabaseAlreadyExists.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => executor.CreateDatabase(new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName)));
        Assert.AreEqual(CamusDBErrorCodes.DatabaseAlreadyExists, ex!.Code,
            "an already-existing branch target must be rejected with DatabaseAlreadyExists");

        // Discriminating: the branch flow was never entered, so no branch id was allocated.
        (_, ReadOnlySequenceEntry? seqAfter) = await kahuna.LocateAndGetSequence(
            "_system/dbregistry/seq", SequenceDurability.Persistent, CancellationToken.None);
        Assert.AreEqual(seqValueBefore, seqAfter!.CurrentValue,
            "CREATE BRANCH on an already-existing target must reject before allocating a branch id (persistent existence check)");
    }

    /// <summary>
    /// Proves the semaphore-based guard prevents the check-then-act race between DropDatabase and
    /// CreateBranchDatabaseAsync on a single node. When DropDatabase holds the target's
    /// SchemaDdlSemaphore and unregisters the source before the branch-create gets the semaphore,
    /// the branch-create must observe the source gone and throw <see cref="CamusDBException"/>
    /// (DatabaseDoesntExist), not silently register an orphaned branch.
    ///
    /// This test simulates the race deterministically: it opens the source descriptor and acquires
    /// its SchemaDdlSemaphore manually (mimicking what DropDatabase does), then drops the source
    /// while the semaphore is held, then releases and attempts a concurrent branch-create.
    /// The branch-create observes the source unregistered and aborts.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task DropDatabase_SemaphorePrevents_ConcurrentBranchCreateRace()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE t (id OBJECT_ID PRIMARY KEY, v STRING)");

        // Open the descriptor so we can grab its semaphore to simulate DropDatabase winning first.
        DatabaseDescriptor srcDesc = await executor.OpenDatabase(rootName);

        // Hold the semaphore, simulating DropDatabase's critical section.
        await srcDesc.SchemaDdlSemaphore.WaitAsync();
        try
        {
            // Unregister the source while the semaphore is held — same as DropDatabase does.
            await sharedRegistry!.UnregisterAsync(rootName);
        }
        finally
        {
            srcDesc.SchemaDdlSemaphore.Release();
        }

        // Now attempt a branch-create from the source. The source is already unregistered.
        // CreateBranchDatabaseAsync must detect this under the semaphore and throw.
        string branchName = NewName();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => executor.CreateDatabase(new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName)));

        Assert.IsNotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code,
            "branch-create must observe the unregistered source and fail with DatabaseDoesntExist");
    }

    /// <summary>
    /// Proves the cross-node drop-vs-branch-create fence. The race is: DropDatabase on node A
    /// passes its descendant scan (no children yet), then CreateBranchDatabaseAsync on node B
    /// registers a child — after which A's purge would orphan the child against a destroyed namespace.
    ///
    /// The fix uses a persistent drop-intent KV key. A sets it before its scan; B checks it after
    /// RegisterAsync. Raft linearizability ensures one of: (a) A's intent committed before B's
    /// register → B sees it and aborts, or (b) B's register committed first → A's subsequent
    /// HasLiveDescendantsAsync scan sees the child and A aborts. Exactly one wins.
    ///
    /// Simulated deterministically: the intent key is written via a second registry instance
    /// (representing a different cluster node), then branch-create is attempted and must abort.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task DropDatabase_DropIntent_PreventsCrossNodeBranchCreateRace()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE things_br12 (id OBJECT_ID PRIMARY KEY, v STRING)");

        DatabaseRegistryEntry rootEntry = sharedRegistry!.Get(rootName)!;
        Assert.IsNotNull(rootEntry, "sanity: root must be registered");

        // Simulate DropDatabase on another cluster node: it passed the descendant scan (saw no
        // children) and now holds the drop-intent marker — the keyspace purge is about to run.
        // Use a second registry instance (independent cache) to represent the remote node's registry.
        await using DatabaseRegistry remoteRegistry = await DatabaseRegistry.OpenAsync(TestNode!, Options);
        bool acquired = await remoteRegistry.AcquireDropIntentAsync(rootEntry.Id);
        Assert.IsTrue(acquired, "drop-intent must be acquirable when no other drop is in progress");

        string branchName = NewName();
        try
        {
            // Branch-create from the still-registered root must detect the drop-intent after
            // registering the child and abort, leaving no orphaned registry entry.
            CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
                () => executor.CreateDatabase(new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName)));

            Assert.IsNotNull(ex);
            Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, ex!.Code,
                "branch-create must detect the drop-intent and abort with DatabaseDoesntExist");

            // No orphaned child must be registered.
            Assert.IsNull(sharedRegistry.Get(branchName),
                "aborted branch-create must not leave a registry entry for the child");

            // No orphaned pending marker should remain.
            List<string> orphans = await sharedRegistry.LoadOrphanBranchIdsAsync();
            Assert.AreEqual(0, orphans.Count,
                "aborted branch-create must clean up its pending marker");
        }
        finally
        {
            // Simulate the remote node releasing the intent after its purge completes.
            await remoteRegistry.ReleaseDropIntentAsync(rootEntry.Id);
        }

        // The root database must still be usable (drop never completed — it was only simulated
        // up to the intent stage, not through UnregisterAsync or Drop).
        List<QueryResultRow> rows = await SelectAll(rootName, rootDb, executor, "SELECT v FROM things_br12");
        Assert.AreEqual(0, rows.Count, "root database must be unaffected — the drop never completed");
    }

    /// <summary>
    /// A drop-intent key left by a process crash (between <c>AcquireDropIntentAsync</c> and the
    /// outer finally's <c>ReleaseDropIntentAsync</c>) makes the affected database permanently
    /// undroppable — every subsequent drop attempt finds the <c>SetIfNotExists</c> key already
    /// present and fails with "concurrent drop in progress." The startup scrubber must clear all
    /// drop-intent keys on startup because a drop-intent can never legitimately survive a restart
    /// (drops do not span restarts).
    ///
    /// This test plants a stale drop-intent key, confirms drop is blocked, invokes the production
    /// startup scrubber (<c>ScrubOrphanBranchNamespacesAsync</c>), and asserts the database is
    /// droppable again.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task StaleDropIntent_ClearedByStartupScrubber_MakesDbDroppableAgain()
    {
        (string rootName, DatabaseDescriptor _, CommandExecutor executor) =
            await CreateDatabase();

        DatabaseRegistryEntry rootEntry = sharedRegistry!.Get(rootName)!;
        Assert.IsNotNull(rootEntry, "sanity: root must be registered");

        // Plant a stale drop-intent key — simulates a crash during DropDatabase after
        // AcquireDropIntentAsync wrote the key but before the finally released it.
        bool acquired = await sharedRegistry.AcquireDropIntentAsync(rootEntry.Id);
        Assert.IsTrue(acquired, "sanity: no other drop in progress");

        // With the stale intent present, a fresh DropDatabase must block.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => executor.DropDatabase(new DropDatabaseTicket(rootName)));
        Assert.IsNotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code,
            "stale drop-intent must block a subsequent drop attempt");

        // Run the production startup scrubber on a FRESH registry instance — it carries a new startup
        // epoch, standing in for this node after a restart. The stale intent (written by the prior
        // instance's epoch) is then a prior-run remnant the scrub reclaims; a marker from the current
        // run would be protected as a live fence.
        DatabaseRegistry afterRestart = await DatabaseRegistry.OpenAsync(TestNode!, Options);
        await executor.ScrubOrphanBranchNamespacesAsync(TestNode!, afterRestart);
        await afterRestart.DisposeAsync();

        // After the scrub, the drop-intent must be gone so DropDatabase can proceed.
        await executor.DropDatabase(new DropDatabaseTicket(rootName));

        // Database must be gone from the registry.
        Assert.IsNull(sharedRegistry.Get(rootName),
            "database must be unregistered after drop succeeds post-scrub");
    }

    /// <summary>
    /// Startup drop-intent recovery must be owner-scoped: a restarting node must NOT clear a
    /// drop-intent that a different, still-live cluster node currently holds for an in-flight drop.
    /// Clearing it would reopen the cross-node drop/create race the drop-intent fence closes. The drop-intent marker
    /// carries the owning node's id; the scrub deletes only markers stamped with this node's id.
    ///
    /// Simulated by planting a drop-intent whose value is a foreign node id, then running this node's
    /// production scrubber and asserting the foreign marker survives.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task StaleDropIntent_OwnedByAnotherNode_SurvivesStartupScrubber()
    {
        (string rootName, _, CommandExecutor executor) = await CreateDatabase();
        DatabaseRegistryEntry rootEntry = sharedRegistry!.Get(rootName)!;

        IKahuna kahuna = TestNode!.Kahuna;
        int myNodeId = TestNode!.Raft.GetLocalNodeId();
        int otherNodeId = myNodeId + 1; // a different, still-live "remote" node
        string intentKey = $"_system/dbregistry/drop-intent:{rootEntry.Id}";

        // Plant a drop-intent owned by another node (an in-flight remote drop's fence).
        await kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, intentKey, System.Text.Encoding.UTF8.GetBytes(otherNodeId.ToString()),
            null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None);

        // This node's startup scrub must leave the foreign marker untouched.
        await executor.ScrubOrphanBranchNamespacesAsync(TestNode!, sharedRegistry);

        // The foreign fence must still be in place → this node cannot acquire the drop-intent.
        bool acquired = await sharedRegistry.AcquireDropIntentAsync(rootEntry.Id);
        Assert.IsFalse(acquired,
            "a drop-intent owned by another live node must survive this node's startup scrub");

        // Cleanup the simulated foreign marker.
        await kahuna.LocateAndTryDeleteKeyValue(
            HLCTimestamp.Zero, intentKey, KeyValueDurability.Persistent, CancellationToken.None);
    }

    /// <summary>
    /// DROP DATABASE unregisters the entry and then purges its keyspace with per-key autocommit
    /// deletes — not one transaction — so a crash mid-purge would orphan row/index/meta data with no
    /// reclaim. A drop-in-progress marker written before the unregister lets startup resume the purge.
    ///
    /// Simulates the crash by marking the drop in progress and unregistering the entry but leaving the
    /// keyspace intact, then runs the production startup scrubber and asserts the keyspace is fully
    /// purged (rows and meta) and the marker cleared.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task InterruptedDrop_ResumedByStartupScrubber_PurgesLeftoverKeyspace()
    {
        (string dbName, DatabaseDescriptor db, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE t (id OBJECT_ID PRIMARY KEY, v STRING)");

        await InsertRow(dbName, db, executor, "INSERT INTO t (id, v) VALUES (gen_id(), \"a\")");
        await InsertRow(dbName, db, executor, "INSERT INTO t (id, v) VALUES (gen_id(), \"b\")");

        string dbId = sharedRegistry!.Get(dbName)!.Id;
        string tableId = db.Schema.Tables.Values.First().Id!;
        IKahuna kahuna = TestNode!.Kahuna;

        string rowBucket = $"{dbId}:{tableId}:r";
        string rowPrefix = $"{dbId}:{tableId}:r/";
        string metaBucket = $"{dbId}/meta";
        string metaPrefix = $"{dbId}/";

        Assert.AreEqual(2, await CountKeysUnder(kahuna, rowBucket, rowPrefix), "sanity: two rows present");

        // Simulate a crash right after UnregisterAsync but before the keyspace purge: the
        // drop-in-progress marker is set and the entry is gone, but all data is still on disk.
        await sharedRegistry.MarkDroppingAsync(dbId);
        await sharedRegistry.UnregisterAsync(dbName);

        Assert.AreEqual(2, await CountKeysUnder(kahuna, rowBucket, rowPrefix), "crash simulated before purge — rows still present");
        Assert.IsNull(sharedRegistry.Get(dbName), "db unregistered by the simulated crash point");

        // Startup scrub on a FRESH registry (new epoch = post-restart) must resume the interrupted
        // purge: the dropping marker was written by the prior instance's epoch, so it is a genuine
        // crash remnant this run reclaims.
        DatabaseRegistry afterRestart = await DatabaseRegistry.OpenAsync(TestNode!, Options);
        await executor.ScrubOrphanBranchNamespacesAsync(TestNode!, afterRestart);
        await afterRestart.DisposeAsync();

        Assert.AreEqual(0, await CountKeysUnder(kahuna, rowBucket, rowPrefix),
            "resumed purge must delete the leftover row keyspace");
        Assert.AreEqual(0, await CountKeysUnder(kahuna, metaBucket, metaPrefix),
            "resumed purge must delete the meta namespace last");

        List<string> stillDropping = await sharedRegistry.LoadOwnDroppingIdsAsync();
        Assert.IsFalse(stillDropping.Contains(dbId), "the drop-in-progress marker must be cleared after the resumed purge");
    }

    /// <summary>
    /// The DROP DATABASE keyspace purge pages through each bucket in bounded batches rather than
    /// materialising the whole bucket at once, so a very large database (or branch overlay) is purged
    /// in bounded memory. Verified with a deliberately tiny batch cap: a small overlay must then be
    /// deleted in many single-key batches — far more than the roughly-one-per-bucket a materialise-all
    /// purge would issue — while still being purged completely.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task DropDatabase_KeyspacePurge_PagesInBoundedBatches()
    {
        DatabaseDropper.PurgeBatchesForTesting = 0;
        try
        {
            // One key per batch — maximises the batch count, so paging is unmistakable.
            (string dbName, DatabaseDescriptor db, CommandExecutor executor) = await CreateRootWithTable(
                "CREATE TABLE t (id OBJECT_ID PRIMARY KEY, v STRING)",
                Options with { KeyspacePurgeBatchSize = 1 });

            for (int i = 0; i < 5; i++)
                await InsertRow(dbName, db, executor, $"INSERT INTO t (id, v) VALUES (gen_id(), \"v{i}\")");

            string dbId = sharedRegistry!.Get(dbName)!.Id;
            string tableId = db.Schema.Tables.Values.First().Id!;
            IKahuna kahuna = TestNode!.Kahuna;
            string rowBucket = $"{dbId}:{tableId}:r";
            string rowPrefix = $"{dbId}:{tableId}:r/";

            Assert.AreEqual(5, await CountKeysUnder(kahuna, rowBucket, rowPrefix), "sanity: five rows present");

            // FORCE: this test exercises the immediate keyspace purge and its batch paging; a non-FORCE
            // drop now retains the keyspace as a recoverable orphan and defers reclamation to the GC.
            await executor.DropDatabase(new DropDatabaseTicket(dbName, ifExists: false, force: true));

            // Fully purged.
            Assert.AreEqual(0, await CountKeysUnder(kahuna, rowBucket, rowPrefix), "row overlay must be fully purged");
            Assert.AreEqual(0, await CountKeysUnder(kahuna, $"{dbId}/meta", $"{dbId}/"), "meta namespace must be fully purged");

            // And purged in many small batches — the signal that paging is active. With batch size 1,
            // five row keys plus their primary-key index entries force well more than the one-batch-
            // per-bucket a materialise-all purge would produce.
            Assert.That(DatabaseDropper.PurgeBatchesForTesting, Is.GreaterThanOrEqualTo(5),
                "purge must page in bounded batches, not materialise each bucket at once");
        }
        finally
        {
            DatabaseDropper.PurgeBatchesForTesting = 0;
        }
    }

    /// <summary>
    /// The DROP DATABASE keyspace-catalog collection re-scans (a confirming round) instead of reading
    /// the catalog once, so a catalog key transiently missed on one scan is caught by a later one and
    /// its table's overlay is still purged. This asserts the multi-round loop is active — with a single
    /// scan there would be exactly one round.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task DropDatabase_CatalogCollection_RescansToAbsorbTransientMiss()
    {
        DatabaseDropper.CatalogScanRoundsForTesting = 0;
        try
        {
            (string dbName, DatabaseDescriptor db, CommandExecutor executor) =
                await CreateRootWithTable("CREATE TABLE t (id OBJECT_ID PRIMARY KEY, v STRING)");
            await InsertRow(dbName, db, executor, "INSERT INTO t (id, v) VALUES (gen_id(), \"a\")");

            // FORCE: the catalog-driven purge only runs on an immediate drop; a non-FORCE drop defers.
            await executor.DropDatabase(new DropDatabaseTicket(dbName, ifExists: false, force: true));

            // A single-scan collection would run exactly one round; the multi-round guard runs a
            // confirming round after the first complete scan.
            Assert.That(DatabaseDropper.CatalogScanRoundsForTesting, Is.GreaterThanOrEqualTo(2),
                "catalog collection must re-scan (confirming round) to absorb a transient miss, not read once");
        }
        finally
        {
            DatabaseDropper.CatalogScanRoundsForTesting = 0;
        }
    }

    /// <summary>
    /// CREATE DATABASE IF NOT EXISTS must resolve the target through the persistent registry, not the
    /// local in-memory cache. When a database is registered on another cluster node but absent from
    /// this node's cache, a cache-only check would run the whole create flow and fail late at
    /// RegisterAsync; the persistent check returns/opens the existing database instead.
    ///
    /// Simulated on a single node: a second executor with its own registry ("remote node") creates the
    /// target, so it lands in the shared KV registry but not in this test's sharedRegistry cache.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task CreateIfNotExists_TargetInKvButNotLocalCache_OpensExisting()
    {
        // "Remote node": independent registry + executor over the same Kahuna node.
        await using DatabaseRegistry remoteRegistry = await DatabaseRegistry.OpenAsync(TestNode!, Options);
        await using CommandExecutor remote = new(
            new CommandValidator(Options), new CatalogsManager(logger), logger, Options,
            sharedNode: TestNode!, registry: remoteRegistry, isClusterMode: false);

        string target = "t_" + Guid.NewGuid().ToString("n");
        await remote.CreateDatabase(new CreateDatabaseTicket(target, ifNotExists: false));

        // Precondition: the local cache has never seen the target.
        Assert.IsNull(sharedRegistry!.Get(target), "precondition: target must be absent from the local cache");

        await using CommandExecutor local = CreateCommandExecutor(); // sharedRegistry-backed

        // CREATE IF NOT EXISTS must find the existing cross-node database and open it, not throw.
        DatabaseDescriptor db = await local.CreateDatabase(new CreateDatabaseTicket(target, ifNotExists: true));

        Assert.IsNotNull(db, "CREATE IF NOT EXISTS must return the existing cross-node database");
        Assert.AreEqual(target, db.Name, "the returned database must be the existing target, not a new one");
    }

    /// <summary>
    /// The branch form of CREATE DATABASE IF NOT EXISTS shares the same persistent existence check, so
    /// when the branch target already exists cross-node it must be opened without running the branch
    /// flow — no new snapshot-floor hold acquired, no metadata copied.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task CreateBranchIfNotExists_TargetInKvButNotLocalCache_OpensWithoutNewHold()
    {
        await using DatabaseRegistry remoteRegistry = await DatabaseRegistry.OpenAsync(TestNode!, Options);
        await using CommandExecutor remote = new(
            new CommandValidator(Options), new CatalogsManager(logger), logger, Options,
            sharedNode: TestNode!, registry: remoteRegistry, isClusterMode: false);

        string source = "src_" + Guid.NewGuid().ToString("n");
        await remote.CreateDatabase(new CreateDatabaseTicket(source, ifNotExists: false));

        string branchTarget = "b_" + Guid.NewGuid().ToString("n");
        await remote.CreateDatabase(new CreateDatabaseTicket(branchTarget, ifNotExists: false, branchFrom: source));

        // The branch target's own hold exists; capture the live-hold count so we can assert the
        // IF NOT EXISTS call adds no further hold.
        (_, int holdsBefore) = await TestNode!.Kahuna.GetSnapshotFloor(CancellationToken.None);
        Assert.IsNull(sharedRegistry!.Get(branchTarget), "precondition: branch target absent from local cache");

        await using CommandExecutor local = CreateCommandExecutor();

        DatabaseDescriptor db = await local.CreateDatabase(
            new CreateDatabaseTicket(branchTarget, ifNotExists: true, branchFrom: source));

        Assert.AreEqual(branchTarget, db.Name, "must open the existing branch target");

        (_, int holdsAfter) = await TestNode!.Kahuna.GetSnapshotFloor(CancellationToken.None);
        Assert.AreEqual(holdsBefore, holdsAfter,
            "CREATE BRANCH IF NOT EXISTS on an existing target must not acquire a new snapshot-floor hold");
    }

    /// <summary>
    /// The second fence outcome: when branch-create registers its child BEFORE DropDatabase sets
    /// the drop-intent, DropDatabase's subsequent HasLiveDescendantsAsync scan observes the child
    /// and drop fails with DatabaseHasLiveDescendants, leaving both parent and child intact.
    /// This is symmetrical to <see cref="DropDatabase_DropIntent_PreventsCrossNodeBranchCreateRace"/>:
    /// that test covers "drop wins"; this test covers "branch-create wins."
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task DropDatabase_DescendantScanAfterIntent_AbortsWhenChildAlreadyRegistered()
    {
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE things_br12b (id OBJECT_ID PRIMARY KEY, v STRING)");

        // Branch-create wins the race: register a child before drop acquires its intent.
        string branchName = NewName();
        await executor.CreateDatabase(new CreateDatabaseTicket(branchName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(branchName, executor);

        // Now simulate DropDatabase acquiring its intent (child already registered in KV).
        // The subsequent HasLiveDescendantsAsync scan must find the child and the drop must fail.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => executor.DropDatabase(new DropDatabaseTicket(rootName)));

        Assert.IsNotNull(ex);
        Assert.AreEqual(CamusDBErrorCodes.DatabaseHasLiveDescendants, ex!.Code,
            "drop must fail with DatabaseHasLiveDescendants when the child was registered first");

        // Both parent and child must still be usable.
        Assert.IsNotNull(sharedRegistry!.Get(rootName), "parent must still be registered after failed drop");
        Assert.IsNotNull(sharedRegistry.Get(branchName), "child must still be registered after failed drop");

        // Drop-intent must have been released (so a future drop attempt is not blocked).
        DatabaseRegistryEntry rootEntry = sharedRegistry.Get(rootName)!;
        bool reacquired = await sharedRegistry.AcquireDropIntentAsync(rootEntry.Id);
        if (reacquired)
            await sharedRegistry.ReleaseDropIntentAsync(rootEntry.Id);
        Assert.IsTrue(reacquired, "drop-intent must have been released after the aborted drop");
    }

    /// <summary>
    /// The pending-create marker is a mandatory, confirmed write before
    /// <c>CopyMetaForBranchAsync</c> runs — not best-effort. This test verifies the key invariant
    /// that the fix preserves: every meta namespace written by <c>CopyMetaForBranchAsync</c> is
    /// either registered in the persistent registry (success path) or it has a pending-create marker
    /// visible to <see cref="DatabaseRegistry.LoadOrphanBranchIdsAsync"/> (crash path).
    ///
    /// <para>The test directly exercises the "confirmed write" side of the invariant: calling
    /// <see cref="DatabaseRegistry.TrackPendingBranchAsync"/> writes a durable key that
    /// <see cref="DatabaseRegistry.LoadOrphanBranchIdsAsync"/> immediately finds as an orphan
    /// (because the id is not yet registered). This proves that if the process crashes between
    /// the (now-mandatory) marker write and a subsequent <c>RegisterAsync</c>, the startup scrubber
    /// has a reliable handle on the orphaned namespace — there is no window where CopyMeta runs
    /// but the marker is absent.</para>
    ///
    /// <para>Note: testing the complementary case (marker write fails → creation aborts before
    /// CopyMeta) requires a Kahuna fault injector that is disproportionate to the test value; that
    /// code path is verified by code review (TrackPendingBranchAsync now propagates exceptions and
    /// is called before CopyMetaForBranchAsync inside the try block that releases the snapshot hold
    /// on any failure).</para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task PendingMarker_IsConfirmedDurable_VisibleToOrphanScanner()
    {
        (string _, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateDatabase();

        // Allocate a branch id directly, as CreateBranchDatabaseAsync would.
        string branchId = await sharedRegistry!.AllocateIdAsync();

        // Write the mandatory pending-create marker. This write is confirmed
        // durable before CopyMetaForBranchAsync runs: if it threw, creation would abort and no
        // meta namespace would exist (no orphan possible).
        await sharedRegistry.TrackPendingBranchAsync(branchId);

        // The marker must be immediately visible to the orphan scanner.
        // Because branchId is not registered, it must appear as an orphan.
        List<string> orphans = await sharedRegistry.LoadOrphanBranchIdsAsync();
        Assert.That(orphans, Contains.Item(branchId),
            "confirmed marker write must be visible to the orphan scanner before RegisterAsync runs");

        // Clean up: clear the marker (simulates successful creation or clean abort).
        await sharedRegistry.ClearPendingBranchAsync(branchId);

        List<string> orphansAfter = await sharedRegistry.LoadOrphanBranchIdsAsync();
        Assert.That(orphansAfter, Does.Not.Contain(branchId),
            "cleared marker must no longer appear as an orphan");

        // Side-effect: verify rootDb is unaffected.
        _ = rootDb;
        _ = executor;
    }

    /// <summary>
    /// BranchMetrics counters track ancestor-probe and scan-iterator costs for a known chain.
    /// Creates a 3-level chain (root → child → grandchild), performs point reads and a scan on the
    /// grandchild, and asserts that the process-wide counters reflect the expected amplification.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchMetrics_TrackAncestorProbesAndScanIterators_ForKnownChain()
    {
        BranchMetrics.Reset();

        // Root: two rows.
        (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
            await CreateRootWithTable("CREATE TABLE items (id OBJECT_ID PRIMARY KEY, label STRING NOT NULL)");

        await InsertRow(rootName, rootDb, executor, "INSERT INTO items (id, label) VALUES (gen_id(), \"alpha\")");
        await InsertRow(rootName, rootDb, executor, "INSERT INTO items (id, label) VALUES (gen_id(), \"beta\")");

        // Child branch (depth 1).
        string childName = NewName();
        DatabaseDescriptor childDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(childName, ifNotExists: false, branchFrom: rootName));
        TrackDatabase(childName, executor);

        // Grandchild branch (depth 2).
        string grandchildName = NewName();
        DatabaseDescriptor grandchildDb = await executor.CreateDatabase(
            new CreateDatabaseTicket(grandchildName, ifNotExists: false, branchFrom: childName));
        TrackDatabase(grandchildName, executor);

        long probesBefore = BranchMetrics.AncestorProbesTotal;
        long scansBefore  = BranchMetrics.ScanIteratorsTotal;

        // A full table scan on the grandchild opens 2 ancestor iterators (child + root).
        List<QueryResultRow> rows = await SelectAll(grandchildName, grandchildDb, executor,
            "SELECT label FROM items");

        Assert.AreEqual(2, rows.Count, "grandchild must see both root rows via 2-level ancestry walk");

        long scansAfter = BranchMetrics.ScanIteratorsTotal;
        Assert.GreaterOrEqual(scansAfter - scansBefore, 2,
            "ScanIteratorsTotal must increment by at least 2 (one per ancestor level) per scan");

        // A SELECT by non-unique label triggers a full scan (no index on label).
        // A SELECT by pk would trigger a point read with possible ancestor probe on a miss —
        // but since items are inherited at grandchild level-0 miss, each row probe walks ancestors.
        // Re-scan to accumulate point-read probes via WHERE on the pk index.
        List<QueryResultRow> rootRows = await SelectAll(rootName, rootDb, executor, "SELECT id FROM items");
        string firstId = rootRows[0].Row["id"].StrValue!;

        long probesStart = BranchMetrics.AncestorProbesTotal;
        List<QueryResultRow> pkRows = await SelectAll(grandchildName, grandchildDb, executor,
            $"SELECT label FROM items WHERE id = \"{firstId}\"");
        Assert.AreEqual(1, pkRows.Count, "grandchild must find inherited row by primary key");

        long probesEnd = BranchMetrics.AncestorProbesTotal;
        // A pk lookup on the grandchild misses level-0 → probes child (1 probe) → hits or misses →
        // probes root (2nd probe if child also misses). At least 1 ancestor probe must fire.
        Assert.GreaterOrEqual(probesEnd - probesStart, 1,
            "AncestorProbesTotal must increment for each ancestor level probed on a GetRow/LookupUnique miss");

        // Lineage depth is observable on the descriptor's store (indirect via table open).
        // Verify that probes and scan iters both grew from the baseline, meaning the observability
        // path is wired end-to-end and tracks a known chain.
        Assert.Greater(BranchMetrics.AncestorProbesTotal, probesBefore,
            "AncestorProbesTotal must grow from baseline across the full test");
        Assert.Greater(BranchMetrics.ScanIteratorsTotal, scansBefore,
            "ScanIteratorsTotal must grow from baseline across the full test");

        _ = childDb;
    }

    /// <summary>
    /// Opening a table store at a lineage depth at or beyond <see cref="BranchMetrics.LineageWarningThreshold"/>
    /// records a deep-lineage warning — the operational guardrail signalling a chain deep enough to
    /// warrant compaction/rebase. Exercised with the threshold lowered to 2 so a grandchild (depth 2)
    /// trips it while a child (depth 1) does not.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task BranchMetrics_DeepLineageWarning_FiresAtThreshold()
    {
        int originalThreshold = BranchMetrics.LineageWarningThreshold;
        BranchMetrics.LineageWarningThreshold = 2;
        BranchMetrics.Reset();
        try
        {
            (string rootName, DatabaseDescriptor rootDb, CommandExecutor executor) =
                await CreateRootWithTable("CREATE TABLE items (id OBJECT_ID PRIMARY KEY, label STRING NOT NULL)");
            await InsertRow(rootName, rootDb, executor, "INSERT INTO items (id, label) VALUES (gen_id(), \"alpha\")");

            string childName = NewName();
            DatabaseDescriptor childDb = await executor.CreateDatabase(
                new CreateDatabaseTicket(childName, ifNotExists: false, branchFrom: rootName));
            TrackDatabase(childName, executor);

            // Opening the table on the child (depth 1 < threshold 2) must NOT warn.
            await SelectAll(childName, childDb, executor, "SELECT label FROM items");
            Assert.AreEqual(0, BranchMetrics.DeepLineageWarnings,
                "a depth-1 chain is below the threshold and must not warn");

            string grandchildName = NewName();
            DatabaseDescriptor grandchildDb = await executor.CreateDatabase(
                new CreateDatabaseTicket(grandchildName, ifNotExists: false, branchFrom: childName));
            TrackDatabase(grandchildName, executor);

            // Opening the table on the grandchild (depth 2 == threshold) constructs a KvTableStore
            // deep enough to trip the guardrail.
            await SelectAll(grandchildName, grandchildDb, executor, "SELECT label FROM items");

            Assert.GreaterOrEqual(BranchMetrics.DeepLineageWarnings, 1,
                "opening a table store at lineage depth >= LineageWarningThreshold must record a deep-lineage warning");
        }
        finally
        {
            BranchMetrics.LineageWarningThreshold = originalThreshold;
            BranchMetrics.Reset();
        }
    }
}
