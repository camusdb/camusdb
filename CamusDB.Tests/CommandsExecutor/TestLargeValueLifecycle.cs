/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Large values across the rest of a table's life: the per-column storage strategy in DDL, the explicit
/// storage rewrite and its reverse, time travel across a rewrite, branch ancestry, drop and relink, the
/// keyspace purge, and the mutation count of out-of-line values.
/// </summary>
[NonParallelizable]
internal sealed class TestLargeValueLifecycle : SharedNodeBaseTest
{
    private static readonly Random Rng = new(4242);

    private static string Incompressible(int length)
    {
        const string alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        char[] chars = new char[length];
        lock (Rng)
        {
            for (int i = 0; i < length; i++)
                chars[i] = alphabet[Rng.Next(alphabet.Length)];
        }
        return new string(chars);
    }

    private static Dictionary<string, ColumnValue> Body(string body) => new() { { "@body", new ColumnValue(ColumnType.String, body) } };

    private static async Task Ddl(CommandExecutor executor, string db, string sql)
        => await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db, sql, null));

    private static async Task Dml(CommandExecutor executor, string db, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Select(CommandExecutor executor, string db, string sql)
    {
        KvTransaction tx = KvTransaction.CreateReadOnly();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
        return await cursor.ToListAsync();
    }

    private static string? Text(QueryResultRow row, string column)
        => row.Row.TryGetValue(column, out ColumnValue? value) && value.Type != ColumnType.Null ? value.StrValue : null;

    private async Task<List<(string key, ReadOnlyKeyValueEntry entry)>> ScanAsync(string bucket)
    {
        List<(string, ReadOnlyKeyValueEntry)> keys = [];
        await foreach ((string key, ReadOnlyKeyValueEntry entry) in SharedKahuna.LocateAndScanRange(
            HLCTimestamp.Zero, bucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(bucket + "/", StringComparison.Ordinal) && entry.Value is { Length: > 1 } && entry.Value[0] == (byte)BranchKvKind.Value)
                keys.Add((key, entry));
        }
        return keys;
    }

    private Task<List<(string key, ReadOnlyKeyValueEntry entry)>> LargeValueKeys(DatabaseDescriptor database, string table)
        => ScanAsync($"{database.Id}:{database.Schema.Tables[table].EffectiveStorageId}|v");

    private Task<List<(string key, ReadOnlyKeyValueEntry entry)>> RowKeys(DatabaseDescriptor database, string table)
        => ScanAsync($"{database.Id}:{database.Schema.Tables[table].EffectiveStorageId}|r");

    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> NewDatabase(CamusDBOptions? options = null)
    {
        string db = "db_" + Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor(options ?? Options);
        TrackDatabase(db, executor);
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(db, ifNotExists: false));
        return (db, database, executor);
    }

    // ── Storage strategy DDL ────────────────────────────────────────────────

    [Test]
    public async Task CreateTable_WithStorageClauses_PersistsAndRendersThem()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await NewDatabase();

        await Ddl(executor, db,
            "CREATE TABLE docs (id object_id PRIMARY KEY, body string STORAGE external, embedding bytes STORAGE PLAIN, notes string NOT NULL STORAGE main COMMENT 'n')");

        List<TableColumnSchema> columns = database.Schema.Tables["docs"].Columns!;
        Assert.AreEqual(ColumnStorageStrategy.External, columns.Single(c => c.Name == "body").Storage);
        Assert.AreEqual(ColumnStorageStrategy.Plain, columns.Single(c => c.Name == "embedding").Storage);
        Assert.AreEqual(ColumnStorageStrategy.Main, columns.Single(c => c.Name == "notes").Storage);
        Assert.IsNull(columns.Single(c => c.Name == "id").Storage);

        string ddl = Text((await Select(executor, db, "SHOW CREATE TABLE docs")).Single(), "Create Table")
                     ?? (await Select(executor, db, "SHOW CREATE TABLE docs")).Single().Row.Values.Select(v => v.StrValue).First(v => v?.StartsWith("CREATE") == true)!;
        StringAssert.Contains("`BODY` STRING", ddl.ToUpperInvariant());
        StringAssert.Contains("STORAGE EXTERNAL", ddl);
        StringAssert.Contains("STORAGE PLAIN", ddl);
        StringAssert.Contains("STORAGE MAIN", ddl);

        // The rendered DDL creates the same strategies again.
        (string db2, DatabaseDescriptor database2, CommandExecutor executor2) = await NewDatabase();
        await Ddl(executor2, db2, ddl);
        List<TableColumnSchema> copy = database2.Schema.Tables["docs"].Columns!;
        Assert.AreEqual(ColumnStorageStrategy.External, copy.Single(c => c.Name == "body").Storage);
        Assert.AreEqual(ColumnStorageStrategy.Main, copy.Single(c => c.Name == "notes").Storage);
    }

    [Test]
    public async Task SetStorage_IsRenderedByShowCreateTable()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await NewDatabase();

        await Ddl(executor, db, "CREATE TABLE docs (id object_id PRIMARY KEY, body string, notes string STORAGE plain)");

        // A column that never received a strategy renders no clause: the reader default is EXTENDED,
        // so an unrendered column re-creates identically.
        string before = await ShowCreateTable(executor, db);
        StringAssert.Contains("`body` STRING NULL,", before);
        StringAssert.DoesNotContain("`body` STRING NULL STORAGE", before);

        // An explicit EXTENDED is rendered although it is the default, because the user asked for it.
        await Ddl(executor, db, "ALTER TABLE docs ALTER COLUMN body SET STORAGE EXTENDED");
        Assert.AreEqual(ColumnStorageStrategy.Extended, database.Schema.Tables["docs"].Columns!.Single(c => c.Name == "body").Storage);
        StringAssert.Contains("`body` STRING NULL STORAGE EXTENDED", await ShowCreateTable(executor, db));

        // A later ALTER replaces the rendered strategy instead of adding a second clause.
        await Ddl(executor, db, "ALTER TABLE docs ALTER COLUMN notes SET STORAGE main");
        string after = await ShowCreateTable(executor, db);
        StringAssert.Contains("`notes` STRING NULL STORAGE MAIN", after);
        StringAssert.DoesNotContain("STORAGE PLAIN", after);

        // The rendered DDL re-creates both altered strategies.
        (string copyDb, DatabaseDescriptor copyDatabase, CommandExecutor copyExecutor) = await NewDatabase();
        await Ddl(copyExecutor, copyDb, after);
        List<TableColumnSchema> copy = copyDatabase.Schema.Tables["docs"].Columns!;
        Assert.AreEqual(ColumnStorageStrategy.Extended, copy.Single(c => c.Name == "body").Storage);
        Assert.AreEqual(ColumnStorageStrategy.Main, copy.Single(c => c.Name == "notes").Storage);
    }

    /// <summary>Returns the single DDL string of <c>SHOW CREATE TABLE docs</c>.</summary>
    private static async Task<string> ShowCreateTable(CommandExecutor executor, string db)
    {
        QueryResultRow row = (await Select(executor, db, "SHOW CREATE TABLE docs")).Single();
        return Text(row, "Create Table")
               ?? row.Row.Values.Select(v => v.StrValue).First(v => v?.StartsWith("CREATE") == true)!;
    }

    [Test]
    public async Task StorageStrategy_OnAFixedWidthColumn_IsRejected()
    {
        (string db, _, CommandExecutor executor) = await NewDatabase();

        CamusDBException create = Assert.ThrowsAsync<CamusDBException>(() =>
            Ddl(executor, db, "CREATE TABLE t (id object_id PRIMARY KEY, n int64 STORAGE external)"))!;
        Assert.AreEqual(CamusDBErrorCodes.ColumnStorageNotApplicable, create.Code);

        await Ddl(executor, db, "CREATE TABLE t (id object_id PRIMARY KEY, n int64, s string)");

        CamusDBException alter = Assert.ThrowsAsync<CamusDBException>(() =>
            Ddl(executor, db, "ALTER TABLE t ALTER COLUMN n SET STORAGE plain"))!;
        Assert.AreEqual(CamusDBErrorCodes.ColumnStorageNotApplicable, alter.Code);

        CamusDBException unknown = Assert.ThrowsAsync<CamusDBException>(() =>
            Ddl(executor, db, "ALTER TABLE t ALTER s SET STORAGE compressed"))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, unknown.Code);
    }

    [Test]
    public async Task StorageStillWorksAsAnIdentifier()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await NewDatabase();

        await Ddl(executor, db, "CREATE TABLE storage (id object_id PRIMARY KEY, storage string STORAGE plain)");
        await Dml(executor, db, "INSERT INTO storage (id, storage) VALUES (gen_id(), 'x')");
        Assert.AreEqual("x", Text((await Select(executor, db, "SELECT storage FROM storage")).Single(), "storage"));
    }

    [Test]
    public async Task SetStorage_ChangesFutureWritesOnly_AndNeverBreaksExistingRows()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await NewDatabase();
        await Ddl(executor, db, "CREATE TABLE docs (id object_id PRIMARY KEY, title string, body string)");

        string first = Incompressible(5000);
        await Dml(executor, db, "INSERT INTO docs (id, title, body) VALUES (gen_id(), 'first', @body)", Body(first));
        List<(string key, ReadOnlyKeyValueEntry entry)> before = await RowKeys(database, "docs");
        Assert.AreEqual(1, (await LargeValueKeys(database, "docs")).Count);

        await Ddl(executor, db, "ALTER TABLE docs ALTER COLUMN body SET STORAGE PLAIN");
        Assert.AreEqual(ColumnStorageStrategy.Plain, database.Schema.Tables["docs"].Columns!.Single(c => c.Name == "body").Storage);

        List<(string key, ReadOnlyKeyValueEntry entry)> after = await RowKeys(database, "docs");
        Assert.AreEqual(before.Single().entry.Revision, after.Single().entry.Revision, "SET STORAGE must not rewrite a stored row");
        Assert.AreEqual(1, (await LargeValueKeys(database, "docs")).Count, "SET STORAGE must not touch an out-of-line value");

        string second = Incompressible(5000);
        await Dml(executor, db, "INSERT INTO docs (id, title, body) VALUES (gen_id(), 'second', @body)", Body(second));
        Assert.AreEqual(1, (await LargeValueKeys(database, "docs")).Count, "a PLAIN column keeps a new value inline");

        List<QueryResultRow> rows = await Select(executor, db, "SELECT title, body FROM docs");
        Assert.AreEqual(first, Text(rows.Single(r => Text(r, "title") == "first"), "body"));
        Assert.AreEqual(second, Text(rows.Single(r => Text(r, "title") == "second"), "body"));
    }

    // ── Explicit rewrite ────────────────────────────────────────────────────

    [Test]
    public async Task RewriteStorage_ConvertsOldRows_KeepsVersionAndIndexes_AndIsIdempotent()
    {
        CamusDBOptions inlineOptions = Options with { LargeValueThresholdBytes = 0, LargeValueCompressionEnabled = false };
        (string db, DatabaseDescriptor database, CommandExecutor inlineEngine) = await NewDatabase(inlineOptions);

        await Ddl(inlineEngine, db, "CREATE TABLE docs (id object_id PRIMARY KEY, title string, body string, n int64)");
        await Ddl(inlineEngine, db, "ALTER TABLE docs ADD INDEX title_idx (title)");

        Dictionary<string, string> bodies = [];
        for (int i = 0; i < 25; i++)
        {
            string body = i % 3 == 0 ? Incompressible(3000 + i) : i % 3 == 1 ? string.Concat(Enumerable.Repeat("compressible ", 300)) : "tiny " + i;
            bodies["t" + i] = body;
            await Dml(inlineEngine, db, $"INSERT INTO docs (id, title, body, n) VALUES (gen_id(), 't{i}', @body, {i})", Body(body));
        }

        string indexBucket = $"{database.Id}:{database.Schema.Tables["docs"].EffectiveStorageId}|i:{database.Schema.Tables["docs"].Indexes!.Single(ix => ix.Name == "title_idx").KvId}";
        List<(string key, ReadOnlyKeyValueEntry entry)> indexBefore = await ScanAsync(indexBucket);
        Assert.AreEqual(0, (await LargeValueKeys(database, "docs")).Count);

        int versionBefore = database.Schema.Tables["docs"].Version;

        CommandExecutor largeValueEngine = CreateCommandExecutor(Options with { LargeValueRewriteBatchRows = 7 });
        await Ddl(largeValueEngine, db, "ALTER TABLE docs REWRITE STORAGE");

        DatabaseDescriptor reopened = await largeValueEngine.OpenDatabase(db);
        Assert.AreEqual(9, (await LargeValueKeys(reopened, "docs")).Count, "every incompressible value above the threshold moved out of line");
        Assert.AreEqual(versionBefore, reopened.Schema.Tables["docs"].Version, "a rewrite must not bump the schema version");

        foreach ((string key, ReadOnlyKeyValueEntry entry) in await RowKeys(reopened, "docs"))
            Assert.AreEqual(versionBefore, RowStorageForms.StoredVersion(BinaryPrimitives.ReadUInt32LittleEndian(entry.Value.AsSpan(1))));

        List<(string key, ReadOnlyKeyValueEntry entry)> indexAfter = await ScanAsync(indexBucket);
        CollectionAssert.AreEqual(indexBefore.Select(k => (k.key, k.entry.Revision)), indexAfter.Select(k => (k.key, k.entry.Revision)), "a rewrite must not touch index entries");

        foreach (CommandExecutor engine in new[] { largeValueEngine, inlineEngine })
        {
            List<QueryResultRow> rows = await Select(engine, db, "SELECT title, body FROM docs");
            Assert.AreEqual(25, rows.Count);
            foreach (QueryResultRow row in rows)
                Assert.AreEqual(bodies[Text(row, "title")!], Text(row, "body"));
        }

        Assert.AreEqual(1, (await Select(largeValueEngine, db, "SELECT body FROM docs WHERE title = 't3'")).Count, "the index still finds the row");

        // A second run writes nothing.
        List<(string key, long revision)> rowsAfterFirst = (await RowKeys(reopened, "docs")).Select(k => (k.key, k.entry.Revision)).ToList();
        await Ddl(largeValueEngine, db, "ALTER TABLE docs REWRITE STORAGE");
        List<(string key, long revision)> rowsAfterSecond = (await RowKeys(reopened, "docs")).Select(k => (k.key, k.entry.Revision)).ToList();
        CollectionAssert.AreEqual(rowsAfterFirst, rowsAfterSecond, "re-running the rewrite over a converted table must write nothing");

        // The reverse mode returns every row to inline, uncompressed form.
        await Ddl(largeValueEngine, db, "ALTER TABLE docs REWRITE STORAGE INLINE");
        Assert.AreEqual(0, (await LargeValueKeys(reopened, "docs")).Count);
        foreach ((string _, ReadOnlyKeyValueEntry entry) in await RowKeys(reopened, "docs"))
            Assert.IsFalse(RowStorageForms.HasTrailer(entry.Value.AsSpan(1)), "an inline row carries no storage-form trailer");

        List<QueryResultRow> finalRows = await Select(inlineEngine, db, "SELECT title, body FROM docs");
        foreach (QueryResultRow row in finalRows)
            Assert.AreEqual(bodies[Text(row, "title")!], Text(row, "body"));

        // The progress cursor is removed when a run completes.
        (KeyValueResponseType cursorType, _) = await SharedKahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, $"{reopened.Id}/meta/storagerewrite:{reopened.Schema.Tables["docs"].EffectiveStorageId}", -1,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None);
        Assert.AreNotEqual(KeyValueResponseType.Get, cursorType);
    }

    [Test]
    public async Task HistoricalRead_BeforeARewrite_ReturnsTheSameValues()
    {
        CamusDBOptions inlineOptions = Options with { LargeValueThresholdBytes = 0, LargeValueCompressionEnabled = false };
        (string db, DatabaseDescriptor database, CommandExecutor inlineEngine) = await NewDatabase(inlineOptions);
        await Ddl(inlineEngine, db, "CREATE TABLE docs (id object_id PRIMARY KEY, title string, body string)");

        string body = Incompressible(6000);
        await Dml(inlineEngine, db, "INSERT INTO docs (id, title, body) VALUES (gen_id(), 'a', @body)", Body(body));

        await Task.Delay(60);
        long snapshotMs = SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(SharedNode.Raft.GetLocalNodeId()).L;
        await Task.Delay(60);

        CommandExecutor largeValueEngine = CreateCommandExecutor(Options);
        await Ddl(largeValueEngine, db, "ALTER TABLE docs REWRITE STORAGE");
        DatabaseDescriptor reopened = await largeValueEngine.OpenDatabase(db);
        Assert.AreEqual(1, (await LargeValueKeys(reopened, "docs")).Count);

        List<QueryResultRow> historical = await Select(largeValueEngine, db, $"SELECT body FROM docs AS OF SYSTEM TIME {snapshotMs}");
        Assert.AreEqual(body, Text(historical.Single(), "body"));
        Assert.AreEqual(body, Text((await Select(largeValueEngine, db, "SELECT body FROM docs")).Single(), "body"));
    }

    // ── Branch databases ────────────────────────────────────────────────────

    private static async Task<(string name, DatabaseDescriptor branch)> Fork(CommandExecutor executor, string parent)
    {
        string name = "db_" + Guid.NewGuid().ToString("n");
        DatabaseDescriptor branch = (await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            null!, parent, $"CREATE DATABASE {name} BRANCH FROM {parent}", null))).Database!;
        return (name, branch);
    }

    [Test]
    public async Task Branch_ReadsInheritedValues_AndItsWritesNeverReachTheParent()
    {
        (string root, DatabaseDescriptor rootDb, CommandExecutor executor) = await NewDatabase();
        await Ddl(executor, root, "CREATE TABLE docs (id object_id PRIMARY KEY, title string, body string)");

        string a = Incompressible(5000), b = Incompressible(5000);
        await Dml(executor, root, "INSERT INTO docs (id, title, body) VALUES (gen_id(), 'a', @body)", Body(a));
        await Dml(executor, root, "INSERT INTO docs (id, title, body) VALUES (gen_id(), 'b', @body)", Body(b));
        List<(string key, ReadOnlyKeyValueEntry entry)> parentValues = await LargeValueKeys(rootDb, "docs");
        Assert.AreEqual(2, parentValues.Count);

        (string branchName, DatabaseDescriptor branch) = await Fork(executor, root);
        TrackDatabase(branchName, executor);

        List<QueryResultRow> inherited = await Select(executor, branchName, "SELECT title, body FROM docs");
        Assert.AreEqual(a, Text(inherited.Single(r => Text(r, "title") == "a"), "body"));
        Assert.AreEqual(b, Text(inherited.Single(r => Text(r, "title") == "b"), "body"));

        // A small-column update on the branch carries the inherited pointer: no value is written anywhere.
        await Dml(executor, branchName, "UPDATE docs SET title = 'a2' WHERE title = 'a'");
        Assert.AreEqual(0, (await LargeValueKeys(branch, "docs")).Count, "the branch holds no value of its own");
        Assert.AreEqual(a, Text((await Select(executor, branchName, "SELECT body FROM docs WHERE title = 'a2'")).Single(), "body"));

        // Overwriting a large value on the branch writes into the branch only.
        string b2 = Incompressible(5000);
        await Dml(executor, branchName, "UPDATE docs SET body = @body WHERE title = 'b'", Body(b2));
        Assert.AreEqual(b2, Text((await Select(executor, branchName, "SELECT body FROM docs WHERE title = 'b'")).Single(), "body"));
        Assert.AreEqual(b, Text((await Select(executor, root, "SELECT body FROM docs WHERE title = 'b'")).Single(), "body"));

        // Deleting on the branch hides the row there and leaves the parent intact.
        await Dml(executor, branchName, "DELETE FROM docs WHERE title = 'b'");
        Assert.AreEqual(1, (await Select(executor, branchName, "SELECT body FROM docs")).Count);

        List<(string key, ReadOnlyKeyValueEntry entry)> parentAfter = await LargeValueKeys(rootDb, "docs");
        CollectionAssert.AreEqual(parentValues.Select(k => (k.key, k.entry.Revision)), parentAfter.Select(k => (k.key, k.entry.Revision)), "no branch write may reach the parent's values");
        Assert.AreEqual(2, (await Select(executor, root, "SELECT body FROM docs")).Count);

        // A branch of the branch reads through two ancestry levels.
        (string grandchild, DatabaseDescriptor _) = await Fork(executor, branchName);
        TrackDatabase(grandchild, executor);
        Assert.AreEqual(a, Text((await Select(executor, grandchild, "SELECT body FROM docs WHERE title = 'a2'")).Single(), "body"));
    }

    [Test]
    public async Task RewriteOfTheParent_AfterAFork_DoesNotDisturbTheBranch()
    {
        CamusDBOptions inlineOptions = Options with { LargeValueThresholdBytes = 0, LargeValueCompressionEnabled = false };
        (string root, DatabaseDescriptor _, CommandExecutor inlineEngine) = await NewDatabase(inlineOptions);
        await Ddl(inlineEngine, root, "CREATE TABLE docs (id object_id PRIMARY KEY, title string, body string)");

        string body = Incompressible(7000);
        await Dml(inlineEngine, root, "INSERT INTO docs (id, title, body) VALUES (gen_id(), 'a', @body)", Body(body));

        (string branchName, DatabaseDescriptor _) = await Fork(inlineEngine, root);
        TrackDatabase(branchName, inlineEngine);

        CommandExecutor largeValueEngine = CreateCommandExecutor(Options);
        await Ddl(largeValueEngine, root, "ALTER TABLE docs REWRITE STORAGE");

        Assert.AreEqual(body, Text((await Select(largeValueEngine, root, "SELECT body FROM docs")).Single(), "body"));
        Assert.AreEqual(body, Text((await Select(inlineEngine, branchName, "SELECT body FROM docs")).Single(), "body"));
    }

    // ── Drop, relink and purge ──────────────────────────────────────────────

    [Test]
    public async Task DeferredDrop_ThenRelink_ReadsEveryValue_AndThePurgeRemovesThem()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await NewDatabase();
        await Ddl(executor, db, "CREATE TABLE docs (id object_id PRIMARY KEY, title string, body string)");

        string body = Incompressible(5000);
        await Dml(executor, db, "INSERT INTO docs (id, title, body) VALUES (gen_id(), 'a', @body)", Body(body));
        string tableId = database.Schema.Tables["docs"].Id!;

        await executor.DropTable(new DropTableTicket(db, "docs", ifExists: false));
        await Ddl(executor, db, $"CREATE TABLE docs_back RELINK TO \"{tableId}\"");
        Assert.AreEqual(body, Text((await Select(executor, db, "SELECT body FROM docs_back")).Single(), "body"));

        await executor.DropTable(new DropTableTicket(db, "docs_back", ifExists: false));
        Assert.AreEqual(1, (await ScanAsync($"{database.Id}:{tableId}|v")).Count, "a deferred drop keeps the values for a later relink");

        DatabaseDropper dropper = new(new DatabaseDescriptors(), logger, Options);
        Assert.IsTrue(await dropper.PurgeTableKeyspaceAsync(SharedKahuna, database.Id, tableId));
        Assert.AreEqual(0, (await ScanAsync($"{database.Id}:{tableId}|v")).Count, "the purge must reach the large-value key space");
        Assert.AreEqual(0, (await ScanAsync($"{database.Id}:{tableId}|r")).Count);
    }

    // ── Mutation accounting ─────────────────────────────────────────────────

    [Test]
    public async Task EachOutOfLineValue_CountsAsOneMutation()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await NewDatabase(Options with { MaxMutationsPerTransaction = 10 });
        await Ddl(executor, db, "CREATE TABLE docs (id object_id PRIMARY KEY, a string, b string)");

        Dictionary<string, ColumnValue> twoValues = new()
        {
            { "@a", new ColumnValue(ColumnType.String, Incompressible(3000)) },
            { "@b", new ColumnValue(ColumnType.String, Incompressible(3000)) },
        };

        // A row costs its row key, its primary-key index entry and one mutation per out-of-line value:
        // 1 + 1 + 2 = 4. Two rows cost 8, within the limit of 10.
        KvTransaction tx = await database.Transactions.BeginAsync();
        for (int i = 0; i < 2; i++)
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO docs (id, a, b) VALUES (gen_id(), @a, @b)", twoValues));

        // The third row would reach 12.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() =>
            executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, "INSERT INTO docs (id, a, b) VALUES (gen_id(), @a, @b)", twoValues)))!;
        Assert.AreEqual(CamusDBErrorCodes.TransactionMutationLimitExceeded, ex.Code);
        await database.Transactions.RollbackIfNotCompletedAsync(tx);

        Assert.AreEqual(0, (await Select(executor, db, "SELECT a FROM docs")).Count);
        Assert.AreEqual(0, (await LargeValueKeys(database, "docs")).Count, "an aborted insert leaves no out-of-line value behind");
    }

    // ── Read paths beyond the full scan ─────────────────────────────────────

    [Test]
    public async Task IndexLookups_AndJoins_ReturnLargeValues()
    {
        (string db, DatabaseDescriptor _, CommandExecutor executor) = await NewDatabase();
        await Ddl(executor, db, "CREATE TABLE docs (id object_id PRIMARY KEY, title string UNIQUE, body string)");
        await Ddl(executor, db, "CREATE TABLE tags (id object_id PRIMARY KEY, title string, tag string)");

        Dictionary<string, string> bodies = [];
        for (int i = 0; i < 12; i++)
        {
            string body = Incompressible(2500 + i);
            bodies["d" + i] = body;
            await Dml(executor, db, $"INSERT INTO docs (id, title, body) VALUES (gen_id(), 'd{i}', @body)", Body(body));
            await Dml(executor, db, $"INSERT INTO tags (id, title, tag) VALUES (gen_id(), 'd{i}', 'tag{i % 3}')");
        }

        Assert.AreEqual(bodies["d7"], Text((await Select(executor, db, "SELECT body FROM docs WHERE title = 'd7'")).Single(), "body"));

        List<QueryResultRow> joined = await Select(executor, db,
            "SELECT tags.tag, docs.body FROM tags INNER JOIN docs ON tags.title = docs.title WHERE tags.tag = 'tag1'");
        Assert.AreEqual(4, joined.Count);
        foreach (QueryResultRow row in joined)
            Assert.IsTrue(bodies.Values.Contains(row.Row.First(kv => kv.Key.EndsWith("body", StringComparison.OrdinalIgnoreCase)).Value.StrValue));
    }
}
