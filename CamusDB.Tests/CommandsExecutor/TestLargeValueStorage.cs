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

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end behaviour of compressed and out-of-line column values through SQL: a value above the
/// threshold lives under its own key, a compressible value stays inline in compressed form, results
/// never change, a query that does not name the large column never fetches it, an update of a small
/// column leaves the large value's key untouched, and a delete removes every key the row points at.
/// </summary>
[NonParallelizable]
internal sealed class TestLargeValueStorage : SharedNodeBaseTest
{
    private static readonly Random Rng = new(20260917);

    /// <summary>A string LZ4 cannot shrink: random letters and digits.</summary>
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

    private static async Task<List<QueryResultRow>> QueryAsync(CommandExecutor executor, string db, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));

            List<QueryResultRow> rows = [];
            await foreach (QueryResultRow row in cursor)
                rows.Add(row);

            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task NonQueryAsync(CommandExecutor executor, string db, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        if (sql.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase) || sql.StartsWith("ALTER", StringComparison.OrdinalIgnoreCase))
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(null!, db, sql, parameters));
            return;
        }

        if (sql.StartsWith("SET CLUSTER", StringComparison.OrdinalIgnoreCase) || sql.StartsWith("RESET CLUSTER", StringComparison.OrdinalIgnoreCase))
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(null!, db, sql, parameters));
            return;
        }

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

    private async Task<List<(string key, ReadOnlyKeyValueEntry entry)>> LargeValueKeysAsync(DatabaseDescriptor database, string table)
    {
        string tableId = database.Schema.Tables[table].EffectiveStorageId;
        string bucket = $"{database.Id}:{tableId}|v";

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

    private static string? Text(QueryResultRow row, string column)
        => row.Row.TryGetValue(column, out ColumnValue? value) && value.Type != ColumnType.Null ? value.StrValue : null;

    private async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> CreateDocsAsync(CamusDBOptions? options = null)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = options is null
            ? await CreateDatabase()
            : await CreateDatabase(options);

        await NonQueryAsync(executor, db, "CREATE TABLE docs (id object_id PRIMARY KEY, title string, body string)");
        return (db, database, executor);
    }

    private static Dictionary<string, ColumnValue> Params(string title, string body) => new()
    {
        { "@title", new ColumnValue(ColumnType.String, title) },
        { "@body", new ColumnValue(ColumnType.String, body) },
    };

    [Test]
    public async Task LargeIncompressibleValue_MovesOutOfLine_AndReadsBackExactly()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDocsAsync();
        string body = Incompressible(6000);

        await NonQueryAsync(executor, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), @title, @body)", Params("t1", body));

        List<(string key, ReadOnlyKeyValueEntry entry)> keys = await LargeValueKeysAsync(database, "docs");
        Assert.AreEqual(1, keys.Count, "a 6 KB value that does not compress must live under its own key");

        QueryResultRow row = (await QueryAsync(executor, db, "SELECT * FROM docs")).Single();
        Assert.AreEqual(body, Text(row, "body"));
        Assert.AreEqual("t1", Text(row, "title"));
    }

    [Test]
    public async Task CompressibleValue_StaysInline_AndReadsBackExactly()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDocsAsync();
        string body = string.Concat(Enumerable.Repeat("the same sentence over and over. ", 600));

        await NonQueryAsync(executor, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), @title, @body)", Params("t1", body));

        Assert.AreEqual(0, (await LargeValueKeysAsync(database, "docs")).Count, "a value that compresses below the threshold stays inline");

        QueryResultRow row = (await QueryAsync(executor, db, "SELECT body FROM docs")).Single();
        Assert.AreEqual(body, Text(row, "body"));
    }

    [Test]
    public async Task QueryThatDoesNotNameTheLargeColumn_FetchesNoLargeValue()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDocsAsync();

        for (int i = 0; i < 20; i++)
            await NonQueryAsync(executor, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), @title, @body)", Params("t" + i, Incompressible(4000)));

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(db, "docs"));

        long before = table.Store.LargeValueFetchCalls;
        List<QueryResultRow> titles = await QueryAsync(executor, db, "SELECT title FROM docs WHERE title <> 'x'");
        Assert.AreEqual(20, titles.Count);
        Assert.AreEqual(before, table.Store.LargeValueFetchCalls, "a query that never reads body must not fetch it");

        List<QueryResultRow> bodies = await QueryAsync(executor, db, "SELECT body FROM docs");
        Assert.AreEqual(20, bodies.Count);
        Assert.IsTrue(bodies.All(r => Text(r, "body")!.Length == 4000));
        // The first window holds 16 rows and the next one 32, so twenty values cost two batched fetches, not twenty.
        Assert.AreEqual(2, table.Store.LargeValueFetchCalls - before, "values are fetched per scan window, never per row");

        List<QueryResultRow> all = await QueryAsync(executor, db, "SELECT * FROM docs");
        Assert.IsTrue(all.All(r => Text(r, "body")!.Length == 4000), "SELECT * must resolve every value, never a placeholder");
    }

    [Test]
    public async Task ScanAndBatchRead_CloseOnTheDecodedByteBound_WithoutChangingResults()
    {
        // Each body is a 4000-byte out-of-line value, so a bound of 10,000 bytes fits two rows at a time.
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDocsAsync(Options with { LargeValueResolveBatchBytes = 10_000 });

        Dictionary<string, string> bodies = [];
        for (int i = 0; i < 20; i++)
        {
            bodies["t" + i] = Incompressible(4000);
            await NonQueryAsync(executor, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), @title, @body)", Params("t" + i, bodies["t" + i]));
        }

        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(db, "docs"));

        long before = table.Store.LargeValueFetchCalls;
        List<QueryResultRow> rows = await QueryAsync(executor, db, "SELECT title, body FROM docs");
        Assert.AreEqual(20, rows.Count);
        foreach (QueryResultRow row in rows)
            Assert.AreEqual(bodies[Text(row, "title")!], Text(row, "body"));
        Assert.AreEqual(10, table.Store.LargeValueFetchCalls - before,
            "a scan window closes before a row that would take it past the byte bound, whatever its row count");

        // A batch point read of all twenty rows is resolved in chunks under the same bound.
        List<ObjectIdValue> rowIds = rows.Select(r => r.RowId).ToList();
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            before = table.Store.LargeValueFetchCalls;
            ReadOnlyMemory<byte>?[] batch = await table.Store.GetRowsBatch(tx, rowIds);
            Assert.AreEqual(10, table.Store.LargeValueFetchCalls - before, "a batch read resolves in chunks under the byte bound");
            Assert.IsTrue(batch.All(r => r is { } data && !RowStorageForms.HasTrailer(data.Span)), "every row of the batch is fully resolved");
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    [Test]
    public async Task RowsLargerThanTheByteBound_EachReadCorrectly()
    {
        (string db, _, CommandExecutor executor) = await CreateDocsAsync(Options with { LargeValueResolveBatchBytes = 1_000 });

        string compressible = string.Concat(Enumerable.Repeat("a long and repetitive sentence. ", 400));
        string incompressible = Incompressible(5000);
        await NonQueryAsync(executor, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), @title, @body)", Params("inline-compressed", compressible));
        await NonQueryAsync(executor, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), @title, @body)", Params("out-of-line", incompressible));
        await NonQueryAsync(executor, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), @title, @body)", Params("small", "short"));

        Dictionary<string, string?> read = (await QueryAsync(executor, db, "SELECT title, body FROM docs")).ToDictionary(r => Text(r, "title")!, r => Text(r, "body"));
        Assert.AreEqual(compressible, read["inline-compressed"]);
        Assert.AreEqual(incompressible, read["out-of-line"]);
        Assert.AreEqual("short", read["small"]);
    }

    [Test]
    public async Task UpdateOfSmallColumn_LeavesTheLargeValueKeyUntouched()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDocsAsync();
        string body = Incompressible(5000);

        await NonQueryAsync(executor, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), @title, @body)", Params("before", body));
        (string key, ReadOnlyKeyValueEntry entry) original = (await LargeValueKeysAsync(database, "docs")).Single();

        await NonQueryAsync(executor, db, "UPDATE docs SET title = 'after' WHERE title = 'before'");

        (string key, ReadOnlyKeyValueEntry entry) after = (await LargeValueKeysAsync(database, "docs")).Single();
        Assert.AreEqual(original.key, after.key);
        Assert.AreEqual(original.entry.Revision, after.entry.Revision, "the large value must not be rewritten by an update of another column");

        QueryResultRow row = (await QueryAsync(executor, db, "SELECT title, body FROM docs")).Single();
        Assert.AreEqual("after", Text(row, "title"));
        Assert.AreEqual(body, Text(row, "body"));
    }

    [Test]
    public async Task UpdateOfSmallColumn_KeepsLargeValuesThatACheckOrAnIndexReads()
    {
        // The covering index holds a copy of a large value, so this engine lifts the INCLUDE size limit.
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with { MaxIndexIncludeTupleBytes = 0 });
        await NonQueryAsync(executor, db,
            "CREATE TABLE docs (id object_id PRIMARY KEY, title string, n int64, body string CHECK (body <> ''), note string)");
        await NonQueryAsync(executor, db, "CREATE INDEX idx_title ON docs (title) INCLUDE (note)");

        string body = Incompressible(5000);
        string note = Incompressible(4000);
        await NonQueryAsync(executor, db, "INSERT INTO docs (id, title, n, body, note) VALUES (GEN_ID(), 't0', 1, @body, @note)", new()
        {
            { "@body", new ColumnValue(ColumnType.String, body) },
            { "@note", new ColumnValue(ColumnType.String, note) },
        });

        List<(string key, ReadOnlyKeyValueEntry entry)> before = await LargeValueKeysAsync(database, "docs");
        Assert.AreEqual(2, before.Count);

        await NonQueryAsync(executor, db, "UPDATE docs SET n = 2 WHERE title = 't0'");

        List<(string key, ReadOnlyKeyValueEntry entry)> after = await LargeValueKeysAsync(database, "docs");
        CollectionAssert.AreEqual(before.Select(k => (k.key, k.entry.Revision)), after.Select(k => (k.key, k.entry.Revision)),
            "a column that a CHECK or an index reads is decoded, but an update that does not assign it must not write it again");

        QueryResultRow row = (await QueryAsync(executor, db, "SELECT n, body, note FROM docs WHERE title = 't0'")).Single();
        Assert.AreEqual(2, row.Row["n"].LongValue);
        Assert.AreEqual(body, Text(row, "body"));
        Assert.AreEqual(note, Text(row, "note"));

        // The covering index still returns the carried value.
        QueryResultRow covered = (await QueryAsync(executor, db, "SELECT title, note FROM docs WHERE title = 't0'")).Single();
        Assert.AreEqual(note, Text(covered, "note"));

        // The CHECK still sees the value it validates.
        CamusDBException? violation = Assert.ThrowsAsync<CamusDBException>(() => NonQueryAsync(executor, db, "UPDATE docs SET body = '' WHERE title = 't0'"));
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, violation?.Code);
    }

    [Test]
    public async Task UpdateTransitions_OverwriteShrinkAndGrow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDocsAsync();

        await NonQueryAsync(executor, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), 'k', @body)",
            new() { { "@body", new ColumnValue(ColumnType.String, Incompressible(5000)) } });

        // Changed and still large: the same key, a new value.
        string second = Incompressible(7000);
        await NonQueryAsync(executor, db, "UPDATE docs SET body = @body WHERE title = 'k'", new() { { "@body", new ColumnValue(ColumnType.String, second) } });
        Assert.AreEqual(1, (await LargeValueKeysAsync(database, "docs")).Count);
        Assert.AreEqual(second, Text((await QueryAsync(executor, db, "SELECT body FROM docs")).Single(), "body"));

        // Shrank below the threshold: the key is removed and the value is inline.
        await NonQueryAsync(executor, db, "UPDATE docs SET body = 'small' WHERE title = 'k'");
        Assert.AreEqual(0, (await LargeValueKeysAsync(database, "docs")).Count);
        Assert.AreEqual("small", Text((await QueryAsync(executor, db, "SELECT body FROM docs")).Single(), "body"));

        // Grew above the threshold: a key is written again.
        string fourth = Incompressible(9000);
        await NonQueryAsync(executor, db, "UPDATE docs SET body = @body WHERE title = 'k'", new() { { "@body", new ColumnValue(ColumnType.String, fourth) } });
        Assert.AreEqual(1, (await LargeValueKeysAsync(database, "docs")).Count);
        Assert.AreEqual(fourth, Text((await QueryAsync(executor, db, "SELECT body FROM docs")).Single(), "body"));
    }

    [Test]
    public async Task Delete_RemovesEveryLargeValueKeyTheRowPointsAt()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDocsAsync();

        await NonQueryAsync(executor, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), @title, @body)", Params(Incompressible(3000), Incompressible(3000)));
        await NonQueryAsync(executor, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), 'keep', @body)",
            new() { { "@body", new ColumnValue(ColumnType.String, Incompressible(3000)) } });

        Assert.AreEqual(3, (await LargeValueKeysAsync(database, "docs")).Count);

        await NonQueryAsync(executor, db, "DELETE FROM docs WHERE title <> 'keep'");

        Assert.AreEqual(1, (await LargeValueKeysAsync(database, "docs")).Count, "the deleted row's two values must be gone, the kept row's value must stay");
        Assert.AreEqual(1, (await QueryAsync(executor, db, "SELECT body FROM docs")).Count);
    }

    [Test]
    public async Task RowsWrittenInline_KeepReading_OnAnEngineWithTheFeatureOn()
    {
        // Two engines over one store: a component fixes its configuration when it is built, so one
        // engine cannot exercise both settings.
        (string db, DatabaseDescriptor database, CommandExecutor inlineEngine) = await CreateDocsAsync(
            Options with { LargeValueThresholdBytes = 0, LargeValueCompressionEnabled = false });

        string oldBody = Incompressible(5000);
        string compressibleOld = string.Concat(Enumerable.Repeat("abc ", 2000));
        await NonQueryAsync(inlineEngine, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), 'old', @body)", new() { { "@body", new ColumnValue(ColumnType.String, oldBody) } });
        await NonQueryAsync(inlineEngine, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), 'old2', @body)", new() { { "@body", new ColumnValue(ColumnType.String, compressibleOld) } });
        Assert.AreEqual(0, (await LargeValueKeysAsync(database, "docs")).Count, "with the feature off every value stays inline");

        CommandExecutor largeValueEngine = CreateCommandExecutor(Options);

        string newBody = Incompressible(5000);
        await NonQueryAsync(largeValueEngine, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), 'new', @body)", new() { { "@body", new ColumnValue(ColumnType.String, newBody) } });
        DatabaseDescriptor reopened = await largeValueEngine.OpenDatabase(db);
        Assert.AreEqual(1, (await LargeValueKeysAsync(reopened, "docs")).Count, "a row written by the second engine uses the new form");

        foreach (CommandExecutor engine in new[] { largeValueEngine, inlineEngine })
        {
            List<QueryResultRow> rows = await QueryAsync(engine, db, "SELECT title, body FROM docs");
            Assert.AreEqual(oldBody, Text(rows.Single(r => Text(r, "title") == "old"), "body"));
            Assert.AreEqual(compressibleOld, Text(rows.Single(r => Text(r, "title") == "old2"), "body"));
            Assert.AreEqual(newBody, Text(rows.Single(r => Text(r, "title") == "new"), "body"),
                "an engine with the feature off still reads every stored form, because the marks are per cell");
        }
    }

    [Test]
    public async Task ValueThatDoesNotMatchItsPointer_IsNeverReturned()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDocsAsync();
        string body = Incompressible(5000);
        await NonQueryAsync(executor, db, "INSERT INTO docs (id, title, body) VALUES (GEN_ID(), 't', @body)", Params("t", body));

        (string key, ReadOnlyKeyValueEntry _) = (await LargeValueKeysAsync(database, "docs")).Single();

        // Replace the stored value behind the row's back, as a write that the row never saw would.
        byte[] other = [(byte)BranchKvKind.Value, .. System.Text.Encoding.UTF8.GetBytes(Incompressible(5000))];
        (KeyValueResponseType setType, _, _) = await SharedKahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, other, null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, CancellationToken.None);
        Assert.AreEqual(KeyValueResponseType.Set, setType);

        // The raw write goes through a separate path, so a read may still see the original version for a
        // moment. The safety property holds either way: a read returns the original value, which matches
        // the pointer, or it refuses. It never returns the replaced bytes. Once the replacement is visible,
        // a read without a snapshot re-reads a bounded number of times, then asks the caller to retry.
        CamusDBException? refused = null;
        for (int attempt = 0; attempt < 100 && refused is null; attempt++)
        {
            try
            {
                string? read = Text((await QueryAsync(executor, db, "SELECT body FROM docs")).Single(), "body");
                Assert.AreEqual(body, read, "a value that does not match its row's pointer must never be returned");
                await Task.Delay(20);
            }
            catch (CamusDBException ex)
            {
                refused = ex;
            }
        }

        Assert.IsNotNull(refused, "the replaced value never became visible to a read");
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, refused!.Code);

        // The title alone never touches the value, so it still reads.
        Assert.AreEqual("t", Text((await QueryAsync(executor, db, "SELECT title FROM docs")).Single(), "title"));
    }
}
