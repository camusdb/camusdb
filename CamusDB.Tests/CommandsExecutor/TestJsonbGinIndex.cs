/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Transactions.Models;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// GIN indexes over jsonb columns. The main check is differential: the same documents go into a
/// table with a GIN index and into a table without one, and every predicate must return the same
/// rows from both, while EXPLAIN shows the GIN scan on the indexed table. The other tests cover
/// the index across UPDATE, DELETE, rollback, TRUNCATE, close and reopen, time-travel reads, DDL,
/// and the range locks a Serializable read takes over the entries it scans.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestJsonbGinIndex : BaseTest
{
    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db.Name, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private static async Task Exec(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null, KvTransaction? tx = null)
    {
        KvTransaction own = tx ?? await db.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(own, db.Name, sql, parameters));
            if (tx is null)
                await db.Transactions.CommitAsync(own);
        }
        finally
        {
            // A statement that fails leaves its own transaction open; release its locks.
            if (tx is null)
                await db.Transactions.RollbackIfNotCompletedAsync(own);
        }
    }

    private static async Task<List<QueryResultRow>> Select(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null, KvTransaction? tx = null)
    {
        KvTransaction own = tx ?? await db.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(own, db.Name, sql, parameters));
            return await cursor.ToListAsync();
        }
        finally
        {
            if (tx is null)
                await db.Transactions.RollbackIfNotCompletedAsync(own);
        }
    }

    /// <summary>An autocommit read, which <c>AS OF SYSTEM TIME</c> needs.</summary>
    private static async Task<long[]> AutocommitIds(CommandExecutor executor, DatabaseDescriptor db, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(KvTransaction.CreateReadOnly(), db.Name, sql, null));
        return (await cursor.ToListAsync()).Select(r => r.Row["id"].LongValue).OrderBy(i => i).ToArray();
    }

    private static async Task<long[]> Ids(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null) =>
        (await Select(executor, db, sql, parameters)).Select(r => r.Row["id"].LongValue).OrderBy(i => i).ToArray();

    private static async Task<string> Explain(CommandExecutor executor, DatabaseDescriptor db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        List<QueryResultRow> rows = await Select(executor, db, "EXPLAIN " + sql, parameters);
        StringBuilder plan = new();
        foreach (QueryResultRow row in rows)
        {
            // One plan node per row: its name in "node", its arguments in "detail".
            plan.Append(row.Row["node"].StrValue).Append('(').Append(row.Row["detail"].StrValue).Append(")\n");
        }
        return plan.ToString();
    }

    /// <summary>The number of entries stored in the GIN index <paramref name="indexName"/>.</summary>
    private static async Task<int> CountEntries(CommandExecutor executor, DatabaseDescriptor db, string tableName, string indexName)
    {
        TableDescriptor table = await executor.OpenTable(new OpenTableTicket(db.Name, tableName));
        TableIndexSchema index = table.AllIndexes[indexName];
        KvTransaction tx = await db.Transactions.BeginAsync();
        try
        {
            int count = 0;
            await foreach (var _ in table.Store.ScanIndex(tx, index.KvId, JsonbGinIndexKeys.KeyTypes, from: null, to: null, unique: false))
                count++;
            return count;
        }
        finally
        {
            await db.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Documents and predicates
    // ---------------------------------------------------------------------------------------------

    private static readonly string LongKey = new('k', 80);

    private static readonly string LongString = new('s', 100);

    private static string Scalar(Random random) => random.Next(14) switch
    {
        0 => "0",
        1 => "1",
        2 => "1.0",
        3 => "2.50",
        4 => "-3",
        5 => "\"x\"",
        6 => "\"y\"",
        7 => "\"vip\"",
        8 => "true",
        9 => "false",
        10 => "null",
        11 => "\"" + LongString + "\"",
        12 => "\"ü\"",
        _ => "12345678901234567890123",
    };

    private static readonly string[] Keys = ["a", "b", "c", "tags", "n", "ключ", LongKey];

    private static string Value(Random random, int depth)
    {
        int pick = random.Next(depth >= 2 ? 6 : 10);
        if (pick < 6)
            return Scalar(random);

        if (pick == 6)
            return random.Next(2) == 0 ? "{}" : "[]";

        if (pick == 7)
        {
            int count = random.Next(1, 4);
            return "[" + string.Join(", ", Enumerable.Range(0, count).Select(_ => Value(random, depth + 1))) + "]";
        }

        return Object(random, depth + 1);
    }

    private static string Object(Random random, int depth)
    {
        int count = random.Next(1, 4);
        HashSet<string> used = [];
        List<string> fields = [];
        for (int i = 0; i < count; i++)
        {
            string key = Keys[random.Next(Keys.Length)];
            if (used.Add(key))
                fields.Add("\"" + key + "\": " + Value(random, depth));
        }
        return "{" + string.Join(", ", fields) + "}";
    }

    private static string? Document(Random random) => random.Next(20) switch
    {
        0 => null,
        1 => Scalar(random),
        2 or 3 => "[" + string.Join(", ", Enumerable.Range(0, random.Next(0, 4)).Select(_ => Value(random, 1))) + "]",
        _ => Object(random, 0),
    };

    private static string Literal(string text) => "'" + text.Replace("'", "''") + "'";

    /// <summary>
    /// Predicates over <c>doc</c>. Each one runs on both tables; <c>uses</c> marks the ones the GIN
    /// index must serve.
    /// </summary>
    private static readonly (string Where, bool Uses)[] Predicates =
    [
        ("doc @> '{\"a\": 1}'", true),
        ("doc @> '{\"a\": 1.0}'", true),
        ("doc @> '{\"n\": 2.5}'", true),
        ("doc @> '{\"tags\": [\"vip\"]}'", true),
        ("doc @> '{\"b\": {\"c\": \"x\"}}'", true),
        ("doc @> '{\"a\": {}}'", true),
        ("doc @> '{\"tags\": []}'", true),
        ("doc @> '{\"a\": null}'", true),
        ("doc @> '{\"a\": 1, \"c\": true}'", true),
        ("doc @> '[1]'", true),
        ("doc @> '[\"x\", 1]'", true),
        ("doc @> '[{\"a\": 1}]'", true),
        ("doc @> '[[]]'", true),
        ("doc @> '{\"ключ\": \"ü\"}'", true),
        ("doc @> '{\"" + LongKey + "\": \"" + LongString + "\"}'", true),
        ("doc @> '{\"c\": 12345678901234567890123}'", true),
        ("doc @> '{}'", false),
        ("doc @> '[]'", false),
        ("doc @> '1'", false),
        ("doc @> '\"x\"'", false),
        ("'{\"a\": 1}' <@ doc", true),
        ("jsonb_contains(doc, '{\"b\": true}')", true),
        ("doc ? 'a'", true),
        ("doc ? 'x'", true),
        ("doc ? 'vip'", true),
        ("doc ? 'ключ'", true),
        ("doc ? '" + LongKey + "'", true),
        ("doc ?| array['a', 'zz']", true),
        ("doc ?| '{c,tags}'", true),
        ("doc ?& array['a', 'c']", true),
        ("doc ?& array['b']", true),
        ("doc ->> 'a' = '1'", true),
        ("doc ->> 'a' = '1.0'", true),
        ("doc ->> 'c' = 'true'", true),
        ("doc ->> 'c' = 'x'", true),
        ("'y' = doc ->> 'b'", true),
        ("doc ->> 'n' = '-3'", true),
        ("doc ->> 'b' = '{}'", false),
        ("doc -> 'a' = '1'", true),
        ("doc -> 'a' = '\"x\"'", true),
        ("doc -> 'b' = '{}'", true),
        ("doc -> 'tags' = '[\"vip\"]'", true),
        ("doc @> '{\"a\": 1}' AND doc ? 'c'", true),
        ("doc ? 'a' AND doc ->> 'c' = 'x'", true),
        ("doc @> '{\"a\": 1}' AND id > 50", true),
        ("doc @> '{\"a\": 1}' OR doc ? 'b'", false),
        ("NOT (doc ? 'a')", false),
        ("doc <@ '{\"a\": 1, \"b\": 2}'", false),
    ];

    private async Task<(DatabaseDescriptor db, CommandExecutor executor)> SetupPair(int rows, bool indexAfterLoad, int seed = 7)
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, db, indexAfterLoad
            ? "CREATE TABLE gin_t (id int64 PRIMARY KEY, doc jsonb)"
            : "CREATE TABLE gin_t (id int64 PRIMARY KEY, doc jsonb, INVERTED INDEX gin_doc (doc))");
        await ExecDDL(executor, db, "CREATE TABLE plain_t (id int64 PRIMARY KEY, doc jsonb)");

        Random random = new(seed);
        StringBuilder values = new();
        for (int i = 1; i <= rows; i++)
        {
            string? document = Document(random);
            if (values.Length > 0)
                values.Append(", ");
            values.Append('(').Append(i.ToString(CultureInfo.InvariantCulture)).Append(", ")
                .Append(document is null ? "NULL" : Literal(document)).Append(')');
        }

        await Exec(executor, db, "INSERT INTO gin_t (id, doc) VALUES " + values);
        await Exec(executor, db, "INSERT INTO plain_t (id, doc) VALUES " + values);

        if (indexAfterLoad)
            await ExecDDL(executor, db, "CREATE INDEX gin_doc ON gin_t USING GIN (doc jsonb_path_ops)");

        return (db, executor);
    }

    private static async Task AssertSameRows(CommandExecutor executor, DatabaseDescriptor db, string because)
    {
        int nonEmpty = 0;
        foreach ((string where, bool uses) in Predicates)
        {
            long[] expected = await Ids(executor, db, "SELECT id FROM plain_t WHERE " + where);
            long[] actual = await Ids(executor, db, "SELECT id FROM gin_t WHERE " + where);
            CollectionAssert.AreEqual(expected, actual, $"{because}: WHERE {where}");
            if (expected.Length > 0)
                nonEmpty++;

            string plan = await Explain(executor, db, "SELECT id FROM gin_t WHERE " + where);
            if (uses)
                StringAssert.Contains("gin-scan(index=gin_doc", plan, $"WHERE {where} must use the GIN index");
            else
                StringAssert.DoesNotContain("gin-scan", plan, $"WHERE {where} has no GIN form");
        }

        Assert.Greater(nonEmpty, Predicates.Length / 2, "the documents must make most predicates match something");
    }

    // ---------------------------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------------------------

    [TestCase(false, TestName = "Differential_IndexDeclaredWithTheTable")]
    [TestCase(true, TestName = "Differential_IndexBackfilledOverExistingRows")]
    public async Task Differential_EveryPredicateReturnsTheRowsOfATableScan(bool indexAfterLoad)
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupPair(300, indexAfterLoad);
        await AssertSameRows(executor, db, "after load");

        // Writes keep the index in step: changed documents, NULLs, deletes, and a new row.
        foreach (string table in new[] { "gin_t", "plain_t" })
        {
            await Exec(executor, db, $"UPDATE {table} SET doc = doc || '{{\"a\": 1, \"c\": \"x\"}}' WHERE id % 7 = 0 AND jsonb_typeof(doc) = 'object'");
            await Exec(executor, db, $"UPDATE {table} SET doc = doc - 'a' WHERE id % 5 = 0 AND jsonb_typeof(doc) = 'object'");
            await Exec(executor, db, $"UPDATE {table} SET doc = NULL WHERE id % 11 = 0");
            await Exec(executor, db, $"UPDATE {table} SET doc = '{{\"tags\": [\"vip\", 1], \"b\": {{\"c\": \"x\"}}}}' WHERE id % 13 = 0");
            await Exec(executor, db, $"DELETE FROM {table} WHERE id % 9 = 0");
            await Exec(executor, db, $"INSERT INTO {table} (id, doc) VALUES (1000, '{{\"a\": 1, \"c\": true, \"b\": true}}')");

            // UPDATE and DELETE locate their rows through the same plan, so a GIN predicate drives them.
            await Exec(executor, db, $"UPDATE {table} SET doc = doc || '{{\"seen\": true}}' WHERE doc @> '{{\"c\": \"x\"}}'");
            await Exec(executor, db, $"DELETE FROM {table} WHERE doc ? 'vip'");
        }

        StringAssert.Contains("gin-scan(index=gin_doc", await Explain(executor, db, "SELECT id FROM gin_t WHERE doc ? 'seen'"));
        CollectionAssert.AreEqual(
            await Ids(executor, db, "SELECT id FROM plain_t WHERE doc ? 'seen'"),
            await Ids(executor, db, "SELECT id FROM gin_t WHERE doc ? 'seen'"));
        CollectionAssert.IsNotEmpty(await Ids(executor, db, "SELECT id FROM gin_t WHERE doc ? 'seen'"));

        // A locking read through the GIN scan.
        KvTransaction locking = await db.Transactions.BeginAsync();
        long[] locked = (await Select(executor, db, "SELECT id FROM gin_t WHERE doc @> '{\"a\": 1}' FOR UPDATE", tx: locking))
            .Select(r => r.Row["id"].LongValue).OrderBy(i => i).ToArray();
        await db.Transactions.CommitAsync(locking);
        CollectionAssert.AreEqual(await Ids(executor, db, "SELECT id FROM plain_t WHERE doc @> '{\"a\": 1}'"), locked);

        await AssertSameRows(executor, db, "after writes");
    }

    [Test]
    public async Task Parameters_ServeTheGinScan()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupPair(120, indexAfterLoad: false);

        Dictionary<string, ColumnValue> p = new() { ["@p"] = new(ColumnType.String, "{\"a\": 1}") };
        CollectionAssert.AreEqual(
            await Ids(executor, db, "SELECT id FROM plain_t WHERE doc @> @p", p),
            await Ids(executor, db, "SELECT id FROM gin_t WHERE doc @> @p", p));
        StringAssert.Contains("gin-scan", await Explain(executor, db, "SELECT id FROM gin_t WHERE doc @> @p", p));

        // The plan cache does not keep a GIN decision: the next literal computes its own entries.
        Dictionary<string, ColumnValue> q = new() { ["@p"] = new(ColumnType.String, "{\"c\": \"x\"}") };
        CollectionAssert.AreEqual(
            await Ids(executor, db, "SELECT id FROM plain_t WHERE doc @> @p", q),
            await Ids(executor, db, "SELECT id FROM gin_t WHERE doc @> @p", q));
    }

    [Test]
    public async Task Entries_FollowRollbackTruncateAndDropIndex()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, "CREATE TABLE docs (id int64 PRIMARY KEY, doc jsonb)");
        await ExecDDL(executor, db, "CREATE INVERTED INDEX gin_doc ON docs (doc)");

        // {"a": 1, "t": ["x", "x"]} has two distinct entries; NULL and a scalar add 0 and 1.
        await Exec(executor, db, "INSERT INTO docs (id, doc) VALUES (1, '{\"a\": 1, \"t\": [\"x\", \"x\"]}'), (2, NULL), (3, '\"x\"')");
        Assert.AreEqual(3, await CountEntries(executor, db, "docs", "gin_doc"));

        KvTransaction tx = await db.Transactions.BeginAsync();
        await Exec(executor, db, "INSERT INTO docs (id, doc) VALUES (4, '{\"a\": 2}')", tx: tx);
        await Exec(executor, db, "UPDATE docs SET doc = '{\"z\": true}' WHERE id = 1", tx: tx);
        await db.Transactions.RollbackAsync(tx);

        Assert.AreEqual(3, await CountEntries(executor, db, "docs", "gin_doc"), "a rolled-back write leaves no entry");
        CollectionAssert.AreEqual(new long[] { 1 }, await Ids(executor, db, "SELECT id FROM docs WHERE doc @> '{\"a\": 1}'"));
        CollectionAssert.IsEmpty(await Ids(executor, db, "SELECT id FROM docs WHERE doc ? 'z'"));

        // An update that keeps an entry writes only the difference.
        await Exec(executor, db, "UPDATE docs SET doc = '{\"a\": 1, \"b\": 2}' WHERE id = 1");
        Assert.AreEqual(3, await CountEntries(executor, db, "docs", "gin_doc"));
        CollectionAssert.AreEqual(new long[] { 1 }, await Ids(executor, db, "SELECT id FROM docs WHERE doc @> '{\"b\": 2}'"));
        CollectionAssert.IsEmpty(await Ids(executor, db, "SELECT id FROM docs WHERE doc ? 't'"));

        await ExecDDL(executor, db, "TRUNCATE TABLE docs");
        Assert.AreEqual(0, await CountEntries(executor, db, "docs", "gin_doc"), "TRUNCATE removes the entries");

        await Exec(executor, db, "INSERT INTO docs (id, doc) VALUES (5, '{\"a\": 1}')");
        CollectionAssert.AreEqual(new long[] { 5 }, await Ids(executor, db, "SELECT id FROM docs WHERE doc @> '{\"a\": 1}'"));

        await ExecDDL(executor, db, "DROP INDEX gin_doc ON docs");
        StringAssert.DoesNotContain("gin-scan", await Explain(executor, db, "SELECT id FROM docs WHERE doc @> '{\"a\": 1}'"));
        CollectionAssert.AreEqual(new long[] { 5 }, await Ids(executor, db, "SELECT id FROM docs WHERE doc @> '{\"a\": 1}'"));
    }

    [Test]
    public async Task Index_SurvivesCloseAndReopen_AndShowsInTheCatalog()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, "CREATE TABLE docs (id int64 PRIMARY KEY, doc jsonb, INVERTED INDEX gin_doc (doc))");
        await Exec(executor, db, "INSERT INTO docs (id, doc) VALUES (1, '{\"a\": 1}'), (2, '{\"a\": 2}')");

        string created = (await Select(executor, db, "SHOW CREATE TABLE docs")).Single().Row.Values
            .Select(v => v.StrValue).First(v => v is not null && v.Contains("CREATE TABLE"))!;
        StringAssert.Contains("INVERTED INDEX `gin_doc` (`doc`)", created);

        List<QueryResultRow> indexes = await Select(executor, db, "SHOW INDEXES FROM docs");
        Assert.AreEqual("GIN", indexes.Single(r => r.Row["Key_name"].StrValue == "gin_doc").Row["Index_type"].StrValue);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        StringAssert.Contains("gin-scan(index=gin_doc", await Explain(executor, reopened, "SELECT id FROM docs WHERE doc @> '{\"a\": 2}'"));
        CollectionAssert.AreEqual(new long[] { 2 }, await Ids(executor, reopened, "SELECT id FROM docs WHERE doc @> '{\"a\": 2}'"));

        // The SHOW CREATE TABLE text builds the same index again.
        await ExecDDL(executor, reopened, created.Replace("`docs`", "`docs_copy`"));
        await Exec(executor, reopened, "INSERT INTO docs_copy (id, doc) VALUES (1, '{\"a\": 2}')");
        StringAssert.Contains("gin-scan(index=gin_doc", await Explain(executor, reopened, "SELECT id FROM docs_copy WHERE doc @> '{\"a\": 2}'"));
    }

    [Test]
    public async Task ForceIndex_NamesTheGinIndex()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupPair(60, indexAfterLoad: false);

        StringAssert.Contains("gin-scan(index=gin_doc",
            await Explain(executor, db, "SELECT id FROM gin_t@{FORCE_INDEX=gin_doc} WHERE doc ? 'a'"));
        CollectionAssert.AreEqual(
            await Ids(executor, db, "SELECT id FROM plain_t WHERE doc ? 'a'"),
            await Ids(executor, db, "SELECT id FROM gin_t@{FORCE_INDEX=gin_doc} WHERE doc ? 'a'"));

        // A hinted GIN index the predicate cannot use reads the primary rows.
        StringAssert.DoesNotContain("gin-scan",
            await Explain(executor, db, "SELECT id FROM gin_t@{FORCE_INDEX=gin_doc} WHERE id = 3"));
        CollectionAssert.AreEqual(new long[] { 3 },
            await Ids(executor, db, "SELECT id FROM gin_t@{FORCE_INDEX=gin_doc} WHERE id = 3"));
    }

    [Test]
    public async Task TimeTravelRead_SeesTheEntriesOfItsSnapshot()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, "CREATE TABLE docs (id int64 PRIMARY KEY, doc jsonb, INVERTED INDEX gin_doc (doc))");
        await Exec(executor, db, "INSERT INTO docs (id, doc) VALUES (1, '{\"state\": \"old\"}')");

        await Task.Delay(60);
        long snapshotMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await Task.Delay(60);

        await Exec(executor, db, "UPDATE docs SET doc = '{\"state\": \"new\"}' WHERE id = 1");

        CollectionAssert.AreEqual(new long[] { 1 },
            await AutocommitIds(executor, db, $"SELECT id FROM docs AS OF SYSTEM TIME {snapshotMs} WHERE doc @> '{{\"state\": \"old\"}}'"));
        CollectionAssert.IsEmpty(
            await AutocommitIds(executor, db, $"SELECT id FROM docs AS OF SYSTEM TIME {snapshotMs} WHERE doc @> '{{\"state\": \"new\"}}'"));
        CollectionAssert.AreEqual(new long[] { 1 }, await Ids(executor, db, "SELECT id FROM docs WHERE doc @> '{\"state\": \"new\"}'"));
    }

    [Test]
    public async Task Ddl_RefusesWhatAGinIndexCannotHold()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, "CREATE TABLE docs (id int64 PRIMARY KEY, doc jsonb, name string)");

        CamusDBException? notJsonb = Assert.ThrowsAsync<CamusDBException>(() => ExecDDL(executor, db, "CREATE INDEX g ON docs USING GIN (name)"));
        StringAssert.Contains("needs a jsonb column", notJsonb!.Message);

        Assert.ThrowsAsync<CamusDBException>(() => ExecDDL(executor, db, "CREATE INDEX g ON docs USING GIN (doc, name)"));
        Assert.ThrowsAsync<CamusDBException>(() => ExecDDL(executor, db, "CREATE INDEX g ON docs USING GIN (doc DESC)"));
        Assert.ThrowsAsync<CamusDBException>(() => ExecDDL(executor, db, "CREATE INDEX g ON docs USING GIN (doc text_ops)"));
        Assert.ThrowsAsync<CamusDBException>(() => ExecDDL(executor, db, "CREATE INDEX g ON docs USING HASH (doc)"));
        Assert.ThrowsAsync<CamusDBException>(() => ExecDDL(executor, db, "CREATE INDEX g ON docs (doc)"),
            "an ordered index over jsonb is refused: jsonb has no key encoding");
        Assert.ThrowsAsync<CamusDBException>(() => ExecDDL(executor, db,
            "CREATE TABLE bad (id int64 PRIMARY KEY, name string, INVERTED INDEX g (name))"));
    }

    [Test]
    public async Task Serializable_GinScanLocksTheEntriesItReads()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, db, "CREATE TABLE docs (id int64 PRIMARY KEY, doc jsonb, INVERTED INDEX gin_doc (doc))");
        await Exec(executor, db, "INSERT INTO docs (id, doc) VALUES (1, '{\"kind\": \"a\"}')");

        KvTransaction scanner = await db.Transactions.BeginAsync(CamusIsolationLevel.Serializable, CamusTransactionMode.ReadWrite);
        CollectionAssert.AreEqual(new long[] { 1 },
            (await Select(executor, db, "SELECT id FROM docs WHERE doc @> '{\"kind\": \"a\"}'", tx: scanner)).Select(r => r.Row["id"].LongValue).ToArray());

        // A row the predicate would match writes an entry inside the locked range.
        KvTransaction phantom = await db.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(
            () => Exec(executor, db, "INSERT INTO docs (id, doc) VALUES (2, '{\"kind\": \"a\", \"x\": 1}')", tx: phantom));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMustRetry, ex!.Code);
        await db.Transactions.RollbackAsync(phantom);

        // A row with other entries does not touch the locked range.
        KvTransaction other = await db.Transactions.BeginAsync();
        await Exec(executor, db, "INSERT INTO docs (id, doc) VALUES (3, '{\"kind\": \"b\"}')", tx: other);
        await db.Transactions.CommitAsync(other);

        await db.Transactions.CommitAsync(scanner);
    }

    [Test]
    public async Task MutationLimit_RefusesADocumentWithTooManyEntries_AndNamesTheIndex()
    {
        (_, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase(Options with { MaxMutationsPerTransaction = 40 });
        await ExecDDL(executor, db, "CREATE TABLE docs (id int64 PRIMARY KEY, doc jsonb, INVERTED INDEX gin_doc (doc))");

        string wide = "{" + string.Join(", ", Enumerable.Range(0, 60).Select(i => $"\"k{i}\": {i}")) + "}";

        CamusDBException? insert = Assert.ThrowsAsync<CamusDBException>(
            () => Exec(executor, db, $"INSERT INTO docs (id, doc) VALUES (1, {Literal(wide)})"));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMutationLimitExceeded, insert!.Code);
        StringAssert.Contains("GIN index 'gin_doc'", insert.Message);
        StringAssert.Contains("60 entries", insert.Message);
        Assert.AreEqual(0, await CountEntries(executor, db, "docs", "gin_doc"), "a refused insert writes no entry");
        CollectionAssert.IsEmpty(await Ids(executor, db, "SELECT id FROM docs"));

        await Exec(executor, db, "INSERT INTO docs (id, doc) VALUES (2, '{\"a\": 1}')");

        // An update counts the entries it adds, not the ones the document keeps.
        CamusDBException? update = Assert.ThrowsAsync<CamusDBException>(
            () => Exec(executor, db, $"UPDATE docs SET doc = {Literal(wide)} WHERE id = 2"));
        Assert.AreEqual(CamusDBErrorCodes.TransactionMutationLimitExceeded, update!.Code);
        StringAssert.Contains("GIN index 'gin_doc'", update.Message);
        CollectionAssert.AreEqual(new long[] { 2 }, await Ids(executor, db, "SELECT id FROM docs WHERE doc @> '{\"a\": 1}'"));

        await Exec(executor, db, "UPDATE docs SET doc = doc || '{\"b\": 2}' WHERE id = 2");
        CollectionAssert.AreEqual(new long[] { 2 }, await Ids(executor, db, "SELECT id FROM docs WHERE doc @> '{\"a\": 1, \"b\": 2}'"));
    }

    [Test]
    public async Task Branch_ReadsTheParentEntries_AndKeepsItsOwnWrites()
    {
        // BRANCH FROM names the root in SQL, so its name must start with a letter.
        string rootName = "gr_" + Guid.NewGuid().ToString("n");
        CommandExecutor executor = CreateCommandExecutor();
        DatabaseDescriptor root = await executor.CreateDatabase(new CreateDatabaseTicket(rootName, ifNotExists: false));
        TrackDatabase(rootName, executor);
        await ExecDDL(executor, root, "CREATE TABLE docs (id int64 PRIMARY KEY, doc jsonb, INVERTED INDEX gin_doc (doc))");
        await Exec(executor, root, "INSERT INTO docs (id, doc) VALUES (1, '{\"a\": 1}'), (2, '{\"a\": 2}')");

        string branchName = "br_" + Guid.NewGuid().ToString("n");
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: null!, database: rootName,
            sql: $"CREATE DATABASE {branchName} BRANCH FROM {rootName}", parameters: null));
        TrackDatabase(branchName, executor);
        DatabaseDescriptor branch = await executor.OpenDatabase(branchName);

        const string Where = "SELECT id FROM docs WHERE doc @> '{\"a\": 1}'";
        StringAssert.Contains("gin-scan(index=gin_doc", await Explain(executor, branch, Where));
        CollectionAssert.AreEqual(new long[] { 1 }, await Ids(executor, branch, Where), "the branch reads the entries it inherited");

        await Exec(executor, branch, "INSERT INTO docs (id, doc) VALUES (3, '{\"a\": 1}')");
        await Exec(executor, branch, "UPDATE docs SET doc = '{\"a\": 1}' WHERE id = 2");
        await Exec(executor, branch, "DELETE FROM docs WHERE id = 1");

        CollectionAssert.AreEqual(new long[] { 2, 3 }, await Ids(executor, branch, Where));
        CollectionAssert.AreEqual(new long[] { 1 }, await Ids(executor, root, Where), "the parent does not see the branch writes");
        CollectionAssert.AreEqual(new long[] { 2 }, await Ids(executor, root, "SELECT id FROM docs WHERE doc @> '{\"a\": 2}'"));
    }

    [Test]
    public async Task Analyze_SkipsTheDocumentColumn_AndKeepsTheGinScan()
    {
        (DatabaseDescriptor db, CommandExecutor executor) = await SetupPair(80, indexAfterLoad: false);

        KvTransaction tx = await db.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> analyze) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db.Name, "ANALYZE gin_t", null));
        await foreach (QueryResultRow _ in analyze) { }
        await db.Transactions.CommitAsync(tx);

        List<QueryResultRow> statistics = await Select(executor, db, "SHOW STATISTICS FOR gin_t");
        Assert.IsTrue(statistics.Any(r => r.Row["kind"].StrValue == "table"), "ANALYZE ran");
        Assert.IsFalse(statistics.Any(r => r.Row["kind"].StrValue == "column" && r.Row["target"].StrValue == "doc"),
            "a document column has no histogram, bounds or distinct count");

        // With statistics the cost-based path runs when a conjunct bounds an ordered index.
        foreach (string where in new[] { "doc @> '{\"a\": 1}'", "doc ? 'c' AND id > 0", "doc ->> 'c' = 'x' AND id < 1000" })
        {
            CollectionAssert.AreEqual(
                await Ids(executor, db, "SELECT id FROM plain_t WHERE " + where),
                await Ids(executor, db, "SELECT id FROM gin_t WHERE " + where), where);
        }
        StringAssert.Contains("gin-scan(index=gin_doc", await Explain(executor, db, "SELECT id FROM gin_t WHERE doc @> '{\"a\": 1}'"));
    }
}
