/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;

using static CamusDB.Tests.CommandsExecutor.WriteReturningTestSql;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for <c>UPDATE … RETURNING</c> through the engine's two SQL entry points. An
/// UPDATE returns the new image of each row it changed. The values a test checks against are read
/// back from storage with a separate SELECT, so a returned value that differs from the stored value
/// fails here.
/// </summary>
[NonParallelizable]
public sealed class TestUpdateReturning : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupRobots(CamusDBOptions? options = null)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = options is null
            ? await CreateDatabase()
            : await CreateDatabase(options);

        await Ddl(executor, dbname,
            "CREATE TABLE robots (id oid NOT NULL DEFAULT(gen_id()), name string, year int64 DEFAULT(1999), note string, PRIMARY KEY (id))");
        await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO robots (name, year, note) VALUES ('alpha', 2001, 'a-note'), ('beta', 2002, 'b-note'), ('gamma', 2003, 'g-note')");
        return (dbname, database, executor);
    }

    // -----------------------------------------------------------------------
    // Values
    // -----------------------------------------------------------------------

    [Test]
    public async Task ReturnsTheNewImageOfEachChangedRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "UPDATE robots SET year = year + 10 WHERE year >= 2002 RETURNING name, year");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEqual(new[] { "name", "year" }, ColumnNames(result.ReturningColumns!));
        Assert.AreEqual(ColumnType.String, result.ReturningColumns![0].Type);
        Assert.AreEqual(ColumnType.Integer64, result.ReturningColumns![1].Type);
        Assert.AreEqual(2, result.ReturningRows!.Count);

        Dictionary<string, long> returned = result.ReturningRows.ToDictionary(
            r => Cell(r, result.ReturningColumns[0]).StrValue!,
            r => Cell(r, result.ReturningColumns[1]).LongValue);

        CollectionAssert.AreEquivalent(new[] { "beta", "gamma" }, returned.Keys);
        Assert.AreEqual(2012, returned["beta"]);
        Assert.AreEqual(2013, returned["gamma"]);

        List<QueryResultRow> stored = await Select(executor, database, dbname, "SELECT name, year FROM robots WHERE year >= 2012");
        CollectionAssert.AreEquivalent(
            returned.Select(kv => (kv.Key, kv.Value)),
            stored.Select(r => (r.Row["name"].StrValue!, r.Row["year"].LongValue)));
    }

    [Test]
    public async Task WithoutReturningTheResultCarriesNoRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "UPDATE robots SET year = 1 WHERE name = 'alpha'");

        Assert.AreEqual(1, result.ModifiedRows);
        Assert.IsNull(result.ReturningColumns);
        Assert.IsNull(result.ReturningRows);
    }

    /// <summary>
    /// <c>*</c> covers every column in schema order, including a column the statement neither assigns
    /// nor filters on. The row id is the id that was stored.
    /// </summary>
    [Test]
    public async Task StarReturnsEveryColumnOfTheNewRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "UPDATE robots SET year = 1970 WHERE name = 'beta' RETURNING *");

        CollectionAssert.AreEqual(new[] { "id", "name", "year", "note" }, ColumnNames(result.ReturningColumns!));
        Assert.AreEqual(1, result.ReturningRows!.Count);
        Assert.AreEqual(ColumnType.Id, Cell(result, 0, 0).Type);
        Assert.AreEqual("beta", Cell(result, 0, 1).StrValue);
        Assert.AreEqual(1970, Cell(result, 0, 2).LongValue);
        Assert.AreEqual("b-note", Cell(result, 0, 3).StrValue);

        List<QueryResultRow> stored = await Select(executor, database, dbname, "SELECT id FROM robots WHERE name = 'beta'");
        Assert.AreEqual(stored[0].Row["id"].StrValue, Cell(result, 0, 0).StrValue);
    }

    [Test]
    public async Task ExpressionsAliasesPlaceholdersAndQualifiedNamesAreEvaluatedOnTheNewRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        Dictionary<string, ColumnValue> parameters = new() { ["@shift"] = new ColumnValue(ColumnType.Integer64, 100) };

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "UPDATE robots SET year = 10 WHERE name = 'alpha' " +
            "RETURNING year * 2 AS doubled, year + @shift AS shifted, robots.note, NAME",
            parameters);

        CollectionAssert.AreEqual(new[] { "doubled", "shifted", "note", "NAME" }, ColumnNames(result.ReturningColumns!));
        Assert.AreEqual(20, Cell(result, 0, 0).LongValue);
        Assert.AreEqual(110, Cell(result, 0, 1).LongValue);
        Assert.AreEqual("a-note", Cell(result, 0, 2).StrValue);
        Assert.AreEqual("alpha", Cell(result, 0, 3).StrValue);
    }

    [Test]
    public async Task CoercedValuesReturnTheStoredType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname, "CREATE TABLE prices (id oid NOT NULL DEFAULT(gen_id()), label string, amount float64, PRIMARY KEY (id))");
        await NonQueryCommitted(executor, database, dbname, "INSERT INTO prices (label, amount) VALUES ('a', 1.5)");

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "UPDATE prices SET amount = 10 WHERE label = 'a' RETURNING amount");

        Assert.AreEqual(ColumnType.Float64, result.ReturningColumns![0].Type);
        Assert.AreEqual(ColumnType.Float64, Cell(result, 0, 0).Type);
        Assert.AreEqual(10.0, Cell(result, 0, 0).FloatValue);
    }

    /// <summary>
    /// Every SET expression reads the old row, so <c>SET a = b, b = a</c> swaps the two values, and
    /// RETURNING reports the swapped (new) values.
    /// </summary>
    [Test]
    public async Task ASwapReturnsTheSwappedValues()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname, "CREATE TABLE pairs (id oid NOT NULL DEFAULT(gen_id()), a int64, b int64, PRIMARY KEY (id))");
        await NonQueryCommitted(executor, database, dbname, "INSERT INTO pairs (a, b) VALUES (1, 2)");

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "UPDATE pairs SET a = b, b = a WHERE a = 1 RETURNING a, b");

        Assert.AreEqual(2, Cell(result, 0, 0).LongValue);
        Assert.AreEqual(1, Cell(result, 0, 1).LongValue);
    }

    [Test]
    public async Task NoMatchReturnsTheSchemaAndNoRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "UPDATE robots SET year = 1 WHERE year > 3000 RETURNING id, name");

        Assert.AreEqual(0, result.ModifiedRows);
        CollectionAssert.AreEqual(new[] { "id", "name" }, ColumnNames(result.ReturningColumns!));
        Assert.IsNotNull(result.ReturningRows);
        Assert.AreEqual(0, result.ReturningRows!.Count);
    }

    [Test]
    public async Task LimitReturnsOnlyTheChangedRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "UPDATE robots SET note = 'limited' WHERE year > 0 LIMIT 2 RETURNING name, note");

        Assert.AreEqual(2, result.ModifiedRows);
        Assert.AreEqual(2, result.ReturningRows!.Count);
        Assert.IsTrue(result.ReturningRows.All(r => Cell(r, result.ReturningColumns![1]).StrValue == "limited"));

        List<QueryResultRow> stored = await Select(executor, database, dbname, "SELECT name FROM robots WHERE note = 'limited'");
        CollectionAssert.AreEquivalent(
            stored.Select(r => r.Row["name"].StrValue),
            SortedStrings(result, "name"));
    }

    /// <summary>
    /// A statement that writes in several chunks projects each chunk as it is written. Every row of
    /// every chunk must come back once.
    /// </summary>
    [Test]
    public async Task ManyChunksReturnEveryRowOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { SpillEnabled = true, ForceSpillThresholdRows = 2 });
        await Ddl(executor, dbname, "CREATE TABLE items (id oid NOT NULL DEFAULT(gen_id()), n int64, tag string, PRIMARY KEY (id))");
        await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO items (n, tag) VALUES (1, 'x'), (2, 'x'), (3, 'x'), (4, 'x'), (5, 'x'), (6, 'x'), (7, 'x')");

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "UPDATE items SET tag = 'y' WHERE n > 0 RETURNING n, tag");

        Assert.AreEqual(7, result.ModifiedRows);
        CollectionAssert.AreEquivalent(
            new long[] { 1, 2, 3, 4, 5, 6, 7 },
            result.ReturningRows!.Select(r => Cell(r, result.ReturningColumns![0]).LongValue));
        Assert.IsTrue(result.ReturningRows!.All(r => Cell(r, result.ReturningColumns![1]).StrValue == "y"));
    }

    // -----------------------------------------------------------------------
    // Large values: the update decodes only the columns it needs
    // -----------------------------------------------------------------------

    private async Task<int> LargeValueKeyCount(DatabaseDescriptor database, string table)
    {
        string bucket = $"{database.Id}:{database.Schema.Tables[table].EffectiveStorageId}|v";
        int count = 0;
        await foreach ((string key, ReadOnlyKeyValueEntry entry) in SharedKahuna.LocateAndScanRange(
            HLCTimestamp.Zero, bucket, null, true, null, true, 1000,
            HLCTimestamp.Zero, KeyValueDurability.Persistent, CancellationToken.None))
        {
            if (key.StartsWith(bucket + "/", StringComparison.Ordinal) && entry.Value is { Length: > 1 } && entry.Value[0] == (byte)BranchKvKind.Value)
                count++;
        }
        return count;
    }

    /// <summary>
    /// An update of a small column carries the row's large values without decoding them. RETURNING
    /// must still return them whole: the write phase decodes the columns the list reads. Without that,
    /// the large columns would come back NULL with no error. The large values stay out of line after
    /// the update, so decoding them did not make the update write them again.
    /// </summary>
    [TestCase("RETURNING *")]
    [TestCase("RETURNING body, payload, n")]
    public async Task CarriedLargeValuesAreReturnedWhole(string returning)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname,
            "CREATE TABLE docs (id oid NOT NULL DEFAULT(gen_id()), label string, n int64, body string STORAGE external, payload bytes STORAGE external, PRIMARY KEY (id))");

        string body = Incompressible(200_000);
        byte[] blob = IncompressibleBytes(150_000);
        await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO docs (label, n, body, payload) VALUES ('big', 1, @body, @blob)",
            new() { ["@body"] = new ColumnValue(ColumnType.String, body), ["@blob"] = new ColumnValue(blob) });

        int keysBefore = await LargeValueKeyCount(database, "docs");
        Assert.That(keysBefore, Is.GreaterThanOrEqualTo(2), "the test needs both large values out of line");

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            $"UPDATE docs SET n = 2 WHERE label = 'big' {returning}");

        Assert.AreEqual(1, result.ReturningRows!.Count);
        Assert.AreEqual(2, Cell(result, 0, "n").LongValue);
        Assert.AreEqual(body, Cell(result, 0, "body").StrValue);
        CollectionAssert.AreEqual(blob, Cell(result, 0, "payload").BytesValue);

        Assert.AreEqual(keysBefore, await LargeValueKeyCount(database, "docs"), "the update must keep the large values where they were");

        List<QueryResultRow> stored = await Select(executor, database, dbname, "SELECT n, body, payload FROM docs WHERE label = 'big'");
        Assert.AreEqual(2, stored[0].Row["n"].LongValue);
        Assert.AreEqual(body, stored[0].Row["body"].StrValue);
        CollectionAssert.AreEqual(blob, stored[0].Row["payload"].BytesValue);
    }

    // -----------------------------------------------------------------------
    // Concurrency
    // -----------------------------------------------------------------------

    /// <summary>
    /// A competing transaction commits between the locate scan and the write phase: it moves one
    /// located row out of the WHERE and changes another one. The write phase locks, reads again and
    /// re-checks, so the first row is not changed, not counted and not returned, and the second row
    /// returns the value computed from the competing commit.
    /// </summary>
    [Test]
    public async Task ARowAConcurrentCommitChangedReturnsTheLockedReadOnly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname, "CREATE TABLE tasks (id oid NOT NULL DEFAULT(gen_id()), name string, state string, v int64, PRIMARY KEY (id))");
        await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO tasks (name, state, v) VALUES ('a', 'open', 1), ('b', 'open', 1)");

        RowUpdater updater = executor.RowUpdaterForTests;
        bool hookRan = false;

        updater.TestBeforeWriteHook = async () =>
        {
            updater.TestBeforeWriteHook = null;
            hookRan = true;
            await NonQueryCommitted(executor, database, dbname, "UPDATE tasks SET state = 'closed' WHERE name = 'a'");
            await NonQueryCommitted(executor, database, dbname, "UPDATE tasks SET v = 100 WHERE name = 'b'");
        };

        ExecuteNonSQLResult result;
        KvTransaction tx = await database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted, locking: KeyValueTransactionLocking.Pessimistic);
        try
        {
            result = await NonQuery(executor, dbname, tx,
                "UPDATE tasks SET v = v + 1 WHERE state = 'open' RETURNING name, v");
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            updater.TestBeforeWriteHook = null;
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        Assert.IsTrue(hookRan, "the competing commit never ran inside the write window");
        Assert.AreEqual(1, result.ModifiedRows);
        Assert.AreEqual(1, result.ReturningRows!.Count);
        Assert.AreEqual("b", Cell(result, 0, 0).StrValue);
        Assert.AreEqual(101, Cell(result, 0, 1).LongValue);

        List<QueryResultRow> stored = await Select(executor, database, dbname, "SELECT name, v FROM tasks");
        Dictionary<string, long> values = stored.ToDictionary(r => r.Row["name"].StrValue!, r => r.Row["v"].LongValue);
        Assert.AreEqual(1, values["a"]);
        Assert.AreEqual(101, values["b"]);
    }

    /// <summary>
    /// A comparison in the list returns Bool cells and declares a Bool column, so a client that reads
    /// values by the declared type gets the type the cells have. The declared type must be right also
    /// when no row matches, because then no cell exists to show the type.
    /// </summary>
    [Test]
    public async Task ABooleanExpressionDeclaresABoolColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "UPDATE robots SET year = year - 2002 WHERE year >= 2002 RETURNING name, year > 0 AS positive");

        Assert.AreEqual(ColumnType.Bool, result.ReturningColumns![1].Type);
        Dictionary<string, bool> positive = result.ReturningRows!.ToDictionary(
            r => Cell(r, result.ReturningColumns[0]).StrValue!,
            r =>
            {
                ColumnValue cell = Cell(r, result.ReturningColumns[1]);
                Assert.AreEqual(ColumnType.Bool, cell.Type);
                return cell.BoolValue;
            });
        Assert.IsFalse(positive["beta"]);
        Assert.IsTrue(positive["gamma"]);

        ExecuteNonSQLResult empty = await NonQueryCommitted(executor, database, dbname,
            "UPDATE robots SET year = 1 WHERE year > 3000 RETURNING year > 0 AS positive");
        Assert.AreEqual(0, empty.ReturningRows!.Count);
        Assert.AreEqual(ColumnType.Bool, empty.ReturningColumns![0].Type);

        (IReadOnlyList<DerivedColumnSchema> selectSchema, List<QueryResultRow> selectRows) = await QueryCommitted(executor, database, dbname,
            "SELECT year > 0 AS positive FROM robots WHERE year > 3000");
        Assert.AreEqual(0, selectRows.Count);
        Assert.AreEqual(ColumnType.Bool, selectSchema[0].Type, "SELECT shares the inference");
    }

    /// <summary>
    /// A list without <c>*</c> projects from rows that hold only the columns it reads. A constant reads
    /// no column at all, and a column whose stored value is NULL must still come back NULL, not missing.
    /// </summary>
    [Test]
    public async Task ANarrowListReturnsConstantsAndNulls()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await NonQueryCommitted(executor, database, dbname, "UPDATE robots SET note = NULL WHERE name = 'alpha'");

        ExecuteNonSQLResult only = await NonQueryCommitted(executor, database, dbname,
            "UPDATE robots SET year = 5 WHERE name = 'beta' RETURNING 1 AS one");
        Assert.AreEqual(1, only.ReturningRows!.Count);
        Assert.AreEqual(1, Cell(only, 0, "one").LongValue);

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "UPDATE robots SET year = 5 WHERE name = 'alpha' RETURNING 1 AS one, note, year");
        Assert.AreEqual(1, result.ReturningRows!.Count);
        Assert.AreEqual(1, Cell(result, 0, "one").LongValue);
        Assert.AreEqual(ColumnType.Null, Cell(result, 0, "note").Type);
        Assert.AreEqual(5, Cell(result, 0, "year").LongValue);
    }

    // -----------------------------------------------------------------------
    // Cluster: a second executor stands in for a follower
    // -----------------------------------------------------------------------

    /// <summary>
    /// A second executor over the same shared node has its own descriptor cache, as a follower does.
    /// It opens the table before the first executor adds a column through the replicated schema log.
    /// The statement it runs binds RETURNING against the table its attempt opened, so <c>*</c> includes
    /// the new column with its default, and the change is visible through the first executor.
    /// </summary>
    [Test]
    public async Task AFollowerThatOpenedTheTableBeforeASchemaChangeReturnsTheNewColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor leader) = await SetupRobots();

        CommandExecutor follower = CreateCommandExecutor();
        TrackDatabase(dbname, follower);
        DatabaseDescriptor followerDatabase = await follower.OpenDatabase(dbname);
        _ = await follower.OpenTable(new OpenTableTicket(dbname, "robots"));

        await Ddl(leader, dbname, "ALTER TABLE robots ADD COLUMN grade int64 DEFAULT (7)");

        ExecuteNonSQLResult result = await NonQueryCommitted(follower, followerDatabase, dbname,
            "UPDATE robots SET year = 1 WHERE name = 'beta' RETURNING *");

        CollectionAssert.AreEqual(new[] { "id", "name", "year", "note", "grade" }, ColumnNames(result.ReturningColumns!));
        Assert.AreEqual(1, result.ReturningRows!.Count);
        Assert.AreEqual("beta", Cell(result, 0, "name").StrValue);
        Assert.AreEqual("b-note", Cell(result, 0, "note").StrValue);
        Assert.AreEqual(7, Cell(result, 0, "grade").LongValue);
        Assert.AreEqual(1, Cell(result, 0, "year").LongValue);

        List<QueryResultRow> seenByLeader = await Select(leader, database, dbname, "SELECT name FROM robots WHERE year = 2002 OR year = 1");
        Assert.AreEqual(1, seenByLeader.Count(r => r.Row["name"].StrValue == "beta"));
    }

    // -----------------------------------------------------------------------
    // Refusals and failures
    // -----------------------------------------------------------------------

    [TestCase("UPDATE robots SET year = 1 WHERE name = 'alpha' RETURNING missing", CamusDBErrorCodes.UnknownColumn)]
    [TestCase("UPDATE robots SET year = 1 WHERE name = 'alpha' RETURNING COUNT(*)", CamusDBErrorCodes.InvalidInput)]
    [TestCase("UPDATE robots SET year = 1 WHERE name = 'alpha' RETURNING year + COUNT(*)", CamusDBErrorCodes.InvalidInput)]
    [TestCase("UPDATE robots SET year = 1 WHERE name = 'alpha' RETURNING (SELECT COUNT(*) FROM robots)", CamusDBErrorCodes.InvalidInput)]
    [TestCase("UPDATE robots SET year = 1 WHERE name = 'alpha' RETURNING nextval('robot_no')", CamusDBErrorCodes.SequenceCallNotAllowedHere)]
    public async Task RefusedListsFailBeforeAnyRowIsChanged(string sql, string expectedCode)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await Ddl(executor, dbname, "CREATE SEQUENCE robot_no");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(
            async () => await NonQueryCommitted(executor, database, dbname, sql));
        Assert.AreEqual(expectedCode, error!.Code, error.Message);

        // The same refusal with the count-only flag: the list is still checked.
        CamusDBException? discarded = Assert.ThrowsAsync<CamusDBException>(
            async () => await NonQueryCommitted(executor, database, dbname, sql, discardReturningRows: true));
        Assert.AreEqual(expectedCode, discarded!.Code, discarded.Message);

        List<QueryResultRow> stored = await Select(executor, database, dbname, "SELECT year FROM robots WHERE name = 'alpha'");
        Assert.AreEqual(2001, stored[0].Row["year"].LongValue);
    }

    [Test]
    public async Task AUniqueViolationReturnsNothingAndChangesNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname, "CREATE TABLE codes (id oid NOT NULL DEFAULT(gen_id()), code string, PRIMARY KEY (id))");
        await Ddl(executor, dbname, "CREATE UNIQUE INDEX ux_code ON codes (code)");
        await NonQueryCommitted(executor, database, dbname, "INSERT INTO codes (code) VALUES ('a'), ('b')");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            await NonQueryCommitted(executor, database, dbname, "UPDATE codes SET code = 'a' WHERE code = 'b' RETURNING code"));
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, error!.Code, error.Message);

        List<QueryResultRow> stored = await Select(executor, database, dbname, "SELECT code FROM codes");
        CollectionAssert.AreEquivalent(new[] { "a", "b" }, stored.Select(r => r.Row["code"].StrValue));
    }

    [Test]
    public async Task ACheckViolationReturnsNothingAndChangesNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname, "CREATE TABLE stock (id oid NOT NULL DEFAULT(gen_id()), qty int64 CHECK (qty >= 0), PRIMARY KEY (id))");
        await NonQueryCommitted(executor, database, dbname, "INSERT INTO stock (qty) VALUES (5)");

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await NonQueryCommitted(executor, database, dbname, "UPDATE stock SET qty = -1 WHERE qty = 5 RETURNING qty"));

        List<QueryResultRow> stored = await Select(executor, database, dbname, "SELECT qty FROM stock");
        Assert.AreEqual(5, stored[0].Row["qty"].LongValue);
    }

    [Test]
    public async Task ARollbackUndoesTheReturnedChange()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult result = await NonQuery(executor, dbname, tx,
            "UPDATE robots SET year = 1 WHERE name = 'alpha' RETURNING year");
        Assert.AreEqual(1, Cell(result, 0, 0).LongValue);
        await database.Transactions.RollbackAsync(tx);

        List<QueryResultRow> stored = await Select(executor, database, dbname, "SELECT year FROM robots WHERE name = 'alpha'");
        Assert.AreEqual(2001, stored[0].Row["year"].LongValue);
    }

    // -----------------------------------------------------------------------
    // Entry points and the count-only flag
    // -----------------------------------------------------------------------

    [Test]
    public async Task TheQueryEntryPointReturnsTheSameRowsAndSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) = await QueryCommitted(executor, database, dbname,
            "UPDATE robots SET year = 1 WHERE name = 'gamma' RETURNING name, year");

        CollectionAssert.AreEqual(new[] { "name", "year" }, ColumnNames(schema));
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("gamma", Cell(rows[0], schema[0]).StrValue);
        Assert.AreEqual(1, Cell(rows[0], schema[1]).LongValue);

        List<QueryResultRow> stored = await Select(executor, database, dbname, "SELECT year FROM robots WHERE name = 'gamma'");
        Assert.AreEqual(1, stored[0].Row["year"].LongValue);
    }

    [Test]
    public async Task TheCountOnlyFlagUpdatesAndReturnsTheCountOnly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "UPDATE robots SET year = 7 WHERE year > 0 RETURNING *", discardReturningRows: true);

        Assert.AreEqual(3, result.ModifiedRows);
        Assert.IsNull(result.ReturningColumns);
        Assert.IsNull(result.ReturningRows);
        Assert.AreEqual(3, (await Select(executor, database, dbname, "SELECT id FROM robots WHERE year = 7")).Count);
    }
}
