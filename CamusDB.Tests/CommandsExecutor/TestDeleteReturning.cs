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

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

using static CamusDB.Tests.CommandsExecutor.WriteReturningTestSql;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for <c>DELETE … RETURNING</c> through the engine's two SQL entry points. A
/// DELETE returns each row it removed. The delete path decodes only the columns it needs from each
/// row, so several tests return columns that are in no index and not in the WHERE: those values must
/// come back, not NULL.
/// </summary>
[NonParallelizable]
public sealed class TestDeleteReturning : SharedNodeBaseTest
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
    public async Task ReturnsEachDeletedRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        List<QueryResultRow> before = await Select(executor, database, dbname, "SELECT id, name FROM robots WHERE year >= 2002");

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "DELETE FROM robots WHERE year >= 2002 RETURNING id, name, year");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEqual(new[] { "id", "name", "year" }, ColumnNames(result.ReturningColumns!));
        Assert.AreEqual(2, result.ReturningRows!.Count);
        CollectionAssert.AreEqual(new[] { "beta", "gamma" }, SortedStrings(result, "name"));
        CollectionAssert.AreEquivalent(
            before.Select(r => r.Row["id"].StrValue),
            result.ReturningRows.Select(r => Cell(r, result.ReturningColumns![0]).StrValue));

        List<QueryResultRow> remaining = await Select(executor, database, dbname, "SELECT name FROM robots");
        CollectionAssert.AreEqual(new[] { "alpha" }, remaining.Select(r => r.Row["name"].StrValue));
    }

    [Test]
    public async Task WithoutReturningTheResultCarriesNoRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "DELETE FROM robots WHERE name = 'alpha'");

        Assert.AreEqual(1, result.ModifiedRows);
        Assert.IsNull(result.ReturningColumns);
        Assert.IsNull(result.ReturningRows);
    }

    /// <summary>
    /// The WHERE reads only <c>name</c> and the table has no secondary index, so the delete decodes
    /// <c>name</c> alone unless it widens the decode for RETURNING. <c>*</c> and a named column that
    /// is not in the WHERE must both come back with their stored values.
    /// </summary>
    [TestCase("RETURNING *")]
    [TestCase("RETURNING note, year")]
    public async Task ColumnsOutsideTheWhereAreReturned(string returning)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            $"DELETE FROM robots WHERE name = 'beta' {returning}");

        Assert.AreEqual(1, result.ReturningRows!.Count);
        Assert.AreEqual("b-note", Cell(result, 0, "note").StrValue);
        Assert.AreEqual(2002, Cell(result, 0, "year").LongValue);
    }

    [Test]
    public async Task ExpressionsAliasesPlaceholdersAndQualifiedNamesAreEvaluatedOnTheDeletedRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        Dictionary<string, ColumnValue> parameters = new() { ["@shift"] = new ColumnValue(ColumnType.Integer64, 100) };

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "DELETE FROM robots WHERE name = 'alpha' " +
            "RETURNING year * 2 AS doubled, year + @shift AS shifted, robots.note, NAME",
            parameters);

        CollectionAssert.AreEqual(new[] { "doubled", "shifted", "note", "NAME" }, ColumnNames(result.ReturningColumns!));
        Assert.AreEqual(4002, Cell(result, 0, 0).LongValue);
        Assert.AreEqual(2101, Cell(result, 0, 1).LongValue);
        Assert.AreEqual("a-note", Cell(result, 0, 2).StrValue);
        Assert.AreEqual("alpha", Cell(result, 0, 3).StrValue);
    }

    [Test]
    public async Task NoMatchReturnsTheSchemaAndNoRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "DELETE FROM robots WHERE year > 3000 RETURNING id, name");

        Assert.AreEqual(0, result.ModifiedRows);
        CollectionAssert.AreEqual(new[] { "id", "name" }, ColumnNames(result.ReturningColumns!));
        Assert.IsNotNull(result.ReturningRows);
        Assert.AreEqual(0, result.ReturningRows!.Count);
    }

    [Test]
    public async Task LimitReturnsOnlyTheDeletedRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "DELETE FROM robots WHERE year > 0 LIMIT 2 RETURNING name");

        Assert.AreEqual(2, result.ModifiedRows);
        Assert.AreEqual(2, result.ReturningRows!.Count);

        List<QueryResultRow> remaining = await Select(executor, database, dbname, "SELECT name FROM robots");
        Assert.AreEqual(1, remaining.Count);
        CollectionAssert.DoesNotContain(SortedStrings(result, "name"), remaining[0].Row["name"].StrValue);
    }

    [Test]
    public async Task ManyChunksReturnEveryRowOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { SpillEnabled = true, ForceSpillThresholdRows = 2 });
        await Ddl(executor, dbname, "CREATE TABLE items (id oid NOT NULL DEFAULT(gen_id()), n int64, tag string, PRIMARY KEY (id))");
        await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO items (n, tag) VALUES (1, 'x'), (2, 'x'), (3, 'x'), (4, 'x'), (5, 'x'), (6, 'x'), (7, 'x')");

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "DELETE FROM items WHERE n > 0 RETURNING n, tag");

        Assert.AreEqual(7, result.ModifiedRows);
        CollectionAssert.AreEquivalent(
            new long[] { 1, 2, 3, 4, 5, 6, 7 },
            result.ReturningRows!.Select(r => Cell(r, result.ReturningColumns![0]).LongValue));
        Assert.IsTrue(result.ReturningRows!.All(r => Cell(r, result.ReturningColumns![1]).StrValue == "x"));
    }

    /// <summary>
    /// A deleted row's large values are stored out of line. The delete normally names their keys
    /// without fetching them; with RETURNING it must fetch the ones the list reads and return them whole.
    /// </summary>
    [Test]
    public async Task LargeValuesAreReturnedWhole()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname,
            "CREATE TABLE docs (id oid NOT NULL DEFAULT(gen_id()), label string, body string STORAGE external, payload bytes STORAGE external, PRIMARY KEY (id))");

        string body = Incompressible(200_000);
        byte[] payload = IncompressibleBytes(150_000);
        await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO docs (label, body, payload) VALUES ('big', @body, @payload)",
            new() { ["@body"] = new ColumnValue(ColumnType.String, body), ["@payload"] = new ColumnValue(payload) });

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "DELETE FROM docs WHERE label = 'big' RETURNING body, payload");

        Assert.AreEqual(1, result.ReturningRows!.Count);
        Assert.AreEqual(body, Cell(result, 0, "body").StrValue);
        CollectionAssert.AreEqual(payload, Cell(result, 0, "payload").BytesValue);
        Assert.AreEqual(0, (await Select(executor, database, dbname, "SELECT id FROM docs")).Count);
    }

    /// <summary>
    /// A WHERE with an <c>IN (SELECT …)</c> is rewritten before the delete runs; the RETURNING list
    /// still projects the removed rows.
    /// </summary>
    [Test]
    public async Task AWhereSubqueryStillReturnsTheRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await Ddl(executor, dbname, "CREATE TABLE retired (id oid NOT NULL DEFAULT(gen_id()), name string, PRIMARY KEY (id))");
        await NonQueryCommitted(executor, database, dbname, "INSERT INTO retired (name) VALUES ('alpha'), ('gamma')");

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "DELETE FROM robots WHERE name IN (SELECT name FROM retired) RETURNING name, note");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEqual(new[] { "alpha", "gamma" }, SortedStrings(result, "name"));
        CollectionAssert.AreEqual(new[] { "a-note", "g-note" }, SortedStrings(result, "note"));
    }

    // -----------------------------------------------------------------------
    // Concurrency
    // -----------------------------------------------------------------------

    /// <summary>
    /// A competing transaction commits between the locate scan and the write phase: it moves one
    /// located row out of the WHERE and deletes another one. The write phase locks, reads again and
    /// re-checks, so neither row is deleted by this statement, counted or returned.
    /// </summary>
    [Test]
    public async Task RowsAConcurrentCommitMovedOrDeletedAreNotReturned()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname, "CREATE TABLE tasks (id oid NOT NULL DEFAULT(gen_id()), name string, state string, PRIMARY KEY (id))");
        await NonQueryCommitted(executor, database, dbname,
            "INSERT INTO tasks (name, state) VALUES ('a', 'done'), ('b', 'done'), ('c', 'done')");

        RowDeleter deleter = executor.RowDeleterForTests;
        bool hookRan = false;

        deleter.TestBeforeWriteHook = async () =>
        {
            deleter.TestBeforeWriteHook = null;
            hookRan = true;
            await NonQueryCommitted(executor, database, dbname, "UPDATE tasks SET state = 'open' WHERE name = 'a'");
            await NonQueryCommitted(executor, database, dbname, "DELETE FROM tasks WHERE name = 'b'");
        };

        ExecuteNonSQLResult result;
        KvTransaction tx = await database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted, locking: KeyValueTransactionLocking.Pessimistic);
        try
        {
            result = await NonQuery(executor, dbname, tx, "DELETE FROM tasks WHERE state = 'done' RETURNING name");
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            deleter.TestBeforeWriteHook = null;
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        Assert.IsTrue(hookRan, "the competing commit never ran inside the write window");
        Assert.AreEqual(1, result.ModifiedRows);
        CollectionAssert.AreEqual(new[] { "c" }, SortedStrings(result, "name"));

        List<QueryResultRow> remaining = await Select(executor, database, dbname, "SELECT name FROM tasks");
        CollectionAssert.AreEqual(new[] { "a" }, remaining.Select(r => r.Row["name"].StrValue));
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
            "DELETE FROM robots WHERE year >= 2002 RETURNING name, year > 2002 AS positive");

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
            "DELETE FROM robots WHERE year > 3000 RETURNING year > 0 AS positive");
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
            "DELETE FROM robots WHERE name = 'beta' RETURNING 1 AS one");
        Assert.AreEqual(1, only.ReturningRows!.Count);
        Assert.AreEqual(1, Cell(only, 0, "one").LongValue);

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "DELETE FROM robots WHERE name = 'alpha' RETURNING 1 AS one, note, year");
        Assert.AreEqual(1, result.ReturningRows!.Count);
        Assert.AreEqual(1, Cell(result, 0, "one").LongValue);
        Assert.AreEqual(ColumnType.Null, Cell(result, 0, "note").Type);
        Assert.AreEqual(2001, Cell(result, 0, "year").LongValue);
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
            "DELETE FROM robots WHERE name = 'beta' RETURNING *");

        CollectionAssert.AreEqual(new[] { "id", "name", "year", "note", "grade" }, ColumnNames(result.ReturningColumns!));
        Assert.AreEqual(1, result.ReturningRows!.Count);
        Assert.AreEqual("beta", Cell(result, 0, "name").StrValue);
        Assert.AreEqual("b-note", Cell(result, 0, "note").StrValue);
        Assert.AreEqual(7, Cell(result, 0, "grade").LongValue);
        Assert.AreEqual(2002, Cell(result, 0, "year").LongValue);

        List<QueryResultRow> seenByLeader = await Select(leader, database, dbname, "SELECT name FROM robots WHERE year = 2002 OR year = 1");
        Assert.AreEqual(0, seenByLeader.Count(r => r.Row["name"].StrValue == "beta"));
    }

    // -----------------------------------------------------------------------
    // Refusals and failures
    // -----------------------------------------------------------------------

    [TestCase("DELETE FROM robots WHERE name = 'alpha' RETURNING missing", CamusDBErrorCodes.UnknownColumn)]
    [TestCase("DELETE FROM robots WHERE name = 'alpha' RETURNING COUNT(*)", CamusDBErrorCodes.InvalidInput)]
    [TestCase("DELETE FROM robots WHERE name = 'alpha' RETURNING (SELECT COUNT(*) FROM robots)", CamusDBErrorCodes.InvalidInput)]
    [TestCase("DELETE FROM robots WHERE name = 'alpha' RETURNING nextval('robot_no')", CamusDBErrorCodes.SequenceCallNotAllowedHere)]
    public async Task RefusedListsFailBeforeAnyRowIsDeleted(string sql, string expectedCode)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();
        await Ddl(executor, dbname, "CREATE SEQUENCE robot_no");

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(
            async () => await NonQueryCommitted(executor, database, dbname, sql));
        Assert.AreEqual(expectedCode, error!.Code, error.Message);

        CamusDBException? discarded = Assert.ThrowsAsync<CamusDBException>(
            async () => await NonQueryCommitted(executor, database, dbname, sql, discardReturningRows: true));
        Assert.AreEqual(expectedCode, discarded!.Code, discarded.Message);

        Assert.AreEqual(3, (await Select(executor, database, dbname, "SELECT id FROM robots")).Count);
    }

    /// <summary>
    /// A parent row that a child row still references cannot be deleted: the statement fails, returns
    /// no rows, and deletes nothing.
    /// </summary>
    [Test]
    public async Task AForeignKeyViolationReturnsNothingAndDeletesNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname, "CREATE TABLE parents (id int64 PRIMARY KEY NOT NULL, name string)");
        await Ddl(executor, dbname, "CREATE TABLE children (id int64 PRIMARY KEY NOT NULL, parent_id int64 REFERENCES parents (id))");
        await NonQueryCommitted(executor, database, dbname, "INSERT INTO parents (id, name) VALUES (1, 'p1'), (2, 'p2')");
        await NonQueryCommitted(executor, database, dbname, "INSERT INTO children (id, parent_id) VALUES (10, 1)");

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await NonQueryCommitted(executor, database, dbname, "DELETE FROM parents WHERE id > 0 RETURNING name"));

        Assert.AreEqual(2, (await Select(executor, database, dbname, "SELECT id FROM parents")).Count);

        // The parent nothing references can still be deleted, and is returned.
        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname, "DELETE FROM parents WHERE id = 2 RETURNING name");
        Assert.AreEqual("p2", Cell(result, 0, 0).StrValue);
    }

    [Test]
    public async Task ARollbackRestoresTheReturnedRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteNonSQLResult result = await NonQuery(executor, dbname, tx, "DELETE FROM robots WHERE year > 0 RETURNING name");
        Assert.AreEqual(3, result.ReturningRows!.Count);
        await database.Transactions.RollbackAsync(tx);

        Assert.AreEqual(3, (await Select(executor, database, dbname, "SELECT id FROM robots")).Count);
    }

    // -----------------------------------------------------------------------
    // Entry points and the count-only flag
    // -----------------------------------------------------------------------

    [Test]
    public async Task TheQueryEntryPointReturnsTheSameRowsAndSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        (IReadOnlyList<DerivedColumnSchema> schema, List<QueryResultRow> rows) = await QueryCommitted(executor, database, dbname,
            "DELETE FROM robots WHERE name = 'gamma' RETURNING name, note");

        CollectionAssert.AreEqual(new[] { "name", "note" }, ColumnNames(schema));
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("gamma", Cell(rows[0], schema[0]).StrValue);
        Assert.AreEqual("g-note", Cell(rows[0], schema[1]).StrValue);
        Assert.AreEqual(2, (await Select(executor, database, dbname, "SELECT id FROM robots")).Count);
    }

    [Test]
    public async Task TheCountOnlyFlagDeletesAndReturnsTheCountOnly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobots();

        ExecuteNonSQLResult result = await NonQueryCommitted(executor, database, dbname,
            "DELETE FROM robots WHERE year > 0 RETURNING *", discardReturningRows: true);

        Assert.AreEqual(3, result.ModifiedRows);
        Assert.IsNull(result.ReturningColumns);
        Assert.IsNull(result.ReturningRows);
        Assert.AreEqual(0, (await Select(executor, database, dbname, "SELECT id FROM robots")).Count);
    }
}
