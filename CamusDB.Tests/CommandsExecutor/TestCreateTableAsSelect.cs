
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for <c>CREATE TABLE … AS SELECT</c>: the schema it derives from the source
/// query, the synthesized primary key, <c>WITH NO DATA</c>, <c>IF NOT EXISTS</c>, the projections it
/// refuses because they cannot become a named typed column, and the compensating drop that keeps a
/// failed load from leaving a table behind.
/// </summary>
public sealed class TestCreateTableAsSelect : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupSource(int rows = 4)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "src",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        for (int i = 0; i < rows; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "src",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "robot " + i) },
                        { "year", new(ColumnType.Integer64, 2000 + i) },
                    }
                }
            ));
        }

        await database.Transactions.CommitAsync(txn);

        return (dbname, database, executor);
    }

    private static Task<ExecuteDDLSQLResult> Ddl(
        CommandExecutor executor, string dbname, KvTransaction txn, string sql)
        => executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: txn, database: dbname, sql: sql, parameters: null));

    private static async Task<List<QueryResultRow>> Query(
        CommandExecutor executor, string dbname, KvTransaction txn, string sql)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: dbname, sql: sql, parameters: null));

        return await cursor.ToListAsync();
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreatesTableAndLoadsEveryRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 4);

        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteDDLSQLResult result = await Ddl(executor, dbname, txn,
            "CREATE TABLE copied AS SELECT name, year FROM src");

        Assert.IsTrue(result.Success);
        Assert.AreEqual(4, result.ModifiedRows);

        List<QueryResultRow> rows = await Query(executor, dbname, txn, "SELECT * FROM copied");

        Assert.AreEqual(4, rows.Count);
        CollectionAssert.AreEquivalent(
            new[] { "robot 0", "robot 1", "robot 2", "robot 3" },
            rows.Select(r => r.Row["name"].StrValue));

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// The derived column types must be the types the same SELECT would report, and the synthesized
    /// key is an oid that is present, non-null and distinct per row.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestDerivedColumnTypesAndSynthesizedKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 3);

        KvTransaction txn = await database.Transactions.BeginAsync();

        await Ddl(executor, dbname, txn, "CREATE TABLE copied AS SELECT name, year FROM src");

        List<QueryResultRow> columns = await Query(executor, dbname, txn, "SHOW COLUMNS FROM copied");

        // The synthesized key leads, then the projected columns in order.
        CollectionAssert.AreEqual(
            new[] { "id", "name", "year" },
            columns.Select(c => c.Row["Field"].StrValue));

        Assert.AreEqual("OID", columns[0].Row["Type"].StrValue, "the synthesized key is an oid");
        Assert.AreEqual("STRING", columns[1].Row["Type"].StrValue);
        Assert.AreEqual("INT64", columns[2].Row["Type"].StrValue);

        List<QueryResultRow> rows = await Query(executor, dbname, txn, "SELECT * FROM copied");
        Assert.AreEqual(3, rows.Select(r => r.Row["id"].StrValue).Distinct().Count(),
            "every row gets its own generated key");

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// When the source already projects a column called <c>id</c>, the synthesized key must not
    /// collide with it — it takes the next free name and the projected column keeps its own values.
    /// The fallback must also avoid <c>_id</c>, which the create-table validator reserves.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestSynthesizedKeyAvoidsCollisionWithProjectedId()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 2);

        KvTransaction txn = await database.Transactions.BeginAsync();

        await Ddl(executor, dbname, txn, "CREATE TABLE copied AS SELECT id, name FROM src");

        List<QueryResultRow> columns = await Query(executor, dbname, txn, "SHOW COLUMNS FROM copied");

        CollectionAssert.AreEqual(
            new[] { "id2", "id", "name" },
            columns.Select(c => c.Row["Field"].StrValue));

        List<QueryResultRow> source = await Query(executor, dbname, txn, "SELECT * FROM src");
        List<QueryResultRow> copied = await Query(executor, dbname, txn, "SELECT * FROM copied");

        CollectionAssert.AreEquivalent(
            source.Select(r => r.Row["id"].StrValue),
            copied.Select(r => r.Row["id"].StrValue),
            "the projected id keeps the source's values");

        await database.Transactions.CommitAsync(txn);
    }

    [Test]
    [NonParallelizable]
    public async Task TestAggregateSourceTypesTheColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 5);

        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteDDLSQLResult result = await Ddl(executor, dbname, txn,
            "CREATE TABLE counted AS SELECT COUNT(*) AS total FROM src");

        Assert.AreEqual(1, result.ModifiedRows);

        List<QueryResultRow> rows = await Query(executor, dbname, txn, "SELECT * FROM counted");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(5, rows[0].Row["total"].LongValue);

        await database.Transactions.CommitAsync(txn);
    }

    [Test]
    [NonParallelizable]
    public async Task TestWithNoDataCreatesAnEmptyTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 4);

        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteDDLSQLResult result = await Ddl(executor, dbname, txn,
            "CREATE TABLE empty_copy AS SELECT name, year FROM src WITH NO DATA");

        Assert.IsTrue(result.Success);
        Assert.AreEqual(0, result.ModifiedRows);

        Assert.IsEmpty(await Query(executor, dbname, txn, "SELECT * FROM empty_copy"));

        // The schema is still derived from the source.
        List<QueryResultRow> columns = await Query(executor, dbname, txn, "SHOW COLUMNS FROM empty_copy");
        CollectionAssert.AreEqual(new[] { "id", "name", "year" }, columns.Select(c => c.Row["Field"].StrValue));

        await database.Transactions.CommitAsync(txn);
    }

    [Test]
    [NonParallelizable]
    public async Task TestWithDataIsTheDefaultAndMayBeExplicit()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 3);

        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteDDLSQLResult result = await Ddl(executor, dbname, txn,
            "CREATE TABLE copied AS SELECT name FROM src WITH DATA");

        Assert.AreEqual(3, result.ModifiedRows);

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// IF NOT EXISTS over an existing table is a no-op that must not run the source query — asserted
    /// by pointing the source at the target itself, which would otherwise copy rows into it.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestIfNotExistsOverExistingTableDoesNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 4);

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.AreEqual(4, (await Ddl(executor, dbname, txn,
            "CREATE TABLE copied AS SELECT name FROM src")).ModifiedRows);

        ExecuteDDLSQLResult second = await Ddl(executor, dbname, txn,
            "CREATE TABLE IF NOT EXISTS copied AS SELECT name FROM copied");

        Assert.IsFalse(second.Success);
        Assert.AreEqual(0, second.ModifiedRows);
        Assert.AreEqual(4, (await Query(executor, dbname, txn, "SELECT * FROM copied")).Count,
            "the source query must not have run");

        await database.Transactions.CommitAsync(txn);
    }

    [Test]
    [NonParallelizable]
    public async Task TestCreatingAnExistingTableWithoutIfNotExistsFails()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 2);

        KvTransaction txn = await database.Transactions.BeginAsync();

        await Ddl(executor, dbname, txn, "CREATE TABLE copied AS SELECT name FROM src");

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await Ddl(executor, dbname, txn, "CREATE TABLE copied AS SELECT name FROM src"));

        await database.Transactions.CommitAsync(txn);
    }

    [Test]
    [NonParallelizable]
    public async Task TestUnaliasedExpressionIsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 2);

        KvTransaction txn = await database.Transactions.BeginAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Ddl(executor, dbname, txn, "CREATE TABLE bad AS SELECT year + 1 FROM src"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("alias", ex.Message);

        // Nothing was created.
        Assert.ThrowsAsync<CamusDBException>(async () => await Query(executor, dbname, txn, "SELECT * FROM bad"));

        await database.Transactions.CommitAsync(txn);
    }

    [Test]
    [NonParallelizable]
    public async Task TestNullProjectionIsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 2);

        KvTransaction txn = await database.Transactions.BeginAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Ddl(executor, dbname, txn, "CREATE TABLE bad AS SELECT NULL AS x FROM src"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("CAST", ex.Message);

        await database.Transactions.CommitAsync(txn);
    }

    [Test]
    [NonParallelizable]
    public async Task TestDuplicateOutputNamesAreRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 2);

        KvTransaction txn = await database.Transactions.BeginAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Ddl(executor, dbname, txn, "CREATE TABLE bad AS SELECT name, year AS name FROM src"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("more than one output column", ex.Message);

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// <c>SELECT *</c> over a join produces qualified output names, which cannot be column names.
    /// The statement is refused with an explanation rather than creating odd columns.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestSelectStarOverJoinIsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 2);

        KvTransaction setup = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "labels",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        await database.Transactions.CommitAsync(setup);

        KvTransaction txn = await database.Transactions.BeginAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Ddl(executor, dbname, txn,
                "CREATE TABLE bad AS SELECT * FROM src s JOIN labels l ON s.year = l.year"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("join", ex.Message);

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// An explicit, aliased projection over a join is the supported form and must work.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestAliasedJoinProjectionIsSupported()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 3);

        KvTransaction setup = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "labels",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("year", ColumnType.Integer64),
                new("label", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        await executor.Insert(new InsertTicket(
            txnState: setup,
            databaseName: dbname,
            tableName: "labels",
            values: new()
            {
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "year", new(ColumnType.Integer64, 2001) },
                    { "label", new(ColumnType.String, "the-label") },
                }
            }
        ));

        await database.Transactions.CommitAsync(setup);

        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteDDLSQLResult result = await Ddl(executor, dbname, txn,
            "CREATE TABLE joined AS SELECT s.name AS robot, l.label AS label " +
            "FROM src s JOIN labels l ON s.year = l.year");

        Assert.AreEqual(1, result.ModifiedRows);

        List<QueryResultRow> rows = await Query(executor, dbname, txn, "SELECT * FROM joined");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("robot 1", rows[0].Row["robot"].StrValue);
        Assert.AreEqual("the-label", rows[0].Row["label"].StrValue);

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// Nothing but the shape is inherited: the source's NOT NULL and its indexes must not appear on
    /// the new table, which is what standard CTAS does.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestConstraintsAndIndexesAreNotInherited()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 2);

        KvTransaction setup = await database.Transactions.BeginAsync();
        await Ddl(executor, dbname, setup, "CREATE INDEX src_year ON src (year)");
        await database.Transactions.CommitAsync(setup);

        KvTransaction txn = await database.Transactions.BeginAsync();

        await Ddl(executor, dbname, txn, "CREATE TABLE copied AS SELECT name, year FROM src");

        // src.name is NOT NULL; the copy's is nullable, so a NULL update succeeds.
        List<QueryResultRow> columns = await Query(executor, dbname, txn, "SHOW COLUMNS FROM copied");
        Assert.AreEqual("YES", columns.Single(c => c.Row["Field"].StrValue == "name").Row["Null"].StrValue,
            "NOT NULL is not inherited");

        // Only the synthesized primary key exists — the source's secondary index is not copied.
        List<QueryResultRow> indexes = await Query(executor, dbname, txn, "SHOW INDEXES FROM copied");
        CollectionAssert.DoesNotContain(indexes.Select(i => i.Row["Key_name"].StrValue), "src_year");

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// A load that fails must not leave the table behind: the compensating drop runs, so the name is
    /// free again and a corrected statement can reuse it. Driven by a mutation ceiling small enough
    /// that the copy cannot fit.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestFailedLoadDropsTheTableItCreated()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { MaxMutationsPerTransaction = 2 });

        KvTransaction setup = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "src",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        await database.Transactions.CommitAsync(setup);

        for (int i = 0; i < 5; i++)
        {
            KvTransaction seed = await database.Transactions.BeginAsync();
            await executor.Insert(new InsertTicket(
                txnState: seed,
                databaseName: dbname,
                tableName: "src",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "row " + i) },
                    }
                }
            ));
            await database.Transactions.CommitAsync(seed);
        }

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await Ddl(executor, dbname, txn, "CREATE TABLE copied AS SELECT name FROM src"));

        await database.Transactions.RollbackAsync(txn);

        // The table must be gone — querying it fails rather than returning an empty result.
        KvTransaction verify = await database.Transactions.BeginAsync();
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await Query(executor, dbname, verify, "SELECT * FROM copied"));
        Assert.AreEqual(CamusDBErrorCodes.TableDoesntExist, ex!.Code);
        await database.Transactions.CommitAsync(verify);
    }

    /// <summary>
    /// CTAS is reachable through the no-rows endpoint too, because clients route any non-SELECT
    /// statement to whichever one they use for those.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestReachableThroughTheNonQueryEndpoint()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 3);

        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(
            txnState: txn, database: dbname, sql: "CREATE TABLE copied AS SELECT name FROM src", parameters: null));

        Assert.AreEqual(3, result.ModifiedRows);
        Assert.AreEqual(3, (await Query(executor, dbname, txn, "SELECT * FROM copied")).Count);

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// A time-travel source is accepted and creates the table from the historical result — the
    /// recovery path, covered in depth by <see cref="TestInsertSelectAsOfSystemTime"/>. A snapshot
    /// predating this test's database yields an empty (but correctly shaped) table.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestAsOfSystemTimeSourceIsAccepted()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 2);

        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteDDLSQLResult result = await Ddl(executor, dbname, txn,
            "CREATE TABLE recovered AS SELECT name FROM src AS OF SYSTEM TIME '-1h'");

        Assert.IsTrue(result.Success);
        Assert.AreEqual(0, result.ModifiedRows);

        CollectionAssert.AreEqual(
            new[] { "id", "name" },
            (await Query(executor, dbname, txn, "SHOW COLUMNS FROM recovered")).Select(c => c.Row["Field"].StrValue));

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// The created table and its rows survive a close/reopen of the database — the schema was really
    /// persisted, not just registered in memory.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestCreatedTableSurvivesReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupSource(rows: 3);

        KvTransaction txn = await database.Transactions.BeginAsync();
        await Ddl(executor, dbname, txn, "CREATE TABLE copied AS SELECT name, year FROM src");
        await database.Transactions.CommitAsync(txn);

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));

        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);
        KvTransaction after = await reopened.Transactions.BeginAsync();

        List<QueryResultRow> rows = await Query(executor, dbname, after, "SELECT * FROM copied");
        Assert.AreEqual(3, rows.Count);

        List<QueryResultRow> columns = await Query(executor, dbname, after, "SHOW COLUMNS FROM copied");
        CollectionAssert.AreEqual(new[] { "id", "name", "year" }, columns.Select(c => c.Row["Field"].StrValue));

        await reopened.Transactions.CommitAsync(after);
    }
}
