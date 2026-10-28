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

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for <c>INSERT … ON CONFLICT</c> and <c>UPSERT INTO</c> through
/// <see cref="CommandExecutor.ExecuteNonSQLQuery"/>. Every expected value is read back from storage
/// with a separate SELECT after the statement committed, so a statement that reports one thing and
/// stores another fails here.
/// </summary>
[NonParallelizable]
public sealed class TestInsertOnConflict : SharedNodeBaseTest
{
    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static async Task Ddl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>Runs one no-rows statement in its own transaction and commits it.</summary>
    private static async Task<ExecuteNonSQLResult> Run(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            await database.Transactions.CommitAsync(tx);
            return result;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<CamusDBException> RunFails(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            return Assert.ThrowsAsync<CamusDBException>(
                async () => await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null)))!;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>The committed rows of a query, each as "c1|c2|…" over the named columns.</summary>
    private static async Task<List<string>> Rows(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql, params string[] columns)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

            List<string> rows = new();
            await foreach (QueryResultRow row in cursor)
                rows.Add(string.Join("|", columns.Select(c => Show(row.Row[c]))));

            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static string Show(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "NULL",
        ColumnType.Integer64 => value.LongValue.ToString(),
        ColumnType.String => value.StrValue!,
        ColumnType.Bool => value.BoolValue ? "true" : "false",
        _ => value.ToString()
    };

    private static List<string> Returned(ExecuteNonSQLResult result)
    {
        List<string> rows = new();
        foreach (QueryResultRow row in result.ReturningRows!)
            rows.Add(string.Join("|", result.ReturningColumns!.Select(c => Show(row.Row[c.RowKey]))));
        return rows;
    }

    /// <summary>
    /// <c>kv</c>: a primary key <c>k</c>, a nullable unique <c>email</c>, a counter <c>n</c> and a column
    /// <c>tag</c> with a default.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> Setup(CamusDBOptions? options = null)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = options is null
            ? await CreateDatabase()
            : await CreateDatabase(options);

        await Ddl(executor, dbname,
            "CREATE TABLE kv (k int64 NOT NULL, email string, n int64, tag string DEFAULT ('fresh'), PRIMARY KEY (k), UNIQUE KEY kv_email (email))");

        await Run(executor, database, dbname, "INSERT INTO kv (k, email, n, tag) VALUES (1, 'a@x', 10, 'old'), (2, 'b@x', 20, 'old')");
        return (dbname, database, executor);
    }

    private const string AllRows = "SELECT k, email, n, tag FROM kv ORDER BY k";

    private static readonly string[] AllColumns = ["k", "email", "n", "tag"];

    // -----------------------------------------------------------------------
    // DO NOTHING
    // -----------------------------------------------------------------------

    [Test]
    public async Task DoNothingSkipsTakenKeyAndInsertsFreeKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        ExecuteNonSQLResult result = await Run(executor, database, dbname,
            "INSERT INTO kv (k, email, n) VALUES (1, 'z@x', 99), (3, 'c@x', 30) ON CONFLICT (k) DO NOTHING");

        Assert.AreEqual(1, result.ModifiedRows);
        CollectionAssert.AreEqual(
            new[] { "1|a@x|10|old", "2|b@x|20|old", "3|c@x|30|fresh" },
            await Rows(executor, database, dbname, AllRows, AllColumns));
    }

    [Test]
    public async Task DoNothingWithoutTargetArbitratesOnEveryUniqueIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        // Row 3 conflicts on email only, row 4 on nothing.
        ExecuteNonSQLResult result = await Run(executor, database, dbname,
            "INSERT INTO kv (k, email, n) VALUES (3, 'a@x', 30), (4, 'd@x', 40) ON CONFLICT DO NOTHING");

        Assert.AreEqual(1, result.ModifiedRows);
        CollectionAssert.AreEqual(
            new[] { "1|a@x|10|old", "2|b@x|20|old", "4|d@x|40|fresh" },
            await Rows(executor, database, dbname, AllRows, AllColumns));
    }

    [Test]
    public async Task NullKeyNeverConflicts()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        ExecuteNonSQLResult result = await Run(executor, database, dbname,
            "INSERT INTO kv (k, email, n) VALUES (3, NULL, 30), (4, NULL, 40) ON CONFLICT (email) DO NOTHING");

        Assert.AreEqual(2, result.ModifiedRows);
        Assert.AreEqual(4, (await Rows(executor, database, dbname, AllRows, AllColumns)).Count);
    }

    [Test]
    public async Task NonArbiterUniqueViolationIsAnError()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        CamusDBException ex = await RunFails(executor, database, dbname,
            "INSERT INTO kv (k, email, n) VALUES (3, 'a@x', 30) ON CONFLICT (k) DO NOTHING");

        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code);
        Assert.AreEqual(2, (await Rows(executor, database, dbname, AllRows, AllColumns)).Count);
    }

    [Test]
    public async Task DoNothingSkipsTheSecondRowOfTheSameKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        ExecuteNonSQLResult result = await Run(executor, database, dbname,
            "INSERT INTO kv (k, email, n) VALUES (3, 'c@x', 30), (3, 'd@x', 31) ON CONFLICT (k) DO NOTHING");

        Assert.AreEqual(1, result.ModifiedRows);
        CollectionAssert.Contains(await Rows(executor, database, dbname, AllRows, AllColumns), "3|c@x|30|fresh");
    }

    [Test]
    public async Task DoNothingSkipsTheSecondRowOfTheSameKeyAcrossChunks()
    {
        // One row per chunk: the second row's probe finds the row the first chunk inserted.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup(Options with { ForceSpillThresholdRows = 1 });

        ExecuteNonSQLResult result = await Run(executor, database, dbname,
            "INSERT INTO kv (k, email, n) VALUES (3, 'c@x', 30), (4, 'd@x', 40), (3, 'e@x', 31) ON CONFLICT (k) DO NOTHING");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEqual(
            new[] { "1|a@x|10|old", "2|b@x|20|old", "3|c@x|30|fresh", "4|d@x|40|fresh" },
            await Rows(executor, database, dbname, AllRows, AllColumns));
    }

    // -----------------------------------------------------------------------
    // DO UPDATE
    // -----------------------------------------------------------------------

    [Test]
    public async Task DoUpdateReadsStoredAndExcludedRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        ExecuteNonSQLResult result = await Run(executor, database, dbname,
            "INSERT INTO kv (k, email, n) VALUES (1, 'a@x', 5), (3, 'c@x', 30) " +
            "ON CONFLICT (k) DO UPDATE SET n = kv.n + excluded.n, tag = excluded.tag");

        Assert.AreEqual(2, result.ModifiedRows);

        // excluded.tag is the proposed row's default, because the statement did not list tag.
        CollectionAssert.AreEqual(
            new[] { "1|a@x|15|fresh", "2|b@x|20|old", "3|c@x|30|fresh" },
            await Rows(executor, database, dbname, AllRows, AllColumns));
    }

    [Test]
    public async Task DoUpdateBareNameIsTheStoredRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        await Run(executor, database, dbname, "INSERT INTO kv (k, n) VALUES (2, 7) ON CONFLICT (k) DO UPDATE SET n = n * 2 + excluded.n");

        CollectionAssert.Contains(await Rows(executor, database, dbname, AllRows, AllColumns), "2|b@x|47|old");
    }

    [Test]
    public async Task DoUpdateWhereFalseSkipsTheRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        ExecuteNonSQLResult result = await Run(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (1, 5), (2, 6) ON CONFLICT (k) DO UPDATE SET n = excluded.n WHERE kv.n > 15");

        Assert.AreEqual(1, result.ModifiedRows);
        CollectionAssert.AreEqual(
            new[] { "1|a@x|10|old", "2|b@x|6|old" },
            await Rows(executor, database, dbname, AllRows, AllColumns));
    }

    [Test]
    public async Task DoUpdateWhereCanReadExcluded()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        ExecuteNonSQLResult result = await Run(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (1, 50), (2, 1) ON CONFLICT (k) DO UPDATE SET n = excluded.n WHERE excluded.n > kv.n");

        Assert.AreEqual(1, result.ModifiedRows);
        CollectionAssert.AreEqual(
            new[] { "1|a@x|50|old", "2|b@x|20|old" },
            await Rows(executor, database, dbname, AllRows, AllColumns));
    }

    [Test]
    public async Task DoUpdateOnAnotherUniqueIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        await Run(executor, database, dbname,
            "INSERT INTO kv (k, email, n) VALUES (9, 'b@x', 1) ON CONFLICT (email) DO UPDATE SET n = kv.n + 1");

        CollectionAssert.AreEqual(
            new[] { "1|a@x|10|old", "2|b@x|21|old" },
            await Rows(executor, database, dbname, AllRows, AllColumns));
    }

    [Test]
    public async Task DoUpdateSetThatMakesADuplicateKeyFails()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        CamusDBException ex = await RunFails(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (1, 0) ON CONFLICT (k) DO UPDATE SET email = 'b@x'");

        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code);
    }

    [Test]
    public async Task DoUpdateCannotAssignThePrimaryKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        CamusDBException ex = await RunFails(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (1, 0) ON CONFLICT (k) DO UPDATE SET k = 5");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("primary key", ex.Message);
    }

    [Test]
    public async Task DoUpdateCannotAffectARowASecondTime()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        CamusDBException sameExisting = await RunFails(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (1, 1), (1, 2) ON CONFLICT (k) DO UPDATE SET n = excluded.n");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, sameExisting.Code);
        StringAssert.Contains("a second time", sameExisting.Message);

        CamusDBException sameNew = await RunFails(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (7, 1), (7, 2) ON CONFLICT (k) DO UPDATE SET n = excluded.n");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, sameNew.Code);

        CollectionAssert.AreEqual(
            new[] { "1|a@x|10|old", "2|b@x|20|old" },
            await Rows(executor, database, dbname, AllRows, AllColumns));
    }

    [Test]
    public async Task DoUpdateCannotAffectARowASecondTimeAcrossChunks()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup(Options with { ForceSpillThresholdRows = 1 });

        CamusDBException inserted = await RunFails(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (7, 1), (8, 1), (7, 2) ON CONFLICT (k) DO UPDATE SET n = excluded.n");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, inserted.Code);

        CamusDBException updated = await RunFails(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (1, 1), (8, 1), (1, 2) ON CONFLICT (k) DO UPDATE SET n = excluded.n");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, updated.Code);
    }

    [Test]
    public async Task ManyRowsAcrossChunksUpdateAndInsert()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup(Options with { ForceSpillThresholdRows = 3 });

        string values = string.Join(", ", Enumerable.Range(1, 10).Select(i => $"({i}, {i})"));
        ExecuteNonSQLResult result = await Run(executor, database, dbname,
            $"INSERT INTO kv (k, n) VALUES {values} ON CONFLICT (k) DO UPDATE SET n = kv.n + excluded.n");

        Assert.AreEqual(10, result.ModifiedRows);

        List<string> rows = await Rows(executor, database, dbname, "SELECT k, n FROM kv ORDER BY k", "k", "n");
        CollectionAssert.AreEqual(
            new[] { "1|11", "2|22" }.Concat(Enumerable.Range(3, 8).Select(i => $"{i}|{i}")).ToArray(),
            rows);
    }

    [Test]
    public async Task InsertSelectOnConflictDoUpdate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        await Ddl(executor, dbname, "CREATE TABLE src (k int64 NOT NULL, n int64, PRIMARY KEY (k))");
        await Run(executor, database, dbname, "INSERT INTO src (k, n) VALUES (2, 5), (3, 6)");

        ExecuteNonSQLResult result = await Run(executor, database, dbname,
            "INSERT INTO kv (k, n) SELECT k, n FROM src ON CONFLICT (k) DO UPDATE SET n = kv.n + excluded.n RETURNING k, n");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEquivalent(new[] { "2|25", "3|6" }, Returned(result));
        CollectionAssert.AreEqual(
            new[] { "1|10", "2|25", "3|6" },
            await Rows(executor, database, dbname, "SELECT k, n FROM kv ORDER BY k", "k", "n"));
    }

    [Test]
    public async Task InsertSelectFromTheSameTableTerminates()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        ExecuteNonSQLResult result = await Run(executor, database, dbname,
            "INSERT INTO kv (k, n) SELECT k, n FROM kv ON CONFLICT (k) DO UPDATE SET n = kv.n + 1");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEqual(
            new[] { "1|11", "2|21" },
            await Rows(executor, database, dbname, "SELECT k, n FROM kv ORDER BY k", "k", "n"));
    }

    [Test]
    public async Task UncorrelatedSubqueryInSetIsReplacedByItsValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        await Run(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (1, 0) ON CONFLICT (k) DO UPDATE SET n = (SELECT max(n) FROM kv) + 1");

        CollectionAssert.Contains(await Rows(executor, database, dbname, "SELECT k, n FROM kv ORDER BY k", "k", "n"), "1|21");
    }

    // -----------------------------------------------------------------------
    // RETURNING
    // -----------------------------------------------------------------------

    [Test]
    public async Task ReturningListsInsertedAndUpdatedRowsInOrderButNotSkippedOnes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        ExecuteNonSQLResult result = await Run(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (3, 30), (1, 1), (2, 2), (4, 40) " +
            "ON CONFLICT (k) DO UPDATE SET n = kv.n + excluded.n WHERE kv.k = 1 RETURNING k, n, tag");

        Assert.AreEqual(3, result.ModifiedRows);
        CollectionAssert.AreEqual(new[] { "3|30|fresh", "1|11|old", "4|40|fresh" }, Returned(result));
    }

    [Test]
    public async Task ReturningSkipsDoNothingRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        ExecuteNonSQLResult result = await Run(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (1, 1), (5, 5) ON CONFLICT DO NOTHING RETURNING k");

        CollectionAssert.AreEqual(new[] { "5" }, Returned(result));
    }

    // -----------------------------------------------------------------------
    // Arbiter resolution
    // -----------------------------------------------------------------------

    [Test]
    public async Task OnConstraintNamesAUniqueIndexOrThePrimaryKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        await Run(executor, database, dbname,
            "INSERT INTO kv (k, email, n) VALUES (9, 'a@x', 0) ON CONFLICT ON CONSTRAINT kv_email DO UPDATE SET n = kv.n + 100");
        await Run(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (2, 0) ON CONFLICT ON CONSTRAINT kv_pkey DO UPDATE SET n = kv.n + 1000");

        CollectionAssert.AreEqual(
            new[] { "1|110", "2|1020" },
            await Rows(executor, database, dbname, "SELECT k, n FROM kv ORDER BY k", "k", "n"));
    }

    [Test]
    public async Task UnknownOrNonUniqueArbiterIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();
        await Ddl(executor, dbname, "CREATE INDEX kv_n ON kv (n)");

        CamusDBException noIndex = await RunFails(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (1, 0) ON CONFLICT (n) DO NOTHING");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, noIndex.Code);
        StringAssert.Contains("No unique index", noIndex.Message);

        CamusDBException notUnique = await RunFails(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (1, 0) ON CONFLICT ON CONSTRAINT kv_n DO NOTHING");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, notUnique.Code);

        CamusDBException unknown = await RunFails(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (1, 0) ON CONFLICT ON CONSTRAINT nope DO NOTHING");
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, unknown.Code);
    }

    [Test]
    public async Task CompositeTargetMatchesInAnyOrder()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(executor, dbname, "CREATE TABLE pairs (a int64 NOT NULL, b int64 NOT NULL, n int64, PRIMARY KEY (a, b))");
        await Run(executor, database, dbname, "INSERT INTO pairs (a, b, n) VALUES (1, 2, 1)");

        await Run(executor, database, dbname,
            "INSERT INTO pairs (a, b, n) VALUES (1, 2, 5), (2, 1, 7) ON CONFLICT (b, a) DO UPDATE SET n = pairs.n + excluded.n");

        CollectionAssert.AreEqual(
            new[] { "1|2|6", "2|1|7" },
            await Rows(executor, database, dbname, "SELECT a, b, n FROM pairs ORDER BY a", "a", "b", "n"));
    }

    [Test]
    public async Task UnknownQualifierInSetIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        CamusDBException ex = await RunFails(executor, database, dbname,
            "INSERT INTO kv (k, n) VALUES (100, 0) ON CONFLICT (k) DO UPDATE SET n = other.n");

        Assert.AreEqual(CamusDBErrorCodes.UnknownColumn, ex.Code);
    }

    // -----------------------------------------------------------------------
    // UPSERT
    // -----------------------------------------------------------------------

    [Test]
    public async Task UpsertAssignsListedColumnsAndKeepsUnlistedOnes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        ExecuteNonSQLResult result = await Run(executor, database, dbname,
            "UPSERT INTO kv (k, n) VALUES (1, 100), (3, 300) RETURNING k, email, n, tag");

        Assert.AreEqual(2, result.ModifiedRows);

        // Row 1 keeps its stored email and tag; row 3 gets the default tag.
        CollectionAssert.AreEqual(new[] { "1|a@x|100|old", "3|NULL|300|fresh" }, Returned(result));
        CollectionAssert.AreEqual(
            new[] { "1|a@x|100|old", "2|b@x|20|old", "3|NULL|300|fresh" },
            await Rows(executor, database, dbname, AllRows, AllColumns));
    }

    [Test]
    public async Task UpsertMatchesTheEquivalentOnConflictStatement()
    {
        (string dbname1, DatabaseDescriptor database1, CommandExecutor executor1) = await Setup();
        (string dbname2, DatabaseDescriptor database2, CommandExecutor executor2) = await Setup();

        await Run(executor1, database1, dbname1, "UPSERT INTO kv (k, email, n) VALUES (2, 'q@x', 1), (5, 'e@x', 5)");
        await Run(executor2, database2, dbname2,
            "INSERT INTO kv (k, email, n) VALUES (2, 'q@x', 1), (5, 'e@x', 5) ON CONFLICT (k) DO UPDATE SET email = excluded.email, n = excluded.n");

        CollectionAssert.AreEqual(
            await Rows(executor2, database2, dbname2, AllRows, AllColumns),
            await Rows(executor1, database1, dbname1, AllRows, AllColumns));
    }

    [Test]
    public async Task UpsertWithoutColumnListAssignsEveryColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        await Run(executor, database, dbname, "UPSERT INTO kv VALUES (2, 'n@x', 7, 'new')");

        CollectionAssert.Contains(await Rows(executor, database, dbname, AllRows, AllColumns), "2|n@x|7|new");
    }

    [Test]
    public async Task UpsertSelect()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();
        await Ddl(executor, dbname, "CREATE TABLE src (k int64 NOT NULL, n int64, PRIMARY KEY (k))");
        await Run(executor, database, dbname, "INSERT INTO src (k, n) VALUES (1, 1), (9, 9)");

        ExecuteNonSQLResult result = await Run(executor, database, dbname, "UPSERT INTO kv (k, n) SELECT k, n FROM src");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEqual(
            new[] { "1|a@x|1|old", "2|b@x|20|old", "9|NULL|9|fresh" },
            await Rows(executor, database, dbname, AllRows, AllColumns));
    }

    [Test]
    public async Task OnConflictInsideAnExplicitTransactionSeesItsOwnRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Setup();

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO kv (k, n) VALUES (7, 1)", null));
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
                "INSERT INTO kv (k, n) VALUES (7, 5) ON CONFLICT (k) DO UPDATE SET n = kv.n + excluded.n", null));
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "UPSERT INTO kv (k, n) VALUES (7, 100)", null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        CollectionAssert.Contains(await Rows(executor, database, dbname, "SELECT k, n FROM kv ORDER BY k", "k", "n"), "7|100");
    }
}
