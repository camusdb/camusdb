
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
/// End-to-end tests for <c>INSERT INTO … SELECT</c> driven through real SQL, covering the
/// positional column mapping, defaults and coercion, the all-or-nothing failure behavior, the
/// self-insert case that would not terminate if the source were streamed, and the per-transaction
/// mutation ceiling.
/// </summary>
public sealed class TestInsertSelect : SharedNodeBaseTest
{
    /// <summary>
    /// Creates <c>src</c> (seeded with <paramref name="rows"/> rows) and an empty <c>dest</c> with
    /// the same shape. Both have a plain <c>oid</c> primary key so a copy can carry ids across.
    /// </summary>
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTables(int rows = 5)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        foreach (string table in new[] { "src", "dest" })
        {
            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname,
                tableName: table,
                columns: new ColumnInfo[]
                {
                    new("id", ColumnType.Id),
                    new("name", ColumnType.String),
                    new("year", ColumnType.Integer64),
                },
                constraints: new ConstraintInfo[]
                {
                    new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
                },
                ifNotExists: false
            ));
        }

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

    private static async Task<int> ExecuteNonQuery(
        CommandExecutor executor, string dbname, KvTransaction txn, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: dbname, sql: sql, parameters: parameters));

        return result.ModifiedRows;
    }

    private static async Task<List<QueryResultRow>> Query(
        CommandExecutor executor, string dbname, KvTransaction txn, string sql)
    {
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: txn, database: dbname, sql: sql, parameters: null));

        return await cursor.ToListAsync();
    }

    [Test]
    [NonParallelizable]
    public async Task TestCopyEveryRowAndValue()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 5);

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.AreEqual(5, await ExecuteNonQuery(executor, dbname, txn, "INSERT INTO dest SELECT * FROM src"));

        List<QueryResultRow> source = await Query(executor, dbname, txn, "SELECT * FROM src");
        List<QueryResultRow> copied = await Query(executor, dbname, txn, "SELECT * FROM dest");

        Assert.AreEqual(5, copied.Count);
        CollectionAssert.AreEquivalent(
            source.Select(r => r.Row["name"].StrValue),
            copied.Select(r => r.Row["name"].StrValue));
        CollectionAssert.AreEquivalent(
            source.Select(r => r.Row["id"].StrValue),
            copied.Select(r => r.Row["id"].StrValue));

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// The mapping is positional, not by name: a source projection listed in the opposite order to
    /// the target column list must land in the order the TARGET list gives.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestMappingIsPositionalNotByName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction setup = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "pairs",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("a", ColumnType.String),
                new("b", ColumnType.String),
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
            tableName: "pairs",
            values: new()
            {
                new()
                {
                    { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "a", new(ColumnType.String, "value-of-a") },
                    { "b", new(ColumnType.String, "value-of-b") },
                }
            }
        ));

        await database.Transactions.CommitAsync(setup);

        KvTransaction txn = await database.Transactions.BeginAsync();

        // Target (a, b) fed by a source projecting (b, a): b's value must land in a.
        Assert.AreEqual(1, await ExecuteNonQuery(
            executor, dbname, txn,
            "INSERT INTO pairs (id, a, b) SELECT GEN_ID(), b, a FROM pairs"));

        List<QueryResultRow> swapped = (await Query(executor, dbname, txn, "SELECT * FROM pairs"))
            .Where(r => r.Row["a"].StrValue == "value-of-b")
            .ToList();

        Assert.AreEqual(1, swapped.Count, "the source's first output column must land in the first target column");
        Assert.AreEqual("value-of-a", swapped[0].Row["b"].StrValue);

        await database.Transactions.CommitAsync(txn);
    }

    [Test]
    [NonParallelizable]
    public async Task TestSubsetOfColumnsLeavesTheRestNull()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 3);

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.AreEqual(3, await ExecuteNonQuery(
            executor, dbname, txn, "INSERT INTO dest (id, name) SELECT id, name FROM src"));

        List<QueryResultRow> copied = await Query(executor, dbname, txn, "SELECT * FROM dest");

        Assert.AreEqual(3, copied.Count);
        foreach (QueryResultRow row in copied)
        {
            Assert.IsNotNull(row.Row["name"].StrValue);
            Assert.AreEqual(ColumnType.Null, row.Row["year"].Type, "an unlisted column with no default is NULL");
        }

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// An unlisted column carrying a function default is evaluated once per row, so a copy of N rows
    /// gets N distinct values — the same guarantee the VALUES form gives.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestFunctionDefaultIsEvaluatedPerRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 4);

        KvTransaction setup = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "generated",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id, defaultFunction: "gen_id"),
                new("name", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        await database.Transactions.CommitAsync(setup);

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.AreEqual(4, await ExecuteNonQuery(
            executor, dbname, txn, "INSERT INTO generated (name) SELECT name FROM src"));

        List<QueryResultRow> rows = await Query(executor, dbname, txn, "SELECT * FROM generated");

        Assert.AreEqual(4, rows.Count);
        Assert.AreEqual(4, rows.Select(r => r.Row["id"].StrValue).Distinct().Count(),
            "each row must get its own generated id");

        await database.Transactions.CommitAsync(txn);
    }

    [Test]
    [NonParallelizable]
    public async Task TestArityMismatchIsRejectedAndInsertsNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 3);

        KvTransaction txn = await database.Transactions.BeginAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteNonQuery(executor, dbname, txn, "INSERT INTO dest (id, name) SELECT * FROM src"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("number of target columns", ex.Message);

        Assert.IsEmpty(await Query(executor, dbname, txn, "SELECT * FROM dest"));

        await database.Transactions.CommitAsync(txn);
    }

    [Test]
    [NonParallelizable]
    public async Task TestDuplicateTargetColumnIsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 1);

        KvTransaction txn = await database.Transactions.BeginAsync();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteNonQuery(executor, dbname, txn, "INSERT INTO dest (id, id) SELECT id, id FROM src"));

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex!.Code);
        StringAssert.Contains("more than once", ex.Message);

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// A source value of a different numeric type is coerced to the target column's declared type,
    /// exactly as a VALUES literal would be.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestValuesAreCoercedToTheTargetColumnType()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 2);

        KvTransaction setup = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "floats",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("year", ColumnType.Float64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        await database.Transactions.CommitAsync(setup);

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.AreEqual(2, await ExecuteNonQuery(
            executor, dbname, txn, "INSERT INTO floats (id, year) SELECT id, year FROM src"));

        foreach (QueryResultRow row in await Query(executor, dbname, txn, "SELECT * FROM floats"))
            Assert.AreEqual(ColumnType.Float64, row.Row["year"].Type);

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// A NOT NULL violation anywhere in the copy aborts the whole statement — no partial prefix of
    /// the source is left behind.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestConstraintViolationInsertsNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 4);

        KvTransaction setup = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "strict",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("required", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        await database.Transactions.CommitAsync(setup);

        KvTransaction txn = await database.Transactions.BeginAsync();

        // "year" is an integer column being fed into a NOT NULL string; the row that violates it
        // must take the whole statement down.
        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteNonQuery(executor, dbname, txn, "INSERT INTO strict (id, required) SELECT id, NULL FROM src"));

        await database.Transactions.RollbackAsync(txn);

        KvTransaction verify = await database.Transactions.BeginAsync();
        Assert.IsEmpty(await Query(executor, dbname, verify, "SELECT * FROM strict"));
        await database.Transactions.CommitAsync(verify);
    }

    [Test]
    [NonParallelizable]
    public async Task TestEmptySourceInsertsNothingAndSucceeds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 3);

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.AreEqual(0, await ExecuteNonQuery(
            executor, dbname, txn, "INSERT INTO dest SELECT * FROM src WHERE year > 9999"));

        Assert.IsEmpty(await Query(executor, dbname, txn, "SELECT * FROM dest"));

        await database.Transactions.CommitAsync(txn);
    }

    [Test]
    [NonParallelizable]
    public async Task TestWhereAndLimitInTheSource()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 10);

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.AreEqual(2, await ExecuteNonQuery(
            executor, dbname, txn, "INSERT INTO dest SELECT * FROM src WHERE year >= 2005 LIMIT 2"));

        Assert.AreEqual(2, (await Query(executor, dbname, txn, "SELECT * FROM dest")).Count);

        await database.Transactions.CommitAsync(txn);
    }

    [Test]
    [NonParallelizable]
    public async Task TestAggregateSource()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 6);

        KvTransaction setup = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "totals",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id, defaultFunction: "gen_id"),
                new("total", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        await database.Transactions.CommitAsync(setup);

        KvTransaction txn = await database.Transactions.BeginAsync();

        // The aggregate is the sole projection (the engine rejects mixing one with other
        // expressions), so the target's id comes from its own gen_id default.
        Assert.AreEqual(1, await ExecuteNonQuery(
            executor, dbname, txn, "INSERT INTO totals (total) SELECT COUNT(*) FROM src"));

        List<QueryResultRow> rows = await Query(executor, dbname, txn, "SELECT * FROM totals");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(6, rows[0].Row["total"].LongValue);

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// A join source is emitted by the join executor, which keys its rows as <c>{alias}.{column}</c>
    /// rather than by bare column name. The copy resolves values by the source's own output names, so
    /// this pins that the two agree — a mismatch would insert NULLs and still report success.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestJoinSource()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 4);

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
                    { "year", new(ColumnType.Integer64, 2002) },
                    { "label", new(ColumnType.String, "the-label") },
                }
            }
        ));

        await database.Transactions.CommitAsync(setup);

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.AreEqual(1, await ExecuteNonQuery(
            executor, dbname, txn,
            "INSERT INTO dest (id, name, year) " +
            "SELECT s.id, l.label, s.year FROM src s JOIN labels l ON s.year = l.year"));

        List<QueryResultRow> rows = await Query(executor, dbname, txn, "SELECT * FROM dest");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("the-label", rows[0].Row["name"].StrValue, "the joined column's value must be copied, not NULL");
        Assert.AreEqual(2002, rows[0].Row["year"].LongValue);
        Assert.AreEqual(ColumnType.Id, rows[0].Row["id"].Type);

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// A FROM-less source (<c>SELECT &lt;expressions&gt;</c>) is a legitimate one-row source and must
    /// behave like the equivalent VALUES statement.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestFromlessSource()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 0);

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.AreEqual(1, await ExecuteNonQuery(
            executor, dbname, txn, "INSERT INTO dest (id, name, year) SELECT GEN_ID(), 'literal', 1999"));

        List<QueryResultRow> rows = await Query(executor, dbname, txn, "SELECT * FROM dest");
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("literal", rows[0].Row["name"].StrValue);
        Assert.AreEqual(1999, rows[0].Row["year"].LongValue);

        await database.Transactions.CommitAsync(txn);
    }

    [Test]
    [NonParallelizable]
    public async Task TestParametersInTheSourceQuery()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 8);

        KvTransaction txn = await database.Transactions.BeginAsync();

        int inserted = await ExecuteNonQuery(
            executor, dbname, txn,
            "INSERT INTO dest SELECT * FROM src WHERE year >= @from",
            new Dictionary<string, ColumnValue> { { "@from", new(ColumnType.Integer64, 2006) } });

        Assert.AreEqual(2, inserted);

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// The self-insert case. Reading the source lazily while writing would make the scan observe the
    /// transaction's own staged writes and never terminate; a correct implementation reads all N rows
    /// first and inserts exactly N.
    /// </summary>
    [Test]
    [NonParallelizable]
    [Timeout(120_000)]
    public async Task TestSelfInsertDoublesTheTableExactlyOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 5);

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.AreEqual(5, await ExecuteNonQuery(
            executor, dbname, txn, "INSERT INTO src (id, name, year) SELECT GEN_ID(), name, year FROM src"));

        List<QueryResultRow> rows = await Query(executor, dbname, txn, "SELECT * FROM src");
        Assert.AreEqual(10, rows.Count);

        // Every original name appears exactly twice.
        foreach (IGrouping<string?, QueryResultRow> group in rows.GroupBy(r => r.Row["name"].StrValue))
            Assert.AreEqual(2, group.Count(), $"'{group.Key}' should appear exactly twice");

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// Same guard, with a WHERE the newly-inserted rows also satisfy: a streaming implementation
    /// would keep finding its own writes.
    /// </summary>
    [Test]
    [NonParallelizable]
    [Timeout(120_000)]
    public async Task TestSelfInsertWithMatchingWhereStillDoublesOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 4);

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.AreEqual(4, await ExecuteNonQuery(
            executor, dbname, txn,
            "INSERT INTO src (id, name, year) SELECT GEN_ID(), name, year FROM src WHERE year >= 2000"));

        Assert.AreEqual(8, (await Query(executor, dbname, txn, "SELECT * FROM src")).Count);

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// Rows written by an uncommitted INSERT … SELECT are invisible to another transaction, and a
    /// rollback discards them.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestRollbackDiscardsTheCopiedRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 3);

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.AreEqual(3, await ExecuteNonQuery(executor, dbname, txn, "INSERT INTO dest SELECT * FROM src"));

        await database.Transactions.RollbackAsync(txn);

        KvTransaction after = await database.Transactions.BeginAsync();
        Assert.IsEmpty(await Query(executor, dbname, after, "SELECT * FROM dest"));
        await database.Transactions.CommitAsync(after);
    }

    /// <summary>
    /// The copy is bounded by the per-transaction mutation ceiling. The engine is built with a small
    /// ceiling rather than having one set on it afterwards, because a component fixes its
    /// configuration when it is constructed.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestMutationLimitIsEnforced()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { MaxMutationsPerTransaction = 3 });

        KvTransaction setup = await database.Transactions.BeginAsync();

        foreach (string table in new[] { "src", "dest" })
        {
            await executor.CreateTable(new CreateTableTicket(
                databaseName: dbname,
                tableName: table,
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
        }

        await database.Transactions.CommitAsync(setup);

        // Seeded one row per transaction: the ceiling under test is per transaction, so a single
        // setup transaction would hit it before the statement being tested ever runs.
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

        // 5 source rows against a 3-mutation budget: the drain refuses as soon as it has read as many
        // rows as the transaction could ever insert, rather than buffering the whole source first.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecuteNonQuery(executor, dbname, txn,
                "INSERT INTO dest (id, name) SELECT GEN_ID(), name FROM src"));

        Assert.AreEqual(CamusDBErrorCodes.TransactionMutationLimitExceeded, ex!.Code);

        await database.Transactions.RollbackAsync(txn);
    }

    /// <summary>
    /// A time-travel source reads at the requested snapshot while the writes stay live. Behavior in
    /// depth is covered by <see cref="TestInsertSelectAsOfSystemTime"/>; this only pins that the
    /// clause is accepted on this path and does not resolve to "now".
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestAsOfSystemTimeSourceIsAccepted()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 3);

        KvTransaction txn = await database.Transactions.BeginAsync();

        // A snapshot from an hour ago predates this test's database entirely, so it reads nothing —
        // the point is that it is accepted and honored rather than silently copying today's rows.
        Assert.AreEqual(0, await ExecuteNonQuery(executor, dbname, txn,
            "INSERT INTO dest SELECT * FROM src AS OF SYSTEM TIME '-1h'"));

        Assert.IsEmpty(await Query(executor, dbname, txn, "SELECT * FROM dest"));

        await database.Transactions.CommitAsync(txn);
    }

    /// <summary>
    /// Secondary indexes on the target are maintained by the copy, so a query that goes through one
    /// finds the inserted rows.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task TestTargetIndexesArePopulated()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTables(rows: 5);

        KvTransaction setup = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: setup, database: dbname, sql: "CREATE INDEX dest_year ON dest (year)", parameters: null));
        await database.Transactions.CommitAsync(setup);

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.AreEqual(5, await ExecuteNonQuery(executor, dbname, txn, "INSERT INTO dest SELECT * FROM src"));

        List<QueryResultRow> found = await Query(
            executor, dbname, txn, "SELECT * FROM dest WHERE year = 2003");

        Assert.AreEqual(1, found.Count);
        Assert.AreEqual(2003, found[0].Row["year"].LongValue);

        await database.Transactions.CommitAsync(txn);
    }
}
