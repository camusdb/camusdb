
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
using Nito.AsyncEx;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// R13 — Error &amp; Semantics Matrix.
///
/// Each test covers one specific error case, asserting:
///   (a) the engine throws <see cref="CamusDBException"/>,
///   (b) the error carries the expected <see cref="CamusDBErrorCodes"/> code, and
///   (c) the message contains a stable substring that identifies the rejection reason.
///
/// "already correct" = the throw site pre-existed; the test just formalises the assertion.
/// "fixed here"      = the throw site was absent or wrong; this task corrected it.
/// </summary>
[TestFixture]
public sealed class TestErrorMatrix : BaseTest
{
    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupRobotsTableAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("role", ColumnType.String),
                new("year", ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        for (int i = 0; i < 5; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "robots",
                values: new()
                {
                    new()
                    {
                        { "id",   new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, "Robot " + i) },
                        { "role", new(ColumnType.String, i % 2 == 0 ? "worker" : "guard") },
                        { "year", new(ColumnType.Integer64, 2020 + i) },
                    }
                }
            ));
        }

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupJoinTablesAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        // Both tables have a column named "name" — triggers ambiguity on unqualified reference.
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "parts",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",   ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("part_id", ColumnType.Id),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        string partId = ObjectIdGenerator.Generate().ToString();
        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "parts",
            values: new() { new() { { "id", new(ColumnType.Id, partId) }, { "name", new(ColumnType.String, "Part A") } } }));

        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "robots",
            values: new() { new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) }, { "name", new(ColumnType.String, "R1") }, { "part_id", new(ColumnType.Id, partId) } } }));

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private static async Task<CamusDBException> ExpectThrowsAsync(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            await cursor.ToListAsync();
        })!;

        return ex;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 1. Ambiguous column — already correct
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_AmbiguousColumnInJoinThrows()
    {
        // Both tables have "name". An unqualified reference in WHERE is ambiguous.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupJoinTablesAsync();

        CamusDBException ex = await ExpectThrowsAsync(database, executor, dbname,
            "SELECT name FROM robots r INNER JOIN parts p ON r.id = p.id");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code,
            "Ambiguous column must use InvalidInput code");
        Assert.That(ex.Message, Does.Contain("Ambiguous column"),
            "Message must identify the ambiguous column");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 2. Unknown alias — already correct
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_UnknownAliasInJoinThrows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupJoinTablesAsync();

        CamusDBException ex = await ExpectThrowsAsync(database, executor, dbname,
            "SELECT x.name FROM robots r INNER JOIN parts p ON r.id = p.id");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code,
            "Unknown alias must use InvalidInput code");
        Assert.That(ex.Message, Does.Contain("Unknown alias"),
            "Message must name the unknown alias");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. Aggregate mixed with non-grouped projection (no GROUP BY) — already correct
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_AggregateMixedWithNonGroupedProjectionThrows()
    {
        // SELECT name, COUNT(*) FROM robots  — mixing aggregate with bare column is invalid
        // when no GROUP BY is present and the bare column is not under the aggregate.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTableAsync();

        CamusDBException ex = await ExpectThrowsAsync(database, executor, dbname,
            "SELECT name, COUNT(*) FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code,
            "Mixed aggregate/non-aggregate must use InvalidInput code");
        Assert.That(ex.Message, Does.Contain("Aggregations cannot be accompanied"),
            "Message must describe the mixing restriction");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. Non-grouped column in a GROUP BY query — already correct
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_NonGroupedColumnInGroupByQueryThrows()
    {
        // SELECT name, role, COUNT(*) FROM robots GROUP BY role
        // "name" is not in GROUP BY and is not an aggregate.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTableAsync();

        CamusDBException ex = await ExpectThrowsAsync(database, executor, dbname,
            "SELECT name, role, COUNT(*) FROM robots GROUP BY role");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code,
            "Non-grouped column must use InvalidInput code");
        Assert.That(ex.Message, Does.Contain("Non-aggregate projection must appear in GROUP BY"),
            "Message must identify the restriction");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 5. HAVING without GROUP BY or aggregate projection — already correct
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_HavingWithoutGroupByOrAggregateThrows()
    {
        // SELECT name FROM robots HAVING name = 'Robot 0'
        // HAVING requires either GROUP BY or an aggregate in the projection.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTableAsync();

        CamusDBException ex = await ExpectThrowsAsync(database, executor, dbname,
            "SELECT name FROM robots HAVING name = 'Robot 0'");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code,
            "HAVING without GROUP BY must use InvalidInput code");
        Assert.That(ex.Message, Does.Contain("HAVING requires GROUP BY"),
            "Message must name the requirement");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 6. HAVING references a column not in GROUP BY or aggregate — already correct
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_HavingReferencesColumnNotInScopeThrows()
    {
        // SELECT role, COUNT(*) FROM robots GROUP BY role HAVING name = 'Robot 0'
        // "name" is not a GROUP BY expression and not an aggregate.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTableAsync();

        CamusDBException ex = await ExpectThrowsAsync(database, executor, dbname,
            "SELECT role, COUNT(*) FROM robots GROUP BY role HAVING name = 'Robot 0'");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code,
            "Invalid HAVING column ref must use InvalidInput code");
        Assert.That(ex.Message, Does.Contain("HAVING must reference"),
            "Message must explain the scope restriction");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 6b. HAVING references an identifier that matches no column, GROUP BY
    //     expression, or projection alias — already correct
    //
    //     Distinct from test 6: here the identifier is completely unknown
    //     (not a real column at all), exercising the alias-lookup path rather
    //     than the column-scope path inside ValidatePostAggregateExpression.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_HavingReferencesUnknownAliasThrows()
    {
        // SELECT role, COUNT(*) AS cnt FROM robots GROUP BY role HAVING missing_alias > 0
        // "missing_alias" is not a projection alias, not a GROUP BY column, not an aggregate.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTableAsync();

        CamusDBException ex = await ExpectThrowsAsync(database, executor, dbname,
            "SELECT role, COUNT(*) AS cnt FROM robots GROUP BY role HAVING missing_alias > 0");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code,
            "Unknown HAVING alias must use InvalidInput code");
        Assert.That(ex.Message, Does.Contain("HAVING must reference"),
            "Message must explain the scope restriction");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 7. SELECT DISTINCT combined with GROUP BY — already correct
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_SelectDistinctWithGroupByThrows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTableAsync();

        CamusDBException ex = await ExpectThrowsAsync(database, executor, dbname,
            "SELECT DISTINCT role FROM robots GROUP BY role");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code,
            "DISTINCT + GROUP BY must use InvalidInput code");
        Assert.That(ex.Message, Does.Contain("SELECT DISTINCT with GROUP BY is not supported"),
            "Message must name the unsupported combination");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 8. SELECT DISTINCT with aggregate projection — already correct
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_SelectDistinctWithAggregateProjectionThrows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTableAsync();

        CamusDBException ex = await ExpectThrowsAsync(database, executor, dbname,
            "SELECT DISTINCT COUNT(*) FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code,
            "DISTINCT + aggregate must use InvalidInput code");
        Assert.That(ex.Message, Does.Contain("SELECT DISTINCT cannot be used with aggregate projections"),
            "Message must describe the restriction");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 9. COUNT(DISTINCT col) — parse-time rejection (already correct)
    //
    // TDISTINCT is a reserved token not usable inside a function argument list,
    // so the parser rejects COUNT(DISTINCT col) at parse time with a SqlSyntaxError.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_CountDistinctSyntaxErrorThrows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTableAsync();

        CamusDBException ex = await ExpectThrowsAsync(database, executor, dbname,
            "SELECT COUNT(DISTINCT name) FROM robots");

        Assert.AreEqual(CamusDBErrorCodes.SqlSyntaxError, ex.Code,
            "COUNT(DISTINCT col) must be rejected at parse time with SqlSyntaxError");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 10. Scalar subquery projecting multiple columns — already correct
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_ScalarSubqueryMultipleColumnsThrows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTableAsync();

        CamusDBException ex = await ExpectThrowsAsync(database, executor, dbname,
            "SELECT * FROM robots WHERE year = (SELECT name, year FROM robots LIMIT 1)");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code,
            "Scalar subquery with multiple columns must use InvalidInput code");
        Assert.That(ex.Message, Does.Contain("exactly one column"),
            "Message must state the single-column requirement");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 11. Scalar subquery returning multiple rows — already correct
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_ScalarSubqueryMultipleRowsThrows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTableAsync();

        CamusDBException ex = await ExpectThrowsAsync(database, executor, dbname,
            "SELECT * FROM robots WHERE year = (SELECT year FROM robots)");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code,
            "Scalar subquery with multiple rows must use InvalidInput code");
        Assert.That(ex.Message, Does.Contain("more than one row"),
            "Message must explain the scalar cardinality constraint");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 12. IN subquery with multiple columns — already correct
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_InSubqueryMultipleColumnsThrows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTableAsync();

        CamusDBException ex = await ExpectThrowsAsync(database, executor, dbname,
            "SELECT * FROM robots WHERE id IN (SELECT id, name FROM robots)");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code,
            "IN subquery with multiple columns must use InvalidInput code");
        Assert.That(ex.Message, Does.Contain("exactly one column"),
            "Message must state the single-column requirement");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 13. NOT IN subquery with multiple columns — already correct
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_NotInSubqueryMultipleColumnsThrows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTableAsync();

        CamusDBException ex = await ExpectThrowsAsync(database, executor, dbname,
            "SELECT * FROM robots WHERE id NOT IN (SELECT id, name FROM robots)");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code,
            "NOT IN subquery with multiple columns must use InvalidInput code");
        Assert.That(ex.Message, Does.Contain("exactly one column"),
            "Message must state the single-column requirement");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 14. NOT IN with NULL in subquery — SQL three-valued logic (already correct)
    //
    // SQL semantics: x NOT IN (null, ...) → UNKNOWN → treated as false → row excluded.
    // A NOT IN whose subquery yields any NULL must return zero rows when every non-null
    // candidate either does not match (UNKNOWN) or matches (false), because UNKNOWN
    // propagates to UNKNOWN, never TRUE.
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task R13_NotInWithNullableSubqueryExcludesAllRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupRobotsTableAsync();

        KvTransaction txn = await database.Transactions.BeginAsync();

        // Create a table whose "blocked_year" column is nullable.
        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "blocklist",
            columns: new ColumnInfo[]
            {
                new("id",           ColumnType.Id),
                new("blocked_year", ColumnType.Integer64),  // nullable — intentionally omitted below
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false
        ));

        // Insert one row with NULL blocked_year and one with a real value.
        // The NULL makes the IN subquery return [NULL, 9999], so for every outer row
        // year NOT IN (NULL, 9999) evaluates to UNKNOWN → false → row excluded.
        await executor.Insert(new InsertTicket(txnState: txn, databaseName: dbname, tableName: "blocklist",
            values: new()
            {
                // NULL blocked_year — key deliberately absent so column stays NULL.
                new() { { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) } },
                // A value that matches no robot year, but the NULL already taints the result.
                new()
                {
                    { "id",           new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                    { "blocked_year", new(ColumnType.Integer64, 9999L) },
                },
            }));

        await database.Transactions.CommitAsync(txn);

        KvTransaction readTxn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(
            txnState: readTxn,
            database: dbname,
            sql: "SELECT * FROM robots WHERE year NOT IN (SELECT blocked_year FROM blocklist)",
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(readTxn);

        // All rows excluded: the NULL in the subquery makes NOT IN return UNKNOWN (→ false) for every candidate.
        Assert.AreEqual(0, rows.Count,
            "NOT IN with a NULL in the subquery must exclude all outer rows (SQL UNKNOWN semantics)");
    }
}
