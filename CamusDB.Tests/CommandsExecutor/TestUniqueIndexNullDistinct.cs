
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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Unique indexes follow standard SQL / Postgres / MySQL semantics: NULL is not equal to NULL, so a
/// unique index may hold any number of rows whose indexed column is NULL.
/// </summary>
public class TestUniqueIndexNullDistinct : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupUsers(bool withIndex = true)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, database, dbname,
            "CREATE TABLE users (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, email STRING)");

        if (withIndex)
            await ExecDDL(executor, database, dbname,
                "CREATE UNIQUE INDEX users_email_uq ON users (email)");

        return (dbname, database, executor);
    }

    private static async Task ExecDDL(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecDML(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<CamusDBException> AssertDmlThrows(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await executor.ExecuteNonSQLQuery(ticket))!;
        await database.Transactions.RollbackIfNotCompletedAsync(tx);
        return ex;
    }

    private static async Task<List<QueryResultRow>> Select(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, System.Collections.Generic.IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    [Test]
    [NonParallelizable]
    public async Task MultipleExplicitNulls_AreAllowed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsers();

        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"a\", NULL)");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"b\", NULL)");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"c\", NULL)");

        List<QueryResultRow> rows = await Select(executor, database, dbname, "SELECT name FROM users");
        Assert.AreEqual(3, rows.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task OmittedNullableUniqueColumn_IsAllowed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsers();

        // The unique column 'email' is left out of the column list entirely (previously threw).
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name) VALUES (gen_id(), \"a\")");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name) VALUES (gen_id(), \"b\")");

        List<QueryResultRow> rows = await Select(executor, database, dbname, "SELECT name FROM users");
        Assert.AreEqual(2, rows.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task DuplicateNonNullValue_IsStillRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsers();

        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"a\", \"x@example.com\")");

        CamusDBException ex = await AssertDmlThrows(executor, database, dbname,
            "INSERT INTO users (id, name, email) VALUES (gen_id(), \"b\", \"x@example.com\")");

        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task DistinctNonNullValues_RemainLookupable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsers();

        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"a\", \"a@example.com\")");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"b\", \"b@example.com\")");
        // Plus a couple of NULL rows that must not interfere with point lookups.
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"c\", NULL)");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"d\", NULL)");

        List<QueryResultRow> a = await Select(executor, database, dbname, "SELECT name FROM users WHERE email = \"a@example.com\"");
        Assert.AreEqual(1, a.Count);
        Assert.AreEqual("a", a[0].Row["name"].StrValue);

        List<QueryResultRow> nulls = await Select(executor, database, dbname, "SELECT name FROM users WHERE email IS NULL");
        Assert.AreEqual(2, nulls.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task UpdateValueToNull_FreesTheValueAndCoexistsWithOtherNulls()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsers();

        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"a\", \"u@example.com\")");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"b\", NULL)");

        // value -> NULL removes the unique entry; now another NULL row coexists.
        await ExecDML(executor, database, dbname, "UPDATE users SET email = NULL WHERE name = \"a\"");

        List<QueryResultRow> nulls = await Select(executor, database, dbname, "SELECT name FROM users WHERE email IS NULL");
        Assert.AreEqual(2, nulls.Count);

        // The freed value can now be inserted again.
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"c\", \"u@example.com\")");

        List<QueryResultRow> reused = await Select(executor, database, dbname, "SELECT name FROM users WHERE email = \"u@example.com\"");
        Assert.AreEqual(1, reused.Count);
        Assert.AreEqual("c", reused[0].Row["name"].StrValue);
    }

    [Test]
    [NonParallelizable]
    public async Task UpdateNullToCollidingValue_IsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsers();

        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"a\", \"dup@example.com\")");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"b\", NULL)");

        // NULL -> existing value must collide with the non-NULL row.
        CamusDBException ex = await AssertDmlThrows(executor, database, dbname,
            "UPDATE users SET email = \"dup@example.com\" WHERE name = \"b\"");

        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task DeleteNullRow_DoesNotErrorAndLeavesConstraintIntact()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsers();

        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"a\", NULL)");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"b\", NULL)");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"c\", \"keep@example.com\")");

        await ExecDML(executor, database, dbname, "DELETE FROM users WHERE name = \"a\"");

        List<QueryResultRow> remaining = await Select(executor, database, dbname, "SELECT name FROM users");
        Assert.AreEqual(2, remaining.Count);

        // Non-NULL uniqueness still enforced after deleting a NULL row.
        CamusDBException ex = await AssertDmlThrows(executor, database, dbname,
            "INSERT INTO users (id, name, email) VALUES (gen_id(), \"d\", \"keep@example.com\")");
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code);
    }

    [Test]
    [NonParallelizable]
    public async Task OrderByNullableUniqueColumn_KeepsNullRows()
    {
        // Sort-elision fence: ORDER BY over a nullable unique column must not silently drop NULL rows.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsers();

        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"b\", \"b@example.com\")");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"a\", \"a@example.com\")");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"n1\", NULL)");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"n2\", NULL)");

        List<QueryResultRow> rows = await Select(executor, database, dbname, "SELECT name FROM users ORDER BY email");
        Assert.AreEqual(4, rows.Count, "ORDER BY on a nullable unique column must return all rows, including NULLs");
    }

    [Test]
    [NonParallelizable]
    public async Task BackfillOverExistingNulls_Succeeds()
    {
        // Build the unique index AFTER inserting rows, including multiple NULLs (coordinator backfill).
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsers(withIndex: false);

        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"a\", NULL)");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"b\", NULL)");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"c\", \"c@example.com\")");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"d\", \"d@example.com\")");

        await ExecDDL(executor, database, dbname, "CREATE UNIQUE INDEX users_email_uq ON users (email)");

        List<QueryResultRow> rows = await Select(executor, database, dbname, "SELECT name FROM users");
        Assert.AreEqual(4, rows.Count);

        // After backfill, non-NULL uniqueness is enforced; a NULL row is still permitted.
        CamusDBException ex = await AssertDmlThrows(executor, database, dbname,
            "INSERT INTO users (id, name, email) VALUES (gen_id(), \"e\", \"c@example.com\")");
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code);

        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"f\", NULL)");
        List<QueryResultRow> after = await Select(executor, database, dbname, "SELECT name FROM users WHERE email IS NULL");
        Assert.AreEqual(3, after.Count);
    }
}
