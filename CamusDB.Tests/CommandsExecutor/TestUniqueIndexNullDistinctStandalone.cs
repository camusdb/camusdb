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
/// NULL-distinct unique indexes on a standalone (non-clustered) engine. A standalone node builds an
/// index through a different backfill than a clustered one — the coordinator-driven backfill never
/// runs here — so the rule that a NULL-keyed row carries no unique entry needs its own coverage on
/// this path. Without it, a clustered fixture alone would report the feature as working while
/// <c>CREATE UNIQUE INDEX</c> on a single node still rejected a second NULL row.
/// </summary>
public class TestUniqueIndexNullDistinctStandalone : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupUsers()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, database, dbname,
            "CREATE TABLE users (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, email STRING)");

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
    public async Task BackfillOverExistingNulls_Succeeds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsers();

        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"a\", NULL)");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"b\", NULL)");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"c\", \"c@example.com\")");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"d\", \"d@example.com\")");
        // A row that omits the nullable column entirely must also be skipped by the backfill.
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name) VALUES (gen_id(), \"e\")");

        await ExecDDL(executor, database, dbname, "CREATE UNIQUE INDEX users_email_uq ON users (email)");

        List<QueryResultRow> rows = await Select(executor, database, dbname, "SELECT name FROM users");
        Assert.AreEqual(5, rows.Count);

        // Uniqueness on the non-NULL rows survives the backfill.
        CamusDBException ex = await AssertDmlThrows(executor, database, dbname,
            "INSERT INTO users (id, name, email) VALUES (gen_id(), \"f\", \"c@example.com\")");
        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, ex.Code);

        // A further NULL row still inserts after the index exists.
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"g\", NULL)");

        List<QueryResultRow> nulls = await Select(executor, database, dbname, "SELECT name FROM users WHERE email IS NULL");
        Assert.AreEqual(4, nulls.Count);
    }

    [Test]
    [NonParallelizable]
    public async Task BackfilledIndexServesPointLookupsAndOrderBy()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsers();

        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"b\", \"b@example.com\")");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"a\", \"a@example.com\")");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"n1\", NULL)");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"n2\", NULL)");

        await ExecDDL(executor, database, dbname, "CREATE UNIQUE INDEX users_email_uq ON users (email)");

        List<QueryResultRow> point = await Select(executor, database, dbname, "SELECT name FROM users WHERE email = \"a@example.com\"");
        Assert.AreEqual(1, point.Count);
        Assert.AreEqual("a", point[0].Row["name"].StrValue);

        // Sort elision must not use the index here: a NULL-keyed row has no entry in it.
        List<QueryResultRow> ordered = await Select(executor, database, dbname, "SELECT name FROM users ORDER BY email");
        Assert.AreEqual(4, ordered.Count, "ORDER BY on a nullable unique column must return all rows, including NULLs");
    }

    [Test]
    [NonParallelizable]
    public async Task NonUniqueIndexBackfill_KeepsNullRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsers();

        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"a\", NULL)");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"b\", NULL)");
        await ExecDML(executor, database, dbname, "INSERT INTO users (id, name, email) VALUES (gen_id(), \"c\", \"c@example.com\")");

        // A non-unique index indexes NULL rows too — its keys carry the row id, so they never collide.
        await ExecDDL(executor, database, dbname, "CREATE INDEX users_email_ix ON users (email)");

        List<QueryResultRow> nulls = await Select(executor, database, dbname, "SELECT name FROM users WHERE email IS NULL");
        Assert.AreEqual(2, nulls.Count);

        List<QueryResultRow> ordered = await Select(executor, database, dbname, "SELECT name FROM users ORDER BY email");
        Assert.AreEqual(3, ordered.Count);
    }
}
