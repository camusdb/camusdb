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
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The <c>DEFAULT</c> keyword in <c>INSERT … VALUES</c>, <c>UPSERT</c> and <c>UPDATE … SET</c>.
/// <c>VALUES (DEFAULT)</c> used to take only a constant default, so a function, sequence or identity
/// default stored NULL or failed NOT NULL, and <c>SET x = DEFAULT</c> failed with the internal error
/// CADB0407. A <c>DEFAULT</c> now gives the same value as omitting the column.
/// </summary>
[NonParallelizable]
public sealed class TestDefaultKeyword : SharedNodeBaseTest
{
    private static async Task Ddl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<int> Write(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            int modified = (await executor.ExecuteNonSQLQuery(
                new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null))).ModifiedRows;
            await database.Transactions.CommitAsync(tx);
            return modified;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<IReadOnlyDictionary<string, ColumnValue>>> Rows(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        List<IReadOnlyDictionary<string, ColumnValue>> rows = (await cursor.ToListAsync()).Select(r => r.Row).ToList();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static void Refused(System.Func<Task> act, string code)
    {
        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () => await act());
        Assert.AreEqual(code, error!.Code, error.Message);
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> SeedOrders()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Ddl(database, executor, dbname,
            "CREATE TABLE orders (id OID PRIMARY KEY DEFAULT (gen_id()), created_at TIMESTAMP DEFAULT (now()), " +
            "status STRING DEFAULT ('new'), note STRING NULL)");

        return (dbname, database, executor);
    }

    [Test]
    public async Task DefaultInValuesTakesConstantAndFunctionDefaults()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SeedOrders();

        await Write(database, executor, dbname,
            "INSERT INTO orders (id, created_at, status, note) VALUES (DEFAULT, DEFAULT, DEFAULT, DEFAULT)");

        // Several rows, with DEFAULT in some positions only: each DEFAULT gets its own value.
        await Write(database, executor, dbname,
            "INSERT INTO orders (id, created_at, status, note) VALUES " +
            "(DEFAULT, DEFAULT, 'paid', 'a'), (DEFAULT, DEFAULT, DEFAULT, 'b')");

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Rows(database, executor, dbname, "SELECT id, created_at, status, note FROM orders");
        Assert.AreEqual(3, rows.Count);

        foreach (IReadOnlyDictionary<string, ColumnValue> row in rows)
        {
            Assert.AreNotEqual(ColumnType.Null, row["id"].Type, "gen_id() ran for DEFAULT");
            Assert.AreNotEqual(ColumnType.Null, row["created_at"].Type, "now() ran for DEFAULT");
        }

        Assert.AreEqual(3, rows.Select(r => r["id"].ToString()).Distinct().Count(), "each row got its own id");
        CollectionAssert.AreEquivalent(new[] { "new", "paid", "new" }, rows.Select(r => r["status"].StrValue));
        Assert.AreEqual(1, rows.Count(r => r["note"].Type == ColumnType.Null), "a column with no default is NULL");
    }

    [Test]
    public async Task DefaultInValuesTakesSequenceAndIdentityDefaults()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Ddl(database, executor, dbname, "CREATE SEQUENCE order_no");
        await Ddl(database, executor, dbname, "CREATE TABLE t2 (id INT64 PRIMARY KEY, n INT64 DEFAULT (nextval('order_no')))");
        await Ddl(database, executor, dbname, "CREATE TABLE t3 (id INT64 PRIMARY KEY, k INT64 GENERATED BY DEFAULT AS IDENTITY)");
        await Ddl(database, executor, dbname, "CREATE TABLE t4 (id INT64 PRIMARY KEY, k INT64 GENERATED ALWAYS AS IDENTITY)");

        await Write(database, executor, dbname, "INSERT INTO t2 (id, n) VALUES (1, DEFAULT)");
        await Write(database, executor, dbname, "INSERT INTO t2 (id) VALUES (2)");
        await Write(database, executor, dbname, "INSERT INTO t2 (id, n) VALUES (3, DEFAULT), (4, 100), (5, DEFAULT)");

        Dictionary<long, long> n = (await Rows(database, executor, dbname, "SELECT id, n FROM t2"))
            .ToDictionary(r => r["id"].LongValue, r => r["n"].LongValue);
        Assert.AreEqual(1, n[1], "DEFAULT drew from the sequence");
        Assert.AreEqual(2, n[2]);
        Assert.AreEqual(100, n[4]);
        Assert.That(n[3], Is.GreaterThan(2));
        Assert.That(n[5], Is.GreaterThan(n[3]), "each DEFAULT drew its own value");

        await Write(database, executor, dbname, "INSERT INTO t3 (id, k) VALUES (1, DEFAULT), (2, DEFAULT)");
        CollectionAssert.AreEquivalent(new long[] { 1, 2 },
            (await Rows(database, executor, dbname, "SELECT k FROM t3")).Select(r => r["k"].LongValue));

        // GENERATED ALWAYS accepts DEFAULT, as in PostgreSQL, and still refuses a value.
        await Write(database, executor, dbname, "INSERT INTO t4 (id, k) VALUES (1, DEFAULT)");
        Assert.AreEqual(1, (await Rows(database, executor, dbname, "SELECT k FROM t4"))[0]["k"].LongValue);

        Refused(() => Write(database, executor, dbname, "INSERT INTO t4 (id, k) VALUES (2, 5)"), CamusDBErrorCodes.InvalidInput);
        Refused(() => Write(database, executor, dbname, "INSERT INTO t4 (id, k) VALUES (3, DEFAULT), (4, 7)"), CamusDBErrorCodes.InvalidInput);
    }

    [Test]
    public async Task DefaultInUpsertValuesTakesTheDefault()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Ddl(database, executor, dbname, "CREATE SEQUENCE ticket_no");
        await Ddl(database, executor, dbname,
            "CREATE TABLE tickets (id INT64 PRIMARY KEY, opened TIMESTAMP DEFAULT (now()), state STRING DEFAULT ('open'), " +
            "n INT64 DEFAULT (nextval('ticket_no')))");

        await Write(database, executor, dbname, "UPSERT INTO tickets (id, opened, state, n) VALUES (1, DEFAULT, DEFAULT, DEFAULT)");
        await Write(database, executor, dbname, "INSERT INTO tickets (id, state) VALUES (2, 'closed')");
        await Write(database, executor, dbname, "UPSERT INTO tickets (id, opened, state, n) VALUES (2, DEFAULT, DEFAULT, DEFAULT)");

        Dictionary<long, IReadOnlyDictionary<string, ColumnValue>> rows = (await Rows(database, executor, dbname, "SELECT id, opened, state, n FROM tickets"))
            .ToDictionary(r => r["id"].LongValue);

        foreach (long id in new long[] { 1, 2 })
        {
            Assert.AreNotEqual(ColumnType.Null, rows[id]["opened"].Type, $"row {id}: now() ran");
            Assert.AreEqual("open", rows[id]["state"].StrValue, $"row {id}: the constant default");
            Assert.AreNotEqual(ColumnType.Null, rows[id]["n"].Type, $"row {id}: the sequence default");
        }
    }

    [Test]
    public async Task UpdateSetDefaultAppliesTheDefault()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SeedOrders();

        await Write(database, executor, dbname,
            "INSERT INTO orders (status, note, created_at) VALUES ('paid', 'x', NULL), ('shipped', 'y', NULL)");

        Assert.AreEqual(2, await Write(database, executor, dbname, "UPDATE orders SET status = DEFAULT, note = DEFAULT WHERE TRUE"));
        Assert.AreEqual(2, await Write(database, executor, dbname, "UPDATE orders SET created_at = DEFAULT WHERE status = 'new'"));

        foreach (IReadOnlyDictionary<string, ColumnValue> row in await Rows(database, executor, dbname, "SELECT status, note, created_at FROM orders"))
        {
            Assert.AreEqual("new", row["status"].StrValue, "the constant default");
            Assert.AreEqual(ColumnType.Null, row["note"].Type, "a column with no default is set to NULL");
            Assert.AreNotEqual(ColumnType.Null, row["created_at"].Type, "the function default ran");
        }

        await Ddl(database, executor, dbname, "CREATE SEQUENCE order_no");
        await Ddl(database, executor, dbname, "CREATE TABLE numbered (id INT64 PRIMARY KEY, n INT64 DEFAULT (nextval('order_no')))");
        await Write(database, executor, dbname, "INSERT INTO numbered (id) VALUES (1)");

        Refused(() => Write(database, executor, dbname, "UPDATE numbered SET n = DEFAULT WHERE id = 1"), CamusDBErrorCodes.FeatureNotSupported);
    }
}
