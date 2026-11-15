/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using Kahuna.Shared.KeyValue;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.Queries.Spill;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

using static CamusDB.Tests.CommandsExecutor.WriteReturningTestSql;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests of <c>UPDATE … FROM</c>, <c>DELETE … USING</c> and the target alias of UPDATE and
/// DELETE, through <see cref="ExecuteSQLTicket"/>, on a cluster-mode engine (see
/// <see cref="TestUpdateFromDeleteUsingStandalone"/> for the standalone arm). The expected rows were
/// measured on PostgreSQL 16 with the same statements, except where a test names a CamusDB rule: a target
/// row that matches two rows of the FROM list fails, and RETURNING reads the target only.
/// </summary>
[NonParallelizable]
public sealed class TestUpdateFromDeleteUsing : SharedNodeBaseTest
{
    private async Task<(string, DatabaseDescriptor, CommandExecutor)> Seed()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(database, executor, dbname, "CREATE TABLE accounts (id INT64 PRIMARY KEY, owner STRING, balance INT64, closed BOOL)");
        await Ddl(database, executor, dbname, "CREATE TABLE deltas (did INT64 PRIMARY KEY, account_id INT64, amount INT64)");
        await Dml(database, executor, dbname,
            "INSERT INTO accounts (id, owner, balance, closed) VALUES (1, 'ana', 100, false), (2, 'bo', 200, false), (3, 'cy', 300, true), (4, 'di', 400, false)");
        await Dml(database, executor, dbname,
            "INSERT INTO deltas (did, account_id, amount) VALUES (10, 1, 5), (11, 2, -20), (12, 9, 1000)");
        return (dbname, database, executor);
    }

    private static async Task Ddl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<ExecuteNonSQLResult> Dml(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new(txnState: tx, database: dbname, sql: sql, parameters: parameters));
            await database.Transactions.CommitAsync(tx);
            return result;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<List<QueryResultRow>> Query(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static CamusDBException Refused(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql) =>
        Assert.ThrowsAsync<CamusDBException>(async () => await Dml(database, executor, dbname, sql))!;

    /// <summary>The balance of each account, by id.</summary>
    private static async Task<Dictionary<long, long>> Balances(DatabaseDescriptor database, CommandExecutor executor, string dbname) =>
        (await Query(database, executor, dbname, "SELECT id, balance FROM accounts ORDER BY id"))
            .ToDictionary(r => r.Row["id"].LongValue, r => r.Row["balance"].LongValue);

    private static async Task<List<long>> AccountIds(DatabaseDescriptor database, CommandExecutor executor, string dbname) =>
        (await Query(database, executor, dbname, "SELECT id FROM accounts ORDER BY id")).Select(r => r.Row["id"].LongValue).ToList();

    // ── UPDATE … FROM ──

    [Test]
    public async Task UpdateFrom_AppliesEachMatchingSourceRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts a SET balance = a.balance + d.amount FROM deltas d WHERE d.account_id = a.id");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 105, [2] = 180, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_WithoutAliases_QualifiesByTableName()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts SET balance = accounts.balance + deltas.amount FROM deltas WHERE deltas.account_id = accounts.id");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 105, [2] = 180, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_BareNamesReadTheirOwnRelation()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        // balance and id are target columns, amount and account_id are columns of deltas.
        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts SET balance = balance + amount FROM deltas WHERE account_id = id AND amount > 0");

        Assert.AreEqual(1, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 105, [2] = 200, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_ATargetRowWithoutAMatchIsNotChanged()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        // Delta 12 names account 9, which does not exist: it matches nothing and changes nothing.
        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts a SET balance = 0 FROM deltas d WHERE d.account_id = a.id AND d.did = 12");

        Assert.AreEqual(0, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 100, [2] = 200, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_ATargetRowThatMatchesTwoSourceRowsFailsAndChangesNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await Dml(database, executor, dbname, "INSERT INTO deltas (did, account_id, amount) VALUES (13, 1, 1)");

        // PostgreSQL updates account 1 with one of the two deltas and does not say which.
        CamusDBException error = Refused(database, executor, dbname,
            "UPDATE accounts a SET balance = a.balance + d.amount FROM deltas d WHERE d.account_id = a.id");

        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, error.Code);
        StringAssert.Contains("(id)=(1)", error.Message);
        StringAssert.Contains("'accounts'", error.Message);
        StringAssert.Contains("more than once", error.Message);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 100, [2] = 200, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_ADerivedTableWithGroupByMakesOneMatchPerTargetRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await Dml(database, executor, dbname, "INSERT INTO deltas (did, account_id, amount) VALUES (13, 1, 1)");

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts a SET balance = a.balance + t.s " +
            "FROM (SELECT account_id, SUM(amount) AS s FROM deltas GROUP BY account_id) t WHERE t.account_id = a.id");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 106, [2] = 180, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_ADerivedTableOfConstantRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        // The CamusDB form of a VALUES list in FROM, which a SELECT does not take either.
        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts a SET closed = v.c FROM (SELECT 1 AS k, true AS c UNION ALL SELECT 2, true) v WHERE v.k = a.id");

        Assert.AreEqual(2, result.ModifiedRows);
        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT id FROM accounts WHERE closed ORDER BY id");
        CollectionAssert.AreEqual(new long[] { 1, 2, 3 }, rows.Select(r => r.Row["id"].LongValue).ToArray());
    }

    [Test]
    public async Task UpdateFrom_AChainOfJoins()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await Ddl(database, executor, dbname, "CREATE TABLE owners (name STRING PRIMARY KEY, tier INT64)");
        await Dml(database, executor, dbname, "INSERT INTO owners (name, tier) VALUES ('ana', 2), ('bo', 3)");

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts a SET balance = a.balance * o.tier FROM deltas d JOIN owners o ON o.tier > 0 " +
            "WHERE d.account_id = a.id AND o.name = a.owner");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 200, [2] = 600, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_ACommaListOfRelations()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await Ddl(database, executor, dbname, "CREATE TABLE owners (name STRING PRIMARY KEY, tier INT64)");
        await Dml(database, executor, dbname, "INSERT INTO owners (name, tier) VALUES ('ana', 2), ('bo', 3)");

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts a SET balance = a.balance * o.tier + d.amount FROM deltas d, owners o " +
            "WHERE d.account_id = a.id AND o.name = a.owner");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 205, [2] = 580, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_ATableFunction()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts a SET balance = g FROM generate_series(1, 2) g WHERE g = a.id");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 1, [2] = 2, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_AView()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await Ddl(database, executor, dbname, "CREATE VIEW credits AS SELECT account_id, amount FROM deltas WHERE amount > 0");

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts a SET balance = a.balance + c.amount FROM credits c WHERE c.account_id = a.id");

        Assert.AreEqual(1, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 105, [2] = 200, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_ASelfJoinReadsTheOtherRowsAsTheStatementFoundThem()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        // Each row takes the balance of the next row before the statement changed it, as in PostgreSQL.
        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts a SET balance = b.balance FROM accounts b WHERE b.id = a.id + 1");

        Assert.AreEqual(3, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 200, [2] = 300, [3] = 400, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_AWhereOnTheSourceOnlyUpdatesEveryTargetRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts a SET balance = d.amount FROM deltas d WHERE d.did = 10");

        Assert.AreEqual(4, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 5, [2] = 5, [3] = 5, [4] = 5 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_ANullJoinKeyMatchesOnlyWhatTheWhereSays()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await Dml(database, executor, dbname, "INSERT INTO deltas (did, account_id, amount) VALUES (14, NULL, 3)");

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts a SET balance = 0 FROM deltas d WHERE d.account_id = a.id OR (d.account_id IS NULL AND a.id = 4)");

        Assert.AreEqual(3, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 0, [2] = 0, [3] = 300, [4] = 0 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_ASubqueryInASetValueReadsTheSourceRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts a SET balance = (SELECT MAX(amount) FROM deltas x WHERE x.account_id = d.account_id) + a.balance " +
            "FROM deltas d WHERE d.account_id = a.id");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 105, [2] = 180, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_TakesParametersInEveryClause()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts a SET balance = d.amount * @k FROM (SELECT account_id, amount FROM deltas WHERE amount > @min) d " +
            "WHERE d.account_id = a.id AND a.id <> @skip",
            new Dictionary<string, ColumnValue>
            {
                ["@k"] = new(ColumnType.Integer64, 10),
                ["@min"] = new(ColumnType.Integer64, -100),
                ["@skip"] = new(ColumnType.Integer64, 2),
            });

        Assert.AreEqual(1, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 50, [2] = 200, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_SetValuesReadTheOldImage()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        // owner takes the old balance as text, and balance the amount: neither sees the other's new value.
        await Dml(database, executor, dbname,
            "UPDATE accounts a SET balance = d.amount, owner = CAST(a.balance AS STRING) FROM deltas d WHERE d.account_id = a.id AND a.id = 1");

        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT owner, balance FROM accounts WHERE id = 1");
        Assert.AreEqual("100", rows[0].Row["owner"].StrValue);
        Assert.AreEqual(5, rows[0].Row["balance"].LongValue);
    }

    [Test]
    public async Task UpdateFrom_ReturningReadsTheNewImageOfTheTarget()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts a SET balance = a.balance + d.amount FROM deltas d WHERE d.account_id = a.id RETURNING a.id, balance");

        Assert.AreEqual(2, result.ModifiedRows);
        Dictionary<long, long> returned = Enumerable.Range(0, result.ReturningRows!.Count)
            .ToDictionary(i => Cell(result, i, 0).LongValue, i => Cell(result, i, 1).LongValue);
        CollectionAssert.AreEquivalent(new Dictionary<long, long> { [1] = 105, [2] = 180 }, returned);
    }

    [TestCase("UPDATE accounts a SET balance = 0 FROM deltas d WHERE d.account_id = a.id RETURNING d.amount", "can read only columns of the target", TestName = "UpdateFrom_ReturningOfASourceColumnIsRefused")]
    [TestCase("UPDATE accounts a SET balance = 0 FROM deltas d WHERE d.account_id = a.id RETURNING *", "RETURNING * is not supported", TestName = "UpdateFrom_ReturningStarIsRefused")]
    [TestCase("UPDATE accounts a SET balance = 0 FROM deltas d", "requires a WHERE clause", TestName = "UpdateFrom_WithoutWhereIsRefused")]
    [TestCase("UPDATE accounts a SET balance = 0 FROM deltas d WHERE d.account_id = a.id LIMIT 1", "LIMIT is not supported", TestName = "UpdateFrom_WithLimitIsRefused")]
    [TestCase("UPDATE accounts SET balance = 0 FROM accounts WHERE accounts.id = 1", "specified more than once", TestName = "UpdateFrom_TheTargetTwiceWithoutAnAliasIsRefused")]
    [TestCase("UPDATE accounts a SET id = d.did FROM deltas d WHERE d.account_id = a.id", "Cannot update primary key", TestName = "UpdateFrom_AssigningThePrimaryKeyIsRefused")]
    [TestCase("UPDATE accounts a SET balance = d.missing FROM deltas d WHERE d.account_id = a.id", "missing", TestName = "UpdateFrom_AnUnknownSourceColumnIsRefused")]
    [TestCase("UPDATE accounts a SET balance = 0 FROM deltas d WHERE d.account_id = a.missing", "missing", TestName = "UpdateFrom_AnUnknownTargetColumnIsRefused")]
    public async Task UpdateFrom_RefusedStatementChangesNothing(string sql, string message)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        CamusDBException error = Refused(database, executor, dbname, sql);

        StringAssert.Contains(message, error.Message);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 100, [2] = 200, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_ABareNameThatTwoRelationsHaveIsAmbiguous()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await Ddl(database, executor, dbname, "CREATE TABLE x (xid INT64 PRIMARY KEY, id INT64, amount INT64)");
        await Dml(database, executor, dbname, "INSERT INTO x (xid, id, amount) VALUES (1, 1, 7)");

        CamusDBException error = Refused(database, executor, dbname,
            "UPDATE accounts SET balance = balance + amount FROM x WHERE x.id = id");

        StringAssert.Contains("Ambiguous", error.Message);
    }

    [Test]
    public async Task UpdateFrom_TheTableNameOfAnAliasedTargetIsNotAQualifier()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        // PostgreSQL: invalid reference to FROM-clause entry for table "accounts".
        Refused(database, executor, dbname, "UPDATE accounts a SET balance = 0 FROM deltas d WHERE accounts.id = d.account_id");

        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 100, [2] = 200, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_AStringIntoAnIntegerColumnIsRefusedWithNoMatchingRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await Ddl(database, executor, dbname, "CREATE TABLE owners (name STRING PRIMARY KEY, tier INT64)");

        // owners is empty, so no row reaches the write phase: the type of o.name refuses the statement,
        // with the error a plain UPDATE gives for a string column assigned to an integer column.
        CamusDBException error = Refused(database, executor, dbname,
            "UPDATE accounts a SET balance = o.name FROM owners o WHERE o.name = a.owner");
        CamusDBException plain = Refused(database, executor, dbname, "UPDATE accounts SET balance = owner WHERE id = 0");

        Assert.AreEqual(plain.Code, error.Code);
    }

    [Test]
    public async Task UpdateAlias_WithoutFrom()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE accounts AS a SET balance = a.balance + 1 WHERE a.id = 4 RETURNING a.id, a.balance");

        Assert.AreEqual(1, result.ModifiedRows);
        Assert.AreEqual(4, Cell(result, 0, 0).LongValue);
        Assert.AreEqual(401, Cell(result, 0, 1).LongValue);
    }

    [Test]
    public async Task UpdateAndDeleteAlias_ReturningStarIsTheTargetColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        ExecuteNonSQLResult updated = await Dml(database, executor, dbname,
            "UPDATE accounts AS a SET balance = 7 WHERE a.id = 4 RETURNING *");
        CollectionAssert.AreEqual(new[] { "id", "owner", "balance", "closed" }, ColumnNames(updated.ReturningColumns!));
        Assert.AreEqual(7, Cell(updated, 0, 2).LongValue);

        ExecuteNonSQLResult deleted = await Dml(database, executor, dbname, "DELETE FROM accounts a WHERE a.id = 4 RETURNING *");
        CollectionAssert.AreEqual(new[] { "id", "owner", "balance", "closed" }, ColumnNames(deleted.ReturningColumns!));
        Assert.AreEqual("di", Cell(deleted, 0, 1).StrValue);
    }

    [Test]
    public async Task UpdateFrom_InAnExplicitTransactionSeesItsOwnWrites()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteNonSQLQuery(new(tx, dbname, "INSERT INTO deltas (did, account_id, amount) VALUES (20, 4, 1)", null));
            ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(new(tx, dbname,
                "UPDATE accounts a SET balance = a.balance + d.amount FROM deltas d WHERE d.account_id = a.id", null));
            Assert.AreEqual(3, result.ModifiedRows);
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 105, [2] = 180, [3] = 300, [4] = 401 }, await Balances(database, executor, dbname));
    }

    // ── Primary keys of other shapes ──

    [Test]
    public async Task UpdateFrom_ATargetWithATwoColumnPrimaryKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(database, executor, dbname, "CREATE TABLE stock (shop STRING, sku INT64, qty INT64, PRIMARY KEY (shop, sku))");
        await Ddl(database, executor, dbname, "CREATE TABLE moves (mid INT64 PRIMARY KEY, shop STRING, sku INT64, n INT64)");
        await Dml(database, executor, dbname, "INSERT INTO stock (shop, sku, qty) VALUES ('a', 1, 10), ('a', 2, 20), ('b', 1, 30)");
        await Dml(database, executor, dbname, "INSERT INTO moves (mid, shop, sku, n) VALUES (1, 'a', 2, -5), (2, 'b', 1, 7)");

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE stock s SET qty = s.qty + m.n FROM moves m WHERE m.shop = s.shop AND m.sku = s.sku");

        Assert.AreEqual(2, result.ModifiedRows);
        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT shop, sku, qty FROM stock ORDER BY shop, sku");
        CollectionAssert.AreEqual(new long[] { 10, 15, 37 }, rows.Select(r => r.Row["qty"].LongValue).ToArray());

        await Dml(database, executor, dbname, "INSERT INTO moves (mid, shop, sku, n) VALUES (3, 'b', 1, 1)");
        CamusDBException error = Refused(database, executor, dbname,
            "UPDATE stock s SET qty = m.n FROM moves m WHERE m.shop = s.shop AND m.sku = s.sku");
        StringAssert.Contains("(shop, sku)=(b, 1)", error.Message);
    }

    [Test]
    public async Task UpdateFrom_ATargetWithAnObjectIdPrimaryKey()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await Ddl(database, executor, dbname, "CREATE TABLE docs (id OID PRIMARY KEY DEFAULT (gen_id()), name STRING, rev INT64)");
        await Ddl(database, executor, dbname, "CREATE TABLE bumps (bid INT64 PRIMARY KEY, name STRING)");
        await Dml(database, executor, dbname, "INSERT INTO docs (name, rev) VALUES ('x', 1), ('y', 1), ('z', 1)");
        await Dml(database, executor, dbname, "INSERT INTO bumps (bid, name) VALUES (1, 'x'), (2, 'z')");

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "UPDATE docs d SET rev = d.rev + 1 FROM bumps b WHERE b.name = d.name");

        Assert.AreEqual(2, result.ModifiedRows);
        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT name, rev FROM docs ORDER BY name");
        CollectionAssert.AreEqual(new long[] { 2, 1, 2 }, rows.Select(r => r.Row["rev"].LongValue).ToArray());

        ExecuteNonSQLResult deleted = await Dml(database, executor, dbname, "DELETE FROM docs d USING bumps b WHERE b.name = d.name");
        Assert.AreEqual(2, deleted.ModifiedRows);
    }

    // ── Constraints on the target ──

    [Test]
    public async Task UpdateFrom_AUniqueIndexIsEnforced()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await Ddl(database, executor, dbname, "CREATE UNIQUE INDEX accounts_owner ON accounts (owner)");

        CamusDBException error = Refused(database, executor, dbname,
            "UPDATE accounts a SET owner = 'same' FROM deltas d WHERE d.account_id = a.id");

        Assert.AreEqual(CamusDBErrorCodes.DuplicateUniqueKeyValue, error.Code);
        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT owner FROM accounts WHERE id = 1");
        Assert.AreEqual("ana", rows[0].Row["owner"].StrValue);
    }

    [Test]
    public async Task UpdateFrom_ACheckConstraintIsEnforced()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await Ddl(database, executor, dbname, "ALTER TABLE accounts ADD CONSTRAINT non_negative CHECK (balance >= 0)");

        CamusDBException error = Refused(database, executor, dbname,
            "UPDATE accounts a SET balance = a.balance - 1000 FROM deltas d WHERE d.account_id = a.id");

        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, error.Code);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 100, [2] = 200, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task UpdateFrom_AForeignKeyIsEnforced()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await Ddl(database, executor, dbname, "CREATE TABLE cards (cid INT64 PRIMARY KEY, account_id INT64 REFERENCES accounts (id))");
        await Dml(database, executor, dbname, "INSERT INTO cards (cid, account_id) VALUES (1, 1), (2, 2)");

        // Moves each card to the account its delta names: delta 12 names account 9, which does not exist.
        await Dml(database, executor, dbname, "UPDATE cards c SET account_id = 4 FROM deltas d WHERE d.account_id = c.account_id AND d.did = 10");
        CamusDBException error = Refused(database, executor, dbname,
            "UPDATE cards c SET account_id = d.account_id FROM deltas d WHERE d.did = 12 AND c.cid = 2");

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyViolation, error.Code);
        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT cid, account_id FROM cards ORDER BY cid");
        CollectionAssert.AreEqual(new long[] { 4, 2 }, rows.Select(r => r.Row["account_id"].LongValue).ToArray());
    }

    // ── Concurrency ──

    /// <summary>
    /// A competing transaction commits between the read phase and the write phase: it changes the
    /// balance of one matched row and closes another. The write phase locks, reads again and
    /// re-checks with the source values the read phase found, so the first row adds the delta to the
    /// committed balance, and the closed row is not changed and not counted.
    /// </summary>
    [Test]
    public async Task UpdateFrom_AConcurrentCommitIsReadAfterTheRowLock()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        RowUpdater updater = executor.RowUpdaterForTests;
        bool hookRan = false;

        updater.TestBeforeWriteHook = async () =>
        {
            updater.TestBeforeWriteHook = null;
            hookRan = true;
            await Dml(database, executor, dbname, "UPDATE accounts SET balance = 1000 WHERE id = 1");
            await Dml(database, executor, dbname, "UPDATE accounts SET closed = true WHERE id = 2");
        };

        ExecuteNonSQLResult result;
        KvTransaction tx = await database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted, locking: KeyValueTransactionLocking.Pessimistic);
        try
        {
            result = await executor.ExecuteNonSQLQuery(new(tx, dbname,
                "UPDATE accounts a SET balance = a.balance + d.amount FROM deltas d WHERE d.account_id = a.id AND NOT a.closed", null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            updater.TestBeforeWriteHook = null;
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        Assert.IsTrue(hookRan, "the competing commit never ran inside the write window");
        Assert.AreEqual(1, result.ModifiedRows);
        CollectionAssert.AreEqual(new Dictionary<long, long> { [1] = 1005, [2] = 200, [3] = 300, [4] = 400 }, await Balances(database, executor, dbname));
    }

    [Test]
    public async Task DeleteUsing_AConcurrentCommitIsReadAfterTheRowLock()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        RowDeleter deleter = executor.RowDeleterForTests;
        bool hookRan = false;

        deleter.TestBeforeWriteHook = async () =>
        {
            deleter.TestBeforeWriteHook = null;
            hookRan = true;
            await Dml(database, executor, dbname, "UPDATE accounts SET closed = true WHERE id = 2");
        };

        ExecuteNonSQLResult result;
        KvTransaction tx = await database.Transactions.BeginAsync(
            isolationLevel: CamusIsolationLevel.ReadCommitted, locking: KeyValueTransactionLocking.Pessimistic);
        try
        {
            result = await executor.ExecuteNonSQLQuery(new(tx, dbname,
                "DELETE FROM accounts a USING deltas d WHERE d.account_id = a.id AND NOT a.closed", null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            deleter.TestBeforeWriteHook = null;
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }

        Assert.IsTrue(hookRan, "the competing commit never ran inside the write window");
        Assert.AreEqual(1, result.ModifiedRows);
        CollectionAssert.AreEqual(new long[] { 2, 3, 4 }, await AccountIds(database, executor, dbname));
    }

    // ── Spill ──

    [Test]
    public async Task UpdateFrom_AMatchSetLargerThanTheSpillThresholdWritesInChunks()
    {
        string dataDir = Path.Combine(Path.GetTempPath(), "camusdb_updfrom_spill_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);
        SpillFileManager.AcquireInstanceLock(dataDir);

        try
        {
            (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
                await CreateDatabase(Options with { SpillEnabled = true, ForceSpillThresholdRows = 3, DataDirectory = dataDir });

            await Ddl(database, executor, dbname, "CREATE TABLE items (id INT64 PRIMARY KEY, v INT64)");
            await Ddl(database, executor, dbname, "CREATE TABLE changes (cid INT64 PRIMARY KEY, item_id INT64, v INT64)");
            await Dml(database, executor, dbname,
                "INSERT INTO items (id, v) SELECT g, 0 FROM generate_series(1, 20) g");
            await Dml(database, executor, dbname,
                "INSERT INTO changes (cid, item_id, v) SELECT g, g, g * 10 FROM generate_series(1, 20) g");

            ExecuteNonSQLResult result = await Dml(database, executor, dbname,
                "UPDATE items i SET v = c.v FROM changes c WHERE c.item_id = i.id RETURNING i.id, i.v");

            Assert.AreEqual(20, result.ModifiedRows);
            Assert.AreEqual(20, result.ReturningRows!.Count);

            List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT id, v FROM items ORDER BY id");
            CollectionAssert.AreEqual(Enumerable.Range(1, 20).Select(i => (long)i * 10).ToArray(), rows.Select(r => r.Row["v"].LongValue).ToArray());

            ExecuteNonSQLResult deleted = await Dml(database, executor, dbname,
                "DELETE FROM items i USING changes c WHERE c.item_id = i.id AND c.v > 100");
            Assert.AreEqual(10, deleted.ModifiedRows);
        }
        finally
        {
            SpillFileManager.ReleaseInstanceLock();
            try { Directory.Delete(dataDir, recursive: true); } catch { }
        }
    }

    // ── DELETE … USING ──

    [Test]
    public async Task DeleteUsing_DeletesEachMatchingTargetRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "DELETE FROM accounts a USING deltas d WHERE d.account_id = a.id RETURNING a.id");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEquivalent(new long[] { 1, 2 }, Enumerable.Range(0, 2).Select(i => Cell(result, i, 0).LongValue).ToArray());
        CollectionAssert.AreEqual(new long[] { 3, 4 }, await AccountIds(database, executor, dbname));
    }

    [Test]
    public async Task DeleteUsing_ATargetRowThatMatchesTwiceIsDeletedOnce()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await Dml(database, executor, dbname, "INSERT INTO deltas (did, account_id, amount) VALUES (13, 1, 1)");

        ExecuteNonSQLResult result = await Dml(database, executor, dbname,
            "DELETE FROM accounts USING deltas WHERE deltas.account_id = accounts.id");

        Assert.AreEqual(2, result.ModifiedRows);
        CollectionAssert.AreEqual(new long[] { 3, 4 }, await AccountIds(database, executor, dbname));
    }

    [Test]
    public async Task DeleteAlias_WithoutUsing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        ExecuteNonSQLResult result = await Dml(database, executor, dbname, "DELETE FROM accounts AS a WHERE a.closed RETURNING a.id");

        Assert.AreEqual(1, result.ModifiedRows);
        Assert.AreEqual(3, Cell(result, 0, 0).LongValue);
        CollectionAssert.AreEqual(new long[] { 1, 2, 4 }, await AccountIds(database, executor, dbname));
    }

    [Test]
    public async Task DeleteUsing_AForeignKeyThatReferencesTheRowIsEnforced()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();
        await Ddl(database, executor, dbname, "CREATE TABLE cards (cid INT64 PRIMARY KEY, account_id INT64 REFERENCES accounts (id))");
        await Dml(database, executor, dbname, "INSERT INTO cards (cid, account_id) VALUES (1, 2)");

        CamusDBException error = Refused(database, executor, dbname,
            "DELETE FROM accounts a USING deltas d WHERE d.account_id = a.id");

        Assert.AreEqual(CamusDBErrorCodes.ForeignKeyRestrictDelete, error.Code);
        CollectionAssert.AreEqual(new long[] { 1, 2, 3, 4 }, await AccountIds(database, executor, dbname));
    }

    [TestCase("DELETE FROM accounts a USING deltas d", "requires a WHERE clause", TestName = "DeleteUsing_WithoutWhereIsRefused")]
    [TestCase("DELETE FROM accounts a USING deltas d WHERE d.account_id = a.id LIMIT 1", "LIMIT is not supported", TestName = "DeleteUsing_WithLimitIsRefused")]
    [TestCase("DELETE FROM accounts a USING deltas d WHERE d.account_id = a.id RETURNING d.did", "can read only columns of the target", TestName = "DeleteUsing_ReturningOfASourceColumnIsRefused")]
    [TestCase("DELETE FROM accounts USING accounts WHERE accounts.id = 1", "specified more than once", TestName = "DeleteUsing_TheTargetTwiceWithoutAnAliasIsRefused")]
    public async Task DeleteUsing_RefusedStatementDeletesNothing(string sql, string message)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await Seed();

        CamusDBException error = Refused(database, executor, dbname, sql);

        StringAssert.Contains(message, error.Message);
        CollectionAssert.AreEqual(new long[] { 1, 2, 3, 4 }, await AccountIds(database, executor, dbname));
    }

    // ── USING is a reserved word ──

    [Test]
    public async Task AColumnNamedUsingNeedsBackticks()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        Assert.ThrowsAsync<CamusDBException>(async () => await Ddl(database, executor, dbname, "CREATE TABLE u (id INT64 PRIMARY KEY, using INT64)"));

        await Ddl(database, executor, dbname, "CREATE TABLE u (id INT64 PRIMARY KEY, `using` INT64)");
        await Dml(database, executor, dbname, "INSERT INTO u (id, `using`) VALUES (1, 2)");
        List<QueryResultRow> rows = await Query(database, executor, dbname, "SELECT `using` FROM u");
        Assert.AreEqual(2, rows[0].Row["using"].LongValue);
    }
}
