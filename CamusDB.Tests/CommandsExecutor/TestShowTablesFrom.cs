/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>SHOW TABLES FROM|IN db [LIKE 'pattern']</c> through <c>ExecuteSQLQuery</c>. The oracle is
/// <c>SHOW TABLES</c> run with the target as the context database: the two must give the same rows
/// whatever the ticket's own database is, including no database at all.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestShowTablesFrom : BaseTest
{
    // A letter first: a name that starts with a digit does not lex as a bare identifier.
    private static string NewName() => "stf" + Guid.NewGuid().ToString("n");

    private async Task<(string name, DatabaseDescriptor database)> NewDatabase(CommandExecutor executor)
    {
        string name = NewName();
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(name, ifNotExists: false));
        TrackDatabase(name, executor);
        return (name, database);
    }

    private static async Task Ddl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    /// <summary>
    /// Runs a listing with no transaction, the way every transport runs a server-level query, and
    /// returns the <c>tables</c> column.
    /// </summary>
    private static async Task<List<string>> List(CommandExecutor executor, string contextDb, string sql)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: null!, database: contextDb, sql: sql, parameters: null));

        List<string> tables = [];
        await foreach (QueryResultRow row in cursor)
            tables.Add(row.Row["tables"].StrValue!);
        return tables;
    }

    /// <summary>The oracle: plain SHOW TABLES inside a transaction on <paramref name="dbname"/>.</summary>
    private static async Task<List<string>> ShowTablesIn(CommandExecutor executor, string dbname, string sql = "SHOW TABLES")
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction tx = await database.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));

        List<string> tables = [];
        await foreach (QueryResultRow row in cursor)
            tables.Add(row.Row["tables"].StrValue!);
        await database.Transactions.CommitAsync(tx);
        return tables;
    }

    /// <summary>
    /// A database with two tables, a view, a materialized view and a sequence. Only the two tables
    /// may be listed.
    /// </summary>
    private async Task<string> SeedCatalog(CommandExecutor executor)
    {
        (string name, _) = await NewDatabase(executor);
        await Ddl(executor, name, "CREATE TABLE orders (id int64 PRIMARY KEY NOT NULL, total int64 NULL)");
        await Ddl(executor, name, "CREATE TABLE order_items (id int64 PRIMARY KEY NOT NULL, qty int64 NULL)");
        await Ddl(executor, name, "CREATE VIEW big_orders AS SELECT id FROM orders WHERE total > 10");
        await Ddl(executor, name, "CREATE MATERIALIZED VIEW order_totals AS SELECT id, total FROM orders");
        await Ddl(executor, name, "CREATE SEQUENCE order_seq");
        return name;
    }

    [Test]
    public async Task MatchesShowTablesInTheTarget_ForEveryContextDatabase()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string target = await SeedCatalog(executor);
        (string other, _) = await NewDatabase(executor);
        await Ddl(executor, other, "CREATE TABLE unrelated (id int64 PRIMARY KEY NOT NULL)");

        List<string> expected = await ShowTablesIn(executor, target);
        CollectionAssert.AreEquivalent(new[] { "orders", "order_items" }, expected);

        // No context database, the target itself, and a different database: the rows never change.
        foreach (string contextDb in new[] { "", target, other })
        {
            CollectionAssert.AreEquivalent(expected, await List(executor, contextDb, $"SHOW TABLES FROM {target}"), contextDb);
            CollectionAssert.AreEquivalent(expected, await List(executor, contextDb, $"SHOW TABLES IN {target}"), contextDb);
        }
    }

    [TestCase("order%")]
    [TestCase("order\\_%")]
    [TestCase("orders")]
    [TestCase("nothing%")]
    public async Task LikeFilter_MatchesShowTablesLikeInTheTarget(string pattern)
    {
        CommandExecutor executor = CreateCommandExecutor();
        string target = await SeedCatalog(executor);
        (string other, _) = await NewDatabase(executor);

        List<string> expected = await ShowTablesIn(executor, target, $"SHOW TABLES LIKE '{pattern}'");

        CollectionAssert.AreEquivalent(expected, await List(executor, other, $"SHOW TABLES FROM {target} LIKE '{pattern}'"));
        CollectionAssert.AreEquivalent(expected, await List(executor, "", $"SHOW TABLES IN {target} LIKE '{pattern}'"));
    }

    [Test]
    public async Task BackQuotedDatabaseName_ResolvesLikeTheBareName()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string target = await SeedCatalog(executor);

        foreach (string sql in new[] { $"SHOW TABLES FROM `{target}`", $"SHOW TABLES IN `{target}` LIKE 'orders'" })
            CollectionAssert.IsSubsetOf(await List(executor, "", sql), new[] { "orders", "order_items" }, sql);

        CollectionAssert.AreEquivalent(new[] { "orders", "order_items" }, await List(executor, "", $"SHOW TABLES FROM `{target}`"));
    }

    [Test]
    public async Task NameCase_ResolvesTheSameWayAsShowBranchesFrom()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string target = await SeedCatalog(executor);
        string upper = target.ToUpperInvariant();

        // Both statements resolve the name through the registry, so they must agree on whether a
        // name in another case names the database.
        bool branchesResolves = await Resolves(() => List(executor, "", $"SHOW BRANCHES FROM {upper}"));
        bool tablesResolves = await Resolves(() => List(executor, "", $"SHOW TABLES FROM {upper}"));
        Assert.AreEqual(branchesResolves, tablesResolves);
    }

    private static async Task<bool> Resolves(Func<Task> statement)
    {
        try
        {
            await statement();
            return true;
        }
        catch (CamusDBException e) when (e.Code == CamusDBErrorCodes.DatabaseDoesntExist)
        {
            return false;
        }
    }

    [Test]
    public async Task UnknownDatabase_IsDatabaseDoesntExist()
    {
        CommandExecutor executor = CreateCommandExecutor();
        (string other, _) = await NewDatabase(executor);
        string missing = NewName();

        foreach (string contextDb in new[] { "", other })
        {
            CamusDBException e = Assert.ThrowsAsync<CamusDBException>(
                async () => await List(executor, contextDb, $"SHOW TABLES FROM {missing}"))!;
            Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, e.Code);
            Assert.AreEqual($"Database '{missing}' does not exist", e.Message);
        }
    }

    [Test]
    public async Task DroppedDatabase_IsDatabaseDoesntExist()
    {
        CommandExecutor executor = CreateCommandExecutor();
        (string target, _) = await NewDatabase(executor);
        await Ddl(executor, target, "CREATE TABLE t (id int64 PRIMARY KEY NOT NULL)");
        CollectionAssert.AreEquivalent(new[] { "t" }, await List(executor, "", $"SHOW TABLES FROM {target}"));

        await executor.DropDatabase(new DropDatabaseTicket(target));

        CamusDBException e = Assert.ThrowsAsync<CamusDBException>(
            async () => await List(executor, "", $"SHOW TABLES FROM {target}"))!;
        Assert.AreEqual(CamusDBErrorCodes.DatabaseDoesntExist, e.Code);
    }

    [Test]
    public async Task TableCreatedAfterAListing_IsListedNext()
    {
        CommandExecutor executor = CreateCommandExecutor();
        (string target, _) = await NewDatabase(executor);

        Assert.IsEmpty(await List(executor, "", $"SHOW TABLES FROM {target}"));

        await Ddl(executor, target, "CREATE TABLE late (id int64 PRIMARY KEY NOT NULL)");

        CollectionAssert.AreEquivalent(new[] { "late" }, await List(executor, "", $"SHOW TABLES FROM {target}"));
    }

    [Test]
    public async Task InsideAnOpenTransactionOnAnotherDatabase_LeavesThatTransactionUsable()
    {
        CommandExecutor executor = CreateCommandExecutor();
        string target = await SeedCatalog(executor);
        (string other, DatabaseDescriptor otherDb) = await NewDatabase(executor);
        await Ddl(executor, other, "CREATE TABLE kv (id int64 PRIMARY KEY NOT NULL, v int64 NULL)");

        KvTransaction tx = await otherDb.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, other, "INSERT INTO kv (id, v) VALUES (1, 10)", null));

        // A transport sends a server-level query outside the session's transaction. A direct caller
        // that passes the transaction gets the same rows, and the transaction is not touched.
        CollectionAssert.AreEquivalent(new[] { "orders", "order_items" }, await List(executor, other, $"SHOW TABLES FROM {target}"));
        (_, IAsyncEnumerable<QueryResultRow> withTx) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, other, $"SHOW TABLES FROM {target}", null));
        CollectionAssert.AreEquivalent(new[] { "orders", "order_items" }, (await withTx.ToListAsync()).Select(r => r.Row["tables"].StrValue));

        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, other, "INSERT INTO kv (id, v) VALUES (2, 20)", null));
        await otherDb.Transactions.CommitAsync(tx);

        KvTransaction check = await otherDb.Transactions.BeginAsync();
        (_, IAsyncEnumerable<QueryResultRow> rows) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(check, other, "SELECT id FROM kv", null));
        Assert.AreEqual(2, (await rows.ToListAsync()).Count);
        await otherDb.Transactions.CommitAsync(check);
    }
}
