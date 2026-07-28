/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Alias-qualified predicates (<c>WHERE u.col = …</c>, the shape every ORM generates) must match
/// indexes exactly like their unqualified equivalents. Before the alias-resolution pass in the
/// planner, the qualified name never matched an index column, so every aliased single-table query
/// silently ran as a full table scan — and under Serializable took a whole-table shared range lock.
/// </summary>
public class TestAliasedPredicateIndexMatching : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupWalletsTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "wallets",
            columns: new ColumnInfo[]
            {
                new("usersId",    ColumnType.Id),
                new("currencyId", ColumnType.Id),
                new("amount",     ColumnType.Integer64),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[]
                {
                    new("usersId", OrderType.Ascending),
                    new("currencyId", OrderType.Ascending),
                }),
            },
            ifNotExists: false
        ));

        for (int i = 0; i < 4; i++)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "wallets",
                values: new()
                {
                    new()
                    {
                        { "usersId",    new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "currencyId", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "amount",     new(ColumnType.Integer64, 100 + i) },
                    }
                }
            ));
        }

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> ExplainAsync(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: txn, database: dbname, sql: sql, parameters: null);
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    private static string ScanNodeOf(List<QueryResultRow> rows)
    {
        foreach (QueryResultRow row in rows)
        {
            string? node = row.Row["node"].StrValue;
            if (node is "table-scan" or "index-range-scan" or "index-lookup" or "index-in-list-scan")
                return node!;
        }

        return "(no scan node)";
    }

    [Test]
    public async Task TestAliasedEqualityPrefix_UsesIndexRangeScan()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupWalletsTable();

        string id = ObjectIdGenerator.Generate().ToString();

        List<QueryResultRow> unaliased = await ExplainAsync(executor, database, dbname,
            $"EXPLAIN SELECT currencyId FROM wallets WHERE usersId = \"{id}\"");
        List<QueryResultRow> aliased = await ExplainAsync(executor, database, dbname,
            $"EXPLAIN SELECT u.currencyId FROM wallets u WHERE u.usersId = \"{id}\"");

        Assert.That(ScanNodeOf(unaliased), Is.EqualTo("index-range-scan"));
        Assert.That(ScanNodeOf(aliased), Is.EqualTo(ScanNodeOf(unaliased)),
            "Aliased predicate must produce the same access path as the unqualified predicate");
    }

    [Test]
    public async Task TestAliasedFullKeyEquality_UsesIndexAccessPath()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupWalletsTable();

        string userId = ObjectIdGenerator.Generate().ToString();
        string currencyId = ObjectIdGenerator.Generate().ToString();

        List<QueryResultRow> aliased = await ExplainAsync(executor, database, dbname,
            "EXPLAIN SELECT u.amount FROM wallets u " +
            $"WHERE u.usersId = \"{userId}\" AND u.currencyId = \"{currencyId}\"");

        Assert.That(ScanNodeOf(aliased), Is.Not.EqualTo("table-scan"),
            "Full-PK aliased equality must not degrade to a full table scan");
    }

    [Test]
    public async Task TestAliasedInList_UsesIndexAccessPath()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupWalletsTable();

        string a = ObjectIdGenerator.Generate().ToString();
        string b = ObjectIdGenerator.Generate().ToString();

        List<QueryResultRow> unaliased = await ExplainAsync(executor, database, dbname,
            $"EXPLAIN SELECT amount FROM wallets WHERE usersId IN (\"{a}\", \"{b}\")");
        List<QueryResultRow> aliased = await ExplainAsync(executor, database, dbname,
            $"EXPLAIN SELECT u.amount FROM wallets u WHERE u.usersId IN (\"{a}\", \"{b}\")");

        Assert.That(ScanNodeOf(aliased), Is.EqualTo(ScanNodeOf(unaliased)),
            "Aliased IN-list must produce the same access path as the unqualified IN-list");
    }

    [Test]
    public async Task TestAliasedPredicate_ReturnsSameRowsAsUnaliased()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupWalletsTable();

        // Fetch one existing row's key through a full scan, then verify the aliased
        // (index-driven) predicate finds the same row the unaliased one does.
        List<QueryResultRow> all = await ExplainRowsAsync(executor, database, dbname,
            "SELECT usersId FROM wallets");
        Assert.That(all, Is.Not.Empty);

        string existingId = all[0].Row["usersId"].StrValue!;

        List<QueryResultRow> unaliased = await ExplainRowsAsync(executor, database, dbname,
            $"SELECT amount FROM wallets WHERE usersId = \"{existingId}\"");
        List<QueryResultRow> aliased = await ExplainRowsAsync(executor, database, dbname,
            $"SELECT u.amount FROM wallets u WHERE u.usersId = \"{existingId}\"");

        Assert.That(aliased.Count, Is.EqualTo(unaliased.Count));
        Assert.That(aliased.Count, Is.GreaterThan(0));
        Assert.That(aliased[0].Row["amount"].LongValue, Is.EqualTo(unaliased[0].Row["amount"].LongValue));
    }

    private static Task<List<QueryResultRow>> ExplainRowsAsync(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql) => ExplainAsync(executor, database, dbname, sql);
}
