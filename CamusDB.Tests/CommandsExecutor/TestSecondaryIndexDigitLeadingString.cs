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
/// An equality lookup through a non-unique secondary index on a string column must find a value
/// that begins with a digit exactly like a value that begins with a letter. The lookup path must
/// not treat the constant as numeric because of its first character.
/// </summary>
[NonParallelizable]
public class TestSecondaryIndexDigitLeadingString : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupUsersTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname,
            tableName: "users",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id, notNull: true),
                new("publicId", ColumnType.String),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[]
                {
                    new("id", OrderType.Ascending),
                }),
                new(ConstraintType.IndexMulti, "publicId_idx", new ColumnIndexInfo[]
                {
                    new("publicId", OrderType.Ascending),
                }),
            },
            ifNotExists: false
        ));

        KvTransaction txn = await database.Transactions.BeginAsync();

        string[] publicIds = { "2NMIKZQK6Q", "9DRZTHF6J4", "ABMMLDVAZU", "LML8WUJQUU" };

        foreach (string publicId in publicIds)
        {
            await executor.Insert(new InsertTicket(
                txnState: txn,
                databaseName: dbname,
                tableName: "users",
                values: new()
                {
                    new()
                    {
                        { "id",       new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "publicId", new(ColumnType.String, publicId) },
                    }
                }
            ));
        }

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> QueryAsync(
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

    [Test]
    [TestCase("2NMIKZQK6Q")]
    [TestCase("9DRZTHF6J4")]
    [TestCase("ABMMLDVAZU")]
    [TestCase("LML8WUJQUU")]
    public async Task TestIndexedEqualityFindsValue(string publicId)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsersTable();

        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            $"SELECT * FROM users WHERE publicId = '{publicId}'");

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Row["publicId"].StrValue, Is.EqualTo(publicId));
    }

    [Test]
    public async Task TestIndexedEqualityUsesIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsersTable();

        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            "EXPLAIN SELECT * FROM users WHERE publicId = '2NMIKZQK6Q'");

        List<string?> nodes = rows.ConvertAll(r => r.Row["node"].StrValue);
        Assert.That(nodes, Does.Contain("index-range-scan").Or.Contain("index-lookup"),
            "the equality lookup must go through the secondary index; nodes: " + string.Join(", ", nodes));
    }

    [Test]
    [TestCase("2NMIKZQK6Q")]
    [TestCase("ABMMLDVAZU")]
    public async Task TestIndexedEqualityFindsValue_BacktickedColumn(string publicId)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsersTable();

        List<QueryResultRow> rows = await QueryAsync(executor, database, dbname,
            $"SELECT `u`.`id`, `u`.`publicId` FROM users AS `u` WHERE `u`.`publicId` = '{publicId}' LIMIT 1");

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Row["publicId"].StrValue, Is.EqualTo(publicId));
    }

    [Test]
    [TestCase("2NMIKZQK6Q")]
    [TestCase("ABMMLDVAZU")]
    public async Task TestIndexedEqualityFindsValue_Parameterized(string publicId)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupUsersTable();

        KvTransaction txn = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(
            txnState: txn,
            database: dbname,
            sql: "SELECT * FROM users WHERE publicId = @publicId",
            parameters: new()
            {
                { "@publicId", new(ColumnType.String, publicId) },
            });
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(txn);

        Assert.That(rows, Has.Count.EqualTo(1));
        Assert.That(rows[0].Row["publicId"].StrValue, Is.EqualTo(publicId));
    }
}
