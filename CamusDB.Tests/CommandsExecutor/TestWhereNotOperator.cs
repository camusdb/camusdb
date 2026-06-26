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

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for the unary prefix NOT operator in a WHERE clause
/// (for example: SELECT * FROM robots WHERE NOT enabled).
/// </summary>
[TestFixture]
public sealed class TestWhereNotOperator : BaseTest
{
    private static async Task<List<QueryResultRow>> QueryAsync(
        CommandExecutor executor, DatabaseDescriptor db, string dbname, string sql)
    {
        KvTransaction tx = await db.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);
        return rows;
    }

    private async Task<(string, DatabaseDescriptor, CommandExecutor)> SeedRobots()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await CreateDatabase();

        await executor.CreateTable(new CreateTableTicket(
            databaseName: dbname, tableName: "robots",
            columns: new ColumnInfo[]
            {
                new("id",      ColumnType.Id,     notNull: true),
                new("name",    ColumnType.String, notNull: true),
                new("enabled", ColumnType.Bool),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) }),
            },
            ifNotExists: false));

        async Task Insert(string name, ColumnValue enabled)
        {
            KvTransaction tx = await db.Transactions.BeginAsync();
            await executor.Insert(new InsertTicket(tx, dbname, "robots",
            [
                new()
                {
                    ["id"]      = new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()),
                    ["name"]    = new(ColumnType.String, name),
                    ["enabled"] = enabled,
                }
            ]));
            await db.Transactions.CommitAsync(tx);
        }

        await Insert("on1",  ColumnValue.FromBool(true));
        await Insert("off1", ColumnValue.FromBool(false));
        await Insert("on2",  ColumnValue.FromBool(true));
        await Insert("unk",  ColumnValue.Null);   // NULL enabled — three-valued logic

        return (dbname, db, executor);
    }

    [Test]
    public async Task NotEnabled_ReturnsOnlyDisabledRows()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SeedRobots();

        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname,
            "SELECT name FROM robots WHERE NOT enabled");

        // Only the explicitly-false row matches; NULL is excluded (NOT NULL is unknown).
        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("off1", rows[0].Row["name"].StrValue);
    }

    [Test]
    public async Task Enabled_ReturnsOnlyEnabledRows_NullExcluded()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SeedRobots();

        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname,
            "SELECT name FROM robots WHERE enabled");

        Assert.AreEqual(2, rows.Count);
        Assert.That(rows.Select(r => r.Row["name"].StrValue), Is.EquivalentTo(new[] { "on1", "on2" }));
    }

    [Test]
    public async Task NotEnabled_CombinedWithAnd_BindsTighterThanAnd()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SeedRobots();

        // (NOT enabled) AND name = 'off1' — the NOT applies only to enabled.
        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname,
            "SELECT name FROM robots WHERE NOT enabled AND name = \"off1\"");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual("off1", rows[0].Row["name"].StrValue);
    }

    [Test]
    public async Task NotParenthesizedComparison_NegatesPredicate()
    {
        (string dbname, DatabaseDescriptor db, CommandExecutor executor) = await SeedRobots();

        // NOT (name = 'off1') — everything except off1.
        List<QueryResultRow> rows = await QueryAsync(executor, db, dbname,
            "SELECT name FROM robots WHERE NOT (name = \"off1\")");

        Assert.That(rows.Select(r => r.Row["name"].StrValue), Is.EquivalentTo(new[] { "on1", "on2", "unk" }));
    }
}
