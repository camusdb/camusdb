
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
/// A query may ORDER BY a column that is not in the SELECT list, referencing it through the
/// table's alias ("SELECT id, ref FROM t AS e ... ORDER BY e.created_at"). Projection pushdown
/// must still fetch the sort column from storage even though it is alias-qualified and absent
/// from the projection, otherwise the sorter cannot find it and the query fails.
/// </summary>
public sealed class TestOrderByNonProjectedColumn : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)>
        SetupTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "environments",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("database_ref", ColumnType.String, notNull: true),
                new("created_at", ColumnType.Integer64, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        KvTransaction txn = await database.Transactions.BeginAsync();

        // Insert rows out of created_at order so a correct sort is observable.
        foreach ((string reference, long createdAt) in new[]
                 {
                     ("ref-c", 300L),
                     ("ref-a", 100L),
                     ("ref-b", 200L),
                 })
        {
            InsertTicket ticket = new(
                txnState: txn,
                databaseName: dbname,
                tableName: "environments",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "database_ref", new(ColumnType.String, reference) },
                        { "created_at", new(ColumnType.Integer64, createdAt) },
                    }
                });
            await executor.Insert(ticket);
        }

        await database.Transactions.CommitAsync(txn);
        return (dbname, database, executor);
    }

    private static async Task<List<QueryResultRow>> RunSql(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();

        ExecuteSQLTicket ticket = new(
            txnState: txn,
            database: dbname,
            sql: sql,
            parameters: null);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();

        await database.Transactions.CommitAsync(txn);
        return rows;
    }

    [Test]
    public async Task OrderByAliasQualifiedNonProjectedColumn_SortsCorrectly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT id, database_ref FROM environments AS e ORDER BY e.created_at");

        Assert.AreEqual(3, rows.Count, "Expected all three rows back");

        List<string> orderedRefs = rows.Select(r => r.Row["database_ref"].StrValue!).ToList();
        Assert.AreEqual(new List<string> { "ref-a", "ref-b", "ref-c" }, orderedRefs,
            "Rows must be sorted by the non-projected created_at column");
    }

    [Test]
    public async Task OrderByBareNonProjectedColumn_SortsCorrectly()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTable();

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT id, database_ref FROM environments ORDER BY created_at");

        Assert.AreEqual(3, rows.Count, "Expected all three rows back");

        List<string> orderedRefs = rows.Select(r => r.Row["database_ref"].StrValue!).ToList();
        Assert.AreEqual(new List<string> { "ref-a", "ref-b", "ref-c" }, orderedRefs,
            "Rows must be sorted by the non-projected created_at column");
    }
}
