/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
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
using CamusDB.Tests.SqlFuzz;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Runs each statement of <see cref="DecidedDivergences"/>, so an entry cannot stay on the list after
/// the behavior changes. The PostgreSQL side of an entry is a recorded measurement, and this fixture
/// does not need a PostgreSQL server.
/// </summary>
[NonParallelizable]
public sealed class TestPostgreSqlDivergences : SharedNodeBaseTest
{
    private static IEnumerable<TestCaseData> Entries() =>
        DecidedDivergences.All.Select(entry => new TestCaseData(entry).SetName(entry.Name));

    private static async Task Prepare(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        if (sql.StartsWith("CREATE", StringComparison.Ordinal))
            await executor.ExecuteDDLSQL(new(txnState: tx, database: dbname, sql: sql, parameters: null));
        else
            _ = await executor.ExecuteNonSQLQuery(new(txnState: tx, database: dbname, sql: sql, parameters: null));

        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<string> Outcome(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            string outcome = "no rows";

            if (sql.StartsWith("SELECT", StringComparison.Ordinal))
            {
                (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
                List<QueryResultRow> rows = await cursor.ToListAsync();

                if (rows.Count > 0)
                    outcome = Render(rows[0].Row.First().Value);
            }
            else
            {
                _ = await executor.ExecuteNonSQLQuery(new(txnState: tx, database: dbname, sql: sql, parameters: null));
                outcome = "written";
            }

            await database.Transactions.CommitAsync(tx);
            return outcome;
        }
        catch (CamusDBException ex)
        {
            await database.Transactions.RollbackAsync(tx);
            return ex.Code;
        }
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Bool => value.BoolValue.ToString(),
        ColumnType.Integer64 => value.LongValue.ToString(),
        _ => value.ToString() ?? "",
    };

    [TestCaseSource(nameof(Entries))]
    public async Task HoldsAsRecorded(DecidedDivergence entry)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        foreach (string sql in entry.Setup)
            await Prepare(database, executor, dbname, sql);

        Assert.AreEqual(entry.CamusDb, await Outcome(database, executor, dbname, entry.Statement), entry.Statement);
    }

    [Test]
    public void EveryEntryStatesItsReason()
    {
        foreach (DecidedDivergence entry in DecidedDivergences.All)
        {
            Assert.IsNotEmpty(entry.Reason, entry.Name);
            Assert.AreNotEqual(entry.CamusDb, entry.PostgreSql, entry.Name);
        }
    }
}
