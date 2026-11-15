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
using CamusDB.Core.SQLParser;
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
        else if (sql.StartsWith("SELECT", StringComparison.Ordinal))
        {
            // A setup SELECT runs for its effect, such as a sequence value that it draws.
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            _ = await cursor.ToListAsync();
        }
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

            if (IsRead(sql))
            {
                (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
                List<QueryResultRow> rows = await cursor.ToListAsync();

                if (rows.Count > 0)
                    outcome = Render(rows[0].Row.First().Value);
            }
            else if (sql.StartsWith("CREATE", StringComparison.Ordinal))
            {
                await executor.ExecuteDDLSQL(new(txnState: tx, database: dbname, sql: sql, parameters: null));
                outcome = "written";
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

    /// <summary>
    /// True for a statement that returns rows: a SELECT, or a WITH in front of a SELECT. A WITH that does
    /// not parse is sent to the query path too, so its error is the outcome.
    /// </summary>
    private static bool IsRead(string sql)
    {
        if (sql.StartsWith("SELECT", StringComparison.Ordinal))
            return true;

        if (!sql.StartsWith("WITH", StringComparison.Ordinal))
            return false;

        try
        {
            return SQLParserProcessor.Parse(sql).nodeType == NodeType.Select;
        }
        catch (CamusDBException)
        {
            return true;
        }
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "NULL",
        ColumnType.Bool => value.BoolValue.ToString(),
        ColumnType.Integer64 => value.LongValue.ToString(),
        ColumnType.Float64 => value.FloatValue.ToString("R", System.Globalization.CultureInfo.InvariantCulture),
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
