/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Config;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>max_table_function_rows</c>: one read of a function in <c>FROM</c> makes at most that many rows.
/// The row past the limit fails the statement with
/// <see cref="CamusDBErrorCodes.TableFunctionRowLimitExceeded"/>. Every case goes through SQL. Most cases
/// use a limit of 100 rows, so a series of 101 rows shows the refusal.
/// </summary>
[NonParallelizable]
public sealed class TestTableFunctionRowLimit : BaseTest
{
    private const long Limit = 100;

    internal static async Task<List<QueryResultRow>> Query(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string db,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            ExecuteSQLTicket ticket = new(txnState: tx, database: db, sql: sql, parameters: parameters);
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static long[] Longs(List<QueryResultRow> rows, string column) =>
        rows.Select(row => row.Row[column].LongValue).ToArray();

    private Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> WithLimit(long limit) =>
        CreateDatabase(Options with { MaxTableFunctionRows = limit });

    private static CamusDBException Refused(CommandExecutor executor, DatabaseDescriptor database, string db, string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        CamusDBException error = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, database, db, sql, parameters))!;
        Assert.That(error.Code, Is.EqualTo(CamusDBErrorCodes.TableFunctionRowLimitExceeded), error.Message);
        return error;
    }

    [Test]
    public async Task ASeriesOfExactlyTheLimit_ReturnsEveryRow()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await WithLimit(Limit);

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT i FROM generate_series(1, 100) i");

        Assert.That(Longs(rows, "i"), Is.EqualTo(Enumerable.Range(1, 100).Select(i => (long)i).ToArray()));
    }

    [Test]
    public async Task TheRowPastTheLimit_FailsTheStatement()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await WithLimit(Limit);

        CamusDBException error = Refused(executor, database, db, "SELECT COUNT(*) AS c FROM generate_series(1, 101)");

        Assert.That(error.Message, Does.Contain("generate_series")
            .And.Contain("max_table_function_rows")
            .And.Contain("100 rows"));
    }

    [Test]
    public async Task ALimitThatStopsTheRead_ReturnsItsRows()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await WithLimit(Limit);

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT i FROM generate_series(1, 1000000000) i LIMIT 10");

        Assert.That(Longs(rows, "i"), Is.EqualTo(Enumerable.Range(1, 10).Select(i => (long)i).ToArray()));
    }

    [Test]
    public async Task RowsThatAFilterDrops_AreCounted()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await WithLimit(Limit);

        // The function makes 55 rows to find five that pass the filter: under the limit.
        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT i FROM generate_series(1, 1000) i WHERE i > 50 LIMIT 5");
        Assert.That(Longs(rows, "i"), Is.EqualTo(new long[] { 51, 52, 53, 54, 55 }));

        // No row passes, so the read goes on to the row past the limit.
        Refused(executor, database, db, "SELECT i FROM generate_series(1, 1000000000) i WHERE i < 0");
    }

    [Test]
    public async Task AParameterArgument_IsCheckedWithItsValue()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await WithLimit(Limit);

        const string sql = "SELECT i FROM generate_series(1, @to) i";

        List<QueryResultRow> rows = await Query(executor, database, db, sql,
            new() { { "@to", new ColumnValue(ColumnType.Integer64, 100) } });
        Assert.That(rows, Has.Count.EqualTo(100));

        Refused(executor, database, db, sql, new() { { "@to", new ColumnValue(ColumnType.Integer64, 101) } });
    }

    [Test]
    public async Task EachReadOfAJoinCountsOnItsOwn()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await WithLimit(Limit);

        // Each side makes 60 rows. The two sides together make more than 100, and the join can read
        // one side again for each row of the other, so only a count for each read lets this run.
        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT COUNT(*) AS c FROM generate_series(1, 60) a, generate_series(1, 60) b");

        Assert.That(Longs(rows, "c"), Is.EqualTo(new long[] { 3600 }));
    }

    [Test]
    public async Task ASortThatSpilled_DeletesItsFilesWhenTheReadIsRefused()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { MaxTableFunctionRows = Limit, ForceSpillThresholdRows = 10 });

        string spillRoot = Path.Combine(Options.DataDirectory, "tmp", "spill");
        string[] SpillFiles() => Directory.Exists(spillRoot)
            ? Directory.GetFiles(spillRoot, "*.spill", SearchOption.AllDirectories)
            : [];

        // The same sort under the limit spills: its run files exist while the merge returns rows.
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: db,
            sql: "SELECT i FROM generate_series(1, 100) i ORDER BY i DESC", parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);

        await using (IAsyncEnumerator<QueryResultRow> reader = cursor.GetAsyncEnumerator())
        {
            Assert.That(await reader.MoveNextAsync(), Is.True);
            Assert.That(SpillFiles(), Is.Not.Empty, "a sort with a spill threshold of 10 rows writes runs to disk");
        }

        await database.Transactions.RollbackIfNotCompletedAsync(tx);
        Assert.That(SpillFiles(), Is.Empty);

        Refused(executor, database, db, "SELECT i FROM generate_series(1, 101) i ORDER BY i DESC");

        Assert.That(SpillFiles(), Is.Empty, "the refused sort wrote runs before the row past the limit, and must delete them");
    }

    [Test]
    public async Task EveryFunctionInFrom_HasTheLimit()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await WithLimit(2);

        List<QueryResultRow> rows = await Query(executor, database, db,
            "SELECT key FROM jsonb_each('{\"a\": 1, \"b\": 2}')");
        Assert.That(rows, Has.Count.EqualTo(2));

        CamusDBException error = Refused(executor, database, db,
            "SELECT key FROM jsonb_each('{\"a\": 1, \"b\": 2, \"c\": 3}')");
        Assert.That(error.Message, Does.Contain("jsonb_each"));
    }

    [Test]
    public async Task ZeroTurnsTheLimitOff()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await WithLimit(0);

        List<QueryResultRow> rows = await Query(executor, database, db, "SELECT COUNT(*) AS c FROM generate_series(1, 1000)");

        Assert.That(Longs(rows, "c"), Is.EqualTo(new long[] { 1000 }));
    }

    [Test]
    public async Task TheDefaultLimit_IsFiveMillionRows()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException error = Refused(executor, database, db, "SELECT COUNT(*) AS c FROM generate_series(1, 5000001)");

        Assert.That(error.Message, Does.Contain("5000000 rows"));
    }
}

/// <summary>
/// <c>max_table_function_rows</c> is a runtime setting: the leaf scan reads it from its database's
/// options at the start of each read, so a published change applies to the next statement without a
/// restart.
/// </summary>
[NonParallelizable]
internal sealed class TestTableFunctionRowLimitIsARuntimeSetting : BaseTest
{
    [Test]
    public async Task APublishedLimitAppliesToTheNextStatement()
    {
        CamusDBOptions initial = Options with { MaxTableFunctionRows = 100 };
        CamusDBOptionsHolder holder = new(initial);

        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabaseWith(CreateCommandExecutor(initial, holder));

        const string sql = "SELECT i FROM generate_series(1, 150) i";

        CamusDBException refused = Assert.ThrowsAsync<CamusDBException>(
            () => TestTableFunctionRowLimit.Query(executor, database, db, sql))!;
        Assert.That(refused.Code, Is.EqualTo(CamusDBErrorCodes.TableFunctionRowLimitExceeded));

        holder.Publish(initial with { MaxTableFunctionRows = 200 });
        Assert.That(await TestTableFunctionRowLimit.Query(executor, database, db, sql), Has.Count.EqualTo(150));
    }
}
