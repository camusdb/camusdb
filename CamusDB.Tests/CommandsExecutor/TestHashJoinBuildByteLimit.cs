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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Config;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// <c>hash_join_max_build_bytes</c>: with spill off, a hash-join build whose estimated bytes pass the
/// limit runs as a nested-loop join, as it does at <c>hash_join_max_build_rows</c>, and a nested-loop
/// join keeps its right side in memory only under the same byte limit. Each table holds four rows
/// of about 2 KB each (a 1,000-character string), so a limit of 1,024 bytes stops a build at its
/// second row while the row limit is far away. Every case goes through SQL and reads
/// <c>EXPLAIN (ANALYZE)</c> to see which join ran.
/// </summary>
[NonParallelizable]
internal sealed class TestHashJoinBuildByteLimit : BaseTest
{
    // The pad columns are in the select list, so the rows of both sides carry them. A column that
    // no part of the statement reads is not decoded, and its bytes are not held.
    private const string JoinSql = "SELECT a.id AS aid, b.id AS bid, a.pad AS apad, b.pad AS bpad FROM a JOIN b ON a.k = b.k";

    private static readonly (long, long)[] Pairs = [(1, 11), (2, 11), (3, 12), (3, 13)];

    private CamusDBOptions SpillOff(long maxBuildBytes) =>
        Options with { SpillEnabled = false, HashJoinMaxBuildRows = 1000, HashJoinMaxBuildBytes = maxBuildBytes };

    private static async Task Run(CommandExecutor executor, DatabaseDescriptor database, string db, string sql, bool ddl)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            ExecuteSQLTicket ticket = new(tx, db, sql, null);
            if (ddl)
                await executor.ExecuteDDLSQL(ticket);
            else
                await executor.ExecuteNonSQLQuery(ticket);
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> Query(CommandExecutor executor, DatabaseDescriptor database, string db, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>Two tables of four wide rows each. The join on <c>k</c> has the rows in <see cref="Pairs"/>.</summary>
    private static async Task<(string db, DatabaseDescriptor database, CommandExecutor executor)> Seed(
        (string db, DatabaseDescriptor database, CommandExecutor executor) engine)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = engine;
        string pad = new('x', 1000);

        await Run(executor, database, db, "CREATE TABLE a (id INT64 PRIMARY KEY, k INT64, pad STRING)", ddl: true);
        await Run(executor, database, db, "CREATE TABLE b (id INT64 PRIMARY KEY, k INT64, pad STRING)", ddl: true);
        await Run(executor, database, db,
            $"INSERT INTO a (id, k, pad) VALUES (1, 1, '{pad}'), (2, 1, '{pad}'), (3, 2, '{pad}'), (4, 3, '{pad}')", ddl: false);
        await Run(executor, database, db,
            $"INSERT INTO b (id, k, pad) VALUES (11, 1, '{pad}'), (12, 2, '{pad}'), (13, 2, '{pad}'), (14, 4, '{pad}')", ddl: false);

        return engine;
    }

    private static (long, long)[] Joined(List<QueryResultRow> rows) => rows
        .Select(row => (row.Row["aid"].LongValue, row.Row["bid"].LongValue))
        .OrderBy(pair => pair)
        .ToArray();

    /// <summary>Runs the join, checks its rows, and returns the detail of the one join row of its EXPLAIN (ANALYZE).</summary>
    private static async Task<string> JoinDetail(CommandExecutor executor, DatabaseDescriptor database, string db, string node)
    {
        Assert.That(Joined(await Query(executor, database, db, JoinSql)), Is.EqualTo(Pairs));

        List<QueryResultRow> plan = await Query(executor, database, db, "EXPLAIN (ANALYZE) " + JoinSql);
        List<QueryResultRow> joins = plan.Where(row => row.Row["node"].StrValue == node).ToList();
        Assert.That(joins, Has.Count.EqualTo(1), string.Join("\n", plan.Select(row => row.Row["node"].StrValue + " " + row.Row["detail"].StrValue)));

        return joins[0].Row["detail"].StrValue!;
    }

    private async Task<string> HashJoinDetail(CamusDBOptions options)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seed(await CreateDatabase(options));
        executor.Statistics.ForceHashJoinForTesting = true;
        return await JoinDetail(executor, database, db, "hash-join");
    }

    [Test]
    public async Task AWideBuildPastTheByteLimit_RunsAsTheNestedLoopFallback()
    {
        Assert.That(await HashJoinDetail(SpillOff(maxBuildBytes: 1024)), Does.Contain("ran=nested-loop-fallback"));
    }

    [Test]
    public async Task TheSameBuildUnderTheDefaultByteLimit_RunsAsAHashJoin()
    {
        Assert.That(await HashJoinDetail(Options with { SpillEnabled = false, HashJoinMaxBuildRows = 1000 }), Does.Not.Contain("ran="));
    }

    [Test]
    public async Task ZeroRemovesTheByteLimit()
    {
        Assert.That(await HashJoinDetail(SpillOff(maxBuildBytes: 0)), Does.Not.Contain("ran="));
    }

    [Test]
    public async Task WithSpillOn_TheByteLimitDoesNotApply()
    {
        // The spill caps (500,000 rows, 256 MiB) are far above this build, so it stays an in-memory hash join.
        Assert.That(await HashJoinDetail(Options with { SpillEnabled = true, HashJoinMaxBuildBytes = 1024 }), Does.Not.Contain("ran="));
    }

    [Test]
    public async Task ANestedLoopJoin_KeepsAWideRightSideOnlyUnderTheByteLimit()
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await Seed(await CreateDatabase(SpillOff(maxBuildBytes: 1024)));
        executor.Statistics.ForceNestedLoopForTesting = true;

        // The right side passes the byte limit, so the spool drops it and each left row reads it again.
        Assert.That(await JoinDetail(executor, database, db, "nested-loop-join"), Does.Contain("ran=nested-loop-rescan"));

        (string db2, DatabaseDescriptor database2, CommandExecutor executor2) =
            await Seed(await CreateDatabase(Options with { SpillEnabled = false, HashJoinMaxBuildRows = 1000 }));
        executor2.Statistics.ForceNestedLoopForTesting = true;

        // Under the default byte limit the spool keeps the right side, and the join reads it once.
        Assert.That(await JoinDetail(executor2, database2, db2, "nested-loop-join"), Does.Not.Contain("ran=nested-loop-rescan"));
    }

    [Test]
    public async Task APublishedByteLimitAppliesToTheNextStatement()
    {
        CamusDBOptions initial = Options with { SpillEnabled = false, HashJoinMaxBuildRows = 1000 };
        CamusDBOptionsHolder holder = new(initial);

        (string db, DatabaseDescriptor database, CommandExecutor executor) =
            await Seed(await CreateDatabaseWith(CreateCommandExecutor(initial, holder)));
        executor.Statistics.ForceHashJoinForTesting = true;

        Assert.That(await JoinDetail(executor, database, db, "hash-join"), Does.Not.Contain("ran="));

        holder.Publish(initial with { HashJoinMaxBuildBytes = 1024 });
        Assert.That(await JoinDetail(executor, database, db, "hash-join"), Does.Contain("ran=nested-loop-fallback"));
    }
}
