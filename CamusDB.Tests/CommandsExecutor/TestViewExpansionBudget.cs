/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// View expansion is bounded by work, not only by depth. A graph whose every level reads the level
/// below twice expands 2^n copies of the bottom view at depth n — far inside
/// <c>max_view_expansion_depth</c> — so any user who may create views could turn later queries into a
/// CPU and memory denial of service.
/// </summary>
[NonParallelizable]
public sealed class TestViewExpansionBudget : SharedNodeBaseTest
{
    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
            await database.Transactions.CommitAsync(tx);
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
            List<QueryResultRow> rows = await cursor.ToListAsync();
            await database.Transactions.CommitAsync(tx);
            return rows;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx);
        }
    }

    /// <summary>v0 reads t; each v(i) joins v(i-1) to itself, so reading v(n) expands 2^(n+1)-1 views.</summary>
    private static async Task CreateDoublingChain(DatabaseDescriptor database, CommandExecutor executor, string dbname, int levels)
    {
        await ExecDdl(database, executor, dbname, "CREATE TABLE t (id int64 PRIMARY KEY)");
        await ExecDdl(database, executor, dbname, "CREATE VIEW v0 AS SELECT id FROM t");

        for (int i = 1; i <= levels; i++)
            await ExecDdl(database, executor, dbname,
                $"CREATE VIEW v{i} AS SELECT a.id FROM v{i - 1} a INNER JOIN v{i - 1} b ON a.id = b.id");
    }

    /// <summary>
    /// The definition that would expand past the budget is refused when it is created — binding its
    /// body expands the views it reads under the same budget — and the refusal is fast.
    /// </summary>
    [Test]
    public async Task ADoublingChainIsRefusedAtCreateBeforeItCostsAnything()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { MaxViewExpandedRelations = 100 });

        // v5 binds 2 * (2^5 - 1) = 62 expansions: within budget.
        await CreateDoublingChain(database, executor, dbname, levels: 5);

        Stopwatch watch = Stopwatch.StartNew();
        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDdl(database, executor, dbname, "CREATE VIEW v6 AS SELECT a.id FROM v5 a INNER JOIN v5 b ON a.id = b.id"));
        watch.Stop();

        Assert.AreEqual(CamusDBErrorCodes.ViewComplexityLimitExceeded, refused!.Code);
        StringAssert.Contains("max_view_expanded_relations", refused.Message);
        Assert.IsFalse(database.Schema.Views.ContainsKey("v6"), "a refused definition is not created");
        Assert.Less(watch.ElapsedMilliseconds, 5_000, "the budget stops the expansion early, not after it ran");

        // What was created within budget still reads.
        Assert.IsEmpty(await ExecQuery(database, executor, dbname, "SELECT id FROM v5"));
    }

    /// <summary>
    /// A read is bounded by the same budget, so a graph created under a larger budget — or by a node
    /// whose setting was raised — stops deterministically once the setting is lowered.
    /// </summary>
    [Test]
    public async Task AReadOverBudgetFailsDeterministically()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await CreateDoublingChain(database, executor, dbname, levels: 8);

        // 2^9 - 1 = 511 expansions under the default budget of 10000.
        Assert.IsEmpty(await ExecQuery(database, executor, dbname, "SELECT id FROM v8"));

        executor.ApplyOptions(Options with { MaxViewExpandedRelations = 200 });
        try
        {
            for (int attempt = 0; attempt < 3; attempt++)
            {
                CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () =>
                    await ExecQuery(database, executor, dbname, "SELECT id FROM v8"));
                Assert.AreEqual(CamusDBErrorCodes.ViewComplexityLimitExceeded, refused!.Code);
            }

            // A read within budget is untouched.
            Assert.IsEmpty(await ExecQuery(database, executor, dbname, "SELECT id FROM v6"));
        }
        finally
        {
            executor.ApplyOptions(Options);
        }
    }

    /// <summary>The node budget bounds a few views with very large bodies the relation count would not.</summary>
    [Test]
    public async Task TheNodeBudgetBoundsLargeBodies()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) =
            await CreateDatabase(Options with { MaxViewExpandedNodes = 5_000 });

        await ExecDdl(database, executor, dbname, "CREATE TABLE t (id int64 PRIMARY KEY)");

        StringBuilder wide = new("CREATE VIEW wide AS SELECT id FROM t WHERE ");
        wide.Append(string.Join(" OR ", Enumerable.Range(0, 400).Select(i => $"id = {i}")));
        await ExecDdl(database, executor, dbname, wide.ToString());

        long bodyNodes = NodeAstWalk.Count(SQLParserProcessor.Parse(database.Schema.Views["wide"].Definition!.Sql));
        Assert.Greater(bodyNodes * 4, 5_000, "four copies of the body must be over the node budget");

        CamusDBException? refused = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecQuery(database, executor, dbname,
                "SELECT a.id FROM wide a, wide b, wide c, wide d"));
        Assert.AreEqual(CamusDBErrorCodes.ViewComplexityLimitExceeded, refused!.Code);
        StringAssert.Contains("max_view_expanded_nodes", refused.Message);
    }

    /// <summary>One definition is bounded at CREATE by its size, its columns and its direct dependencies.</summary>
    [Test]
    public async Task OneDefinitionIsBoundedAtCreate()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase(Options with
        {
            MaxViewOutputColumns = 3,
            MaxViewDirectDependencies = 2,
            MaxViewDefinitionBytes = 400,
            MaxViewDefinitionNodes = 60,
        });

        await ExecDdl(database, executor, dbname, "CREATE TABLE t (id int64 PRIMARY KEY, a int64, b int64, c int64)");
        await ExecDdl(database, executor, dbname, "CREATE TABLE u (id int64 PRIMARY KEY)");
        await ExecDdl(database, executor, dbname, "CREATE TABLE w (id int64 PRIMARY KEY)");

        async Task Refused(string sql, string setting)
        {
            CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () => await ExecDdl(database, executor, dbname, sql));
            Assert.AreEqual(CamusDBErrorCodes.ViewComplexityLimitExceeded, error!.Code, sql);
            StringAssert.Contains(setting, error.Message, sql);
        }

        await Refused("CREATE VIEW cols AS SELECT id, a, b, c FROM t", "max_view_output_columns");
        await Refused("CREATE MATERIALIZED VIEW mcols AS SELECT id, a, b, c FROM t", "max_view_output_columns");
        await Refused("CREATE VIEW deps AS SELECT t.id FROM t, u, w", "max_view_direct_dependencies");
        await Refused("CREATE VIEW nodes AS SELECT id FROM t WHERE " +
            string.Join(" OR ", Enumerable.Range(0, 20).Select(i => $"a = {i}")), "max_view_definition_");

        // Within every limit is fine, and nothing refused was created.
        await ExecDdl(database, executor, dbname, "CREATE VIEW ok AS SELECT id, a FROM t");
        CollectionAssert.AreEquivalent(new[] { "ok" }, database.Schema.Views.Keys);
        Assert.IsFalse(database.Schema.Tables.ContainsKey("mcols"));
    }
}
