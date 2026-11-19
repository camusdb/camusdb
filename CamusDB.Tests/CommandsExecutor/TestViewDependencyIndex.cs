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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// The reverse dependency index that drop, rename and cycle checks answer from. It is kept current
/// delta by delta, so the property that matters is that it never drifts from the forward lists it is
/// derived from: after any DDL, and after a reopen, it equals an index rebuilt from those lists.
/// </summary>
[NonParallelizable]
public sealed class TestViewDependencyIndex : SharedNodeBaseTest
{
    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(txnState: tx, database: dbname, sql: sql, parameters: null));
        await database.Transactions.CommitAsync(tx);
    }

    private static void AssertMatchesForwardEdges(DatabaseDescriptor database, string after)
    {
        ViewDependencyIndex rebuilt = ViewDependencyIndex.Build(database.Schema.StoredDefinitions().ToList());
        Assert.IsTrue(database.Schema.ViewDependencies.SameEdgesAs(rebuilt),
            $"after {after}, the maintained index differs from one rebuilt from the forward lists");
    }

    [Test]
    public async Task TheMaintainedIndexNeverDriftsFromTheForwardLists()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        string[] statements =
        [
            "CREATE TABLE t (id int64 PRIMARY KEY, a int64, b int64)",
            "CREATE TABLE u (id int64 PRIMARY KEY, c int64)",
            "CREATE VIEW v1 AS SELECT id, a FROM t",
            "CREATE VIEW v2 AS SELECT v1.id, u.c FROM v1 INNER JOIN u ON v1.id = u.id",
            "CREATE MATERIALIZED VIEW m1 AS SELECT id, a FROM v1",
            "CREATE VIEW v3 AS SELECT id FROM m1 WHERE a > 1",
            "CREATE OR REPLACE VIEW v1 AS SELECT id, a FROM t WHERE b > 0",
            "ALTER VIEW v2 RENAME TO v2b",
            "ALTER TABLE u RENAME TO u2",
            "ALTER TABLE t RENAME COLUMN b TO bb",
            "ALTER MATERIALIZED VIEW m1 RENAME TO m1b",
            "REFRESH MATERIALIZED VIEW m1b",
            "DROP VIEW v3",
            "DROP VIEW v1 CASCADE",
            "CREATE VIEW v4 AS SELECT id FROM u2",
            "DROP VIEW v4",
        ];

        foreach (string sql in statements)
        {
            await ExecDdl(database, executor, dbname, sql);
            AssertMatchesForwardEdges(database, sql);
        }

        Assert.IsEmpty(ViewDependencyGraph.DirectDependentsOfTable(database.Schema, database.Schema.Tables["t"].Id!),
            "the cascade took everything that read t");
    }

    [Test]
    public async Task TheIndexAnswersEveryDependencyQuestionAndSurvivesAReopen()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname, "CREATE TABLE t (id int64 PRIMARY KEY, a int64)");
        await ExecDdl(database, executor, dbname, "CREATE VIEW base AS SELECT id, a FROM t");
        for (int i = 0; i < 20; i++)
            await ExecDdl(database, executor, dbname, $"CREATE VIEW d{i} AS SELECT id FROM base WHERE a > {i}");
        await ExecDdl(database, executor, dbname, "CREATE MATERIALIZED VIEW mat AS SELECT id FROM d0");
        await ExecDdl(database, executor, dbname, "CREATE VIEW over_mat AS SELECT id FROM mat");

        string tId = database.Schema.Tables["t"].Id!;
        string baseId = database.Schema.Views["base"].Id!;
        string aColumnId = database.Schema.Tables["t"].Columns!.Single(c => c.Name == "a").Id;

        CollectionAssert.AreEquivalent(new[] { "base" }, ViewDependencyGraph.DirectDependentsOfTable(database.Schema, tId));
        Assert.AreEqual(20, ViewDependencyGraph.DirectDependentsOfView(database.Schema, baseId).Count);
        CollectionAssert.Contains(ViewDependencyGraph.DependentsOfColumn(database.Schema, aColumnId), "base");

        List<string> transitive = ViewDependencyGraph.TransitiveDependentsOfView(database.Schema, baseId);
        Assert.AreEqual(22, transitive.Count, "20 views, the materialized view over d0, and the view over it");
        Assert.Less(transitive.IndexOf("over_mat"), transitive.IndexOf("mat"), "dependents first");
        Assert.Less(transitive.IndexOf("mat"), transitive.IndexOf("d0"), "dependents first");

        // A reopen loads the maps wholesale; the index is rebuilt from the forward lists and is the same.
        ViewDependencyIndex before = database.Schema.ViewDependencies;
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname);

        Assert.IsTrue(reopened.Schema.ViewDependencies.SameEdgesAs(before));
        Assert.AreEqual(20, ViewDependencyGraph.DirectDependentsOfView(reopened.Schema, baseId).Count);
    }

    /// <summary>Adding an edge twice or removing an absent one changes nothing, so a replay cannot double-add.</summary>
    [Test]
    public void EdgeChangesAreIdempotent()
    {
        ViewDefinition definition = new() { DependsOnTableIds = ["T1"], DependsOnViewIds = ["V1"], DependsOnColumnIds = ["C1"] };

        ViewDependencyIndex once = ViewDependencyIndex.Empty.Replace("D", null, definition);
        ViewDependencyIndex twice = once.Replace("D", null, definition);

        Assert.IsTrue(once.SameEdgesAs(twice));
        CollectionAssert.AreEquivalent(new[] { "D" }, twice.DependentsOfRelation("T1"));

        ViewDependencyIndex removed = twice.Replace("D", definition, null).Replace("D", definition, null);
        Assert.IsTrue(removed.SameEdgesAs(ViewDependencyIndex.Empty));
    }
}
