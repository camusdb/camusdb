/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A boolean column written as a bare predicate (<c>WHERE enabled</c>) must produce the same
/// equality bound as <c>WHERE enabled = true</c>, so an index the column leads stays usable.
///
/// <para>These tests drive the real SQL entry point with the alias-qualified shape ORMs emit,
/// because that combination is what regressed: without the normalization the bare boolean carried
/// no bound, the leading column of the composite index had no equality, and the whole query fell
/// back to a full table scan even though every other predicate matched an index column.</para>
///
/// <para>They also pin the three-valued semantics the rewrite must preserve: a NULL boolean
/// satisfies neither the bare form nor its negation.</para>
/// </summary>
[NonParallelizable]
public class TestBareBooleanPredicateIndexUse : BaseTest
{
    private const string IndexName = "enabled_kind_nextfire_idx";

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupTriggersTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "workflow_triggers",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("workflowId", ColumnType.String, notNull: true),
                new("kind", ColumnType.Integer64, notNull: true),
                // Nullable on purpose: it lets the NULL-semantics tests below store an unknown.
                new("enabled", ColumnType.Bool),
                new("nextFireAt", ColumnType.Integer64, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        AlterIndexTicket addIndex = new(
            databaseName: dbname,
            tableName: "workflow_triggers",
            indexName: IndexName,
            columns: new ColumnIndexInfo[]
            {
                new("enabled", OrderType.Ascending),
                new("kind", OrderType.Ascending),
                new("nextFireAt", OrderType.Ascending),
            },
            operation: AlterIndexOperation.AddIndex);

        await executor.AlterIndex(addIndex);

        return (dbname, database, executor);
    }

    /// <summary>
    /// Inserts one trigger. A null <paramref name="enabled"/> stores SQL NULL, which the
    /// three-valued-logic tests rely on.
    /// </summary>
    private static async Task InsertTriggers(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        IEnumerable<(string workflowId, long kind, bool? enabled, long nextFireAt)> triggers)
    {
        KvTransaction txn = await database.Transactions.BeginAsync();

        foreach ((string workflowId, long kind, bool? enabled, long nextFireAt) in triggers)
        {
            InsertTicket ticket = new(
                txnState: txn,
                databaseName: dbname,
                tableName: "workflow_triggers",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "workflowId", new(ColumnType.String, workflowId) },
                        { "kind", new(ColumnType.Integer64, kind) },
                        { "enabled", enabled is null ? new(ColumnType.Null, "") : ColumnValue.FromBool(enabled.Value) },
                        { "nextFireAt", new(ColumnType.Integer64, nextFireAt) },
                    }
                });

            await executor.Insert(ticket);
        }

        await database.Transactions.CommitAsync(txn);
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

    /// <summary>Flattens EXPLAIN output to a single searchable "node detail node detail …" string.</summary>
    private static async Task<string> ExplainText(
        CommandExecutor executor,
        DatabaseDescriptor database,
        string dbname,
        string sql)
    {
        List<QueryResultRow> rows = await RunSql(executor, database, dbname, "EXPLAIN " + sql);

        return string.Join(" ", rows.Select(r =>
        {
            string node = r.Row.TryGetValue("node", out ColumnValue? nv) ? nv?.StrValue ?? "" : "";
            string detail = r.Row.TryGetValue("detail", out ColumnValue? dv) ? dv?.StrValue ?? "" : "";
            return node + " " + detail;
        }));
    }

    /// <summary>
    /// Removes the plan shape hash, which is derived from the AST and so differs between two
    /// spellings of the same predicate even when the chosen plan is identical.
    /// </summary>
    private static string StripPlanShape(string explain) =>
        Regex.Replace(explain, "shape=[0-9a-f]+", "shape=<hash>");

    private static readonly (string workflowId, long kind, bool? enabled, long nextFireAt)[] SampleTriggers =
    [
        ("wf-a", 1, true,  50),
        ("wf-b", 1, true,  75),
        ("wf-c", 1, true,  500),   // outside the nextFireAt window
        ("wf-d", 1, false, 60),    // disabled
        ("wf-e", 2, true,  60),    // wrong kind
        ("wf-f", 1, null,  60),    // unknown enabled
    ];

    [Test]
    public async Task BareBooleanPredicateUsesCompositeIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTriggersTable();
        await InsertTriggers(executor, database, dbname, SampleTriggers);

        string explain = await ExplainText(executor, database, dbname,
            "SELECT w.id, w.nextFireAt FROM workflow_triggers AS w "
            + "WHERE w.kind = 1 AND w.enabled AND w.nextFireAt > 0 AND w.nextFireAt <= 100 "
            + "ORDER BY w.nextFireAt");

        Assert.That(explain, Does.Contain("index-range-scan").IgnoreCase,
            $"A bare boolean leading the index must still produce an equality bound.\nGot:\n{explain}");
        Assert.That(explain, Does.Contain(IndexName),
            $"EXPLAIN must name {IndexName}.\nGot:\n{explain}");
        Assert.That(explain, Does.Not.Contain("table-scan").IgnoreCase,
            $"The query must not fall back to a full table scan.\nGot:\n{explain}");
    }

    [Test]
    public async Task BareBooleanPredicateReturnsTheSameRowsAsTheExplicitEqualityForm()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTriggersTable();
        await InsertTriggers(executor, database, dbname, SampleTriggers);

        const string predicate =
            "FROM workflow_triggers AS w WHERE w.kind = 1 AND {0} AND w.nextFireAt > 0 AND w.nextFireAt <= 100 "
            + "ORDER BY w.nextFireAt";

        List<QueryResultRow> bare = await RunSql(executor, database, dbname,
            "SELECT w.workflowId " + string.Format(predicate, "w.enabled"));

        List<QueryResultRow> explicitForm = await RunSql(executor, database, dbname,
            "SELECT w.workflowId " + string.Format(predicate, "w.enabled = true"));

        List<string> bareNames = bare.Select(r => r.Row["workflowId"].StrValue!).ToList();
        List<string> explicitNames = explicitForm.Select(r => r.Row["workflowId"].StrValue!).ToList();

        CollectionAssert.AreEqual(new[] { "wf-a", "wf-b" }, bareNames,
            "Only enabled kind-1 triggers inside the window may match");
        CollectionAssert.AreEqual(explicitNames, bareNames,
            "The bare form must return exactly what the explicit equality form returns");
    }

    [Test]
    public async Task NegatedBareBooleanPredicateUsesIndexAndMatchesOnlyFalseRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTriggersTable();
        await InsertTriggers(executor, database, dbname, SampleTriggers);

        const string sql =
            "SELECT w.workflowId FROM workflow_triggers AS w "
            + "WHERE w.kind = 1 AND NOT w.enabled AND w.nextFireAt > 0 AND w.nextFireAt <= 100";

        string explain = await ExplainText(executor, database, dbname, sql);
        Assert.That(explain, Does.Contain(IndexName),
            $"NOT on a bare boolean must still bind the leading index column.\nGot:\n{explain}");

        List<QueryResultRow> rows = await RunSql(executor, database, dbname, sql);

        CollectionAssert.AreEqual(
            new[] { "wf-d" },
            rows.Select(r => r.Row["workflowId"].StrValue!).ToList(),
            "NOT enabled must match only rows where enabled is false — never the NULL row");
    }

    /// <summary>
    /// The rewrite must not turn unknown into a match: under three-valued logic a NULL boolean
    /// satisfies neither <c>enabled</c> nor <c>NOT enabled</c>, and an equality seek on true/false
    /// must not reach the NULL index key either.
    /// </summary>
    [Test]
    public async Task NullBooleanSatisfiesNeitherTheBareFormNorItsNegation()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTriggersTable();
        await InsertTriggers(executor, database, dbname, SampleTriggers);

        List<QueryResultRow> enabledRows = await RunSql(executor, database, dbname,
            "SELECT w.workflowId FROM workflow_triggers AS w WHERE w.enabled");

        List<QueryResultRow> notEnabledRows = await RunSql(executor, database, dbname,
            "SELECT w.workflowId FROM workflow_triggers AS w WHERE NOT w.enabled");

        List<string> matched =
        [
            .. enabledRows.Select(r => r.Row["workflowId"].StrValue!),
            .. notEnabledRows.Select(r => r.Row["workflowId"].StrValue!),
        ];

        Assert.That(matched, Does.Not.Contain("wf-f"),
            "A NULL boolean must not match the bare form or its negation");
        CollectionAssert.AreEquivalent(new[] { "wf-a", "wf-b", "wf-c", "wf-e" },
            enabledRows.Select(r => r.Row["workflowId"].StrValue!).ToList());
        CollectionAssert.AreEquivalent(new[] { "wf-d" },
            notEnabledRows.Select(r => r.Row["workflowId"].StrValue!).ToList());
    }

    /// <summary>
    /// <c>IS TRUE</c> is a truth test rather than a comparison, but in a WHERE clause it selects
    /// exactly what <c>= true</c> selects, so it must reach the index too.
    /// </summary>
    [Test]
    public async Task IsTruePredicateUsesIndexAndMatchesOnlyTrueRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTriggersTable();
        await InsertTriggers(executor, database, dbname, SampleTriggers);

        const string sql = "SELECT w.workflowId FROM workflow_triggers AS w WHERE w.enabled IS TRUE";

        // IS TRUE must plan exactly like the equality it is equivalent to. Asserting the two plans
        // match — rather than asserting a specific access path — keeps the test about the rewrite
        // instead of about whichever path the cost model currently prefers for a lone boolean.
        string explain = await ExplainText(executor, database, dbname, sql);
        string equalityExplain = await ExplainText(executor, database, dbname,
            "SELECT w.workflowId FROM workflow_triggers AS w WHERE w.enabled = true");

        Assert.AreEqual(
            StripPlanShape(equalityExplain).Replace("w.enabled = true", "w.enabled IS TRUE"),
            StripPlanShape(explain),
            "IS TRUE must produce the same plan as = true");

        List<QueryResultRow> rows = await RunSql(executor, database, dbname, sql);

        CollectionAssert.AreEquivalent(
            new[] { "wf-a", "wf-b", "wf-c", "wf-e" },
            rows.Select(r => r.Row["workflowId"].StrValue!).ToList());
    }

    [Test]
    public async Task IsFalsePredicateMatchesOnlyFalseRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTriggersTable();
        await InsertTriggers(executor, database, dbname, SampleTriggers);

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT w.workflowId FROM workflow_triggers AS w WHERE w.enabled IS FALSE");

        CollectionAssert.AreEquivalent(
            new[] { "wf-d" },
            rows.Select(r => r.Row["workflowId"].StrValue!).ToList(),
            "IS FALSE must not match the NULL row");
    }

    /// <summary>
    /// The distinguishing case for the negated truth tests: unlike <c>= false</c> and unlike
    /// <c>NOT enabled</c>, <c>IS NOT TRUE</c> matches the NULL row. This is why it must not be
    /// rewritten into an equality bound on a nullable column.
    /// </summary>
    [Test]
    public async Task IsNotTrueMatchesFalseAndNullRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTriggersTable();
        await InsertTriggers(executor, database, dbname, SampleTriggers);

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT w.workflowId FROM workflow_triggers AS w WHERE w.enabled IS NOT TRUE");

        CollectionAssert.AreEquivalent(
            new[] { "wf-d", "wf-f" },
            rows.Select(r => r.Row["workflowId"].StrValue!).ToList(),
            "IS NOT TRUE must match both the false row and the NULL row");
    }

    [Test]
    public async Task IsNotFalseMatchesTrueAndNullRows()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTriggersTable();
        await InsertTriggers(executor, database, dbname, SampleTriggers);

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT w.workflowId FROM workflow_triggers AS w WHERE w.enabled IS NOT FALSE");

        CollectionAssert.AreEquivalent(
            new[] { "wf-a", "wf-b", "wf-c", "wf-e", "wf-f" },
            rows.Select(r => r.Row["workflowId"].StrValue!).ToList(),
            "IS NOT FALSE must match the true rows and the NULL row");
    }

    /// <summary>
    /// On a NOT NULL column the "or NULL" branch of <c>IS NOT TRUE</c> is unreachable, so the
    /// predicate collapses to <c>= false</c> and may drive the index after all.
    /// </summary>
    [Test]
    public async Task IsNotTrueOnNotNullColumnUsesIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CreateTableTicket tableTicket = new(
            databaseName: dbname,
            tableName: "flags",
            columns: new ColumnInfo[]
            {
                new("id", ColumnType.Id),
                new("name", ColumnType.String, notNull: true),
                new("active", ColumnType.Bool, notNull: true),
            },
            constraints: new ConstraintInfo[]
            {
                new(ConstraintType.PrimaryKey, "~pk", new ColumnIndexInfo[] { new("id", OrderType.Ascending) })
            },
            ifNotExists: false);

        await executor.CreateTable(tableTicket);

        await executor.AlterIndex(new AlterIndexTicket(
            databaseName: dbname,
            tableName: "flags",
            indexName: "active_idx",
            columns: new ColumnIndexInfo[] { new("active", OrderType.Ascending) },
            operation: AlterIndexOperation.AddIndex));

        KvTransaction insertTxn = await database.Transactions.BeginAsync();
        foreach ((string name, bool active) in new[] { ("on-1", true), ("off-1", false), ("off-2", false) })
        {
            await executor.Insert(new InsertTicket(
                txnState: insertTxn,
                databaseName: dbname,
                tableName: "flags",
                values: new()
                {
                    new()
                    {
                        { "id", new(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "name", new(ColumnType.String, name) },
                        { "active", ColumnValue.FromBool(active) },
                    }
                }));
        }
        await database.Transactions.CommitAsync(insertTxn);

        const string sql = "SELECT f.name FROM flags AS f WHERE f.active IS NOT TRUE";

        string explain = await ExplainText(executor, database, dbname, sql);
        Assert.That(explain, Does.Contain("active_idx"),
            $"On a NOT NULL column IS NOT TRUE collapses to = false and must use the index.\nGot:\n{explain}");

        List<QueryResultRow> rows = await RunSql(executor, database, dbname, sql);

        CollectionAssert.AreEquivalent(
            new[] { "off-1", "off-2" },
            rows.Select(r => r.Row["name"].StrValue!).ToList());
    }

    /// <summary>
    /// A bare non-boolean column keeps numeric truthiness (non-zero is true). Rewriting it to
    /// <c>= true</c> would silently change which rows match, so it must stay a residual filter.
    /// </summary>
    [Test]
    public async Task BareIntegerColumnKeepsTruthinessSemantics()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupTriggersTable();
        await InsertTriggers(executor, database, dbname,
        [
            ("wf-zero", 0, true, 10),
            ("wf-one", 1, true, 20),
            ("wf-two", 2, true, 30),
        ]);

        List<QueryResultRow> rows = await RunSql(executor, database, dbname,
            "SELECT w.workflowId FROM workflow_triggers AS w WHERE w.kind");

        CollectionAssert.AreEquivalent(
            new[] { "wf-one", "wf-two" },
            rows.Select(r => r.Row["workflowId"].StrValue!).ToList(),
            "A bare integer predicate matches every non-zero value, not just 1");
    }
}
