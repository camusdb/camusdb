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
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

using Nito.AsyncEx;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A correlated EXISTS used to scan the entire inner table for every outer row. These cover the
/// index-seek access path that replaces it: that the planner picks the seek for the shapes that
/// should get one and declines for the shapes that should not, and that seeking returns exactly the
/// rows the scan did.
///
/// Split deliberately: the planner tests prove the seek is *selected* (behaviour tests cannot
/// distinguish a seek from a scan, since both must return the same rows), while the query tests
/// prove the executor is *correct* when it seeks.
/// </summary>
[NonParallelizable]
public sealed class TestCorrelatedExistsIndexSeek : SharedNodeBaseTest
{
    private static async Task ExecDdl(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        _ = await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        DatabaseDescriptor database,
        CommandExecutor executor,
        string dbname,
        string sql,
        Dictionary<string, ColumnValue>? parameters = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: parameters);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task<TableDescriptor> GetTable(CommandExecutor executor, string dbname, string tableName)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);

        if (db.TableDescriptors.TryGetValue(tableName, out AsyncLazy<TableDescriptor>? lazy))
            return await lazy;

        throw new KeyNotFoundException($"Table '{tableName}' not found in '{dbname}'");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Fixture: the EAV shape the scan cost was measured on.
    //   items(id oid pk, owner string, name string)
    //   item_values(item_id oid, tag string, num int64, PRIMARY KEY (item_id, tag))
    // ─────────────────────────────────────────────────────────────────────────

    private const string ItemA = "65a1f3a1c2e7d50b4f8a9101";
    private const string ItemB = "65a1f3a1c2e7d50b4f8a9102";
    private const string ItemC = "65a1f3a1c2e7d50b4f8a9103";

    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupEavAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE items (id oid primary key, owner string(64) not null, name string(64) not null)");
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE item_values (item_id oid, tag string(64), num int64, PRIMARY KEY (item_id, tag))");

        await ExecNonQuery(database, executor, dbname,
            $"INSERT INTO items (id, owner, name) VALUES "
            + $"('{ItemA}', 'u1', 'a'), ('{ItemB}', 'u1', 'b'), ('{ItemC}', 'u1', 'c')");

        // A: rarity=5 and weight=1;  B: rarity=50;  C: nothing.
        await ExecNonQuery(database, executor, dbname,
            $"INSERT INTO item_values (item_id, tag, num) VALUES "
            + $"('{ItemA}', 'rarity', 5), ('{ItemA}', 'weight', 1), ('{ItemB}', 'rarity', 50)");

        return (dbname, database, executor);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Planner — proves the seek is selected (behaviour alone cannot show this)
    // ─────────────────────────────────────────────────────────────────────────

    private static NodeAst InnerWhereOf(string sql)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);
        SelectQuery query = new SelectQueryCreator().CreateSelectQuery(ast);
        return query.Where!.Expression;
    }

    private static CorrelatedExistsSeekPlan? PlanFor(TableDescriptor innerTable, string innerSql, string innerAlias)
    {
        return CorrelatedExistsSeekPlanner.TryPlan(
            innerTable,
            InnerWhereOf(innerSql),
            innerAlias,
            new HashSet<string>(new[] { innerAlias }, System.StringComparer.Ordinal));
    }

    [Test]
    public async Task Planner_PinsPkPrefix_WhenCorrelationMatchesLeadingKeyColumn()
    {
        (string dbname, _, CommandExecutor executor) = await SetupEavAsync();
        TableDescriptor values = await GetTable(executor, dbname, "item_values");

        CorrelatedExistsSeekPlan? plan = PlanFor(
            values, "SELECT * FROM item_values v WHERE v.item_id = i.id", "v");

        Assert.IsNotNull(plan, "correlation on the leading PK column must produce a seek plan");
        Assert.AreEqual("~pk", plan!.Index.Name);
        Assert.AreEqual(1, plan.PrefixBindings.Count, "only item_id is pinned");
        Assert.IsTrue(plan.Unique);
    }

    [Test]
    public async Task Planner_PinsFullPk_WhenCorrelationAndTagAreBothEqualities()
    {
        (string dbname, _, CommandExecutor executor) = await SetupEavAsync();
        TableDescriptor values = await GetTable(executor, dbname, "item_values");

        CorrelatedExistsSeekPlan? plan = PlanFor(
            values, "SELECT * FROM item_values v WHERE v.item_id = i.id AND v.tag = @tag AND v.num > 3", "v");

        Assert.IsNotNull(plan);
        Assert.AreEqual(2, plan!.PrefixBindings.Count, "item_id and tag are both pinned; num stays residual");
    }

    [Test]
    public async Task Planner_DeclinesWhenLeadingKeyColumnIsNotPinned()
    {
        (string dbname, _, CommandExecutor executor) = await SetupEavAsync();
        TableDescriptor values = await GetTable(executor, dbname, "item_values");

        // tag is the *second* PK column; nothing pins item_id, so no prefix seek exists.
        CorrelatedExistsSeekPlan? plan = PlanFor(values, "SELECT * FROM item_values v WHERE v.tag = 'rarity'", "v");

        Assert.IsNull(plan);
    }

    [Test]
    public async Task Planner_DeclinesForDisjunction()
    {
        (string dbname, _, CommandExecutor executor) = await SetupEavAsync();
        TableDescriptor values = await GetTable(executor, dbname, "item_values");

        // An equality under an OR need not hold for a matching row, so it cannot bound a seek.
        CorrelatedExistsSeekPlan? plan = PlanFor(
            values, "SELECT * FROM item_values v WHERE v.item_id = i.id OR v.num > 3", "v");

        Assert.IsNull(plan);
    }

    [Test]
    public async Task Planner_DeclinesWhenBothSidesAreInnerColumns()
    {
        (string dbname, _, CommandExecutor executor) = await SetupEavAsync();
        TableDescriptor values = await GetTable(executor, dbname, "item_values");

        // v.tag is not resolvable from the outer row, so it cannot build a seek key.
        CorrelatedExistsSeekPlan? plan = PlanFor(
            values, "SELECT * FROM item_values v WHERE v.item_id = v.tag", "v");

        Assert.IsNull(plan);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Execution — the seek must return exactly what the scan did
    // ─────────────────────────────────────────────────────────────────────────

    [Test]
    public async Task Exists_SeeksPkPrefix_ReturnsOnlyCorrelatedMatches()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupEavAsync();

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT name FROM items i "
            + "WHERE EXISTS (SELECT * FROM item_values v WHERE v.item_id = i.id) "
            + "ORDER BY name");

        CollectionAssert.AreEqual(new[] { "a", "b" }, rows.Select(r => r.Row["name"].StrValue).ToList());
    }

    [Test]
    public async Task Exists_SeekWithResidualPredicate_FiltersOnNonKeyColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupEavAsync();

        // Seeks (item_id, tag) and applies num > 10 as a residual: only B's rarity=50 qualifies.
        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT name FROM items i "
            + "WHERE EXISTS (SELECT * FROM item_values v "
            + "WHERE v.item_id = i.id AND v.tag = 'rarity' AND v.num > 10) "
            + "ORDER BY name");

        CollectionAssert.AreEqual(new[] { "b" }, rows.Select(r => r.Row["name"].StrValue).ToList());
    }

    [Test]
    public async Task Exists_SeekWithParameter_BindsAndFilters()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupEavAsync();

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT name FROM items i "
            + "WHERE EXISTS (SELECT * FROM item_values v WHERE v.item_id = i.id AND v.tag = @tag) "
            + "ORDER BY name",
            new Dictionary<string, ColumnValue> { { "@tag", new ColumnValue(ColumnType.String, "weight") } });

        CollectionAssert.AreEqual(new[] { "a" }, rows.Select(r => r.Row["name"].StrValue).ToList());
    }

    [Test]
    public async Task NotExists_SeeksPkPrefix_ReturnsComplement()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupEavAsync();

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT name FROM items i "
            + "WHERE NOT EXISTS (SELECT * FROM item_values v WHERE v.item_id = i.id) "
            + "ORDER BY name");

        CollectionAssert.AreEqual(new[] { "c" }, rows.Select(r => r.Row["name"].StrValue).ToList());
    }

    [Test]
    public async Task Exists_SeekIsIsolatedFromUnrelatedKeys()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupEavAsync();

        // Value rows belonging to an item that does not exist in items must never satisfy any
        // outer row: a seek keyed on item_id can only ever reach the correlated key's entries.
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO item_values (item_id, tag, num) VALUES "
            + "('65a1f3a1c2e7d50b4f8a9999', 'rarity', 7), ('65a1f3a1c2e7d50b4f8a9998', 'rarity', 8)");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT name FROM items i "
            + "WHERE EXISTS (SELECT * FROM item_values v WHERE v.item_id = i.id AND v.tag = 'rarity') "
            + "ORDER BY name");

        CollectionAssert.AreEqual(new[] { "a", "b" }, rows.Select(r => r.Row["name"].StrValue).ToList());
    }

    [Test]
    public async Task Exists_NullCorrelationValue_MatchesNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupEavAsync();

        // A NULL correlation column can never equal an indexed value, so EXISTS is false for that
        // row — the seek must agree with the scan's three-valued comparison rather than seeking NULL.
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE refs (id int64 primary key, target oid)");
        await ExecNonQuery(database, executor, dbname,
            $"INSERT INTO refs (id, target) VALUES (1, '{ItemA}'), (2, NULL)");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT id FROM refs r "
            + "WHERE EXISTS (SELECT * FROM item_values v WHERE v.item_id = r.target) "
            + "ORDER BY id");

        CollectionAssert.AreEqual(new[] { 1L }, rows.Select(r => r.Row["id"].LongValue).ToList());
    }

    [Test]
    public async Task Exists_FallsBackToScan_WhenCorrelationColumnHasNoIndex()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE outer_t (id int64 primary key, name string(64) not null)");
        // note_owner is NOT indexed, so no seek plan exists and the scan path must still be correct.
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE notes (id int64 primary key, note_owner int64, body string(64))");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO outer_t (id, name) VALUES (1, 'x'), (2, 'y'), (3, 'z')");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO notes (id, note_owner, body) VALUES (10, 1, 'n1'), (11, 3, 'n2')");

        TableDescriptor notes = await GetTable(executor, dbname, "notes");
        Assert.IsNull(
            PlanFor(notes, "SELECT * FROM notes n WHERE n.note_owner = o.id", "n"),
            "no index on note_owner, so the planner must decline and leave the scan path");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname,
            "SELECT name FROM outer_t o "
            + "WHERE EXISTS (SELECT * FROM notes n WHERE n.note_owner = o.id) "
            + "ORDER BY name");

        CollectionAssert.AreEqual(new[] { "x", "z" }, rows.Select(r => r.Row["name"].StrValue).ToList());
    }

    [Test]
    public async Task Exists_SeekAndScanAgree_OnTheSameData()
    {
        // Differential parity: the same logical question asked of an indexed inner table (seek) and
        // an unindexed copy of the same rows (scan) must produce identical answers.
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupEavAsync();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE item_values_noidx (id int64 primary key, item_id oid, tag string(64), num int64)");
        await ExecNonQuery(database, executor, dbname,
            $"INSERT INTO item_values_noidx (id, item_id, tag, num) VALUES "
            + $"(1, '{ItemA}', 'rarity', 5), (2, '{ItemA}', 'weight', 1), (3, '{ItemB}', 'rarity', 50)");

        TableDescriptor noidx = await GetTable(executor, dbname, "item_values_noidx");
        Assert.IsNull(
            PlanFor(noidx, "SELECT * FROM item_values_noidx v WHERE v.item_id = i.id", "v"),
            "the copy must genuinely take the scan path for this to be a differential test");

        foreach (string predicate in new[]
        {
            "v.item_id = i.id",
            "v.item_id = i.id AND v.tag = 'rarity'",
            "v.item_id = i.id AND v.num > 10",
            "v.item_id = i.id AND v.tag = 'weight' AND v.num > 100",
        })
        {
            List<QueryResultRow> seeked = await ExecQuery(database, executor, dbname,
                $"SELECT name FROM items i WHERE EXISTS (SELECT * FROM item_values v WHERE {predicate}) ORDER BY name");

            List<QueryResultRow> scanned = await ExecQuery(database, executor, dbname,
                $"SELECT name FROM items i WHERE EXISTS (SELECT * FROM item_values_noidx v WHERE {predicate}) ORDER BY name");

            CollectionAssert.AreEqual(
                scanned.Select(r => r.Row["name"].StrValue).ToList(),
                seeked.Select(r => r.Row["name"].StrValue).ToList(),
                $"seek and scan disagree for: {predicate}");
        }
    }
}
