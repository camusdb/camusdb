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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using ColumnType = CamusDB.Core.Catalogs.Models.ColumnType;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// CASE, COALESCE, IFNULL (NVL) and IF have one result type, by the GoogleSQL rule: their inputs
/// must have a common type, and a pair with none is InvalidInput before any row is read. Before,
/// a CASE returned the value of the branch that ran with that branch's type, so
/// <c>CASE WHEN false THEN 1 ELSE 'a' END</c> returned <c>'a'</c>, and one column could hold an
/// INT64 in one row and a STRING in the next. IF did not exist.
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestConditionalResultTypes : BaseTest
{
    // Pairs with no common type: an expression of each side, typed from the statement.
    private static readonly (string Left, string Right)[] NoCommonTypePairs =
    [
        ("a", "s"),
        ("1", "'x'"),
        ("b", "a"),
        ("true", "1"),
        ("s", "1.5"),
        ("ARRAY[1]", "ARRAY['x']"),
        ("CAST('2026-01-01' AS DATE)", "s"),
        ("d", "s"),
    ];

    private static IEnumerable<string> Forms(string left, string right)
    {
        // The branch that would return the incompatible value never runs in the first form.
        yield return $"CASE WHEN false THEN {left} ELSE {right} END";
        yield return $"CASE WHEN true THEN {left} ELSE {right} END";
        yield return $"CASE id WHEN 1 THEN {left} WHEN 2 THEN {right} END";
        yield return $"CASE WHEN id > 0 THEN {left} WHEN id < 0 THEN NULL ELSE {right} END";
        yield return $"COALESCE({left}, {right})";
        yield return $"COALESCE(NULL, {left}, {right})";
        yield return $"COALESCE({left}, NULL, {right})";
        yield return $"IFNULL({left}, {right})";
        yield return $"NVL({left}, {right})";
        yield return $"IF(false, {left}, {right})";
        yield return $"IF(id > 0, {left}, {right})";
    }

    // ─── No common type ───────────────────────────────────────────────────────

    [Test]
    public async Task EveryForm_WithInputsOfNoCommonType_IsInvalidInput_InBothOrders_OverAnEmptyTable()
    {
        (string db, CommandExecutor executor) = await Seed(rows: 0);

        foreach ((string left, string right) in NoCommonTypePairs)
        {
            foreach ((string l, string r) in new[] { (left, right), (right, left) })
            {
                foreach (string form in Forms(l, r))
                {
                    AssertNoCommonType(() => Query(executor, db, $"SELECT {form} AS v FROM t", null), $"projection {form}");
                    AssertNoCommonType(() => Query(executor, db, $"SELECT id FROM t WHERE ({form}) IS NULL", null), $"WHERE {form}");
                }
            }
        }
    }

    [Test]
    public async Task EveryForm_WithInputsOfNoCommonType_IsInvalidInput_OverRows_AndInDml()
    {
        (string db, CommandExecutor executor) = await Seed(rows: 3);

        foreach ((string left, string right) in NoCommonTypePairs)
        {
            foreach (string form in Forms(left, right).Concat(Forms(right, left)))
            {
                AssertNoCommonType(() => Query(executor, db, $"SELECT {form} AS v FROM t", null), $"rows {form}");
                AssertNoCommonType(() => Query(executor, db, $"SELECT v FROM (SELECT {form} AS v FROM t) d", null), $"derived {form}");
                AssertNoCommonType(() => NonQuery(executor, db, $"DELETE FROM t WHERE ({form}) IS NULL", null), $"DELETE {form}");
                AssertNoCommonType(() => NonQuery(executor, db, $"UPDATE t SET n = 1 WHERE ({form}) IS NULL", null), $"UPDATE WHERE {form}");
            }
        }

        Assert.AreEqual(3, await Count(executor, db, ""));
        Assert.AreEqual(3, await Count(executor, db, "WHERE n IS NULL"), "no UPDATE wrote a row");
    }

    [TestCase("SELECT CASE WHEN false THEN 1 ELSE 'a' END", "CASE", "Integer64", "String")]
    [TestCase("SELECT COALESCE(1, 'a')", "COALESCE", "Integer64", "String")]
    [TestCase("SELECT IFNULL(true, 1)", "IFNULL", "Bool", "Integer64")]
    [TestCase("SELECT IF(true, ARRAY[1], ARRAY['a'])", "IF", "ARRAY<Integer64>", "ARRAY<String>")]
    public async Task TheErrorNamesTheConstructAndBothTypes(string sql, string construct, string left, string right)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, db, sql, null))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, ex.Message);
        StringAssert.StartsWith($"No common type for the results of {construct}: {left} and {right}", ex.Message);
    }

    [Test]
    public async Task UpdateSet_WithInputsOfNoCommonType_FailsAndWritesNothing()
    {
        (string db, CommandExecutor executor) = await Seed(rows: 3);

        AssertNoCommonType(() => NonQuery(executor, db, "UPDATE t SET n = CASE WHEN false THEN a ELSE s END WHERE TRUE", null), "SET CASE");
        AssertNoCommonType(() => NonQuery(executor, db, "UPDATE t SET n = COALESCE(a, s) WHERE TRUE", null), "SET COALESCE");
        AssertNoCommonType(() => NonQuery(executor, db, "UPDATE t SET n = IF(false, a, s) WHERE TRUE", null), "SET IF");

        Assert.AreEqual(3, await Count(executor, db, "WHERE n IS NULL"));
    }

    [Test]
    public async Task ATypedParameterOfTheWrongType_IsInvalidInput_WhenItsBranchRuns()
    {
        // A parameter's type is known only from its value, so the check runs when the value is
        // returned. A branch that does not run is not checked.
        (string db, CommandExecutor executor) = await Seed(rows: 2);
        Dictionary<string, ColumnValue> parameters = new() { ["@p"] = new ColumnValue(ColumnType.String, "x") };

        AssertNoCommonType(() => Query(executor, db, "SELECT CASE WHEN true THEN @p ELSE 1 END AS v FROM t", parameters), "CASE @p");
        AssertNoCommonType(() => Query(executor, db, "SELECT IF(true, @p, a) AS v FROM t", parameters), "IF @p, column");
        AssertNoCommonType(() => Query(executor, db, "SELECT COALESCE(@p, a) AS v FROM t", parameters), "COALESCE @p");

        List<IReadOnlyDictionary<string, ColumnValue>> rows =
            await Query(executor, db, "SELECT CASE WHEN false THEN @p ELSE 1 END AS v FROM t", parameters);
        Assert.AreEqual(2, rows.Count);
    }

    [Test]
    public async Task CoalesceOfBoolAndInt64_WasTheFirstArgument_IsNowInvalidInput()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        AssertNoCommonType(() => Query(executor, db, "SELECT COALESCE(true, 1)", null), "COALESCE(true, 1)");
        AssertNoCommonType(() => Query(executor, db, "SELECT COALESCE(ARRAY[1], ARRAY['a'])", null), "COALESCE arrays");
    }

    // ─── Common types ─────────────────────────────────────────────────────────

    [Test]
    public async Task NumericInputs_WidenEveryRow_ToOneType_AtTheTopLevel_AndInADerivedTable()
    {
        (string db, CommandExecutor executor) = await Seed(rows: 4);

        string[] forms =
        [
            "CASE WHEN id > 2 THEN a ELSE 1.5 END",
            "CASE WHEN id > 2 THEN 1.5 ELSE a END",
            "COALESCE(a, 1.5)",
            "IFNULL(a, 1.5)",
            "IF(id > 2, a, 1.5)",
            "CASE WHEN id > 2 THEN a ELSE f END",
            "COALESCE(n, f)",
        ];

        foreach (string form in forms)
        {
            foreach (string sql in new[] { $"SELECT {form} AS v FROM t", $"SELECT v FROM (SELECT {form} AS v FROM t) d" })
            {
                List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(executor, db, sql, null);
                Assert.AreEqual(4, rows.Count, sql);

                foreach (IReadOnlyDictionary<string, ColumnValue> row in rows)
                    Assert.That(row["v"].Type, Is.EqualTo(ColumnType.Float64).Or.EqualTo(ColumnType.Null), $"{sql}: {row["v"]}");
            }
        }
    }

    [Test]
    public async Task Int64WithNumeric_IsNumeric_AndDateWithDateTime_IsDateTime()
    {
        (string db, CommandExecutor executor) = await Seed(rows: 2);

        foreach (IReadOnlyDictionary<string, ColumnValue> row in await Query(executor, db, "SELECT IF(id = 1, a, p) AS v FROM t", null))
            Assert.AreEqual(ColumnType.Numeric, row["v"].Type);

        string sql = "SELECT CASE WHEN id = 1 THEN CAST('2026-01-01' AS DATE) ELSE CAST('2026-01-02T03:04:05Z' AS DATETIME) END AS v FROM t";
        foreach (IReadOnlyDictionary<string, ColumnValue> row in await Query(executor, db, sql, null))
            Assert.AreEqual(ColumnType.DateTime, row["v"].Type);
    }

    [TestCase("SELECT CASE WHEN true THEN ARRAY[1] ELSE ARRAY[2, 3] END")]
    [TestCase("SELECT COALESCE(NULL, ARRAY['a'])")]
    [TestCase("SELECT IF(false, ARRAY[], ARRAY[1])")]
    [TestCase("SELECT CASE WHEN true THEN 1 END")]
    [TestCase("SELECT CASE WHEN false THEN 1 ELSE NULL END")]
    [TestCase("SELECT COALESCE(NULL, 'a', NULL)")]
    [TestCase("SELECT IF(NULL, 1, NULL)")]
    public async Task InputsWithACommonType_OrNullInputs_Run(string sql)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        Assert.AreEqual(1, (await Query(executor, db, sql, null)).Count, sql);
    }

    [Test]
    public async Task TheColumnMetadata_NamesTheCommonType()
    {
        (string db, CommandExecutor executor) = await Seed(rows: 0);

        QuerySchemaHolder schema = await Schema(executor, db,
            "SELECT CASE WHEN id > 2 THEN a ELSE 1.5 END AS c, COALESCE(a, p) AS k, IF(id > 0, a, f) AS i, " +
            "CASE WHEN id > 0 THEN CAST('2026-01-01' AS DATE) ELSE CAST('2026-01-01T00:00:00Z' AS DATETIME) END AS d, " +
            "CASE WHEN id > 0 THEN d ELSE '2026-02-01' END AS l FROM t");

        Assert.AreEqual(ColumnType.Float64, schema.Schema.Single(c => c.Name == "c").Type);
        Assert.AreEqual(ColumnType.Numeric, schema.Schema.Single(c => c.Name == "k").Type);
        Assert.AreEqual(ColumnType.Float64, schema.Schema.Single(c => c.Name == "i").Type);
        Assert.AreEqual(ColumnType.DateTime, schema.Schema.Single(c => c.Name == "d").Type);

        // Only the type the binding records gives Date here; the metadata builder alone gives String.
        Assert.AreEqual(ColumnType.Date, schema.Schema.Single(c => c.Name == "l").Type);
    }

    [Test]
    public async Task InOrderBy_ANameInsideAnExpression_IsTheColumn_NotTheOutputAlias()
    {
        // As in PostgreSQL, an output alias counts in ORDER BY only as a bare name. Inside the CASE,
        // a is the INT64 column, so 'z' has no common type with it. The check when the statement is
        // bound reads such a name as unknown, to be safe; the check when the value is returned finds
        // the INT64.
        (string db, CommandExecutor executor) = await Seed(rows: 3);

        AssertNoCommonType(() => Query(executor, db,
            "SELECT CAST(a AS STRING) AS a FROM t ORDER BY CASE WHEN id > 1 THEN a ELSE 'z' END", null), "ORDER BY");

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(executor, db,
            "SELECT CAST(a AS STRING) AS a FROM t ORDER BY CASE WHEN id > 1 THEN a ELSE 0 END", null);
        Assert.AreEqual(3, rows.Count);
    }

    // ─── A string literal takes a DATE or DATETIME type ───────────────────────

    [Test]
    public async Task AStringLiteral_TakesTheTemporalType_InEveryForm_AndBothOrders()
    {
        (string db, CommandExecutor executor) = await Seed(rows: 3);

        string[] forms =
        [
            "CASE WHEN id = 1 THEN d ELSE '2026-02-01' END",
            "CASE WHEN id = 1 THEN '2026-02-01' ELSE d END",
            "COALESCE(d, '2026-02-01')",
            "COALESCE('2026-02-01', d)",
            "IFNULL(d, '2026-02-01')",
            "IF(id = 1, d, '2026-02-01')",
            "IF(id = 1, '2026-02-01', d)",
        ];

        foreach (string form in forms)
        {
            foreach (IReadOnlyDictionary<string, ColumnValue> row in await Query(executor, db, $"SELECT {form} AS v FROM t", null))
                Assert.AreEqual(ColumnType.Date, row["v"].Type, form);
        }

        List<IReadOnlyDictionary<string, ColumnValue>> rows =
            await Query(executor, db, "SELECT id FROM t WHERE IF(id = 1, d, '2026-01-02') = CAST('2026-01-02' AS DATE) ORDER BY id", null);
        Assert.AreEqual(new long[] { 2, 3 }, rows.Select(r => r["id"].LongValue).ToArray(), "rows 2 and 3 return the literal, as the date it spells");

        foreach (IReadOnlyDictionary<string, ColumnValue> row in await Query(executor, db,
                     "SELECT CASE WHEN id = 1 THEN dt ELSE '2026-03-01T08:00:00Z' END AS v FROM t", null))
            Assert.AreEqual(ColumnType.DateTime, row["v"].Type);

        QuerySchemaHolder schema = await Schema(executor, db, "SELECT COALESCE(d, '2026-02-01') AS v FROM t");
        Assert.AreEqual(ColumnType.Date, schema.Schema.Single().Type);
    }

    [TestCase("SELECT COALESCE(d, 'abc') AS v FROM t")]
    [TestCase("SELECT CASE WHEN false THEN d ELSE 'abc' END AS v FROM t")]
    [TestCase("SELECT IF(true, d, '2026-13-45') AS v FROM t")]
    [TestCase("SELECT id FROM t WHERE IFNULL(dt, 'soon') IS NULL")]
    public async Task AMalformedLiteral_FailsBeforeAnyRowIsRead(string sql)
    {
        (string db, CommandExecutor executor) = await Seed(rows: 0);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, db, sql, null))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, ex.Message);
        StringAssert.StartsWith("Cannot convert '", ex.Message);
    }

    [Test]
    public async Task OnlyALiteral_TakesTheTemporalType()
    {
        (string db, CommandExecutor executor) = await Seed(rows: 0);

        // A typed STRING column does not combine with a date.
        AssertNoCommonType(() => Query(executor, db, "SELECT CASE WHEN true THEN d ELSE s END AS v FROM t", null), "column");

        // A nested CASE of literals is not a literal, as in GoogleSQL.
        AssertNoCommonType(() => Query(executor, db,
            "SELECT COALESCE(d, CASE WHEN id > 0 THEN '2026-01-01' ELSE '2026-01-02' END) AS v FROM t", null), "nested");

        // Two literals stay a STRING.
        (string db2, _, CommandExecutor executor2) = await CreateDatabase();
        Assert.AreEqual(ColumnType.String, (await Scalar(executor2, db2, "SELECT COALESCE(NULL, '2026-01-01')")).Type);
    }

    [Test]
    public async Task AStringParameter_IsParsedWhenItsBranchRuns()
    {
        (string db, CommandExecutor executor) = await Seed(rows: 2);

        Dictionary<string, ColumnValue> good = new() { ["@p"] = new ColumnValue(ColumnType.String, "2026-05-05") };
        foreach (IReadOnlyDictionary<string, ColumnValue> row in await Query(executor, db, "SELECT CASE WHEN true THEN @p ELSE d END AS v FROM t", good))
            Assert.AreEqual(ColumnType.Date, row["v"].Type);

        Dictionary<string, ColumnValue> bad = new() { ["@p"] = new ColumnValue(ColumnType.String, "not a date") };
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, db, "SELECT CASE WHEN true THEN @p ELSE d END AS v FROM t", bad))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
    }

    [Test]
    public async Task FromlessSelect_AndInsertValues_AreCheckedBeforeEvaluation()
    {
        (string db, CommandExecutor executor) = await Seed(rows: 0);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() =>
            Query(executor, db, "SELECT COALESCE(CAST('2026-01-01' AS DATE), 'abc')", null))!;
        StringAssert.StartsWith("Cannot convert 'abc'", ex.Message);

        AssertNoCommonType(() => NonQuery(executor, db, "INSERT INTO t (id, n) VALUES (9, CASE WHEN true THEN 1 ELSE 'x' END)", null), "INSERT");
        Assert.AreEqual(0, await Count(executor, db, ""));
    }

    // ─── Set operations ───────────────────────────────────────────────────────

    [Test]
    public async Task SetOperations_GiveAStringLiteralTheTemporalTypeOfTheOtherOperand()
    {
        (string db, CommandExecutor executor) = await Seed(rows: 2);

        List<IReadOnlyDictionary<string, ColumnValue>> rows =
            await Query(executor, db, "SELECT d FROM t UNION SELECT '2026-01-01'", null);
        Assert.AreEqual(2, rows.Count, "the literal equals the date of row 1, so UNION keeps one of them");
        Assert.IsTrue(rows.All(r => r["d"].Type == ColumnType.Date));

        rows = await Query(executor, db, "SELECT '2026-01-02' AS d UNION ALL SELECT d FROM t", null);
        Assert.AreEqual(3, rows.Count);
        Assert.IsTrue(rows.All(r => r["d"].Type == ColumnType.Date));

        rows = await Query(executor, db, "SELECT d FROM t INTERSECT SELECT '2026-01-02'", null);
        Assert.AreEqual(1, rows.Count);

        rows = await Query(executor, db, "SELECT d FROM t EXCEPT SELECT '2026-01-02'", null);
        Assert.AreEqual(1, rows.Count);

        rows = await Query(executor, db, "SELECT dt FROM t UNION ALL SELECT '2026-03-01T00:00:00Z'", null);
        Assert.IsTrue(rows.All(r => r["dt"].Type == ColumnType.DateTime));

        QuerySchemaHolder schema = await Schema(executor, db, "SELECT d FROM t UNION ALL SELECT '2026-01-01'");
        Assert.AreEqual(ColumnType.Date, schema.Schema.Single().Type);
    }

    [TestCase("SELECT d FROM t UNION SELECT 'abc'", "Cannot convert 'abc'")]
    [TestCase("SELECT 'abc' UNION ALL SELECT d FROM t", "Cannot convert 'abc'")]
    [TestCase("SELECT d FROM t UNION SELECT s FROM t", "cannot be combined")]
    [TestCase("SELECT d FROM t UNION ALL (SELECT '2026-01-01' UNION SELECT '2026-01-02')", "cannot be combined")]
    public async Task SetOperations_RefuseAMalformedLiteral_AStringColumn_AndANestedLiteralColumn(string sql, string message)
    {
        (string db, CommandExecutor executor) = await Seed(rows: 0);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, db, sql, null))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, ex.Message);
        StringAssert.Contains(message, ex.Message);
    }

    // ─── IF ───────────────────────────────────────────────────────────────────

    [TestCase("SELECT IF(true, 1, 2)", 1L)]
    [TestCase("SELECT IF(false, 1, 2)", 2L)]
    [TestCase("SELECT IF(NULL, 1, 2)", 2L)]
    [TestCase("SELECT IF(1 < 2, 10, 20)", 10L)]
    [TestCase("SELECT if(true, 1, 2)", 1L)]
    [TestCase("SELECT IF(true, 1, 1 / 0)", 1L)]
    [TestCase("SELECT IF(false, 1 / 0, 7)", 7L)]
    [TestCase("SELECT IF(true, IF(false, 1, 2), 3)", 2L)]
    public async Task If_ReturnsTheSelectedResult_AndEvaluatesOnlyThatOne(string sql, long expected)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        ColumnValue value = await Scalar(executor, db, sql);
        Assert.AreEqual(ColumnType.Integer64, value.Type, sql);
        Assert.AreEqual(expected, value.LongValue, sql);
    }

    [Test]
    public async Task If_WithAConditionThatIsNotBoolean_IsInvalidInput()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(() => Query(executor, db, "SELECT IF(1, 2, 3)", null))!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code);
        StringAssert.Contains("IF condition", ex.Message);
    }

    [Test]
    public async Task If_InWhere_Projection_DerivedTable_UpdateSet_AndAView()
    {
        (string db, CommandExecutor executor) = await Seed(rows: 4);

        Assert.AreEqual(2, await Count(executor, db, "WHERE IF(id > 2, true, false)"));

        List<IReadOnlyDictionary<string, ColumnValue>> rows =
            await Query(executor, db, "SELECT id, IF(id > 2, 'big', 'small') AS size FROM t ORDER BY id", null);
        Assert.AreEqual(new[] { "small", "small", "big", "big" }, rows.Select(r => r["size"].StrValue).ToArray());

        rows = await Query(executor, db, "SELECT size FROM (SELECT IF(id > 2, 'big', 'small') AS size FROM t) d WHERE size = 'big'", null);
        Assert.AreEqual(2, rows.Count);

        await NonQuery(executor, db, "UPDATE t SET n = IF(id > 2, 100, 0) WHERE id > 0", null);
        Assert.AreEqual(2, await Count(executor, db, "WHERE n = 100"));

        await Ddl(executor, db, "CREATE VIEW sized AS SELECT id, IF(id > 2, 'big', 'small') AS size FROM t");
        rows = await Query(executor, db, "SELECT id FROM sized WHERE size = 'big'", null);
        Assert.AreEqual(2, rows.Count);
    }

    [Test]
    public async Task If_IsRenderedAsIf_InExplain()
    {
        (string db, CommandExecutor executor) = await Seed(rows: 2);

        List<IReadOnlyDictionary<string, ColumnValue>> plan =
            await Query(executor, db, "EXPLAIN SELECT id FROM t WHERE IF(id > 1, a, 0) = 2", null);

        string text = string.Join("\n", plan.SelectMany(r => r.Values).Where(v => v.Type == ColumnType.String).Select(v => v.StrValue));
        StringAssert.Contains("IF(", text);
    }

    [Test]
    public async Task IfExists_AndIfNotExists_StillParse()
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        await Ddl(executor, db, "CREATE TABLE IF NOT EXISTS u (id int64 PRIMARY KEY NOT NULL)");
        await Ddl(executor, db, "CREATE TABLE IF NOT EXISTS u (id int64 PRIMARY KEY NOT NULL)");
        await Ddl(executor, db, "DROP TABLE IF EXISTS u");
        await Ddl(executor, db, "DROP TABLE IF EXISTS u");
    }

    // ─── Harness ──────────────────────────────────────────────────────────────

    private static void AssertNoCommonType(Func<Task> statement, string label)
    {
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () => await statement(), label)!;
        Assert.AreEqual(CamusDBErrorCodes.InvalidInput, ex.Code, $"{label}: {ex.Message}");
        StringAssert.Contains("No common type for the results of", ex.Message, label);
    }

    /// <summary>
    /// A table <c>t (id int64 PK, a int64, s string, b bool, f float64, p numeric, n int64 NULL,
    /// d date, dt datetime)</c> with <paramref name="rows"/> rows (at most 9): a = id, s = 'id',
    /// b = false, f = id + 0.5, p = id, n = NULL, d = 2026-01-0{id}, dt = 2026-01-0{id}T10:00:00Z.
    /// </summary>
    private async Task<(string Db, CommandExecutor Executor)> Seed(int rows)
    {
        (string db, _, CommandExecutor executor) = await CreateDatabase();

        await Ddl(executor, db,
            "CREATE TABLE t (id int64 PRIMARY KEY NOT NULL, a int64 NULL, s string NULL, b bool NULL, f float64 NULL, p numeric NULL, n int64 NULL, d date NULL, dt datetime NULL)");

        if (rows > 0)
        {
            IEnumerable<string> values = Enumerable.Range(1, rows).Select(i =>
                $"({i}, {i}, '{i}', false, {i}.5, NUMERIC '{i}', CAST('2026-01-0{i}' AS DATE), CAST('2026-01-0{i}T10:00:00Z' AS DATETIME))");
            await NonQuery(executor, db, "INSERT INTO t (id, a, s, b, f, p, d, dt) VALUES " + string.Join(", ", values), null);
        }

        return (db, executor);
    }

    private static async Task Ddl(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, db, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task NonQuery(CommandExecutor executor, string db, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));
            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<long> Count(CommandExecutor executor, string db, string tail)
    {
        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(executor, db, $"SELECT COUNT(*) AS c FROM t {tail}", null);
        Assert.AreEqual(1, rows.Count);
        return rows[0]["c"].LongValue;
    }

    private static async Task<ColumnValue> Scalar(CommandExecutor executor, string db, string sql)
    {
        List<IReadOnlyDictionary<string, ColumnValue>> rows = await Query(executor, db, sql, null);
        Assert.AreEqual(1, rows.Count, sql);
        return rows[0].Values.First();
    }

    private static async Task<QuerySchemaHolder> Schema(CommandExecutor executor, string db, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();
        QuerySchemaHolder holder = new();

        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, null), null, holder);

            await foreach (QueryResultRow _ in rows) { }

            await database.Transactions.CommitAsync(tx);
            return holder;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static async Task<List<IReadOnlyDictionary<string, ColumnValue>>> Query(
        CommandExecutor executor, string db, string sql, Dictionary<string, ColumnValue>? parameters)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(db);
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) =
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, db, sql, parameters));

            List<IReadOnlyDictionary<string, ColumnValue>> result = [];
            await foreach (QueryResultRow row in rows)
                result.Add(row.Row);

            await database.Transactions.CommitAsync(tx);
            return result;
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }
}
