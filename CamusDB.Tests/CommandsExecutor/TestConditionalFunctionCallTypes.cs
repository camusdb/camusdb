/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers.Functions;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// COALESCE, IFNULL, NVL, CASE and IF read the declared type of a function call before any row is read, as
/// PostgreSQL does. Before this rule a call in an argument or a branch that did not run showed no type:
/// <c>COALESCE(1, upper('a'))</c> returned 1 where PostgreSQL refuses it, and <c>COALESCE(n, sqrt(4.0))</c>
/// was a FLOAT64 in one row and an INT64 in the next. Each expected result here was measured on PostgreSQL
/// 16.11 in the C locale with the same rows, unless the test says that CamusDB differs by decision.
///
/// <para>Two functions had a declared type that was not the type of every value. <c>round(int, n)</c> returned
/// an INT64 for n = 0 and a FLOAT64 for any other n, and now returns NUMERIC for every n, as in PostgreSQL.
/// <c>json_value</c> returns the type of the JSON scalar, so it declares that its type comes from its value,
/// and a conditional reads it as an unknown type.</para>
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestConditionalFunctionCallTypes : BaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateTables()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await Commit(database, executor, dbname, "CREATE TABLE t (id INT64 PRIMARY KEY, v INT64, n INT64, f FLOAT64, s STRING)");
        await Commit(database, executor, dbname, "INSERT INTO t (id, v, n, f, s) VALUES (1, 0, NULL, 2.5, 'x'), (2, 2, NULL, NULL, 'y'), (3, 0, 5, 0.5, NULL)");
        await Commit(database, executor, dbname, "CREATE TABLE em (id INT64 PRIMARY KEY, v INT64, n INT64, f FLOAT64, s STRING)");

        return (dbname, database, executor);
    }

    private static async Task Commit(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            if (sql.StartsWith("CREATE", StringComparison.Ordinal))
                await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
            else
                _ = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));

            await database.Transactions.CommitAsync(tx);
        }
        catch
        {
            await database.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    private static string Render(ColumnValue value) => value.Type switch
    {
        ColumnType.Null => "",
        ColumnType.String => value.StrValue!,
        ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Float64 => value.FloatValue.ToString("G17", CultureInfo.InvariantCulture),
        ColumnType.Numeric => NumericMath.Format(value.NumericUnscaled),
        _ => throw new ArgumentException(value.Type.ToString()),
    };

    /// <summary>
    /// The rows of <paramref name="sql"/>: cells join with <c>|</c> and rows with <c> ; </c>, or the error code.
    /// The type of the last cell of each row goes to <paramref name="lastCellTypes"/>.
    /// </summary>
    private static async Task<string> Run(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql, List<ColumnType>? lastCellTypes = null)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
            List<QueryResultRow> rows = await cursor.ToListAsync();

            foreach (QueryResultRow row in rows)
                lastCellTypes?.Add(row.Row.Values.Last().Type);

            return string.Join(" ; ", rows.Select(r => string.Join("|", r.Row.Values.Select(Render))));
        }
        catch (CamusDBException e)
        {
            return e.Code;
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    // ── A call that does not run is typed before any row is read ─────────────────────────

    // PostgreSQL 16.11 refuses each one, also on an empty table, under LIMIT 0 and in a dead branch.
    [TestCase("SELECT COALESCE(1, upper('a')) FROM t")]
    [TestCase("SELECT COALESCE(v, upper(s)) FROM t")]
    [TestCase("SELECT CASE WHEN TRUE THEN 1 ELSE upper('a') END FROM t")]
    [TestCase("SELECT CASE WHEN id > 0 THEN 1 ELSE upper(s) END FROM t")]
    [TestCase("SELECT COALESCE(1, upper('a')) FROM em")]
    [TestCase("SELECT COALESCE(1, upper(s)) FROM t LIMIT 0")]
    [TestCase("SELECT id FROM t WHERE FALSE AND COALESCE(1, upper(s)) = 1")]
    [TestCase("SELECT COALESCE(1, upper(lower(s))) FROM t")]
    [TestCase("SELECT COALESCE(1, upper(s) || 'x') FROM t")]
    [TestCase("SELECT COALESCE(1, md5(s)) FROM t")]
    [TestCase("SELECT COALESCE(1, upper(NULL)) FROM t")]
    [TestCase("SELECT COALESCE(now(), 1) FROM t")]
    [TestCase("SELECT COALESCE(TRUE, length(s)) FROM t")]
    [TestCase("SELECT CASE WHEN id > 0 THEN 1 ELSE CASE WHEN TRUE THEN upper(s) END END FROM t")]
    [TestCase("SELECT COALESCE(1, COALESCE(upper(s), 'a')) FROM t")]
    // PostgreSQL has no IF, IFNULL or NVL. They share the rule of CASE and COALESCE.
    [TestCase("SELECT IF(id > 0, 1, upper(s)) FROM t")]
    [TestCase("SELECT IFNULL(1, upper(s)) FROM em")]
    [TestCase("SELECT NVL(v, upper(s)) FROM t")]
    public async Task ACallWithNoCommonType_IsRefusedBeforeAnyRow(string sql)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo(CamusDBErrorCodes.InvalidInput), sql);
    }

    [Test]
    public async Task TheRefusal_NamesBothTypes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();
        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
                await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, "SELECT COALESCE(1, upper(s)) FROM em", null)));

            Assert.That(error!.Message, Does.Contain("No common type for the results of COALESCE: Integer64 and String"));
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }
    }

    // PostgreSQL 16.11 refuses the CHECK and the view body when it creates them.
    [TestCase("CREATE TABLE ck (id INT64 PRIMARY KEY, a INT64, b STRING, CHECK (COALESCE(a, upper(b)) IS NOT NULL))")]
    [TestCase("CREATE VIEW vw AS SELECT COALESCE(1, upper(s)) AS x FROM t")]
    public async Task ACallWithNoCommonType_IsRefusedByTheDdlThatStoresIt(string ddl)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(() => Commit(database, executor, dbname, ddl));

        Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput), ddl);
    }

    // ── One result type in every row ─────────────────────────────────────────────────────

    // PostgreSQL 16.11 gives these values, with double precision in every row for sqrt and abs of a float, and
    // bigint for length. CamusDB writes 4.0 as a FLOAT64 literal, where PostgreSQL reads it as numeric, so the
    // second case casts it in PostgreSQL to compare the same type.
    [TestCase("COALESCE(1, sqrt(f))", "1|1 ; 2|1 ; 3|1", ColumnType.Float64)]
    [TestCase("COALESCE(n, sqrt(4.0))", "1|2 ; 2|2 ; 3|5", ColumnType.Float64)]
    [TestCase("CASE WHEN id > 0 THEN 1 ELSE sqrt(f) END", "1|1 ; 2|1 ; 3|1", ColumnType.Float64)]
    [TestCase("COALESCE(n, sqrt(f))", "1|1.5811388300841898 ; 2| ; 3|5", ColumnType.Float64)]
    [TestCase("COALESCE(v, abs(f))", "1|0 ; 2|2 ; 3|0", ColumnType.Float64)]
    [TestCase("COALESCE(f, abs(v))", "1|2.5 ; 2|2 ; 3|0.5", ColumnType.Float64)]
    [TestCase("COALESCE(v, length(s))", "1|0 ; 2|2 ; 3|0", ColumnType.Integer64)]
    [TestCase("COALESCE(n, abs(v))", "1|0 ; 2|2 ; 3|5", ColumnType.Integer64)]
    [TestCase("COALESCE(1, length(upper(s)))", "1|1 ; 2|1 ; 3|1", ColumnType.Integer64)]
    [TestCase("COALESCE(s, upper('a'))", "1|x ; 2|y ; 3|A", ColumnType.String)]
    [TestCase("IF(id > 0, 1, sqrt(f))", "1|1 ; 2|1 ; 3|1", ColumnType.Float64)]
    [TestCase("IFNULL(n, sqrt(f))", "1|1.5811388300841898 ; 2| ; 3|5", ColumnType.Float64)]
    public async Task ACallShowsItsDeclaredType_InEveryRow(string expression, string expected, ColumnType type)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<ColumnType> types = [];
        string rows = await Run(database, executor, dbname, $"SELECT id, {expression} FROM t ORDER BY id", types);

        Assert.Multiple(() =>
        {
            Assert.That(rows, Is.EqualTo(expected), expression);
            Assert.That(types.Where(t => t != ColumnType.Null), Is.All.EqualTo(type), expression);
        });
    }

    /// <summary>
    /// A stored column takes the type of the whole expression. Before this rule the CREATE TABLE AS failed with
    /// "Type Float64 cannot be assigned to x (Integer64)", because the binder recorded the type of n alone.
    /// PostgreSQL 16.11 stores double precision, with the values 1.58..., NULL and 5.
    /// </summary>
    [Test]
    public async Task CreateTableAs_StoresTheTypeOfTheWholeExpression()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        await Commit(database, executor, dbname, "CREATE TABLE a AS SELECT id, COALESCE(n, sqrt(f)) AS x FROM t");

        List<ColumnType> types = [];
        string rows = await Run(database, executor, dbname, "SELECT id, x FROM a ORDER BY id", types);

        Assert.Multiple(() =>
        {
            Assert.That(rows, Is.EqualTo("1|1.5811388300841898 ; 2| ; 3|5"));
            Assert.That(types, Is.EqualTo(new[] { ColumnType.Float64, ColumnType.Null, ColumnType.Float64 }));
        });
    }

    /// <summary>
    /// A FROM-less SELECT has no binding that records the result type, so the evaluator of COALESCE and of
    /// CASE reads the declared type of a call that does not run. Before, <c>SELECT COALESCE(1, sqrt(4.0))</c>
    /// was an INT64 there and a FLOAT64 with a FROM clause. PostgreSQL 16.11 gives 1 for each; it reads 4.0
    /// as numeric, so its type is numeric where CamusDB, which reads 4.0 as a FLOAT64, gives FLOAT64.
    /// </summary>
    [TestCase("COALESCE(1, sqrt(4.0))", ColumnType.Float64)]
    [TestCase("COALESCE(NULL, 1, sqrt(4.0))", ColumnType.Float64)]
    [TestCase("COALESCE(1, COALESCE(NULL, sqrt(4.0)))", ColumnType.Float64)]
    [TestCase("IFNULL(1, sqrt(4.0))", ColumnType.Float64)]
    [TestCase("CASE WHEN TRUE THEN 1 ELSE sqrt(4.0) END", ColumnType.Float64)]
    [TestCase("IF(TRUE, 1, sqrt(4.0))", ColumnType.Float64)]
    [TestCase("COALESCE(1, round(15, -1))", ColumnType.Numeric)]
    public async Task AFromlessSelect_ReadsTheDeclaredType(string expression, ColumnType type)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<ColumnType> types = [];
        string value = await Run(database, executor, dbname, $"SELECT {expression}", types);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo("1"), expression);
            Assert.That(types, Is.EqualTo(new[] { type }), expression);
        });
    }

    // ── A call whose type the statement does not show ────────────────────────────────────

    /// <summary>
    /// A call is typed only when the type of each of its arguments is known. A parameter shows its type only
    /// through its value, so <c>upper(@p)</c> is checked when it returns a value. PostgreSQL refuses
    /// <c>COALESCE(1, upper($1))</c> when it prepares the statement; that difference is a decision.
    /// </summary>
    [Test]
    public async Task ACallWithAParameterArgument_IsCheckedWhenItRuns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();
        Dictionary<string, ColumnValue> parameters = new() { ["@p"] = new ColumnValue(ColumnType.String, "a") };

        KvTransaction tx = await database.Transactions.BeginAsync();

        try
        {
            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(tx, dbname, "SELECT COALESCE(1, upper(@p)) AS x FROM t", parameters));
            List<QueryResultRow> rows = await cursor.ToListAsync();

            Assert.That(rows.Select(r => r.Row["x"].LongValue), Is.EqualTo(new long[] { 1, 1, 1 }));
        }
        finally
        {
            await database.Transactions.RollbackAsync(tx);
        }

        KvTransaction second = await database.Transactions.BeginAsync();

        try
        {
            CamusDBException? error = Assert.ThrowsAsync<CamusDBException>(async () =>
            {
                (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                    new ExecuteSQLTicket(second, dbname, "SELECT COALESCE(n, upper(@p)) AS x FROM t", parameters));
                await cursor.ToListAsync();
            });

            Assert.That(error!.Code, Is.EqualTo(CamusDBErrorCodes.InvalidInput));
        }
        finally
        {
            await database.Transactions.RollbackAsync(second);
        }
    }

    /// <summary>
    /// <c>json_value</c> returns the type of the JSON scalar, so a conditional reads it as an unknown type: a
    /// call that does not run raises nothing, and the value of one that runs is checked. PostgreSQL 16 has no
    /// json_value.
    /// </summary>
    [TestCase("SELECT id, COALESCE(1, json_value('{\"a\":\"x\"}', '$.a')) FROM t ORDER BY id", "1|1 ; 2|1 ; 3|1")]
    [TestCase("SELECT id, COALESCE(n, json_value('{\"a\":1}', '$.a')) FROM t ORDER BY id", "1|1 ; 2|1 ; 3|5")]
    [TestCase("SELECT id, COALESCE(s, json_value('{\"a\":\"z\"}', '$.a')) FROM t ORDER BY id", "1|x ; 2|y ; 3|z")]
    [TestCase("SELECT id, COALESCE(s, json_value('{\"a\":1}', '$.a')) FROM t ORDER BY id", CamusDBErrorCodes.InvalidInput)]
    public async Task AFunctionWhoseTypeComesFromItsValue_IsCheckedWhenItRuns(string sql, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        Assert.That(await Run(database, executor, dbname, sql), Is.EqualTo(expected), sql);
    }

    [Test]
    public void DeclaredResultType_IsNullWhereTheDeclarationDoesNotFixIt()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ScalarFunctionEvaluator.DeclaredResultType("upper", [ColumnType.String], builtInOnly: true), Is.EqualTo(new ExpressionType(ColumnType.String)));
            Assert.That(ScalarFunctionEvaluator.DeclaredResultType("sqrt", [ColumnType.Integer64], builtInOnly: true), Is.EqualTo(new ExpressionType(ColumnType.Float64)));
            Assert.That(ScalarFunctionEvaluator.DeclaredResultType("json_value", [ColumnType.String, ColumnType.String], builtInOnly: true), Is.Null, "the type comes from the value");
            Assert.That(ScalarFunctionEvaluator.DeclaredResultType("coalesce", [ColumnType.Integer64], builtInOnly: true), Is.Null, "a conditional has its own rule");
            Assert.That(ScalarFunctionEvaluator.DeclaredResultType("sum", [ColumnType.Integer64], builtInOnly: true), Is.Null, "an aggregate");
            Assert.That(ScalarFunctionEvaluator.DeclaredResultType("upper", [], builtInOnly: true), Is.Null, "an argument count the function refuses");
            Assert.That(ScalarFunctionEvaluator.DeclaredResultType("no_such_function", [], builtInOnly: true), Is.Null);
            Assert.That(ScalarFunctionEvaluator.DeclaredResultType("regexp_match", [ColumnType.String, ColumnType.String], builtInOnly: true), Is.Null, "an array with no declared element type");
        });
    }

    // ── round(int, n) is NUMERIC ─────────────────────────────────────────────────────────

    // PostgreSQL 16.11 gives numeric for each, with the same values. PostgreSQL keeps the scale of the result,
    // so it writes round(5, 1) as 5.0 where CamusDB writes 5.
    [TestCase("round(5, 0)", "5")]
    [TestCase("round(5, 1)", "5")]
    [TestCase("round(15, -1)", "20")]
    [TestCase("round(25, -1)", "30")]
    [TestCase("round(-25, -1)", "-30")]
    [TestCase("round(9223372036854775807, -1)", "9223372036854775810")]
    public async Task RoundOfAnInteger_WithAScale_IsNumeric(string expression, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<ColumnType> types = [];
        string value = await Run(database, executor, dbname, $"SELECT {expression}", types);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo(expected), expression);
            Assert.That(types, Is.EqualTo(new[] { ColumnType.Numeric }), expression);
        });
    }

    /// <summary>
    /// The scale differs by row here, and each row has the same type. Before this rule the rows were FLOAT64,
    /// INT64 and FLOAT64, and the CREATE TABLE AS failed with "Type Float64 cannot be assigned to x (Integer64)".
    /// PostgreSQL 16.11 gives numeric 0, 2 and 3.0.
    /// </summary>
    [Test]
    public async Task RoundOfAnInteger_HasOneTypeInEveryRow_AndCanBeStored()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<ColumnType> selected = [];
        string rows = await Run(database, executor, dbname, "SELECT id, round(id, id - 2) FROM t ORDER BY id", selected);

        await Commit(database, executor, dbname, "CREATE TABLE r AS SELECT id, round(id, id - 2) AS x FROM t");

        List<ColumnType> stored = [];
        string storedRows = await Run(database, executor, dbname, "SELECT id, x FROM r ORDER BY id", stored);

        Assert.Multiple(() =>
        {
            Assert.That(rows, Is.EqualTo("1|0 ; 2|2 ; 3|3"));
            Assert.That(selected, Is.All.EqualTo(ColumnType.Numeric));
            Assert.That(storedRows, Is.EqualTo("1|0 ; 2|2 ; 3|3"));
            Assert.That(stored, Is.All.EqualTo(ColumnType.Numeric));
        });
    }

    /// <summary>
    /// <c>round(i)</c> and <c>trunc(i, n)</c> keep INT64, and their values do not change. PostgreSQL 16.11 gives
    /// 5 as double precision and 10 as numeric. In the COALESCE case trunc does not run, so only its declared type
    /// reaches the result.
    /// </summary>
    [TestCase("round(5)", "5")]
    [TestCase("trunc(15, -1)", "10")]
    [TestCase("trunc(15, 0)", "15")]
    [TestCase("COALESCE(1, trunc(15, -1))", "1")]
    public async Task RoundWithoutAScale_AndTrunc_StayInt64(string expression, string expected)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<ColumnType> types = [];
        string value = await Run(database, executor, dbname, $"SELECT {expression}", types);

        Assert.Multiple(() =>
        {
            Assert.That(value, Is.EqualTo(expected), expression);
            Assert.That(types, Is.EqualTo(new[] { ColumnType.Integer64 }), expression);
        });
    }

    /// <summary>A conditional over round(int, n) is NUMERIC in every row. PostgreSQL 16.11 gives numeric 1.0, 2.0 and 5.</summary>
    [Test]
    public async Task ACoalesceOverRoundOfAnInteger_IsNumericInEveryRow()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateTables();

        List<ColumnType> types = [];
        string rows = await Run(database, executor, dbname, "SELECT id, COALESCE(n, round(id, 1)) FROM t ORDER BY id", types);

        Assert.Multiple(() =>
        {
            Assert.That(rows, Is.EqualTo("1|1 ; 2|2 ; 3|5"));
            Assert.That(types, Is.All.EqualTo(ColumnType.Numeric));
        });
    }
}
