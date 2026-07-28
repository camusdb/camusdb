
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
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Persistence + replication tests for CHECK constraints. Verifies that check
/// constraints survive close/reopen, that the condition AST cache is rebuilt on load,
/// and that the render→parse→render round-trip is stable.
/// </summary>
public sealed class TestCheckConstraintsPersistence : BaseTest
{
    // ── helpers ────────────────────────────────────────────────────────────────

    private async Task ExecDDL(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor database = await executor.OpenDatabase(dbname);
        KvTransaction txn = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
            txnState: txn, database: dbname, sql: sql, parameters: null));
    }

    // ── round-trip: render → parse → render is stable ─────────────────────────

    [Test]
    public void RenderParseRoundTrip_SimpleComparison()
    {
        NodeAst condition = SQLParserProcessor.ParseCondition("price > 0");
        string rendered = PlanRenderer.RenderExpr(condition);
        Assert.AreEqual("price > 0", rendered);

        // Second pass: re-parse the rendered form and render again.
        NodeAst condition2 = SQLParserProcessor.ParseCondition(rendered);
        string rendered2 = PlanRenderer.RenderExpr(condition2);
        Assert.AreEqual(rendered, rendered2, "render → parse → render must be stable");
    }

    [Test]
    public void RenderParseRoundTrip_AndCondition()
    {
        NodeAst condition = SQLParserProcessor.ParseCondition("price > 0 AND price < 1000");
        string rendered = PlanRenderer.RenderExpr(condition);

        NodeAst condition2 = SQLParserProcessor.ParseCondition(rendered);
        string rendered2 = PlanRenderer.RenderExpr(condition2);
        Assert.AreEqual(rendered, rendered2, "AND condition round-trip must be stable");
    }

    [Test]
    public void RenderParseRoundTrip_MultiColumnExpression()
    {
        NodeAst condition = SQLParserProcessor.ParseCondition("price > discounted_price");
        string rendered = PlanRenderer.RenderExpr(condition);

        NodeAst condition2 = SQLParserProcessor.ParseCondition(rendered);
        string rendered2 = PlanRenderer.RenderExpr(condition2);
        Assert.AreEqual(rendered, rendered2, "multi-column expression round-trip must be stable");
    }

    [Test]
    public void RenderParseRoundTrip_OrCondition()
    {
        NodeAst condition = SQLParserProcessor.ParseCondition("status = 1 OR status = 2");
        string rendered = PlanRenderer.RenderExpr(condition);

        NodeAst condition2 = SQLParserProcessor.ParseCondition(rendered);
        string rendered2 = PlanRenderer.RenderExpr(condition2);
        Assert.AreEqual(rendered, rendered2, "OR condition round-trip must be stable");
    }

    // ── ParseCheckConstraintAsts rebuilds from stored text ─────────────────────

    [Test]
    public void ParseCheckConstraintAsts_BuildsConditionFromExpression()
    {
        TableSchema tableSchema = new()
        {
            CheckConstraints = [new CheckConstraintSchema
            {
                Name = "t_price_check",
                Expression = "price > 0",
                ReferencedColumns = ["price"]
            }]
        };

        // ParsedCondition starts null (simulating a freshly deserialized JSON checkpoint).
        Assert.IsNull(tableSchema.CheckConstraints[0].ParsedCondition);

        CatalogsManager.ParseCheckConstraintAsts(tableSchema);

        Assert.IsNotNull(tableSchema.CheckConstraints[0].ParsedCondition,
            "ParsedCondition should be populated after ParseCheckConstraintAsts");
    }

    [Test]
    public void ParseCheckConstraintAsts_IdempotentWhenAlreadyParsed()
    {
        NodeAst original = SQLParserProcessor.ParseCondition("x > 0");
        TableSchema tableSchema = new()
        {
            CheckConstraints = [new CheckConstraintSchema
            {
                Name = "t_x_check",
                Expression = "x > 0",
                ReferencedColumns = ["x"],
                ParsedCondition = original
            }]
        };

        CatalogsManager.ParseCheckConstraintAsts(tableSchema);

        // The original reference must be preserved, not replaced.
        Assert.AreSame(original, tableSchema.CheckConstraints[0].ParsedCondition);
    }

    [Test]
    public void ParseCheckConstraintAsts_NoOpOnTableWithNoChecks()
    {
        TableSchema tableSchema = new() { CheckConstraints = null };
        Assert.DoesNotThrow(() => CatalogsManager.ParseCheckConstraintAsts(tableSchema));
    }

    [Test]
    public void ParseCheckConstraintAsts_MultipleConstraints()
    {
        TableSchema tableSchema = new()
        {
            CheckConstraints =
            [
                new CheckConstraintSchema { Name = "c1", Expression = "a > 0", ReferencedColumns = ["a"] },
                new CheckConstraintSchema { Name = "c2", Expression = "b >= 0", ReferencedColumns = ["b"] }
            ]
        };

        CatalogsManager.ParseCheckConstraintAsts(tableSchema);

        Assert.IsNotNull(tableSchema.CheckConstraints[0].ParsedCondition, "first constraint must be parsed");
        Assert.IsNotNull(tableSchema.CheckConstraints[1].ParsedCondition, "second constraint must be parsed");
    }

    // ── persistence: checks survive close + reopen ─────────────────────────────

    [Test]
    public async Task CheckConstraint_SurvivesCloseAndReopen_ColumnLevel()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY, price int64 CHECK (price > 0))");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        List<CheckConstraintSchema>? checks = db2.Schema.Tables["products"].CheckConstraints;
        Assert.IsNotNull(checks, "CheckConstraints must survive close/reopen");
        Assert.AreEqual(1, checks!.Count);

        CheckConstraintSchema cc = checks[0];
        Assert.AreEqual("products_price_check", cc.Name);
        Assert.AreEqual("price > 0", cc.Expression);
        Assert.That(cc.ReferencedColumns, Does.Contain("price"));

        // AST cache must be rebuilt on load.
        Assert.IsNotNull(cc.ParsedCondition, "ParsedCondition must be rebuilt at table-open");
    }

    [Test]
    public async Task CheckConstraint_SurvivesCloseAndReopen_NamedTableLevel()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE orders (id object_id PRIMARY KEY, qty int64, price int64, " +
            "CONSTRAINT positive_qty CHECK (qty > 0))");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        List<CheckConstraintSchema>? checks = db2.Schema.Tables["orders"].CheckConstraints;
        Assert.IsNotNull(checks);
        Assert.AreEqual(1, checks!.Count);
        Assert.AreEqual("positive_qty", checks[0].Name);
        Assert.AreEqual("qty > 0", checks[0].Expression);
        Assert.IsNotNull(checks[0].ParsedCondition);
    }

    [Test]
    public async Task CheckConstraint_MultipleChecks_AllSurviveReopen()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE employees (" +
            "  id object_id PRIMARY KEY, " +
            "  salary int64 CHECK (salary > 0), " +
            "  bonus int64 CHECK (bonus >= 0))");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        List<CheckConstraintSchema>? checks = db2.Schema.Tables["employees"].CheckConstraints;
        Assert.IsNotNull(checks);
        Assert.AreEqual(2, checks!.Count);

        CheckConstraintSchema? salaryCheck = checks.FirstOrDefault(c => c.Name.Contains("salary"));
        CheckConstraintSchema? bonusCheck = checks.FirstOrDefault(c => c.Name.Contains("bonus"));

        Assert.IsNotNull(salaryCheck);
        Assert.IsNotNull(bonusCheck);
        Assert.AreEqual("salary > 0", salaryCheck!.Expression);
        Assert.AreEqual("bonus >= 0", bonusCheck!.Expression);
        Assert.IsNotNull(salaryCheck.ParsedCondition);
        Assert.IsNotNull(bonusCheck.ParsedCondition);
    }

    [Test]
    public async Task NoCheckConstraints_TableReopensWithNullOrEmpty()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE plain (id object_id PRIMARY KEY, name string)");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        TableSchema tableSchema = db2.Schema.Tables["plain"];
        Assert.That(
            tableSchema.CheckConstraints is null || tableSchema.CheckConstraints.Count == 0,
            "Table without checks must reopen with null/empty CheckConstraints");
    }

    [Test]
    public async Task CheckConstraint_MultiColumnCross_SurvivesReopen()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE products (" +
            "  id object_id PRIMARY KEY, " +
            "  price int64, discounted_price int64, " +
            "  CONSTRAINT valid_discount CHECK (price > discounted_price))");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        List<CheckConstraintSchema>? checks = db2.Schema.Tables["products"].CheckConstraints;
        Assert.IsNotNull(checks);
        Assert.AreEqual(1, checks!.Count);
        Assert.AreEqual("valid_discount", checks[0].Name);
        Assert.That(checks[0].ReferencedColumns, Does.Contain("price"));
        Assert.That(checks[0].ReferencedColumns, Does.Contain("discounted_price"));
        Assert.IsNotNull(checks[0].ParsedCondition);
    }

    [Test]
    public async Task CheckConstraint_ExpressionMatchesOriginalAfterReopen()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE t (id object_id PRIMARY KEY, x int64 CHECK (x > 0 AND x < 1000))");

        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        DatabaseDescriptor db2 = await executor.OpenDatabase(dbname);

        CheckConstraintSchema cc = db2.Schema.Tables["t"].CheckConstraints![0];

        // Re-render the AST rebuilt at load time with the same faithful renderer used to persist it,
        // and verify it is byte-stable: render → parse → render must be a fixed point, otherwise the
        // enforced predicate could drift on a subsequent reopen.
        string rerendered = CheckConditionRenderer.Render(cc.ParsedCondition!);
        Assert.AreEqual(cc.Expression, rerendered,
            "Expression stored at create time and expression rendered from the reloaded AST must match");
    }
}
