
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Threading.Tasks;
using System.Linq;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.CommandsExecutor.Controllers.DDL;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// DDL-level CHECK tests: the CheckConstraintSchema model, ticket builder desugaring (column- and
/// table-level CHECK to named CheckConstraintInfo), auto-naming, referenced-column extraction, and
/// DDL validation (rejects subqueries, aggregates, volatile functions, and unknown columns).
/// INSERT/UPDATE enforcement is covered by the enforcement test suite.
/// </summary>
public sealed class TestCheckConstraintsDDL : SharedNodeBaseTest
{
    // ── helpers ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Builds a <see cref="CreateTableTicket"/> directly from a SQL string without creating
    /// a database. This lets these tests inspect the desugared CheckConstraints list in isolation.
    /// </summary>
    private static CreateTableTicket BuildTicket(string sql)
    {
        NodeAst ast = SQLParserProcessor.Parse(sql);
        SQLExecutorCreateTableCreator creator = new();
        ExecuteSQLTicket sqlTicket = new(txnState: null!, database: "testdb", sql: sql, parameters: null);
        return creator.CreateCreateTableTicket(sqlTicket, ast);
    }

    // ── column-level CHECK desugaring ──────────────────────────────────────────

    [Test]
    public void TestColumnCheckDesugarsSingleConstraint()
    {
        CreateTableTicket ticket = BuildTicket(
            "CREATE TABLE products (price int64 CHECK (price > 0))");

        Assert.AreEqual(1, ticket.CheckConstraints.Length);
        CheckConstraintInfo cc = ticket.CheckConstraints[0];
        Assert.AreEqual("products_price_check", cc.Name);
        Assert.AreEqual("price > 0", cc.Expression);
        Assert.Contains("price", cc.ReferencedColumns);
    }

    [Test]
    public void TestColumnCheckMultipleColumns()
    {
        CreateTableTicket ticket = BuildTicket(
            "CREATE TABLE employees (" +
            "  salary int64 CHECK (salary > 0), " +
            "  bonus int64 CHECK (bonus >= 0))");

        Assert.AreEqual(2, ticket.CheckConstraints.Length);

        CheckConstraintInfo salary = ticket.CheckConstraints.First(c => c.Name.Contains("salary"));
        Assert.AreEqual("employees_salary_check", salary.Name);
        Assert.AreEqual("salary > 0", salary.Expression);
        Assert.Contains("salary", salary.ReferencedColumns);

        CheckConstraintInfo bonus = ticket.CheckConstraints.First(c => c.Name.Contains("bonus"));
        Assert.AreEqual("employees_bonus_check", bonus.Name);
        Assert.AreEqual("bonus >= 0", bonus.Expression);
        Assert.Contains("bonus", bonus.ReferencedColumns);
    }

    [Test]
    public void TestColumnCheckReferencingAnotherColumn()
    {
        // Column-level CHECK can reference any column on the table.
        CreateTableTicket ticket = BuildTicket(
            "CREATE TABLE employees (" +
            "  birth_date string, " +
            "  joined_date string CHECK (joined_date > birth_date))");

        Assert.AreEqual(1, ticket.CheckConstraints.Length);
        CheckConstraintInfo cc = ticket.CheckConstraints[0];
        Assert.AreEqual("employees_joined_date_check", cc.Name);
        Assert.That(cc.ReferencedColumns, Does.Contain("joined_date"));
        Assert.That(cc.ReferencedColumns, Does.Contain("birth_date"));
    }

    [Test]
    public void TestColumnCheckWithAndCondition()
    {
        CreateTableTicket ticket = BuildTicket(
            "CREATE TABLE t (price int64 CHECK (price > 0 AND price < 1000))");

        Assert.AreEqual(1, ticket.CheckConstraints.Length);
        CheckConstraintInfo cc = ticket.CheckConstraints[0];
        Assert.AreEqual("t_price_check", cc.Name);
        Assert.That(cc.Expression, Does.Contain("AND"));
        Assert.Contains("price", cc.ReferencedColumns);
    }

    // ── table-level CHECK (named and unnamed) ─────────────────────────────────

    [Test]
    public void TestTableLevelNamedCheck()
    {
        CreateTableTicket ticket = BuildTicket(
            "CREATE TABLE products (" +
            "  price int64, discounted_price int64, " +
            "  CONSTRAINT valid_discount CHECK (price > discounted_price))");

        Assert.AreEqual(1, ticket.CheckConstraints.Length);
        CheckConstraintInfo cc = ticket.CheckConstraints[0];
        Assert.AreEqual("valid_discount", cc.Name);
        Assert.AreEqual("price > discounted_price", cc.Expression);
        Assert.That(cc.ReferencedColumns, Does.Contain("price"));
        Assert.That(cc.ReferencedColumns, Does.Contain("discounted_price"));
    }

    [Test]
    public void TestTableLevelUnnamedCheckAutoNamed()
    {
        CreateTableTicket ticket = BuildTicket(
            "CREATE TABLE products (price int64, CHECK (price > 0))");

        Assert.AreEqual(1, ticket.CheckConstraints.Length);
        CheckConstraintInfo cc = ticket.CheckConstraints[0];
        // Auto-named as {table}_check{N}
        Assert.That(cc.Name, Does.StartWith("products_check"));
        Assert.AreEqual("price > 0", cc.Expression);
    }

    [Test]
    public void TestMixedColumnAndTableChecks()
    {
        // Column-level + named table-level in one CREATE TABLE.
        CreateTableTicket ticket = BuildTicket(
            "CREATE TABLE orders (" +
            "  quantity int64 CHECK (quantity > 0), " +
            "  price int64, total int64, " +
            "  CONSTRAINT correct_total CHECK (total = quantity))");

        Assert.AreEqual(2, ticket.CheckConstraints.Length);
        Assert.That(ticket.CheckConstraints.Select(c => c.Name), Does.Contain("orders_quantity_check"));
        Assert.That(ticket.CheckConstraints.Select(c => c.Name), Does.Contain("correct_total"));
    }

    // ── no-check table produces empty array ───────────────────────────────────

    [Test]
    public void TestNoCheckConstraintsProducesEmptyArray()
    {
        CreateTableTicket ticket = BuildTicket(
            "CREATE TABLE t (id object_id, name string)");

        Assert.AreEqual(0, ticket.CheckConstraints.Length);
    }

    // ── column-level CHECK is desugared onto the ticket ───────────────────────

    [Test]
    public void TestColumnLevelCheckIsDesugaredToTableCheck()
    {
        CreateTableTicket ticket = BuildTicket(
            "CREATE TABLE t (price int64 CHECK (price > 0))");

        CheckConstraintInfo check = ticket.CheckConstraints.Single();
        Assert.AreEqual("t_price_check", check.Name);
        Assert.AreEqual("price > 0", check.Expression);
        Assert.That(check.ReferencedColumns, Does.Contain("price"));
    }

    [Test]
    public void TestNoColumnCheckProducesNoConstraint()
    {
        CreateTableTicket ticket = BuildTicket(
            "CREATE TABLE t (price int64, name string)");

        Assert.AreEqual(0, ticket.CheckConstraints.Length);
    }

    // ── DDL validation: subqueries rejected ───────────────────────────────────

    [Test]
    public void TestRejectsSubqueryInCheck()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            BuildTicket("CREATE TABLE t (x int64, CHECK (x IN (SELECT x FROM t2)))"))!;

        Assert.That(ex.Message, Does.Contain("subquer"));
    }

    // ── DDL validation: aggregate functions rejected ───────────────────────────

    [Test]
    public void TestRejectsAggregateInCheck()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            BuildTicket("CREATE TABLE t (x int64, CHECK (sum(x) > 0))"))!;

        Assert.That(ex.Message, Does.Contain("aggregate"));
    }

    [Test]
    public void TestRejectsCountAggregateInCheck()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            BuildTicket("CREATE TABLE t (x int64, CHECK (count(x) > 0))"))!;

        Assert.That(ex.Message, Does.Contain("aggregate"));
    }

    // ── DDL validation: volatile functions rejected ────────────────────────────

    [Test]
    public void TestRejectsVolatileFunctionInCheck()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            BuildTicket("CREATE TABLE t (x int64, CHECK (x > now()))"))!;

        // Either "volatile" or "non-deterministic" in the message.
        Assert.That(ex.Message.ToLowerInvariant(), Does.Contain("volatile").Or.Contain("non-deterministic"));
    }

    // ── DDL validation: unknown columns rejected ───────────────────────────────

    [Test]
    public void TestRejectsCheckReferencingUnknownColumn()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            BuildTicket("CREATE TABLE t (x int64 CHECK (x > nonexistent))"))!;

        Assert.That(ex.Message, Does.Contain("nonexistent"));
    }

    [Test]
    public void TestRejectsTableCheckReferencingUnknownColumn()
    {
        CamusDBException ex = Assert.Throws<CamusDBException>(() =>
            BuildTicket("CREATE TABLE t (x int64, CONSTRAINT c1 CHECK (x > ghost_col))"))!;

        Assert.That(ex.Message, Does.Contain("ghost_col"));
    }

    // ── round-trip: create via DDL SQL through executor ───────────────────────

    [Test]
    public async Task TestCreateTableWithCheckViaExecutorSucceeds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        // Exercises the full DDL pipeline: CREATE TABLE with a CHECK should succeed without throwing.
        Assert.DoesNotThrowAsync(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                txnState: txn,
                database: dbname,
                sql: "CREATE TABLE products (id object_id PRIMARY KEY, price int64 CHECK (price > 0))",
                parameters: null)));
    }

    [Test]
    public async Task TestCreateTableWithNamedConstraintCheckViaExecutorSucceeds()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        KvTransaction txn = await database.Transactions.BeginAsync();

        Assert.DoesNotThrowAsync(async () =>
            await executor.ExecuteDDLSQL(new ExecuteSQLTicket(
                txnState: txn,
                database: dbname,
                sql: "CREATE TABLE orders (id object_id PRIMARY KEY, qty int64, price int64, " +
                     "CONSTRAINT positive_qty CHECK (qty > 0))",
                parameters: null)));
    }
}
