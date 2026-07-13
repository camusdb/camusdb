
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Controllers;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Regression tests for correctness gaps found reviewing the CHECK / named-NOT-NULL feature:
/// date-literal and cross-type comparisons in a CHECK, round-trip fidelity of the persisted
/// condition text (parenthesized precedence and IN-lists), per-operator NULL passing enforced
/// end-to-end, "rejected ALTER leaves the schema unchanged", NOT NULL surviving reopen, and the
/// cluster-forwarding DTOs carrying checks and the NOT NULL target column.
/// </summary>
[NonParallelizable]
public sealed class TestCheckConstraintsFixes : BaseTest
{
    private async Task ExecDDL(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private async Task ExecInsert(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    private async Task<DatabaseDescriptor> Reopen(CommandExecutor executor, string dbname)
    {
        await executor.CloseDatabase(new CloseDatabaseTicket(dbname));
        return await executor.OpenDatabase(dbname);
    }

    // ── Date-literal CHECK (spec acceptance criterion; previously threw ArgumentException) ─────

    [Test]
    public async Task DateCheck_AgainstStringLiteral_EnforcedBothWays()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE people (id object_id PRIMARY KEY, birth_date date CHECK (birth_date > '1900-01-01'))");

        // Conforming row is accepted.
        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO people (id, birth_date) VALUES (gen_id(), '2000-06-15')"));

        // Violating row is rejected with a domain error (not a raw ArgumentException / 500).
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO people (id, birth_date) VALUES (gen_id(), '1850-01-01')"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);

        // NULL passes (unknown), even though the column is a date compared to a string literal.
        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname, "INSERT INTO people (id) VALUES (gen_id())"));
    }

    [Test]
    public async Task IncompatibleTypeCheck_SurfacesDomainError_NotRawException()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        // String column compared to an integer literal has no coercion; enforcement must raise a
        // CamusDBException rather than letting ColumnValue.CompareTo escape as an ArgumentException.
        await ExecDDL(executor, dbname,
            "CREATE TABLE t (id object_id PRIMARY KEY, name string CHECK (name > 5))");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO t (id, name) VALUES (gen_id(), 'hello')"))!;
        // Reported as a check rejection (HTTP 400), not a raw ArgumentException (which is a 500).
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
        Assert.AreEqual(400, CamusDBErrorCodes.GetHttpStatus(ex.Code));
        Assert.That(ex.Message, Does.Contain("incompatible"));
    }

    // ── Numeric literal vs float column (integer literal must widen, not error) ───────────────

    [Test]
    public async Task FloatColumnCheck_AgainstIntegerLiteral_Works()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY DEFAULT (gen_id()), price float64 CHECK (price > 0))");

        // Integer literal for a float column is accepted (widened), and the check (price > 0, with
        // 0 an integer literal) evaluates numerically instead of erroring on Float64 vs Integer64.
        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname, "INSERT INTO products (price) VALUES (100)"));
        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname, "INSERT INTO products (price) VALUES (100.0)"));

        // A violating value is still rejected as a check violation (not an incompatible-type error).
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, dbname, "INSERT INTO products (price) VALUES (-5)"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
        Assert.That(ex.Message, Does.Not.Contain("incompatible"));
    }

    // ── Round-trip fidelity: parenthesized precedence must survive persist + reopen ───────────

    [Test]
    public async Task ParenthesizedPrecedence_EnforcedCorrectlyAfterReopen()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        // (a OR b) AND c differs from a OR (b AND c). With a=true, b=false, c=false the grouped
        // form is FALSE (must reject); the drifted form would be TRUE (would accept).
        await ExecDDL(executor, dbname,
            "CREATE TABLE t (id object_id PRIMARY KEY, a bool, b bool, c bool, " +
            "CONSTRAINT grp CHECK ((a OR b) AND c))");

        // Reopen so enforcement runs against the AST re-parsed from the stored text.
        await Reopen(executor, dbname);

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO t (id, a, b, c) VALUES (gen_id(), true, false, false)"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);

        // A row the grouped predicate accepts still inserts (a=false, b=true, c=true → true).
        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO t (id, a, b, c) VALUES (gen_id(), false, true, true)"));
    }

    [Test]
    public void RenderCheckCondition_ParenAndInList_RoundTripsToFixedPoint()
    {
        foreach (string cond in new[]
        {
            "(a OR b) AND c",
            "a OR b AND c",
            "x > 0 AND x < 1000",
            "NOT (a AND b)",
            "status IN (1, 2, 3)",
            "price > discounted_price",
        })
        {
            NodeAst ast = SQLParserProcessor.ParseCondition(cond);
            string rendered = CheckConditionRenderer.Render(ast);

            NodeAst ast2 = SQLParserProcessor.ParseCondition(rendered);
            string rendered2 = CheckConditionRenderer.Render(ast2);

            Assert.AreEqual(rendered, rendered2,
                $"render→parse→render must be a fixed point for '{cond}' (got '{rendered}' then '{rendered2}')");
        }
    }

    [Test]
    public async Task InListCheck_EnforcedAfterReopen()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE t (id object_id PRIMARY KEY, status int64 CHECK (status IN (1, 2, 3)))");

        await Reopen(executor, dbname);

        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname, "INSERT INTO t (id, status) VALUES (gen_id(), 2)"));

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, dbname, "INSERT INTO t (id, status) VALUES (gen_id(), 5)"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
    }

    // ── Per-operator NULL-passes, end-to-end through INSERT ───────────────────────────────────

    [TestCase("x >= 1")]
    [TestCase("x = 5")]
    [TestCase("x <> 5")]
    [TestCase("x < 5")]
    [TestCase("x <= 5")]
    [TestCase("x > 0 OR x < -10")]
    [TestCase("NOT (x > 0)")]
    [TestCase("x BETWEEN 1 AND 10")]
    public async Task NullReferencedColumn_Passes_PerOperator(string condition)
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            $"CREATE TABLE t (id object_id PRIMARY KEY, x int64 CHECK ({condition}))");

        // x omitted → NULL → the check is UNKNOWN, which passes per SQL three-valued logic.
        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname, "INSERT INTO t (id) VALUES (gen_id())"),
            $"NULL operand must make '{condition}' pass");
    }

    // ── Multi-column UPDATE: the price side too (not only discounted_price) ────────────────────

    [Test]
    public async Task Update_MultiColumnCheck_ViolatesOnPriceChange()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY, price int64, discounted_price int64, " +
            "CONSTRAINT valid_discount CHECK (price > discounted_price))");
        await ExecInsert(executor, dbname,
            "INSERT INTO products (id, price, discounted_price) VALUES (gen_id(), 100, 80)");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
        {
            DatabaseDescriptor db = await executor.OpenDatabase(dbname);
            KvTransaction tx = await db.Transactions.BeginAsync();
            await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname,
                "UPDATE products SET price = 50 WHERE discounted_price = 80", null));
            await db.Transactions.CommitAsync(tx);
        })!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
    }

    // ── Rejected ALTER leaves the schema unchanged (verified across reopen) ────────────────────

    [Test]
    public async Task AddCheck_RejectedByExistingRow_LeavesSchemaUnchanged()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE t (id object_id PRIMARY KEY, x int64)");
        await ExecInsert(executor, dbname, "INSERT INTO t (id, x) VALUES (gen_id(), -5)");

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, dbname, "ALTER TABLE t ADD CONSTRAINT positive_x CHECK (x > 0)"));

        DatabaseDescriptor db = await Reopen(executor, dbname);
        List<CheckConstraintSchema>? checks = db.Schema.Tables["t"].CheckConstraints;
        Assert.IsTrue(checks is null || checks.Count == 0,
            "A rejected ADD CONSTRAINT must not leave the check on the (reloaded) schema");

        // And enforcement must not be active: the previously-violating value can still be inserted.
        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname, "INSERT INTO t (id, x) VALUES (gen_id(), -9)"));
    }

    [Test]
    public async Task SetNotNull_RejectedByExistingNull_LeavesSchemaUnchanged()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE t (id object_id PRIMARY KEY, name string)");
        await ExecInsert(executor, dbname, "INSERT INTO t (id) VALUES (gen_id())"); // name = NULL

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, dbname, "ALTER TABLE t ALTER COLUMN name SET NOT NULL"));

        DatabaseDescriptor db = await Reopen(executor, dbname);
        TableColumnSchema nameCol = db.Schema.Tables["t"].Columns!.First(c => c.Name == "name");
        Assert.IsFalse(nameCol.NotNull, "A rejected SET NOT NULL must not flip the NOT NULL flag");
        Assert.IsNull(nameCol.NotNullConstraintName);

        // Enforcement inactive: a further NULL insert still succeeds.
        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname, "INSERT INTO t (id) VALUES (gen_id())"));
    }

    // ── NOT NULL naming and ALTER-added checks survive reopen (real disk round-trip) ──────────

    [Test]
    public async Task NamedNotNull_SurvivesReopen_AndRemainsDroppableByName()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY, " +
            "name string CONSTRAINT products_name_not_null NOT NULL)");

        DatabaseDescriptor db = await Reopen(executor, dbname);
        TableColumnSchema nameCol = db.Schema.Tables["products"].Columns!.First(c => c.Name == "name");
        Assert.IsTrue(nameCol.NotNull, "NOT NULL flag must survive reopen");
        Assert.AreEqual("products_name_not_null", nameCol.NotNullConstraintName,
            "The constraint name must survive reopen (not just the in-memory object)");

        // Droppable by its persisted name, after which a NULL insert succeeds.
        await ExecDDL(executor, dbname, "ALTER TABLE products DROP CONSTRAINT products_name_not_null");
        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname, "INSERT INTO products (id) VALUES (gen_id())"));
    }

    [Test]
    public async Task AlterAddedCheck_SurvivesReopen_AndStaysEnforced()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname, "CREATE TABLE items (id object_id PRIMARY KEY, qty int64)");
        await ExecDDL(executor, dbname, "ALTER TABLE items ADD CONSTRAINT positive_qty CHECK (qty > 0)");

        DatabaseDescriptor db = await Reopen(executor, dbname);
        CheckConstraintSchema cc = db.Schema.Tables["items"].CheckConstraints!.Single();
        Assert.AreEqual("positive_qty", cc.Name);
        Assert.IsNotNull(cc.ParsedCondition, "Re-parsed AST must be rebuilt on load so enforcement runs");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, dbname, "INSERT INTO items (id, qty) VALUES (gen_id(), -1)"))!;
        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
    }

    // ── Duplicate constraint names are rejected at CREATE ─────────────────────────────────────

    [Test]
    public async Task DuplicateConstraintNames_AtCreate_AreRejected()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, dbname,
                "CREATE TABLE t (id object_id PRIMARY KEY, x int64, y int64, " +
                "CONSTRAINT dup CHECK (x > 0), CONSTRAINT dup CHECK (y > 0))"))!;
        Assert.That(ex.Message, Does.Contain("Duplicate").Or.Contain("dup"));
    }

    [Test]
    public async Task UnnamedCheckCollidingWithUserName_AutoRenamesInsteadOfColliding()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        // A user-supplied name that matches an auto-name slot must not collide: the unnamed check
        // skips the taken name and lands on the next free slot.
        Assert.DoesNotThrowAsync(async () =>
            await ExecDDL(executor, dbname,
                "CREATE TABLE t (id object_id PRIMARY KEY, x int64, y int64, " +
                "CONSTRAINT t_check1 CHECK (y > 0), CHECK (x > 0))"));

        DatabaseDescriptor db = await Reopen(executor, dbname);
        List<CheckConstraintSchema> checks = db.Schema.Tables["t"].CheckConstraints!;
        Assert.AreEqual(2, checks.Count);
        Assert.AreEqual(2, checks.Select(c => c.Name).Distinct().Count(), "constraint names must be unique");
    }

    // ── Cluster forwarding DTOs carry checks + the NOT NULL target column (B1/B2 guard) ───────

    [Test]
    public void ForwardCreateTableRequest_RoundTrips_CheckConstraintsAndColumnMetadata()
    {
        JsonSerializerOptions opts = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
        };

        ForwardCreateTableRequest req = new()
        {
            DatabaseName = "db",
            TableName = "products",
            Columns =
            [
                new ColumnInfoRequest { Name = "name", Type = ColumnType.String, NotNull = true,
                    NotNullConstraintName = "products_name_not_null", DefaultFunction = "gen_uuid_v7" }
            ],
            CheckConstraints =
            [
                new CheckConstraintInfoRequest { Name = "positive_price", Expression = "price > 0",
                    ReferencedColumns = ["price"] }
            ],
        };

        ForwardCreateTableRequest round = JsonSerializer.Deserialize<ForwardCreateTableRequest>(
            JsonSerializer.Serialize(req, opts), opts)!;

        Assert.AreEqual(1, round.CheckConstraints.Length, "CHECK constraints must survive the wire round-trip");
        Assert.AreEqual("positive_price", round.CheckConstraints[0].Name);
        Assert.AreEqual("price > 0", round.CheckConstraints[0].Expression);
        Assert.AreEqual("products_name_not_null", round.Columns[0].NotNullConstraintName);
        Assert.AreEqual("gen_uuid_v7", round.Columns[0].DefaultFunction);
    }

    [Test]
    public void ForwardAlterConstraintRequest_RoundTrips_ColumnName()
    {
        JsonSerializerOptions opts = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
        };

        ForwardAlterConstraintRequest req = new()
        {
            DatabaseName = "db",
            TableName = "t",
            Operation = AlterConstraintOperation.SetNotNull,
            ColumnName = "email",
        };

        ForwardAlterConstraintRequest round = JsonSerializer.Deserialize<ForwardAlterConstraintRequest>(
            JsonSerializer.Serialize(req, opts), opts)!;

        Assert.AreEqual("email", round.ColumnName,
            "SET/DROP NOT NULL target column must survive the wire round-trip, else the leader can't resolve it");
        Assert.AreEqual(AlterConstraintOperation.SetNotNull, round.Operation);
    }
}
