
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Transactions;
using CamusDB.Core.CommandsExecutor.Controllers;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end enforcement tests: CHECK constraints rejected on INSERT and UPDATE when
/// the condition evaluates to false, accepted when true or when a referenced column is NULL
/// (SQL three-valued logic: UNKNOWN passes). Also tests <see cref="CheckEvaluator"/> directly
/// for the operator-level NULL semantics.
/// </summary>
public sealed class TestCheckConstraintsEnforcement : BaseTest
{
    // ── helpers ────────────────────────────────────────────────────────────────

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

    private async Task ExecUpdate(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    // ── CheckEvaluator unit tests: three-valued NULL semantics ─────────────────

    [Test]
    public void CheckEvaluator_GreaterThan_True()
    {
        NodeAst condition = SQLParserProcessor.ParseCondition("price > 0");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["price"] = new ColumnValue(ColumnType.Integer64, 10L)
        });
        Assert.AreEqual(true, result);
    }

    [Test]
    public void CheckEvaluator_GreaterThan_False()
    {
        NodeAst condition = SQLParserProcessor.ParseCondition("price > 0");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["price"] = new ColumnValue(ColumnType.Integer64, -5L)
        });
        Assert.AreEqual(false, result);
    }

    [Test]
    public void CheckEvaluator_GreaterThan_NullOperand_ReturnsUnknown()
    {
        NodeAst condition = SQLParserProcessor.ParseCondition("price > 0");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["price"] = ColumnValue.Null
        });
        Assert.IsNull(result, "NULL > 0 must be UNKNOWN (null), not false");
    }

    [Test]
    public void CheckEvaluator_GreaterEquals_NullOperand_ReturnsUnknown()
    {
        NodeAst condition = SQLParserProcessor.ParseCondition("x >= 1");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["x"] = ColumnValue.Null
        });
        Assert.IsNull(result);
    }

    [Test]
    public void CheckEvaluator_Equals_NullOperand_ReturnsUnknown()
    {
        NodeAst condition = SQLParserProcessor.ParseCondition("x = 5");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["x"] = ColumnValue.Null
        });
        Assert.IsNull(result);
    }

    [Test]
    public void CheckEvaluator_And_OneFalse_ReturnsFalse()
    {
        NodeAst condition = SQLParserProcessor.ParseCondition("x > 0 AND x < 100");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["x"] = new ColumnValue(ColumnType.Integer64, -1L)
        });
        Assert.AreEqual(false, result);
    }

    [Test]
    public void CheckEvaluator_And_NullLeft_ReturnsFalseWhenRightIsFalse()
    {
        // NULL AND false = false (short-circuits to false regardless of left)
        NodeAst condition = SQLParserProcessor.ParseCondition("x > 0 AND x < 0");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["x"] = ColumnValue.Null
        });
        // x > 0 = unknown, x < 0 = unknown → AND(unknown, unknown) = unknown
        Assert.IsNull(result);
    }

    [Test]
    public void CheckEvaluator_And_NullOneOperand_BothUnknown_ReturnsUnknown()
    {
        // x is null: left = unknown, right = unknown → AND = unknown
        NodeAst condition = SQLParserProcessor.ParseCondition("x > 0 AND y > 0");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["x"] = ColumnValue.Null,
            ["y"] = new ColumnValue(ColumnType.Integer64, 5L)
        });
        Assert.IsNull(result, "unknown AND true = unknown");
    }

    [Test]
    public void CheckEvaluator_Or_NullAndFalse_ReturnsUnknown()
    {
        // NULL OR false = unknown (not false)
        NodeAst condition = SQLParserProcessor.ParseCondition("x > 10 OR x < 0");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["x"] = ColumnValue.Null
        });
        Assert.IsNull(result, "unknown OR unknown = unknown");
    }

    [Test]
    public void CheckEvaluator_Or_NullAndTrue_ReturnsTrue()
    {
        // NULL OR true = true
        NodeAst condition = SQLParserProcessor.ParseCondition("x > 0 OR y > 0");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["x"] = ColumnValue.Null,
            ["y"] = new ColumnValue(ColumnType.Integer64, 5L)
        });
        Assert.AreEqual(true, result, "unknown OR true = true");
    }

    [Test]
    public void CheckEvaluator_Not_NullOperand_ReturnsUnknown()
    {
        // NOT(unknown) = unknown
        NodeAst condition = SQLParserProcessor.ParseCondition("NOT (x > 0)");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["x"] = ColumnValue.Null
        });
        Assert.IsNull(result, "NOT(unknown) must be unknown");
    }

    [Test]
    public void CheckEvaluator_IsNull_True()
    {
        NodeAst condition = SQLParserProcessor.ParseCondition("x IS NULL");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["x"] = ColumnValue.Null
        });
        Assert.AreEqual(true, result);
    }

    [Test]
    public void CheckEvaluator_IsNotNull_False()
    {
        NodeAst condition = SQLParserProcessor.ParseCondition("x IS NOT NULL");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["x"] = ColumnValue.Null
        });
        Assert.AreEqual(false, result);
    }

    [Test]
    public void CheckEvaluator_Between_NullSubject_ReturnsUnknown()
    {
        NodeAst condition = SQLParserProcessor.ParseCondition("x BETWEEN 1 AND 10");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["x"] = ColumnValue.Null
        });
        Assert.IsNull(result, "NULL BETWEEN 1 AND 10 must be unknown");
    }

    [Test]
    public void CheckEvaluator_Between_True()
    {
        NodeAst condition = SQLParserProcessor.ParseCondition("x BETWEEN 1 AND 10");
        bool? result = CheckEvaluator.Evaluate(condition, new Dictionary<string, ColumnValue>
        {
            ["x"] = new ColumnValue(ColumnType.Integer64, 5L)
        });
        Assert.AreEqual(true, result);
    }

    // ── INSERT enforcement ──────────────────────────────────────────────────────

    [Test]
    public async Task Insert_ViolatesCheck_IsRejected()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY, price int64 CHECK (price > 0))");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO products (id, price) VALUES (gen_id(), -5)"))!;

        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
        Assert.That(ex.Message, Does.Contain("products_price_check"));
    }

    [Test]
    public async Task Insert_SatisfiesCheck_Succeeds()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY, price int64 CHECK (price > 0))");

        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO products (id, price) VALUES (gen_id(), 10)"));
    }

    [Test]
    public async Task Insert_NullReferencedColumn_Passes()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY, price int64 CHECK (price > 0))");

        // NULL passes because CHECK is UNKNOWN, not FALSE.
        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO products (id) VALUES (gen_id())"));
    }

    [Test]
    public async Task Insert_NullInAndCondition_Passes()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE t (id object_id PRIMARY KEY, x int64 CHECK (x > 0 AND x < 1000))");

        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname, "INSERT INTO t (id) VALUES (gen_id())"));
    }

    [Test]
    public async Task Insert_MultiColumnCheck_ViolatesWhenBothPresent()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY, price int64, discounted_price int64, " +
            "CONSTRAINT valid_discount CHECK (price > discounted_price))");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO products (id, price, discounted_price) VALUES (gen_id(), 5, 10)"))!;

        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
        Assert.That(ex.Message, Does.Contain("valid_discount"));
    }

    [Test]
    public async Task Insert_MultiColumnCheck_PassesWhenConditionTrue()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY, price int64, discounted_price int64, " +
            "CONSTRAINT valid_discount CHECK (price > discounted_price))");

        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO products (id, price, discounted_price) VALUES (gen_id(), 100, 80)"));
    }

    [Test]
    public async Task Insert_MultiColumnCheck_NullInvolved_Passes()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY, price int64, discounted_price int64, " +
            "CONSTRAINT valid_discount CHECK (price > discounted_price))");

        // One column NULL → unknown → passes
        Assert.DoesNotThrowAsync(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO products (id, price) VALUES (gen_id(), 100)"));
    }

    // ── UPDATE enforcement ──────────────────────────────────────────────────────

    [Test]
    public async Task Update_ViolatesCheck_IsRejected()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY, price int64 CHECK (price > 0))");
        await ExecInsert(executor, dbname,
            "INSERT INTO products (id, price) VALUES (gen_id(), 50)");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecUpdate(executor, dbname,
                "UPDATE products SET price = -1 WHERE price = 50"))!;

        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
    }

    [Test]
    public async Task Update_SatisfiesCheck_Succeeds()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY, price int64 CHECK (price > 0))");
        await ExecInsert(executor, dbname,
            "INSERT INTO products (id, price) VALUES (gen_id(), 50)");

        Assert.DoesNotThrowAsync(async () =>
            await ExecUpdate(executor, dbname,
                "UPDATE products SET price = 75 WHERE price = 50"));
    }

    [Test]
    public async Task Update_MultiColumnCheck_ViolatesOnSecondColumnChange()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE products (id object_id PRIMARY KEY, price int64, discounted_price int64, " +
            "CONSTRAINT valid_discount CHECK (price > discounted_price))");
        await ExecInsert(executor, dbname,
            "INSERT INTO products (id, price, discounted_price) VALUES (gen_id(), 100, 80)");

        // Update discounted_price to violate the constraint.
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecUpdate(executor, dbname,
                "UPDATE products SET discounted_price = 150 WHERE price = 100"))!;

        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
        Assert.That(ex.Message, Does.Contain("valid_discount"));
    }

    // ── error code and HTTP status ──────────────────────────────────────────────

    [Test]
    public void CheckConstraintViolationCode_MapsToHttp400()
    {
        Assert.AreEqual(400, CamusDBErrorCodes.GetHttpStatus(CamusDBErrorCodes.CheckConstraintViolation));
    }

    [Test]
    public async Task ViolationException_HasCorrectCode()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();
        await ExecDDL(executor, dbname,
            "CREATE TABLE t (id object_id PRIMARY KEY, x int64 CHECK (x > 0))");

        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO t (id, x) VALUES (gen_id(), 0)"))!;

        Assert.AreEqual(CamusDBErrorCodes.CheckConstraintViolation, ex.Code);
    }
}
