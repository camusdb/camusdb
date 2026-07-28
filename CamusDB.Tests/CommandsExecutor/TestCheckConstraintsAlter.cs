
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
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end tests for ALTER TABLE ADD CONSTRAINT CHECK / DROP CONSTRAINT.
/// Covers: adding a CHECK to an empty table, adding a CHECK to a populated table where all
/// rows satisfy the expression, rejecting an ADD when existing rows violate the new constraint,
/// dropping a named constraint, and enforcement after add.
/// </summary>
public sealed class TestCheckConstraintsAlter : BaseTest
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

    private async Task<List<IReadOnlyDictionary<string, ColumnValue>>> ExecQuery(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        (DatabaseDescriptor _, IAsyncEnumerable<QueryResultRow> rows) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(tx, dbname, sql, null));
        List<IReadOnlyDictionary<string, ColumnValue>> result = [];
        await foreach (QueryResultRow row in rows)
            result.Add(row.Row);
        await db.Transactions.CommitAsync(tx);
        return result;
    }

    // ── ADD CONSTRAINT CHECK on empty table ─────────────────────────────────────

    [Test]
    public async Task TestAddCheckConstraintEmptyTable()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64, name string)");

        await ExecDDL(executor, dbname,
            "ALTER TABLE items ADD CONSTRAINT chk_price CHECK (price > 0)");

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        TableSchema? schema = db.Schema.Tables.GetValueOrDefault("items");
        Assert.IsNotNull(schema);
        Assert.IsNotNull(schema!.CheckConstraints);
        Assert.AreEqual(1, schema.CheckConstraints!.Count);
        Assert.AreEqual("chk_price", schema.CheckConstraints[0].Name);
        Assert.AreEqual("price > 0", schema.CheckConstraints[0].Expression);
        Assert.Contains("price", schema.CheckConstraints[0].ReferencedColumns);
    }

    [Test]
    public async Task TestAddCheckConstraintEnforcedOnSubsequentInsert()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64, name string)");

        await ExecDDL(executor, dbname,
            "ALTER TABLE items ADD CONSTRAINT chk_price CHECK (price > 0)");

        // valid insert passes
        await ExecInsert(executor, dbname,
            "INSERT INTO items (id, price, name) VALUES (gen_id(), 10, 'widget')");

        // invalid insert is rejected
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO items (id, price, name) VALUES (gen_id(), -1, 'bad')")
        )!;
        Assert.IsTrue(ex.Message.Contains("chk_price") || ex.Message.Contains("CHECK"));
    }

    // ── ADD CONSTRAINT CHECK on populated table ─────────────────────────────────

    [Test]
    public async Task TestAddCheckConstraintPopulatedTableAllRowsSatisfy()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64, name string)");

        await ExecInsert(executor, dbname,
            "INSERT INTO items (id, price, name) VALUES (gen_id(), 5, 'a')");
        await ExecInsert(executor, dbname,
            "INSERT INTO items (id, price, name) VALUES (gen_id(), 10, 'b')");

        // All rows have price > 0, so ADD should succeed
        await ExecDDL(executor, dbname,
            "ALTER TABLE items ADD CONSTRAINT chk_price CHECK (price > 0)");

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        TableSchema? schema = db.Schema.Tables.GetValueOrDefault("items");
        Assert.IsNotNull(schema);
        Assert.AreEqual(1, schema!.CheckConstraints?.Count ?? 0);
    }

    [Test]
    public async Task TestAddCheckConstraintPopulatedTableViolatingRowRejected()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64, name string)");

        await ExecInsert(executor, dbname,
            "INSERT INTO items (id, price, name) VALUES (gen_id(), 5, 'ok')");
        await ExecInsert(executor, dbname,
            "INSERT INTO items (id, price, name) VALUES (gen_id(), -3, 'bad')");

        // One row has price < 0, so ADD must fail
        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, dbname,
                "ALTER TABLE items ADD CONSTRAINT chk_price CHECK (price > 0)")
        );
    }

    // ── DROP CONSTRAINT ──────────────────────────────────────────────────────────

    [Test]
    public async Task TestDropCheckConstraint()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64, name string)");
        await ExecDDL(executor, dbname,
            "ALTER TABLE items ADD CONSTRAINT chk_price CHECK (price > 0)");
        await ExecDDL(executor, dbname,
            "ALTER TABLE items DROP CONSTRAINT chk_price");

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        TableSchema? schema = db.Schema.Tables.GetValueOrDefault("items");
        Assert.IsNotNull(schema);
        bool constraintPresent = schema!.CheckConstraints?.Any(c => c.Name == "chk_price") == true;
        Assert.IsFalse(constraintPresent);
    }

    [Test]
    public async Task TestDropCheckConstraintAllowsPreviouslyViolatingInsert()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64, name string)");
        await ExecDDL(executor, dbname,
            "ALTER TABLE items ADD CONSTRAINT chk_price CHECK (price > 0)");

        // insert fails while constraint is active
        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO items (id, price, name) VALUES (gen_id(), -1, 'bad')")
        );

        await ExecDDL(executor, dbname,
            "ALTER TABLE items DROP CONSTRAINT chk_price");

        // after drop the same insert must succeed
        await ExecInsert(executor, dbname,
            "INSERT INTO items (id, price, name) VALUES (gen_id(), -1, 'now ok')");

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await ExecQuery(executor, dbname,
            "SELECT price FROM items WHERE price < 0");
        Assert.AreEqual(1, rows.Count);
    }

    // ── error cases ──────────────────────────────────────────────────────────────

    [Test]
    public async Task TestAddDuplicateConstraintNameRejected()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64, name string)");
        await ExecDDL(executor, dbname,
            "ALTER TABLE items ADD CONSTRAINT chk_price CHECK (price > 0)");

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, dbname,
                "ALTER TABLE items ADD CONSTRAINT chk_price CHECK (price < 1000)")
        );
    }

    [Test]
    public async Task TestDropNonExistentConstraintRejected()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64, name string)");

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, dbname,
                "ALTER TABLE items DROP CONSTRAINT no_such_constraint")
        );
    }

    [Test]
    public async Task TestAddCheckReferencingUnknownColumnRejected()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64, name string)");

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, dbname,
                "ALTER TABLE items ADD CONSTRAINT chk_bad CHECK (nonexistent_col > 0)")
        );
    }

    // ── multiple constraints ──────────────────────────────────────────────────────

    [Test]
    public async Task TestMultipleCheckConstraintsAddAndDrop()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64, qty int64, name string)");
        await ExecDDL(executor, dbname,
            "ALTER TABLE items ADD CONSTRAINT chk_price CHECK (price > 0)");
        await ExecDDL(executor, dbname,
            "ALTER TABLE items ADD CONSTRAINT chk_qty CHECK (qty >= 0)");

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        TableSchema? schema = db.Schema.Tables.GetValueOrDefault("items");
        Assert.AreEqual(2, schema!.CheckConstraints?.Count ?? 0);

        await ExecDDL(executor, dbname,
            "ALTER TABLE items DROP CONSTRAINT chk_price");

        db = await executor.OpenDatabase(dbname);
        schema = db.Schema.Tables.GetValueOrDefault("items");
        Assert.AreEqual(1, schema!.CheckConstraints?.Count ?? 0);
        Assert.AreEqual("chk_qty", schema.CheckConstraints![0].Name);
    }
}
