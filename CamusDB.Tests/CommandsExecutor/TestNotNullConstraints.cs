
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
/// End-to-end tests for named NOT NULL constraints:
/// CONSTRAINT name NOT NULL in CREATE TABLE, ALTER TABLE ALTER COLUMN SET/DROP NOT NULL,
/// and DROP CONSTRAINT resolving named NOT NULL constraints.
/// </summary>
[NonParallelizable]
public sealed class TestNotNullConstraints : BaseTest
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

    // ── named CONSTRAINT name NOT NULL at CREATE TABLE time ────────────────────

    [Test]
    public async Task TestNamedNotNullInCreateTable()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE employees (id object_id PRIMARY KEY, name STRING CONSTRAINT employees_name_not_null NOT NULL)");

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        TableSchema? schema = db.Schema.Tables.GetValueOrDefault("employees");
        Assert.IsNotNull(schema);

        TableColumnSchema? nameCol = schema!.Columns?.FirstOrDefault(c => c.Name == "name");
        Assert.IsNotNull(nameCol);
        Assert.IsTrue(nameCol!.NotNull);
        Assert.AreEqual("employees_name_not_null", nameCol.NotNullConstraintName);
    }

    [Test]
    public async Task TestNamedNotNullEnforcedOnInsert()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE employees (id object_id PRIMARY KEY, name STRING CONSTRAINT employees_name_not_null NOT NULL)");

        // null insert should fail
        CamusDBException ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO employees (id, name) VALUES (gen_id(), null)")
        )!;
        Assert.IsNotNull(ex);
    }

    // ── ALTER TABLE ALTER COLUMN SET NOT NULL ───────────────────────────────────

    [Test]
    public async Task TestSetNotNullOnEmptyTable()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64)");

        await ExecDDL(executor, dbname,
            "ALTER TABLE items ALTER COLUMN price SET NOT NULL");

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        TableSchema? schema = db.Schema.Tables.GetValueOrDefault("items");
        Assert.IsNotNull(schema);

        TableColumnSchema? priceCol = schema!.Columns?.FirstOrDefault(c => c.Name == "price");
        Assert.IsNotNull(priceCol);
        Assert.IsTrue(priceCol!.NotNull);
        Assert.AreEqual("items_price_not_null", priceCol.NotNullConstraintName);
    }

    [Test]
    public async Task TestSetNotNullOnPopulatedTableAllRowsHaveValue()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64)");

        await ExecInsert(executor, dbname,
            "INSERT INTO items (id, price) VALUES (gen_id(), 10)");
        await ExecInsert(executor, dbname,
            "INSERT INTO items (id, price) VALUES (gen_id(), 20)");

        await ExecDDL(executor, dbname,
            "ALTER TABLE items ALTER price SET NOT NULL");

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        TableSchema? schema = db.Schema.Tables.GetValueOrDefault("items");
        TableColumnSchema? priceCol = schema?.Columns?.FirstOrDefault(c => c.Name == "price");
        Assert.IsTrue(priceCol?.NotNull);
    }

    [Test]
    public async Task TestSetNotNullRejectedWhenRowsHaveNull()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64)");

        // Insert a row with NULL price.
        await ExecInsert(executor, dbname,
            "INSERT INTO items (id) VALUES (gen_id())");

        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecDDL(executor, dbname,
                "ALTER TABLE items ALTER COLUMN price SET NOT NULL")
        );
    }

    [Test]
    public async Task TestSetNotNullEnforcedAfterAlter()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64)");

        await ExecDDL(executor, dbname,
            "ALTER TABLE items ALTER COLUMN price SET NOT NULL");

        // NULL insert now rejected.
        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO items (id) VALUES (gen_id())")
        );
    }

    // ── ALTER TABLE ALTER COLUMN DROP NOT NULL ──────────────────────────────────

    [Test]
    public async Task TestDropNotNullAllowsNullInsert()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64 NOT NULL)");

        await ExecDDL(executor, dbname,
            "ALTER TABLE items ALTER COLUMN price DROP NOT NULL");

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        TableSchema? schema = db.Schema.Tables.GetValueOrDefault("items");
        TableColumnSchema? priceCol = schema?.Columns?.FirstOrDefault(c => c.Name == "price");
        Assert.IsFalse(priceCol?.NotNull);
        Assert.IsNull(priceCol?.NotNullConstraintName);

        // NULL insert now allowed.
        await ExecInsert(executor, dbname,
            "INSERT INTO items (id) VALUES (gen_id())");

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await ExecQuery(executor, dbname,
            "SELECT id FROM items");
        Assert.AreEqual(1, rows.Count);
    }

    // ── DROP CONSTRAINT resolving a named NOT NULL ──────────────────────────────

    [Test]
    public async Task TestDropConstraintRemovesNamedNotNull()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE employees (id object_id PRIMARY KEY, name STRING CONSTRAINT employees_name_not_null NOT NULL)");

        await ExecDDL(executor, dbname,
            "ALTER TABLE employees DROP CONSTRAINT employees_name_not_null");

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        TableSchema? schema = db.Schema.Tables.GetValueOrDefault("employees");
        TableColumnSchema? nameCol = schema?.Columns?.FirstOrDefault(c => c.Name == "name");
        Assert.IsFalse(nameCol?.NotNull);
        Assert.IsNull(nameCol?.NotNullConstraintName);
    }

    [Test]
    public async Task TestDropConstraintByAutoName()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64)");

        await ExecDDL(executor, dbname,
            "ALTER TABLE items ALTER COLUMN price SET NOT NULL");

        // Auto-name is {table}_{col}_not_null
        await ExecDDL(executor, dbname,
            "ALTER TABLE items DROP CONSTRAINT items_price_not_null");

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        TableSchema? schema = db.Schema.Tables.GetValueOrDefault("items");
        TableColumnSchema? priceCol = schema?.Columns?.FirstOrDefault(c => c.Name == "price");
        Assert.IsFalse(priceCol?.NotNull);
    }

    [Test]
    public async Task TestDropConstraintNamedNotNullAllowsNullInsert()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE employees (id object_id PRIMARY KEY, name STRING CONSTRAINT nn_name NOT NULL)");

        // Insert fails while NOT NULL active.
        Assert.ThrowsAsync<CamusDBException>(async () =>
            await ExecInsert(executor, dbname,
                "INSERT INTO employees (id, name) VALUES (gen_id(), null)")
        );

        await ExecDDL(executor, dbname,
            "ALTER TABLE employees DROP CONSTRAINT nn_name");

        // Insert now allowed after drop.
        await ExecInsert(executor, dbname,
            "INSERT INTO employees (id) VALUES (gen_id())");

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await ExecQuery(executor, dbname,
            "SELECT id FROM employees");
        Assert.AreEqual(1, rows.Count);
    }

    // ── SHOW CREATE TABLE renders named NOT NULL ────────────────────────────────

    [Test]
    public async Task TestShowCreateTableRendersNamedNotNull()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE employees (id object_id PRIMARY KEY, name STRING CONSTRAINT employees_name_not_null NOT NULL)");

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await ExecQuery(executor, dbname,
            "SHOW CREATE TABLE employees");

        Assert.AreEqual(1, rows.Count);
        string ddl = rows[0]["Create Table"].StrValue!;
        Assert.IsTrue(ddl.Contains("CONSTRAINT `employees_name_not_null` NOT NULL"),
            $"Expected named NOT NULL in: {ddl}");
    }

    [Test]
    public async Task TestShowCreateTableRendersSetNotNull()
    {
        (string dbname, _, CommandExecutor executor) = await CreateDatabase();

        await ExecDDL(executor, dbname,
            "CREATE TABLE items (id object_id PRIMARY KEY, price int64)");

        await ExecDDL(executor, dbname,
            "ALTER TABLE items ALTER COLUMN price SET NOT NULL");

        List<IReadOnlyDictionary<string, ColumnValue>> rows = await ExecQuery(executor, dbname,
            "SHOW CREATE TABLE items");

        Assert.AreEqual(1, rows.Count);
        string ddl = rows[0]["Create Table"].StrValue!;
        Assert.IsTrue(ddl.Contains("CONSTRAINT `items_price_not_null` NOT NULL"),
            $"Expected auto-named NOT NULL in: {ddl}");
    }
}
