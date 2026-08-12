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
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A view records the individual table columns its body reads, so a column change that would break
/// it is refused at the moment it is issued rather than surfacing as a failed read later.
///
/// <para>These drive real SQL, because the property under test is what <c>ALTER TABLE</c> does — a
/// test that inspected the recorded id list would pass while the statement it is supposed to block
/// went through.</para>
/// </summary>
[NonParallelizable]
public sealed class TestViewColumnDependencies : SharedNodeBaseTest
{
    private static async Task ExecDdl(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task ExecNonQuery(DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        _ = await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<List<QueryResultRow>> ExecQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    private static async Task SeedOrders(DatabaseDescriptor database, CommandExecutor executor, string dbname)
    {
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE orders (id int64 PRIMARY KEY, customer string(64), total int64, status string(16), note string(64))");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO orders (id, customer, total, status, note) VALUES " +
            "(1, 'acme', 10, 'open', 'a'), (2, 'acme', 20, 'open', 'b'), (3, 'globex', 30, 'closed', 'c')");
    }

    private static async Task<CamusDBException?> CaptureError(Func<Task> action)
    {
        try
        {
            await action();
            return null;
        }
        catch (CamusDBException error)
        {
            return error;
        }
    }

    [Test]
    public async Task DroppingAColumnAViewReadsIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        CamusDBException? error = await CaptureError(() =>
            ExecDdl(database, executor, dbname, "ALTER TABLE orders DROP COLUMN total"));

        Assert.IsNotNull(error, "dropping a column a view projects must not succeed silently");
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, error!.Code);
        StringAssert.Contains("open_orders", error.Message);

        // And the refusal must have changed nothing.
        Assert.AreEqual(2, (await ExecQuery(database, executor, dbname, "SELECT id, total FROM open_orders")).Count);
    }

    /// <summary>
    /// A column read only by the predicate is read just as much as one that is projected — this is
    /// the case a "which columns does it output" analysis would miss.
    /// </summary>
    [Test]
    public async Task DroppingAColumnUsedOnlyInThePredicateIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        CamusDBException? error = await CaptureError(() =>
            ExecDdl(database, executor, dbname, "ALTER TABLE orders DROP COLUMN status"));

        Assert.IsNotNull(error, "the WHERE clause reads the column just as the projection does");
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, error!.Code);
    }

    /// <summary>
    /// A body binds its columns by immutable id, so renaming one is invisible to the view — the same
    /// treatment a table rename already gets. The view's own output name is frozen at creation and
    /// does not follow the base column.
    /// </summary>
    [Test]
    public async Task RenamingAColumnAViewReadsIsTransparent()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        string bodyBeforeRename = database.Schema.Views["open_orders"].Definition!.Sql;

        await ExecDdl(database, executor, dbname, "ALTER TABLE orders RENAME COLUMN total TO amount");

        Assert.AreEqual(bodyBeforeRename, database.Schema.Views["open_orders"].Definition!.Sql,
            "a rename must not touch a body that refers to the column by id");

        Assert.AreEqual(2, (await ExecQuery(database, executor, dbname, "SELECT id, total FROM open_orders")).Count,
            "the view keeps publishing the column name it froze at creation");

        List<QueryResultRow> shown = await ExecQuery(database, executor, dbname, "SHOW CREATE VIEW open_orders");
        StringAssert.Contains("amount", shown[0].Row["create view"].StrValue!,
            "the definition must render the name the column answers to now");
    }

    /// <summary>
    /// A definition written before columns were bound by id still spells the column out, so renaming
    /// it would strand the body — that one is still refused.
    /// </summary>
    [Test]
    public async Task RenamingAColumnANameBoundBodyReadsIsStillRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        // Downgraded to the pre-id form, which is what an older version left on disk. The recorded
        // column ids stay as they are — only the body text loses the binding.
        database.Schema.Views["open_orders"].Definition!.Sql =
            "SELECT id, total FROM orders WHERE status = 'open'";

        CamusDBException? error = await CaptureError(() =>
            ExecDdl(database, executor, dbname, "ALTER TABLE orders RENAME COLUMN total TO amount"));

        Assert.IsNotNull(error, "a body that still names the column in text would be stranded by the rename");
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, error!.Code);
    }

    /// <summary>
    /// The check has to be narrow, or it blocks work the user is entitled to do. A column no view
    /// reads stays droppable and renameable.
    /// </summary>
    [Test]
    public async Task AColumnNoViewReadsIsStillDroppableAndRenameable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        Assert.DoesNotThrowAsync(
            async () => await ExecDdl(database, executor, dbname, "ALTER TABLE orders RENAME COLUMN note TO remark"),
            "no view reads this column, so renaming it must not be refused");

        Assert.DoesNotThrowAsync(
            async () => await ExecDdl(database, executor, dbname, "ALTER TABLE orders DROP COLUMN remark"),
            "no view reads this column, so dropping it must not be refused");
    }

    /// <summary>
    /// With two relations in scope, the analysis must attribute each qualified reference to the
    /// right one — recording the column against both would refuse DDL over a column nothing reads.
    /// </summary>
    [Test]
    public async Task AQualifiedReferenceIsAttributedToTheRelationItNames()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE customers (name string(64) PRIMARY KEY, region string(16), total int64)");

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW eu_orders AS SELECT o.id AS id, o.total AS total FROM orders o " +
            "INNER JOIN customers c ON o.customer = c.name WHERE c.region = 'eu'");

        CamusDBException? refused = await CaptureError(() =>
            ExecDdl(database, executor, dbname, "ALTER TABLE orders DROP COLUMN total"));

        Assert.IsNotNull(refused, "orders.total is read by the view");

        // customers.total shares the name but is read by nobody, so it must remain droppable.
        Assert.DoesNotThrowAsync(
            async () => await ExecDdl(database, executor, dbname, "ALTER TABLE customers DROP COLUMN total"),
            "a same-named column on another relation must not be caught by the check");
    }

    [Test]
    public async Task AColumnReadOnlyInsideASubqueryIsTracked()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE customers (name string(64) PRIMARY KEY, region string(16))");

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW eu_orders AS SELECT id, total FROM orders " +
            "WHERE customer IN (SELECT name FROM customers WHERE region = 'eu')");

        CamusDBException? error = await CaptureError(() =>
            ExecDdl(database, executor, dbname, "ALTER TABLE customers DROP COLUMN region"));

        Assert.IsNotNull(error, "a column read inside a subquery is still read by the view");
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, error!.Code);
    }

    [Test]
    public async Task CreatingAViewStoresItsColumnsByIdAndKeepsAnyQualifier()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT o.id AS id, o.total AS total FROM orders o WHERE o.status = 'open'");

        string body = database.Schema.Views["open_orders"].Definition!.Sql;
        string totalId = database.Schema.Tables["orders"].Columns!
            .Single(c => string.Equals(c.Name, "total", StringComparison.OrdinalIgnoreCase)).Id;

        // Qualified references keep their qualifier: two relations in a join can expose the same
        // column name, so an unqualified rendering could come back as SQL meaning something else.
        StringAssert.Contains("o." + StoredColumnRef.Format(totalId), body);
    }

    /// <summary>
    /// <c>ORDER BY</c> may name one of the select's own output columns rather than a relation's, so
    /// a reference there that matches an output name is deliberately left unbound — and that in turn
    /// keeps the rename refusal in force for it, rather than silently rebinding an alias.
    /// </summary>
    [Test]
    public async Task AnOutputNameUsedInOrderByIsNotBoundToABaseColumn()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW ranked AS SELECT id AS id, customer AS total FROM orders ORDER BY total");

        List<QueryResultRow> rows = await ExecQuery(database, executor, dbname, "SELECT id, total FROM ranked");
        Assert.AreEqual(3, rows.Count, "the view must still read correctly");

        CamusDBException? error = await CaptureError(() =>
            ExecDdl(database, executor, dbname, "ALTER TABLE orders RENAME COLUMN total TO amount"));

        Assert.IsNotNull(error,
            "the ORDER BY reference was left as a name, so the rename must still be refused");
    }

    [Test]
    public async Task AReservedColumnPrefixIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        List<string> accepted = [];

        foreach (string sql in new[]
                 {
                     $"CREATE TABLE shadowed (id int64 PRIMARY KEY, {StoredColumnRef.Prefix}x int64)",
                     $"ALTER TABLE orders ADD COLUMN {StoredColumnRef.Prefix}y int64",
                     $"ALTER TABLE orders RENAME COLUMN note TO {StoredColumnRef.Prefix}z",
                 })
        {
            CamusDBException? error = await CaptureError(() => ExecDdl(database, executor, dbname, sql));

            if (error is null)
                accepted.Add(sql);
            else
                Assert.AreEqual(CamusDBErrorCodes.InvalidInput, error.Code, $"wrong error for: {sql}");
        }

        CollectionAssert.IsEmpty(accepted,
            "a column under the reserved prefix shadows what a stored body refers to");
    }

    [Test]
    public async Task DroppingTheViewReleasesItsColumns()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        Assert.IsNotNull(await CaptureError(() =>
            ExecDdl(database, executor, dbname, "ALTER TABLE orders DROP COLUMN total")));

        await ExecDdl(database, executor, dbname, "DROP VIEW open_orders");

        Assert.DoesNotThrowAsync(
            async () => await ExecDdl(database, executor, dbname, "ALTER TABLE orders DROP COLUMN total"),
            "with the view gone there is nothing left depending on the column");
    }

    /// <summary>
    /// A materialized view's own body reads base columns too. Its definition lives on the relation
    /// rather than in the view map, so a check that only walked the view map would miss it — and the
    /// column would be dropped out from under the next refresh.
    /// </summary>
    [Test]
    public async Task DroppingAColumnAMaterializedViewsBodyReadsIsRefused()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");

        CamusDBException? error = await CaptureError(() =>
            ExecDdl(database, executor, dbname, "ALTER TABLE orders DROP COLUMN total"));

        Assert.IsNotNull(error, "the materialized view's body reads this column on every refresh");
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, error!.Code);
        StringAssert.Contains("open_orders", error.Message);
    }

    /// <summary>
    /// A materialized view is a stored relation with droppable columns of its own, and a plain view
    /// reading one has the same stake in them surviving.
    /// </summary>
    [Test]
    public async Task AColumnOfAMaterializedViewAViewReadsIsProtected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await SeedOrders(database, executor, dbname);

        await ExecDdl(database, executor, dbname,
            "CREATE MATERIALIZED VIEW open_orders AS SELECT id, total FROM orders WHERE status = 'open'");
        await ExecDdl(database, executor, dbname,
            "CREATE VIEW big_open_orders AS SELECT id FROM open_orders WHERE total > 10");

        CamusDBException? error = await CaptureError(() =>
            ExecDdl(database, executor, dbname, "ALTER TABLE open_orders DROP COLUMN total"));

        Assert.IsNotNull(error, "the plain view reads that column of the materialized view");
        Assert.AreEqual(CamusDBErrorCodes.DependentObjectsExist, error!.Code);
    }
}
