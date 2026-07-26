
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// End-to-end coverage for the case-preserving, case-insensitive identifier behavior:
/// identifiers (database/table/column/index names) are stored in the exact case the user
/// wrote them, but every reference to them in SQL matches case-insensitively.
/// </summary>
[NonParallelizable]
public class TestCaseInsensitiveIdentifiers : SharedNodeBaseTest
{
    private async Task RunDdl(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private async Task RunNonQuery(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        await database.Transactions.CommitAsync(tx);
    }

    private async Task<List<QueryResultRow>> RunQuery(CommandExecutor executor, DatabaseDescriptor database, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        (DatabaseDescriptor _, var cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, null));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return rows;
    }

    [Test]
    public async Task CreateTable_PreservesIdentifierCaseInSchema()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await RunDdl(executor, database, dbname,
            "CREATE TABLE Robots (Id OID PRIMARY KEY NOT NULL, RobotName STRING NOT NULL, BuildYear INT64 NOT NULL)");

        // The table is looked up case-insensitively...
        TableSchema schema = executor.Catalogs.GetTableSchema(database, "robots");

        // ...but the stored name and column names preserve the exact case they were created with.
        Assert.AreEqual("Robots", schema.Name);
        CollectionAssert.AreEquivalent(
            new[] { "Id", "RobotName", "BuildYear" },
            schema.Columns!.Select(c => c.Name).ToArray());
    }

    [Test]
    public async Task Query_MatchesTableAndColumnsCaseInsensitively()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await RunDdl(executor, database, dbname,
            "CREATE TABLE Robots (Id OID PRIMARY KEY NOT NULL, RobotName STRING NOT NULL, BuildYear INT64 NOT NULL)");

        // INSERT referencing the columns in a different case than declared.
        await RunNonQuery(executor, database, dbname,
            "INSERT INTO robots (id, robotname, buildyear) VALUES (GEN_ID(), \"astro\", 3000)");

        // SELECT with a different case for the table, column, and WHERE column.
        List<QueryResultRow> rows = await RunQuery(executor, database, dbname,
            "SELECT ROBOTNAME, BUILDYEAR FROM ROBOTS WHERE robotName = \"astro\"");

        Assert.AreEqual(1, rows.Count);
        // The value is reachable regardless of the case used to look it up.
        Assert.AreEqual("astro", rows[0].Row["robotname"].StrValue);
        Assert.AreEqual("astro", rows[0].Row["RobotName"].StrValue);
        Assert.AreEqual(3000, rows[0].Row["buildyear"].LongValue);
    }

    [Test]
    public async Task Update_MatchesColumnCaseInsensitively()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await RunDdl(executor, database, dbname,
            "CREATE TABLE Robots (Id OID PRIMARY KEY NOT NULL, RobotName STRING NOT NULL, BuildYear INT64 NOT NULL)");
        await RunNonQuery(executor, database, dbname,
            "INSERT INTO Robots (Id, RobotName, BuildYear) VALUES (GEN_ID(), \"astro\", 3000)");

        // UPDATE using different case for both the WHERE and SET columns.
        await RunNonQuery(executor, database, dbname,
            "UPDATE robots SET BUILDYEAR = 4000 WHERE robotname = \"astro\"");

        List<QueryResultRow> rows = await RunQuery(executor, database, dbname,
            "SELECT BuildYear FROM Robots WHERE RobotName = \"astro\"");

        Assert.AreEqual(1, rows.Count);
        Assert.AreEqual(4000, rows[0].Row["buildyear"].LongValue,
            "UPDATE with a differently-cased SET column must overwrite the value, not silently drop it");
    }

    [Test]
    public async Task DuplicateColumnCaseVariant_IsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunDdl(executor, database, dbname,
                "CREATE TABLE Dupes (Id OID PRIMARY KEY NOT NULL, Foo STRING, foo INT64)"));

        Assert.AreEqual(CamusDBErrorCodes.DuplicateColumn, ex!.Code);
    }

    [Test]
    public async Task DuplicateTableCaseVariant_IsRejected()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await RunDdl(executor, database, dbname,
            "CREATE TABLE Widgets (Id OID PRIMARY KEY NOT NULL, Name STRING NOT NULL)");

        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await RunDdl(executor, database, dbname,
                "CREATE TABLE widgets (Id OID PRIMARY KEY NOT NULL, Name STRING NOT NULL)"));

        Assert.AreEqual(CamusDBErrorCodes.TableAlreadyExists, ex!.Code);
    }

    [Test]
    public async Task Index_CaseInsensitiveCreateReferenceAndDrop()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await RunDdl(executor, database, dbname,
            "CREATE TABLE Robots (Id OID PRIMARY KEY NOT NULL, RobotName STRING NOT NULL, BuildYear INT64 NOT NULL)");
        await RunDdl(executor, database, dbname,
            "CREATE INDEX YearIdx ON Robots (BuildYear)");

        TableSchema schema = executor.Catalogs.GetTableSchema(database, "robots");
        Assert.IsTrue(schema.Indexes!.Any(ix => ix.Name == "YearIdx"),
            "Index name must be stored in the case it was created with");

        // Drop referencing the index in a different case must succeed.
        await RunDdl(executor, database, dbname, "ALTER TABLE robots DROP INDEX yearidx");

        TableSchema afterDrop = executor.Catalogs.GetTableSchema(database, "robots");
        Assert.IsFalse(afterDrop.Indexes!.Any(ix => string.Equals(ix.Name, "YearIdx", System.StringComparison.OrdinalIgnoreCase)),
            "Index referenced case-insensitively must be dropped");
    }

    [Test]
    public async Task DatabaseName_IsCaseInsensitiveUniqueButCasePreserved()
    {
        string dbname = "MyCaseDb_" + System.Guid.NewGuid().ToString("n").Substring(0, 8);

        CommandExecutor executor = CreateCommandExecutor();
        DatabaseDescriptor database = await executor.CreateDatabase(new CreateDatabaseTicket(name: dbname, ifNotExists: false));
        TrackDatabase(dbname, executor);

        // The created database preserves the original case of its name.
        Assert.AreEqual(dbname, database.Name);

        // A differently-cased name refers to the same database — creating it must be rejected.
        CamusDBException? ex = Assert.ThrowsAsync<CamusDBException>(async () =>
            await executor.CreateDatabase(new CreateDatabaseTicket(name: dbname.ToLowerInvariant(), ifNotExists: false)));

        Assert.AreEqual(CamusDBErrorCodes.DatabaseAlreadyExists, ex!.Code);

        // And it can be opened using a different case than it was created with — resolving to the
        // same underlying database (same id).
        DatabaseDescriptor reopened = await executor.OpenDatabase(dbname.ToUpperInvariant());
        Assert.AreEqual(database.Id, reopened.Id,
            "Opening by a different case must resolve to the same database");
    }
}
