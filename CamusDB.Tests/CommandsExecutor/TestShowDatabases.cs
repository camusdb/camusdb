
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Tests for SHOW DATABASES — lists all registered databases.
/// </summary>
[TestFixture]
internal sealed class TestShowDatabases : BaseTest
{
    private static async Task<List<string>> ShowDatabasesAsync(CommandExecutor executor, string contextDb)
    {
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(
                txnState: null!,
                database: contextDb,
                sql: "SHOW DATABASES",
                parameters: null));

        List<string> names = [];
        await foreach (QueryResultRow row in cursor)
            names.Add(row.Row["Database"].StrValue!);

        return names;
    }

    private async Task<(string name, CommandExecutor executor)> CreateTwoDatabases()
    {
        string nameA = Guid.NewGuid().ToString("n");
        string nameB = Guid.NewGuid().ToString("n");

        CommandExecutor executor = CreateCommandExecutor();

        await executor.CreateDatabase(new CreateDatabaseTicket(nameA, ifNotExists: false));
        await executor.CreateDatabase(new CreateDatabaseTicket(nameB, ifNotExists: false));

        TrackDatabase(nameA, executor);
        TrackDatabase(nameB, executor);

        return (nameA, executor);
    }

    [Test]
    public async Task ShowDatabases_ListsAllRegisteredDatabases()
    {
        string nameA = Guid.NewGuid().ToString("n");
        string nameB = Guid.NewGuid().ToString("n");

        CommandExecutor executor = CreateCommandExecutor();
        await executor.CreateDatabase(new CreateDatabaseTicket(nameA, ifNotExists: false));
        await executor.CreateDatabase(new CreateDatabaseTicket(nameB, ifNotExists: false));
        TrackDatabase(nameA, executor);
        TrackDatabase(nameB, executor);

        List<string> names = await ShowDatabasesAsync(executor, nameA);

        Assert.IsTrue(names.Contains(nameA), $"SHOW DATABASES must include '{nameA}'");
        Assert.IsTrue(names.Contains(nameB), $"SHOW DATABASES must include '{nameB}'");
    }

    [Test]
    public async Task ShowDatabases_WorksWithAnyContextDatabase()
    {
        string nameA = Guid.NewGuid().ToString("n");
        string nameB = Guid.NewGuid().ToString("n");

        CommandExecutor executor = CreateCommandExecutor();
        await executor.CreateDatabase(new CreateDatabaseTicket(nameA, ifNotExists: false));
        await executor.CreateDatabase(new CreateDatabaseTicket(nameB, ifNotExists: false));
        TrackDatabase(nameA, executor);
        TrackDatabase(nameB, executor);

        // Context db = nameB, but both databases must appear in the listing.
        List<string> names = await ShowDatabasesAsync(executor, nameB);

        Assert.IsTrue(names.Contains(nameA));
        Assert.IsTrue(names.Contains(nameB));
    }

    [Test]
    public async Task ShowDatabases_AfterDropReflectsRemovedDatabase()
    {
        string nameA = Guid.NewGuid().ToString("n");
        string nameB = Guid.NewGuid().ToString("n");

        CommandExecutor executor = CreateCommandExecutor();
        await executor.CreateDatabase(new CreateDatabaseTicket(nameA, ifNotExists: false));
        await executor.CreateDatabase(new CreateDatabaseTicket(nameB, ifNotExists: false));
        TrackDatabase(nameA, executor);

        await executor.DropDatabase(new DropDatabaseTicket(nameB, ifExists: false));

        List<string> names = await ShowDatabasesAsync(executor, nameA);

        Assert.IsTrue(names.Contains(nameA));
        Assert.IsFalse(names.Contains(nameB), $"Dropped database '{nameB}' must not appear in SHOW DATABASES");
    }

    [Test]
    public async Task ShowDatabases_AfterRenameReflectsNewName()
    {
        string nameA = Guid.NewGuid().ToString("n");

        CommandExecutor executor = CreateCommandExecutor();
        await executor.CreateDatabase(new CreateDatabaseTicket(nameA, ifNotExists: false));

        string newName = "renamed_" + nameA;
        await executor.RenameDatabase(new RenameDatabaseTicket(nameA, newName));
        TrackDatabase(newName, executor);

        List<string> names = await ShowDatabasesAsync(executor, newName);

        Assert.IsTrue(names.Contains(newName), $"Renamed database must appear as '{newName}'");
        Assert.IsFalse(names.Contains(nameA), $"Old name '{nameA}' must not appear after rename");
    }

    [Test]
    public async Task ShowDatabases_LikePrefix_FiltersResults()
    {
        CommandExecutor executor = CreateCommandExecutor();

        await executor.CreateDatabase(new CreateDatabaseTicket("prod_orders", ifNotExists: false));
        await executor.CreateDatabase(new CreateDatabaseTicket("prod_users", ifNotExists: false));
        await executor.CreateDatabase(new CreateDatabaseTicket("staging_orders", ifNotExists: false));
        TrackDatabase("prod_orders", executor);
        TrackDatabase("prod_users", executor);
        TrackDatabase("staging_orders", executor);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(null!, "prod_orders", "SHOW DATABASES LIKE 'prod_%'", null));

        List<string> names = [];
        await foreach (QueryResultRow row in cursor)
            names.Add(row.Row["Database"].StrValue!);

        Assert.IsTrue(names.Contains("prod_orders"));
        Assert.IsTrue(names.Contains("prod_users"));
        Assert.IsFalse(names.Contains("staging_orders"), "staging_ database must be excluded by LIKE 'prod_%'");
    }

    [Test]
    public async Task ShowDatabases_LikeNoMatch_ReturnsEmpty()
    {
        CommandExecutor executor = CreateCommandExecutor();

        await executor.CreateDatabase(new CreateDatabaseTicket("alpha", ifNotExists: false));
        TrackDatabase("alpha", executor);

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
            new ExecuteSQLTicket(null!, "alpha", "SHOW DATABASES LIKE 'zzz%'", null));

        List<string> names = [];
        await foreach (QueryResultRow row in cursor)
            names.Add(row.Row["Database"].StrValue!);

        Assert.AreEqual(0, names.Count, "LIKE pattern with no match must return empty result set");
    }
}
