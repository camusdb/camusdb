/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Extensions;

/// <summary>
/// Shared helpers for the extension tests. The fixture extensions are built by the test project's
/// <c>CopyExtensionFixtures</c> target into <c>extension-fixtures/&lt;Fixture&gt;/</c>; they are never
/// referenced as assemblies, so a test reaches them only through the engine's extension loader.
///
/// <para>The engine fixes its extension set when it is constructed, so a test passes the list in the
/// options it builds the engine with (<see cref="WithExtensions"/>), never afterwards.</para>
/// </summary>
public abstract class ExtensionTestBase : BaseTest
{
    /// <summary>The absolute path of a fixture DLL, for example <c>FixturePath("Fixture.Good")</c>.</summary>
    protected static string FixturePath(string fixture) =>
        Path.Combine(TestContext.CurrentContext.TestDirectory, "extension-fixtures", fixture, fixture + ".dll");

    /// <summary>The fixture options plus the given SAFE extensions.</summary>
    protected CamusDBOptions WithExtensions(params string[] fixtures) =>
        Options with { ExtensionAssemblies = fixtures.Select(FixturePath).ToList() };

    /// <summary>An engine and a database built with <c>Fixture.Good</c> loaded in SAFE mode.</summary>
    protected Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> CreateDatabaseWithGoodFixture() =>
        CreateDatabase(WithExtensions("Fixture.Good"));

    protected static async Task ExecDdl(CommandExecutor executor, string dbname, string sql)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname, sql, null));
        await db.Transactions.CommitAsync(tx);
    }

    protected static async Task<int> ExecNonQuery(
        CommandExecutor executor, string dbname, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();

        try
        {
            var result = await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
            await db.Transactions.CommitAsync(tx);
            return result.ModifiedRows;
        }
        catch
        {
            await db.Transactions.RollbackAsync(tx);
            throw;
        }
    }

    protected static async Task<List<Dictionary<string, ColumnValue>>> ExecQuery(
        CommandExecutor executor, string dbname, string sql, Dictionary<string, ColumnValue>? parameters = null)
    {
        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        KvTransaction tx = await db.Transactions.BeginAsync();

        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(new ExecuteSQLTicket(tx, dbname, sql, parameters));
        List<QueryResultRow> rows = await cursor.ToListAsync();
        await db.Transactions.CommitAsync(tx);

        return rows.Select(r => r.Row.ToDictionary(kv => kv.Key, kv => kv.Value)).ToList();
    }

    /// <summary>The single value of a one-row, one-column SELECT.</summary>
    protected static async Task<ColumnValue> Scalar(CommandExecutor executor, string dbname, string sql)
    {
        List<Dictionary<string, ColumnValue>> rows = await ExecQuery(executor, dbname, sql);
        Assert.AreEqual(1, rows.Count, sql);
        return rows[0].Values.Single();
    }
}
