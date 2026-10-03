/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// A transaction that writes a row before a staged schema change and commits after the change has
/// read the table. Neither the index backfill nor the foreign-key validation pass sees a write that is
/// not committed yet, and the transaction planned its writes against the schema it started with, so it
/// writes no entry for the new index and runs no check for the new constraint.
///
/// <para><b>Both tests show a known gap and are ignored until it is closed.</b> The fix needs the
/// engine to refuse such a commit, or to make the change wait for every transaction that started
/// before its WriteOnly version. Each test asserts the correct outcome, so it passes once the gap is
/// closed and can be enabled then.</para>
/// </summary>
internal static class SchemaChangeSpanningTransactionScenarios
{
    /// <summary>Why both tests are ignored. A constant, because an attribute argument must be one.</summary>
    internal const string KnownGap =
        "Known gap: a transaction that writes before a staged schema change and commits after its backfill " +
        "or validation is not seen by either. Enable when commits are fenced against the schema version.";

    /// <summary>The orphan must not survive under a Public constraint.</summary>
    public static async Task OrphanCommittedAfterTheValidationIsCaught(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ForeignKeyAlterScenarios.Seed(executor, dbname, withUserIndex: true);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO weather (id, city) VALUES (60, 'atlantis')", null));

        Exception? alterError = await Capture(ForeignKeyAlterScenarios.Ddl(executor, dbname, ForeignKeyAlterScenarios.AddConstraint));
        Exception? commitError = await Capture(database.Transactions.CommitAsync(tx));

        List<long> orphans = await ForeignKeyAlterScenarios.Orphans(executor, dbname);
        bool constraintExists = database.Schema.Tables["weather"].ForeignKeys is { Count: > 0 };

        Assert.IsFalse(constraintExists && orphans.Count > 0,
            $"A Public constraint must not hold over an orphan. ALTER: {alterError?.Message ?? "ok"}; commit: {commitError?.Message ?? "ok"}; orphans: {string.Join(",", orphans)}");
    }

    /// <summary>The new index must hold an entry for every committed row.</summary>
    public static async Task RowCommittedAfterTheIndexBackfillIsIndexed(CommandExecutor executor, DatabaseDescriptor database, string dbname)
    {
        await ForeignKeyAlterScenarios.Seed(executor, dbname, withUserIndex: false);

        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteNonSQLQuery(new ExecuteSQLTicket(tx, dbname, "INSERT INTO weather (id, city) VALUES (60, 'atlantis')", null));

        Exception? indexError = await Capture(ForeignKeyAlterScenarios.Ddl(executor, dbname, "CREATE INDEX weather_city_late ON weather (city)"));
        Exception? commitError = await Capture(database.Transactions.CommitAsync(tx));

        if (indexError is not null || commitError is not null)
            return;

        List<QueryResultRow> rows = await ForeignKeyAlterScenarios.Query(executor, dbname, "SELECT id FROM weather WHERE city = 'atlantis'");

        Assert.AreEqual(1, rows.Count, "The committed row must be found through the new index");
    }

    private static async Task<Exception?> Capture(Task task)
    {
        try
        {
            await task;
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }
}

[TestFixture]
[NonParallelizable]
public sealed class TestSchemaChangeSpanningTransactions : BaseTest
{
    [Test, Ignore(SchemaChangeSpanningTransactionScenarios.KnownGap)]
    public async Task OrphanCommittedAfterTheValidationIsCaught() => await Run(SchemaChangeSpanningTransactionScenarios.OrphanCommittedAfterTheValidationIsCaught);

    [Test, Ignore(SchemaChangeSpanningTransactionScenarios.KnownGap)]
    public async Task RowCommittedAfterTheIndexBackfillIsIndexed() => await Run(SchemaChangeSpanningTransactionScenarios.RowCommittedAfterTheIndexBackfillIsIndexed);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}

[TestFixture]
[NonParallelizable]
public sealed class TestSchemaChangeSpanningTransactionsCluster : SharedNodeBaseTest
{
    [Test, Ignore(SchemaChangeSpanningTransactionScenarios.KnownGap)]
    public async Task OrphanCommittedAfterTheValidationIsCaught() => await Run(SchemaChangeSpanningTransactionScenarios.OrphanCommittedAfterTheValidationIsCaught);

    [Test, Ignore(SchemaChangeSpanningTransactionScenarios.KnownGap)]
    public async Task RowCommittedAfterTheIndexBackfillIsIndexed() => await Run(SchemaChangeSpanningTransactionScenarios.RowCommittedAfterTheIndexBackfillIsIndexed);

    private async Task Run(Func<CommandExecutor, DatabaseDescriptor, string, Task> scenario)
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        await scenario(executor, database, dbname);
    }
}
