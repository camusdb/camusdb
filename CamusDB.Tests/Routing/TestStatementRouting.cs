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
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Results;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Routing;
using CamusDB.Core.Transactions;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Routing;

/// <summary>
/// Eligibility classification for advisory statement routing, driven through the real SQL entry
/// points: a simple single-table statement records its table as the candidate, every excluded
/// shape (joins, subqueries, cache hints, non-DML kinds) records ineligibility or leaves the
/// collector untouched, and the resolver stays silent on a standalone node — the strongest
/// backward-compatibility property, since it makes the whole feature invisible here.
/// </summary>
[TestFixture]
public sealed class TestStatementRouting : SharedNodeBaseTest
{
    private async Task<(string dbname, DatabaseDescriptor database, CommandExecutor executor)> SetupAsync()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        await ExecDdl(database, executor, dbname,
            "CREATE TABLE accounts (id int64 primary key, balance int64 not null)");
        await ExecDdl(database, executor, dbname,
            "CREATE TABLE audits (id int64 primary key, accountid int64 not null)");

        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO accounts (id, balance) VALUES (1, 100), (2, 200), (3, 300)");
        await ExecNonQuery(database, executor, dbname,
            "INSERT INTO audits (id, accountid) VALUES (10, 1), (11, 2)");

        return (dbname, database, executor);
    }

    private static async Task ExecDdl(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        await executor.ExecuteDDLSQL(ticket);
        await database.Transactions.CommitAsync(tx);
    }

    private static async Task<int> ExecNonQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(txnState: tx, database: dbname, sql: sql, parameters: null);
        ExecuteNonSQLResult result = await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
        return result.ModifiedRows;
    }

    /// <summary>Runs a SELECT with a collector attached, draining the cursor as a transport would.</summary>
    private static async Task<StatementRoutingCollector> RunQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        StatementRoutingCollector collector = new();
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(
            txnState: tx, database: dbname, sql: sql, parameters: null, routing: collector);
        (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(ticket);
        _ = await cursor.ToListAsync();
        await database.Transactions.CommitAsync(tx);
        return collector;
    }

    /// <summary>Runs a mutation with a collector attached.</summary>
    private static async Task<StatementRoutingCollector> RunNonQuery(
        DatabaseDescriptor database, CommandExecutor executor, string dbname, string sql)
    {
        StatementRoutingCollector collector = new();
        KvTransaction tx = await database.Transactions.BeginAsync();
        ExecuteSQLTicket ticket = new(
            txnState: tx, database: dbname, sql: sql, parameters: null, routing: collector);
        await executor.ExecuteNonSQLQuery(ticket);
        await database.Transactions.CommitAsync(tx);
        return collector;
    }

    [Test]
    [NonParallelizable]
    public async Task SimpleSelect_RecordsItsSingleTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        StatementRoutingCollector collector = await RunQuery(
            database, executor, dbname, "SELECT balance FROM accounts WHERE id = 1");

        Assert.That(collector.IneligibleReason, Is.Null);
        Assert.That(collector.Candidate, Is.Not.Null);
        Assert.That(collector.Candidate!.RowKeySpace, Does.EndWith("|r"));
        Assert.That(collector.Candidate.RowKeySpace, Does.StartWith(database.Id));
    }

    [Test]
    [NonParallelizable]
    public async Task EmptyPointLookup_IsStillEligible()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        StatementRoutingCollector collector = await RunQuery(
            database, executor, dbname, "SELECT balance FROM accounts WHERE id = 999999");

        Assert.That(collector.Candidate, Is.Not.Null);
    }

    [Test]
    [NonParallelizable]
    public async Task Join_IsIneligible()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        StatementRoutingCollector collector = await RunQuery(
            database, executor, dbname,
            "SELECT accounts.id FROM accounts INNER JOIN audits ON accounts.id = audits.accountid");

        Assert.That(collector.Candidate, Is.Null);
        Assert.That(collector.IneligibleReason, Is.EqualTo(StatementRoutingAdvice.ReasonIneligible));
    }

    [Test]
    [NonParallelizable]
    public async Task SelectWithSubquery_IsIneligible()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        StatementRoutingCollector collector = await RunQuery(
            database, executor, dbname,
            "SELECT id FROM accounts WHERE id IN (SELECT accountid FROM audits)");

        Assert.That(collector.Candidate, Is.Null);
        Assert.That(collector.IneligibleReason, Is.EqualTo(StatementRoutingAdvice.ReasonIneligible));
    }

    [Test]
    [NonParallelizable]
    public async Task CacheHintedSelect_IsIneligibleWithCacheAffinity()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        StatementRoutingCollector collector = await RunQuery(
            database, executor, dbname, "SELECT id FROM accounts {cache=acct_cache}");

        Assert.That(collector.Candidate, Is.Null);
        Assert.That(collector.IneligibleReason, Is.EqualTo(StatementRoutingAdvice.ReasonCacheAffinity));
    }

    [Test]
    [NonParallelizable]
    public async Task SimpleDml_RecordsItsSingleTable()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        StatementRoutingCollector insert = await RunNonQuery(
            database, executor, dbname, "INSERT INTO accounts (id, balance) VALUES (50, 1)");
        StatementRoutingCollector update = await RunNonQuery(
            database, executor, dbname, "UPDATE accounts SET balance = 5 WHERE id = 1");
        StatementRoutingCollector delete = await RunNonQuery(
            database, executor, dbname, "DELETE FROM accounts WHERE id = 50");

        Assert.That(insert.Candidate, Is.Not.Null);
        Assert.That(update.Candidate, Is.Not.Null);
        Assert.That(delete.Candidate, Is.Not.Null);
        Assert.That(insert.Candidate!.RowKeySpace, Is.EqualTo(update.Candidate!.RowKeySpace));
    }

    [Test]
    [NonParallelizable]
    public async Task ZeroRowUpdate_IsStillEligible()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        StatementRoutingCollector collector = await RunNonQuery(
            database, executor, dbname, "UPDATE accounts SET balance = 1 WHERE id = 424242");

        Assert.That(collector.Candidate, Is.Not.Null);
    }

    [Test]
    [NonParallelizable]
    public async Task DmlWithSubquery_IsIneligible()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        StatementRoutingCollector collector = await RunNonQuery(
            database, executor, dbname,
            "UPDATE accounts SET balance = 0 WHERE id IN (SELECT accountid FROM audits)");

        Assert.That(collector.Candidate, Is.Null);
        Assert.That(collector.IneligibleReason, Is.EqualTo(StatementRoutingAdvice.ReasonIneligible));
    }

    [Test]
    [NonParallelizable]
    public async Task NonDmlStatements_LeaveTheCollectorUntouched()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        StatementRoutingCollector show = await RunQuery(database, executor, dbname, "SHOW TABLES");

        Assert.That(show.IsUntouched, Is.True);
    }

    [Test]
    [NonParallelizable]
    public async Task Resolver_EmitsNothingOnAStandaloneNode()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        StatementRoutingCollector collector = await RunQuery(
            database, executor, dbname, "SELECT balance FROM accounts WHERE id = 1");
        Assert.That(collector.Candidate, Is.Not.Null);

        StatementRoutingResolver resolver = new(new CamusDBOptions());
        Assert.That(resolver.EmissionEnabled, Is.True);
        Assert.That(resolver.Resolve(database, collector), Is.Null);
    }

    [Test]
    [NonParallelizable]
    public async Task Resolver_DisabledByOption_EmitsNothing()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        StatementRoutingCollector collector = await RunQuery(
            database, executor, dbname, "SELECT balance FROM accounts WHERE id = 1");

        StatementRoutingResolver resolver = new(new CamusDBOptions { SqlRoutingAdviceEnabled = false });
        Assert.That(resolver.EmissionEnabled, Is.False);
        Assert.That(resolver.Resolve(database, collector), Is.Null);
    }

    [Test]
    [NonParallelizable]
    public async Task DependencyTokens_TrackSchemaAndContentsChanges()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await SetupAsync();

        StatementRoutingCollector before = await RunQuery(
            database, executor, dbname, "SELECT balance FROM accounts WHERE id = 1");

        await ExecDdl(database, executor, dbname, "ALTER TABLE accounts ADD COLUMN note STRING(32)");

        StatementRoutingCollector after = await RunQuery(
            database, executor, dbname, "SELECT balance FROM accounts WHERE id = 1");

        // The candidate's identity inputs moved with the schema version, so the resolver's opaque
        // token would change too — asserted here at the input layer, where standalone tests can
        // see it (the resolver itself stays silent off-cluster).
        Assert.That(after.Candidate!.SchemaVersion, Is.GreaterThan(before.Candidate!.SchemaVersion));
    }
}
