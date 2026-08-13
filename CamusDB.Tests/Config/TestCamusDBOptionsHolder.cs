/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using System.Collections.Generic;
using System.Threading.Tasks;

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Config;
using CamusDB.Tests.CommandsExecutor;

namespace CamusDB.Tests.Config;

/// <summary>
/// The holder is the indirection that makes runtime configuration change possible: the record
/// stays immutable and a change publishes a new one. These tests pin the two behaviors everything
/// else builds on — a publish is visible to holder readers and fans out to an engine built with
/// the holder, while an engine built without one keeps its configuration for life (the contract
/// every existing test, and <c>TestIndependentlyConfiguredEngines</c>, relies on).
/// </summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestCamusDBOptionsHolder : BaseTest
{
    [Test]
    public void PublishSwapsCurrentAndAmbient()
    {
        CamusDBOptions saved = CamusDBConfig.Ambient;

        try
        {
            CamusDBOptionsHolder holder = new(Options);
            CamusDBOptions next = Options with { MaxMutationsPerTransaction = 777 };

            holder.Publish(next);

            Assert.AreSame(next, holder.Current);
            Assert.AreEqual(777, CamusDBConfig.Ambient.MaxMutationsPerTransaction);
        }
        finally
        {
            CamusDBConfig.SetAmbient(saved);
        }
    }

    [Test]
    public void SubscriberSeesPublishesUntilDisposed()
    {
        CamusDBOptions saved = CamusDBConfig.Ambient;

        try
        {
            CamusDBOptionsHolder holder = new(Options);
            List<int> seen = [];

            using (holder.Subscribe(next => seen.Add(next.MaxMutationsPerTransaction)))
                holder.Publish(Options with { MaxMutationsPerTransaction = 1 });

            holder.Publish(Options with { MaxMutationsPerTransaction = 2 });

            CollectionAssert.AreEqual(new[] { 1 }, seen);
        }
        finally
        {
            CamusDBConfig.SetAmbient(saved);
        }
    }

    /// <summary>
    /// The end-to-end claim: an engine built with the holder observes a published swap in its own
    /// options, in the databases it already has open, and in what <c>SHOW VARIABLES</c> reports —
    /// without any restart.
    /// </summary>
    [Test]
    public async Task EngineBuiltWithHolderObservesAPublishedSwap()
    {
        CamusDBOptions saved = CamusDBConfig.Ambient;

        try
        {
            CamusDBOptionsHolder holder = new(Options);
            (string db, DatabaseDescriptor descriptor, CommandExecutor executor) =
                await CreateDatabaseWith(CreateCommandExecutor(Options, holder));

            Assert.AreEqual(20_000, executor.Options.MaxMutationsPerTransaction);

            holder.Publish(Options with { MaxMutationsPerTransaction = 777 });

            Assert.AreEqual(777, executor.Options.MaxMutationsPerTransaction);
            Assert.AreEqual(777, descriptor.Options.MaxMutationsPerTransaction);

            (_, IAsyncEnumerable<QueryResultRow> cursor) = await executor.ExecuteSQLQuery(
                new ExecuteSQLTicket(
                    txnState: null!, database: db,
                    sql: "SHOW VARIABLES LIKE 'max_mutations_per_transaction'", parameters: null));

            await foreach (QueryResultRow row in cursor)
                Assert.AreEqual("777", row.Row["value"].StrValue);
        }
        finally
        {
            CamusDBConfig.SetAmbient(saved);
        }
    }
}
