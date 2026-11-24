/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsValidator;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Concurrent transactions that change the schema on one node, with transactional DDL on; see
/// <see cref="TransactionalDdlStress"/>. Two fixtures run it, because the autocommit statements of an engine in
/// cluster mode take the staged paths.
/// </summary>
internal abstract class TransactionalDdlConcurrencyScenarios : SharedNodeBaseTest
{
    protected abstract bool ClusterMode { get; }

    protected override CamusDBOptions ConfigureOptions(CamusDBOptions defaults) =>
        defaults with { TransactionalDdlEnabled = true };

    protected override CommandExecutor BuildCommandExecutor(CamusDBOptions options) =>
        new(new CommandValidator(options), new CatalogsManager(logger), logger, options,
            sharedNode: SharedNode, registry: sharedRegistry!, isClusterMode: ClusterMode);

    private async Task<TransactionalDdlStress> PrepareAsync(int seed)
    {
        (string db, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();
        return new TransactionalDdlStress(db, [new StressNode(executor, database)], seed);
    }

    [Test]
    public async Task DdlTransactionsOnOneTable([Values(1, 2, 3)] int seed) =>
        await (await PrepareAsync(seed)).DdlTransactionsOnOneTableAsync(workers: 4, rounds: 15);

    [Test]
    public async Task DdlTransactionsThatAddTheSameColumn([Values(1, 2)] int seed) =>
        await (await PrepareAsync(seed)).SameColumnAsync(workers: 4, rounds: 10);

    [Test]
    public async Task DdlTransactionsBesideAutocommitDdl([Values(1, 2, 3)] int seed) =>
        await (await PrepareAsync(seed)).DdlTransactionsBesideAutocommitDdlAsync(workers: 2, rounds: 15);

    [Test]
    public async Task DdlTransactionsBesideWriters([Values(1, 2, 3)] int seed) =>
        await (await PrepareAsync(seed)).DdlTransactionsBesideWritersAsync(
            writers: 4, schemaWorkers: 2, readers: 2, duration: TimeSpan.FromSeconds(6));
}

/// <summary><see cref="TransactionalDdlConcurrencyScenarios"/> on an engine that is not in cluster mode.</summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestTransactionalDdlConcurrencyStandalone : TransactionalDdlConcurrencyScenarios
{
    protected override bool ClusterMode => false;
}

/// <summary><see cref="TransactionalDdlConcurrencyScenarios"/> on an engine in cluster mode.</summary>
[TestFixture]
[NonParallelizable]
internal sealed class TestTransactionalDdlConcurrencyCluster : TransactionalDdlConcurrencyScenarios
{
    protected override bool ClusterMode => true;
}
