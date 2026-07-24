/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq;
using System.Threading.Tasks;

using NUnit.Framework;

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Tests.CommandsExecutor;

/// <summary>
/// Leader-change resume of the staged index-add coordinator job must reconstruct the <b>complete</b>
/// index definition — including per-column sort directions and INCLUDE column ids. Before the fix the
/// persisted job carried neither the directions field nor propagated it into the rebuilt
/// <c>IndexBuildInfo</c>, so a resumed DESCENDING covering index would be replicated as ascending,
/// disagreeing with any descending key encoding an interrupted run had already written.
/// </summary>
[NonParallelizable]
internal sealed class TestIndexIncludeCoordinatorResume : SharedNodeBaseTest
{
    [Test]
    public void PersistedCoordinatorJob_RoundTrips_DirectionsAndIncludeIds()
    {
        PersistedCoordinatorJob job = new()
        {
            TableName = "orders",
            TableId = "tbl-1",
            ElementName = "ix_desc",
            TargetState = SchemaElementState.Public,
            ElementKind = SchemaElementKind.Index,
            IndexId = "idx-1",
            IndexColumnIds = ["col-a", "col-b"],
            IndexType = IndexType.Multi,
            IndexColumnDirections = [OrderType.Ascending, OrderType.Descending],
            IndexIncludeColumnIds = ["col-c"],
            Attempts = 0,
        };

        byte[] bytes = MetaJsonSerializer.Serialize(job, MetaJsonContext.Default.PersistedCoordinatorJob);
        PersistedCoordinatorJob decoded = MetaJsonSerializer.Deserialize(bytes, MetaJsonContext.Default.PersistedCoordinatorJob);

        Assert.IsNotNull(decoded.IndexColumnDirections);
        Assert.AreEqual(2, decoded.IndexColumnDirections!.Length);
        Assert.AreEqual(OrderType.Ascending, decoded.IndexColumnDirections[0]);
        Assert.AreEqual(OrderType.Descending, decoded.IndexColumnDirections[1]);
        Assert.AreEqual(new[] { "col-c" }, decoded.IndexIncludeColumnIds);
    }

    /// <summary>
    /// End-to-end: a persisted job for a not-yet-created DESCENDING covering index, driven through
    /// <see cref="SchemaChangeCoordinator.ResumeJobsAsync"/> (the leader-change path), must replicate
    /// the index into the schema with its descending direction and INCLUDE ids intact. Targets
    /// <c>DeleteOnly</c> so the create step runs without needing the backfill callback (backfill fires
    /// only on WriteOnly → Public).
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task ResumeDescendingCoveringIndex_ReplicatesDirectionsAndIncludes()
    {
        (string dbname, DatabaseDescriptor database, CommandExecutor executor) = await CreateDatabase();

        // Create the table via SQL so the schema/columns are populated with real ids.
        KvTransaction tx = await database.Transactions.BeginAsync();
        await executor.ExecuteDDLSQL(new ExecuteSQLTicket(tx, dbname,
            "CREATE TABLE orders (id oid primary key, year int64 not null, note string(64) not null)", null));
        await database.Transactions.CommitAsync(tx);

        DatabaseDescriptor db = await executor.OpenDatabase(dbname);
        TableSchema table = db.Schema.Tables["orders"];
        string yearId = table.Columns!.First(c => c.Name == "year").Id!;
        string noteId = table.Columns!.First(c => c.Name == "note").Id!;

        // Persist a coordinator job as an interrupted leader would have left it: a descending covering
        // index on (year DESC) INCLUDE (note), not yet created (Absent), target DeleteOnly.
        CatalogsManager catalogs = new(logger);
        PersistedCoordinatorJob job = new()
        {
            TableName = "orders",
            TableId = table.Id!,
            ElementName = "year_desc_cov",
            TargetState = SchemaElementState.DeleteOnly,
            ElementKind = SchemaElementKind.Index,
            IndexId = ObjectIdGenerator.Generate().ToString(),
            IndexColumnIds = [yearId],
            IndexType = IndexType.Multi,
            IndexColumnDirections = [OrderType.Descending],
            IndexIncludeColumnIds = [noteId],
            Attempts = 0,
        };
        await catalogs.PersistCoordinatorJobAsync(db, job);

        // Drive the resume — reconstructs IndexBuildInfo from the persisted record and replicates.
        SchemaChangeCoordinator coordinator = new(catalogs, logger);
        await coordinator.ResumeJobsAsync(db);

        // The replicated index must carry the descending direction and the INCLUDE ids.
        TableIndexSchema created = db.Schema.Tables["orders"].Indexes!.First(ix => ix.Name == "year_desc_cov");
        Assert.AreEqual(OrderType.Descending, created.DirectionAt(0), "resumed index must stay descending");
        Assert.IsNotNull(created.IncludeColumnIds, "resumed index must keep its INCLUDE ids");
        Assert.AreEqual(new[] { noteId }, created.IncludeColumnIds);
    }
}
