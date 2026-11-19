
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Executes <c>CREATE MATERIALIZED VIEW</c> and <c>DROP MATERIALIZED VIEW</c>.
///
/// <para>A materialized view is created as a <b>relation</b> — it has columns, rows, indexes,
/// statistics and a primary key, and all of those already work on a table — carrying its defining
/// query as part of its schema. That is not an implementation shortcut: it is what makes backup and
/// point-in-time recovery, database branching, orphan reclamation, <c>ANALYZE</c>, covering indexes,
/// TTL and the result cache cover materialized views on the day they ship, with no separate
/// integration for any of them.</para>
///
/// <para>Creation and population are deliberately separate steps. The relation is created empty and
/// unpopulated even for <c>WITH DATA</c>, and only then refreshed, so a load that fails leaves a
/// materialized view that reports it holds nothing rather than one that claims data it never
/// received.</para>
/// </summary>
internal sealed class MaterializedViewCreator
{
    /// <summary>
    /// Creates a materialized view, populating it unless <c>WITH NO DATA</c> was given. Returns false
    /// only when <c>IF NOT EXISTS</c> found the name already taken.
    /// </summary>
    internal async Task<(bool Created, int Rows)> CreateAsync(
        CommandExecutor executor,
        MaterializedViewRefresher refresher,
        CatalogsManager catalogs,
        Task<DatabaseRegistry> registryTask,
        DatabaseDescriptor database,
        NodeAst ast,
        ExecuteSQLTicket ticket,
        ILogger<ICamusDB> logger)
    {
        string viewName = ast.leftAst!.yytext!;
        NodeAst bodyAst = ast.rightAst!;
        NodeAst? columnAliasList = ast.extendedOne;
        bool ifNotExists = ast.nodeType == NodeType.CreateMaterializedViewIfNotExists;
        bool withNoData = string.Equals(ast.yytext, "no data", StringComparison.Ordinal);

        // Checked before the body is bound so an IF NOT EXISTS over an existing relation does not take
        // read locks on tables on behalf of a statement that is meant to do nothing.
        if (ifNotExists && (database.Schema.Tables.ContainsKey(viewName) || database.Schema.Views.ContainsKey(viewName)))
            return (false, 0);

        database.Schema.RequireRelationNameAvailable(viewName);

        (ViewDefinition definition, ColumnInfo[] columns, ConstraintInfo[] constraints) =
            await DeriveRelationAsync(executor, database, bodyAst, columnAliasList, ticket).ConfigureAwait(false);

        // Allocated before the delta so every node applies the id the proposer chose, exactly as an
        // ordinary CREATE TABLE does.
        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
        string tableId = await registry.AllocateTableIdAsync().ConfigureAwait(false);

        CreateTableTicket createTicket = new(
            databaseName: ticket.DatabaseName,
            tableName: viewName,
            columns: columns,
            constraints: constraints,
            ifNotExists: false,
            kind: RelationKind.MaterializedView,
            viewDefinition: definition);

        await executor.CreateRelationInDdlTransactionAsync(
            database, createTicket, tableId, validate: true).ConfigureAwait(false);

        if (withNoData)
            return (true, 0);

        try
        {
            int rows = await refresher.RefreshAsync(
                executor, catalogs, registryTask, database, viewName,
                concurrently: false, withNoData: false, ticket, logger).ConfigureAwait(false);

            return (true, rows);
        }
        catch (Exception loadError)
        {
            // The materialized view exists but has never held data, and the statement that created it
            // did not complete. Removing it is what makes the statement retryable — leaving it behind
            // would make the retry fail on the name instead of on whatever actually went wrong.
            try
            {
                await executor.DropStagingRelationAsync(database, viewName).ConfigureAwait(false);
            }
            catch (Exception dropError)
            {
                logger.LogWarning(
                    dropError,
                    "CREATE MATERIALIZED VIEW failed to populate '{ViewName}' and could not remove it again",
                    viewName);

                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"CREATE MATERIALIZED VIEW failed to populate '{viewName}' ({loadError.Message}), and the " +
                    $"empty materialized view could not be removed ({dropError.Message}); drop it manually before retrying");
            }

            throw;
        }
    }

    /// <summary>
    /// Drops one or more materialized views, honoring <c>CASCADE</c>/<c>RESTRICT</c> over the views
    /// that read them.
    /// </summary>
    /// <remarks>
    /// The relation is dropped through the ordinary table-drop path, which is what makes its rows
    /// recoverable by <c>RELINK</c> for the retention window and reclaimable afterwards — a
    /// materialized view holds real data, and destroying it outright would be a harsher rule than the
    /// one tables get.
    /// </remarks>
    internal async Task<bool> DropAsync(
        CommandExecutor executor,
        CatalogsManager catalogs,
        Task<DatabaseRegistry> registryTask,
        DatabaseDescriptor database,
        NodeAst ast,
        bool ifExists,
        ILogger<ICamusDB> logger)
    {
        List<string> names = [];
        FlattenNames(ast.leftAst!, names);

        bool cascade = string.Equals(ast.yytext, "cascade", StringComparison.Ordinal);
        bool droppedAny = false;

        foreach (string name in names)
        {
            if (ifExists && !database.Schema.Tables.ContainsKey(name) && !database.Schema.Views.ContainsKey(name))
                continue;

            TableSchema view = MaterializedViewRefresher.RequireMaterializedView(database, name);

            // Checked before anything is dropped. The relation drop at the end would check it too, but
            // with CASCADE the dependents go first, and a caller who may not drop this materialized
            // view must not be able to remove what reads it on the way to being refused.
            MaterializedViewRefresher.RequireTargetPrivilege(database, name, view);

            // A view whose body reads this materialized view would become a delayed error for its next
            // reader. Refuse unless the statement said CASCADE, as PostgreSQL does.
            ViewDependencyMaintainer.RequireNoDependentViews(database.Schema, name, view.Id!, cascade);

            DatabaseRegistry registry = await registryTask.ConfigureAwait(false);

            if (cascade)
                await DependentRelationDropper.DropTransitiveDependentsAsync(
                    executor, catalogs, registry, database, view.Id!, logger).ConfigureAwait(false);

            if (await DropFencedAsync(executor, catalogs, registry, database, name, view.Id!, logger).ConfigureAwait(false))
                droppedAny = true;
        }

        return droppedAny;
    }

    /// <summary>
    /// Drops one materialized view while holding its refresh fence, so no rebuild of it can be running
    /// when it goes.
    /// </summary>
    /// <remarks>
    /// <para><b>Why the drop takes the fence.</b> A rebuild that is still running when the view is
    /// dropped swaps onto an id that is no longer in the schema. The apply correctly treats that as a
    /// no-op — but that no-op is what would have removed the staging relation from the schema, and the
    /// rebuild then deletes its own ownership record, leaving registered storage nothing points at. The
    /// swap is right to do nothing; what was missing is that the drop excluded nothing.</para>
    ///
    /// <para><b>It refuses, it does not wait.</b> A rebuild can run for as long as its source takes to
    /// scan, and a <c>DROP</c> that stalled for that long would look hung. The fence is retried briefly,
    /// exactly as <c>REFRESH</c> retries it, so a background sweep holding it for a few milliseconds
    /// does not turn the drop away; a real rebuild does, with <c>RefreshAlreadyInProgress</c>.</para>
    ///
    /// <para>Both the reclamation of an abandoned rebuild's staging relation and the relation drop
    /// happen under the fence: a record found while it is held belongs to a run that is gone.</para>
    /// </remarks>
    /// <returns>False when the materialized view was already gone by the time the fence was held.</returns>
    internal static async Task<bool> DropFencedAsync(
        CommandExecutor executor,
        CatalogsManager catalogs,
        DatabaseRegistry registry,
        DatabaseDescriptor database,
        string name,
        string viewId,
        ILogger<ICamusDB> logger)
    {
        string fenceId = RegistryDropMarkers.TableFenceId(database.Id, viewId);

        if (!await MaterializedViewRefresher.AcquireRefreshFenceAsync(registry, fenceId).ConfigureAwait(false))
            throw new CamusDBException(
                CamusDBErrorCodes.RefreshAlreadyInProgress,
                $"Cannot drop materialized view '{name}' while a REFRESH of it is running; wait for the refresh to finish and retry");

        try
        {
            // Re-resolved under the fence. The name was looked up before the fence was taken: a
            // concurrent drop may have removed it, which is the outcome asked for, and a rename may
            // have handed the name to a different relation, which dropping by name would then remove.
            if (!database.Schema.Tables.TryGetValue(name, out TableSchema? current))
                return false;

            if (!string.Equals(current.Id, viewId, StringComparison.Ordinal))
                throw new CamusDBException(
                    CamusDBErrorCodes.ConcurrentSchemaChange,
                    $"Materialized view '{name}' was renamed or dropped while it was being dropped; retry the statement");

            // Reclaim any staging relation an unfinished refresh of this view left behind, before the
            // view itself goes. Once it is gone nothing would look for that storage again — the record
            // is keyed by the view's id, and no later refresh of a dropped view will ever run.
            await MaterializedViewRefresher.ReclaimAbandonedRefreshAsync(
                executor, catalogs, database, viewId, logger).ConfigureAwait(false);

            return await executor.DropTable(
                new DropTableTicket(database.Name, name, ifExists: true, force: false, allowMaterializedView: true)).ConfigureAwait(false);
        }
        finally
        {
            await registry.DropMarkers.ReleaseDropIntentAsync(fenceId).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Binds the body once and derives from it both the stored definition and the relation's column
    /// and constraint definitions.
    ///
    /// <para>Once, not twice, and in that order: the relation's columns are built from the
    /// definition's output column list rather than from the raw body, so an explicit column-alias list
    /// names the stored columns too. Deriving the two independently would let them disagree, and the
    /// disagreement would only surface as a refresh writing into columns whose names no longer match
    /// what the body projects.</para>
    /// </summary>
    private static async Task<(ViewDefinition Definition, ColumnInfo[] Columns, ConstraintInfo[] Constraints)>
        DeriveRelationAsync(
            CommandExecutor executor,
            DatabaseDescriptor database,
            NodeAst bodyAst,
            NodeAst? columnAliasList,
            ExecuteSQLTicket ticket)
    {
        await using SelectRowSource source = await executor
            .BuildViewSourceAsync(database, bodyAst, ticket).ConfigureAwait(false);

        ViewDefinition definition = ViewDefinitionBuilder.Build(
            database.Schema,
            bodyAst,
            source.Projections,
            source.Columns,
            columnAliasList,
            CheckOptionKind.None,
            // Null when authentication is off, in which case no base-relation check applies anyway.
            owner: ticket.Principal?.UserName,
            setOperation: source.SetOperation);

        // The definition's columns are positionally aligned with the source's: they carry the view's
        // final names, and the source carries what the planner knows about each column, including an
        // array's element type, which the stored definition does not record.
        List<ViewColumnSchema> definitionColumns = definition.Columns ?? [];
        List<DerivedColumnSchema> outputs = new(definitionColumns.Count);

        for (int i = 0; i < definitionColumns.Count; i++)
        {
            outputs.Add(new DerivedColumnSchema(definitionColumns[i].Name, definitionColumns[i].Type)
            {
                ElementType = i < source.Columns.Count ? source.Columns[i].ElementType : null,
            });
        }

        (ColumnInfo[] columns, ConstraintInfo[] constraints, string _) =
            CreateTableAsSelectSchemaBuilder.Build(source.Projections, outputs, "CREATE MATERIALIZED VIEW");

        return (definition, columns, constraints);
    }

    private static void FlattenNames(NodeAst node, List<string> into)
    {
        if (node.nodeType == NodeType.IdentifierList)
        {
            if (node.leftAst is not null) FlattenNames(node.leftAst, into);
            if (node.rightAst is not null) FlattenNames(node.rightAst, into);
            return;
        }

        if (node.yytext is not null)
            into.Add(node.yytext);
    }
}
