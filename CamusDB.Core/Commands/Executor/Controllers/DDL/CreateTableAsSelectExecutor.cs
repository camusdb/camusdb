
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Creates a relation and populates it from a query — <c>CREATE TABLE … AS SELECT</c> — and owns the
/// staging primitives a materialized-view refresh builds on (create, drop, and chunked load of a
/// relation inside its own transaction).
///
/// <para><b>The statement is deliberately not atomic across schema and data, and callers must know
/// it.</b> Creating the table commits its own DDL transaction — in cluster mode replicating through
/// Raft — before a single row can be written, so the two cannot be one unit. A failure during the
/// load therefore drops the table again as a compensating action; if that drop also fails, or the
/// process dies between the two, an empty table is left behind and the error says so explicitly
/// rather than pretending the statement was clean.</para>
///
/// <para><b>Binding happens before the table exists, and reading happens after.</b> The source query
/// is bound first to derive the target's column types without opening its cursor, because those
/// types are what the <c>CREATE</c> needs; only once the table is committed is the cursor drained
/// into it. An <c>IF NOT EXISTS</c> over an existing table short-circuits ahead of even the bind, so
/// a statement meant to do nothing takes no locks and reads no rows.</para>
/// </summary>
internal sealed class CreateTableAsSelectExecutor
{
    internal readonly ExecutorContext context;

    internal readonly CatalogsManager catalogs;

    internal readonly SchemaDdlService schemaDdl;

    internal readonly DdlForwardingCoordinator ddlForwarding;

    internal readonly SelectStatementExecutor selectExecutor;

    internal readonly TableCreator tableCreator;

    internal readonly TableIndexAlterer tableIndexAlterer;

    internal readonly QueryExecutor queryExecutor;

    internal readonly RowInserter rowInserter;

    internal readonly DML.RowInsertSelector rowInsertSelector;

    internal CreateTableAsSelectExecutor(
        ExecutorContext context,
        CatalogsManager catalogs,
        SchemaDdlService schemaDdl,
        DdlForwardingCoordinator ddlForwarding,
        SelectStatementExecutor selectExecutor,
        TableCreator tableCreator,
        TableIndexAlterer tableIndexAlterer,
        QueryExecutor queryExecutor,
        RowInserter rowInserter,
        DML.RowInsertSelector rowInsertSelector
    )
    {
        this.context = context;
        this.catalogs = catalogs;
        this.schemaDdl = schemaDdl;
        this.ddlForwarding = ddlForwarding;
        this.selectExecutor = selectExecutor;
        this.tableCreator = tableCreator;
        this.tableIndexAlterer = tableIndexAlterer;
        this.queryExecutor = queryExecutor;
        this.rowInserter = rowInserter;
        this.rowInsertSelector = rowInsertSelector;
    }

    /// <summary>
    /// Executes <c>CREATE TABLE … AS SELECT</c>: derives the new table's schema from the source
    /// query, creates it through the ordinary CREATE TABLE path, then loads the query's rows into it.
    ///
    /// <para><b>The statement is not atomic across schema and data.</b> Creating the table commits its
    /// own DDL transaction (and in cluster mode replicates it through Raft) before a single row can be
    /// written, so the two cannot be one unit. A failure during the load therefore drops the table
    /// again as a compensating action — but if that drop also fails, or the process dies in between,
    /// an empty table is left behind. The load itself runs in the caller's transaction, so the rows
    /// become durable only when the caller commits, while the table already is.</para>
    /// </summary>
    /// <returns>
    /// <c>Created</c> is false only when <c>IF NOT EXISTS</c> found the table already present;
    /// <c>Warning</c> is non-null when the statement succeeded but the caller should look twice
    /// (see <see cref="WarnIfTimeTravelCopyReadNothing"/>).
    /// </returns>
    internal async Task<(bool Created, int Rows, string? Warning)> ExecuteCreateTableAsSelectAsync(
        DatabaseDescriptor database,
        NodeAst ast,
        ExecuteSQLTicket ticket)
    {
        string tableName = ast.leftAst!.yytext!;
        NodeAst sourceAst = ast.rightAst!;
        bool ifNotExists = ast.nodeType == NodeType.CreateTableAsSelectIfNotExists;
        bool withNoData = string.Equals(ast.yytext, "no data", StringComparison.Ordinal);

        // Checked before anything else so an IF NOT EXISTS over an existing table does not execute the
        // source query: running it would take locks and read rows on behalf of a statement that is
        // meant to do nothing.
        if (ifNotExists && catalogs.TableExists(database, tableName))
            return (false, 0, null);

        // Binding produces the output schema without reading any rows — the cursor stays unopened
        // until the table exists.
        await using SelectRowSource source = await selectExecutor.BuildSelectSourceAsync(
            database, sourceAst, ticket, "CREATE TABLE ... AS SELECT").ConfigureAwait(false);

        IReadOnlyList<DerivedColumnSchema> sourceColumns = source.Columns;

        (ColumnInfo[] columns, ConstraintInfo[] constraints, string _) =
            CreateTableAsSelectSchemaBuilder.Build(source.Projections, sourceColumns);

        CreateTableTicket createTableTicket = new(
            databaseName: ticket.DatabaseName,
            tableName: tableName,
            columns: columns,
            constraints: constraints,
            ifNotExists: ifNotExists
        );

        context.Validator.Validate(createTableTicket);

        bool? forwarded = await ddlForwarding.TryForwardCreateTableAsync(database, createTableTicket).ConfigureAwait(false);

        bool created;

        if (forwarded is not null)
        {
            created = forwarded.Value;
        }
        else
        {
            // Allocated before the DDL transaction — only the proposer allocates, and the id travels in
            // the replicated payload so every node applies the same one.
            DatabaseRegistry ctasRegistry = await context.Registry.ConfigureAwait(false);
            string ctasTableId = await ctasRegistry.AllocateTableIdAsync().ConfigureAwait(false);

            created = await schemaDdl.ExecuteDdlInTransaction(database, async tx =>
                await tableCreator.Create(queryExecutor, context.TableOpener, tableIndexAlterer, database, createTableTicket, tx, ctasTableId)
                    .ConfigureAwait(false)
            ).ConfigureAwait(false);
        }

        // Lost an IF NOT EXISTS race against a concurrent creator: the table exists but is not ours,
        // so loading into it would add rows to someone else's table.
        if (!created)
            return (false, 0, null);

        if (withNoData)
            return (true, 0, null);

        try
        {
            InsertSelectTicket loadTicket = new(
                txnState: ticket.TxnState,
                databaseName: ticket.DatabaseName,
                tableName: tableName,
                targetColumns: sourceColumns.Select(column => column.Name).ToList(),
                sourceSelect: sourceAst,
                parameters: ticket.Parameters
            );

            for (int fenceAttempt = 0; ; fenceAttempt++)
            {
                try
                {
                    TableDescriptor createdTable = await context.TableOpener.Open(database, tableName).ConfigureAwait(false);
                    Controllers.Queries.SelectStatementExecutor.PinSchemaVersion(database, createdTable, ticket.TxnState);

                    int loaded = await rowInsertSelector
                        .InsertSelect(rowInserter, context.Statistics, database, createdTable, loadTicket, sourceColumns, source.Cursor)
                        .ConfigureAwait(false);

                    return (true, loaded, WarnIfTimeTravelCopyReadNothing(source, loaded, tableName));
                }
                catch (CamusDBException ex) when (ex.Code == CamusDBErrorCodes.SchemaCatchingUp && fenceAttempt < SelectStatementExecutor.MaxFenceRetries)
                {
                    // In cluster mode the CREATE may have been applied by the leader while this node is
                    // still catching up, so the table it just created is not yet visible here.
                    await Task.Delay(TimeSpan.FromMilliseconds(100 << fenceAttempt)).ConfigureAwait(false);
                }
            }
        }
        catch (Exception loadError)
        {
            // Compensate: the table was created by a statement that did not complete, so leaving it
            // behind would make a retry of the same statement load into a table it did not create.
            try
            {
                await schemaDdl.DropTable(new DropTableTicket(ticket.DatabaseName, tableName, ifExists: true, force: true))
                    .ConfigureAwait(false);
            }
            catch (Exception dropError)
            {
                context.Logger.LogWarning(
                    dropError,
                    "CREATE TABLE AS SELECT failed to load '{TableName}' and could not drop it again; " +
                    "an empty table is left behind",
                    tableName);

                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInput,
                    $"CREATE TABLE ... AS SELECT failed to populate '{tableName}' ({loadError.Message}), and the " +
                    $"empty table could not be removed ({dropError.Message}); drop it manually before retrying");
            }

            throw;
        }
    }

    /// <summary>
    /// Reports a time-travel copy that produced no rows, returning the message so the caller can put
    /// it in the response as well as the log (null when there is nothing to report).
    ///
    /// <para>Zero rows is a legitimate outcome — the source may genuinely have been empty at that
    /// instant, or the WHERE matched nothing — so this cannot be an error. But it is also exactly what
    /// a copy reading past Kahuna's revision-retention window looks like, and a recovery that quietly
    /// creates an empty table is the worst way to find that out. It is returned rather than only
    /// logged because a remote client never sees this node's log: to an HTTP or gRPC caller, a silent
    /// empty recovery and a successful one look identical.</para>
    /// </summary>
    internal string? WarnIfTimeTravelCopyReadNothing(SelectRowSource source, int rows, string tableName)
    {
        if (!source.IsTimeTravel || rows > 0)
            return null;

        string warning =
            $"AS OF SYSTEM TIME copy into '{tableName}' inserted no rows. The source may have been empty at that " +
            "snapshot; the history may be older than the configured revision retention and already reclaimed; or " +
            "the rows were deleted after the snapshot, which time travel cannot recover.";

        context.Logger.LogWarning("{Warning}", warning);

        return warning;
    }

    /// <summary>
    /// Creates a relation a materialized-view statement needs, in its own DDL transaction: either the
    /// materialized view itself or the relation a refresh builds into.
    ///
    /// <para>The ticket validator is applied only when <paramref name="validate"/> asks for it. A
    /// materialized view's own ticket is validated like any other <c>CREATE TABLE</c>; a staging
    /// relation's cannot be, because its generated name is built from characters an identifier may not
    /// contain (see <see cref="Catalogs.Models.MaterializedViewNaming"/>) — which is exactly what the
    /// validator rejects. Nothing goes unchecked either way: a staging relation's columns and
    /// constraints are copied from a relation that was validated when it was created.</para>
    ///
    /// <para>The per-table privilege check is suspended for the duration. The relation does not exist
    /// yet, so no grant can name it; authority for the statement was established at database scope
    /// before this was reached, exactly as for <c>CREATE TABLE</c>.</para>
    /// </summary>
    internal async Task<bool> CreateRelationInDdlTransactionAsync(
        DatabaseDescriptor database, CreateTableTicket ticket, string tableId, bool validate)
    {
        if (validate)
            context.Validator.Validate(ticket);

        using AuthorizationContext.PrivilegeSwap _ = AuthorizationContext.WithRequiredPrivilege(null);

        return await schemaDdl.ExecuteDdlInTransaction(database, tx =>
            tableCreator.Create(queryExecutor, context.TableOpener, tableIndexAlterer, database, ticket, tx, tableId))
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Destroys a relation a materialized-view statement created and no longer wants: the staging
    /// relation of a failed rebuild, or a materialized view whose initial population failed.
    ///
    /// <para><c>force</c>, not deferred: neither is something anyone would want to relink, so
    /// retaining them would only leave orphan records nobody can act on. The per-table privilege check
    /// is suspended for the same reason it is on the way in — this removes an object the running
    /// statement itself created, and a caller who could not name it in a grant would otherwise be
    /// unable to clean up after their own failed statement.</para>
    /// </summary>
    internal async Task DropStagingRelationAsync(DatabaseDescriptor database, string relationName)
    {
        using AuthorizationContext.PrivilegeSwap _ = AuthorizationContext.WithRequiredPrivilege(null);

        await schemaDdl.DropTable(new DropTableTicket(database.Name, relationName, ifExists: true, force: true))
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Writes one chunk of a materialized-view rebuild in its own committed transaction, and reports
    /// how many rows it inserted.
    ///
    /// <para>Its own transaction is the point: a rebuild is unbounded in size and a single one would
    /// hit <see cref="CamusDBOptions.MaxMutationsPerTransaction"/> for any materialized view worth
    /// materializing. Chunked writes are safe here — and only here — because the rows land in a
    /// relation no reader can name, and become visible only when the swap publishes them all at
    /// once.</para>
    ///
    /// <para>The per-table privilege check is suspended for the write. The caller's authority to run
    /// the refresh was established against the materialized view before any of this began; the
    /// staging relation is engine bookkeeping with an id no grant could ever have been written
    /// against, so checking it would refuse every non-superuser refresh.</para>
    /// </summary>
    internal async Task<int> InsertRefreshChunkAsync(
        DatabaseDescriptor database,
        string relationName,
        IReadOnlyList<DerivedColumnSchema> sourceColumns,
        IReadOnlyList<string> targetColumns,
        IReadOnlyList<QueryResultRow> rows)
    {
        KvTransaction tx = await database.Transactions.BeginAsync(
            CamusIsolationLevel.ReadCommitted, CamusTransactionMode.ReadWrite).ConfigureAwait(false);

        try
        {
            using AuthorizationContext.PrivilegeSwap _ = AuthorizationContext.WithRequiredPrivilege(null);

            TableDescriptor staging = await context.TableOpener.Open(database, relationName).ConfigureAwait(false);

            InsertSelectTicket chunkTicket = new(
                txnState: tx,
                databaseName: database.Name,
                tableName: relationName,
                targetColumns: [.. targetColumns],
                sourceSelect: null!,
                parameters: null);

            int inserted = await rowInsertSelector.InsertSelect(
                rowInserter, context.Statistics, database, staging, chunkTicket, sourceColumns,
                Queries.QueryResultStream.FromRows(rows)).ConfigureAwait(false);

            await database.Transactions.CommitAsync(tx).ConfigureAwait(false);
            return inserted;
        }
        finally
        {
            await database.Transactions.RollbackIfNotCompletedAsync(tx).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// This node's current cluster time. Materialized-view refresh stamps its snapshot with it, so
    /// staleness is ordered by the same clock as every other distributed event rather than by a wall
    /// clock that may disagree between nodes.
    /// </summary>
    internal HLCTimestamp ClusterNow()
    {
        if (context.SharedNode is null)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInternalOperation,
                "A storage node is required to resolve the current cluster timestamp.");

        return context.SharedNode.Raft.HybridLogicalClock.SendOrLocalEvent(context.SharedNode.Raft.GetLocalNodeId());
    }}
