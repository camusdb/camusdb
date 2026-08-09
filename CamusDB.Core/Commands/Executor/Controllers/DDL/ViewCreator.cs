
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

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Executes <c>CREATE [OR REPLACE] VIEW</c> and <c>DROP VIEW</c>.
///
/// <para>The body is <b>bound but never executed</b>: binding resolves every referenced relation,
/// column and function and produces the output schema without opening a cursor, so a bad view body
/// is rejected at <c>CREATE</c> time — where the author can fix it — rather than at some later
/// reader's first <c>SELECT</c>.</para>
/// </summary>
internal sealed class ViewCreator
{
    /// <summary>
    /// Creates or replaces a view. Returns false only when a create lost a race it was allowed to
    /// lose; every other failure raises.
    /// </summary>
    internal async Task<bool> CreateAsync(
        CommandExecutor executor,
        CatalogsManager catalogs,
        Task<DatabaseRegistry> registryTask,
        DatabaseDescriptor database,
        NodeAst ast,
        ExecuteSQLTicket ticket,
        bool replace)
    {
        string viewName = ast.leftAst!.yytext!;
        NodeAst bodyAst = ast.rightAst!;
        NodeAst? columnAliasList = ast.extendedOne;
        CheckOptionKind checkOption = ParseCheckOption(ast.yytext);

        // A name held by a table or materialized view is refused before the body is bound: binding
        // takes read locks on the body's sources, which a statement that cannot succeed should not do.
        if (!replace)
            database.Schema.RequireRelationNameAvailable(viewName);
        else if (!CatalogsManager.ViewExists(database, viewName) && database.Schema.Tables.ContainsKey(viewName))
            throw new CamusDBException(
                CamusDBErrorCodes.TableAlreadyExists,
                $"'{viewName}' is a table, not a view; CREATE OR REPLACE VIEW cannot replace it");

        // Expanding views inside the body first means a view over a view stores a body that names the
        // inner view (so the dependency is recorded and a later replace of the inner view is seen),
        // while binding still resolves through to real relations.
        ViewDefinition definition = await BuildDefinitionAsync(
            executor, database, bodyAst, ticket, columnAliasList, checkOption).ConfigureAwait(false);

        if (replace && database.Schema.Views.TryGetValue(viewName, out ViewSchema? existing) && existing.Definition is not null)
        {
            ViewShapeComparer.RequireCompatible(viewName, existing.Definition, definition);
            ViewDependencyGraph.RequireAcyclic(database.Schema, existing.Id!, viewName, definition);

            await catalogs.CreateViewAsync(
                database, existing.Id!, viewName, definition, existing.Comment, replace: true).ConfigureAwait(false);

            return true;
        }

        // Allocated before the delta so every node applies the id the proposer chose, exactly as
        // table creation does. Views share the table-id sequence, so a view and a table can never
        // collide in the meta keyspace.
        DatabaseRegistry registry = await registryTask.ConfigureAwait(false);
        string viewId = await registry.AllocateTableIdAsync().ConfigureAwait(false);

        ViewDependencyGraph.RequireAcyclic(database.Schema, viewId, viewName, definition);

        await catalogs.CreateViewAsync(
            database, viewId, viewName, definition, comment: null, replace: false).ConfigureAwait(false);

        return true;
    }

    /// <summary>
    /// Drops one or more views, honoring <c>CASCADE</c>/<c>RESTRICT</c>.
    /// </summary>
    /// <remarks>
    /// A cascading drop removes dependents <b>before</b> their dependency, so no intermediate state
    /// — on this node or any other applying the log — ever contains a view whose source is already
    /// gone. Each drop is its own replicated delta, which means a failure part-way through leaves the
    /// remaining views intact rather than half-applied; that is the recoverable failure mode, and it
    /// is why the order matters rather than being incidental.
    /// </remarks>
    internal async Task<bool> DropAsync(
        CatalogsManager catalogs,
        DatabaseDescriptor database,
        NodeAst ast,
        bool ifExists)
    {
        List<string> names = [];
        FlattenNames(ast.leftAst!, names);

        bool cascade = string.Equals(ast.yytext, "cascade", StringComparison.Ordinal);
        bool droppedAny = false;

        foreach (string name in names)
        {
            if (!CatalogsManager.ViewExists(database, name))
            {
                if (ifExists)
                    continue;

                // A materialized view is dropped by DROP MATERIALIZED VIEW; saying so beats
                // "does not exist" when the object is plainly there.
                if (database.Schema.Tables.TryGetValue(name, out TableSchema? relation))
                    throw new CamusDBException(
                        CamusDBErrorCodes.ViewDoesntExist,
                        relation.IsMaterializedView
                            ? $"'{name}' is a materialized view; use DROP MATERIALIZED VIEW"
                            : $"'{name}' is a table, not a view; use DROP TABLE");

                throw new CamusDBException(CamusDBErrorCodes.ViewDoesntExist, $"View '{name}' does not exist");
            }

            string viewId = database.Schema.Views[name].Id!;
            List<string> dependents = ViewDependencyGraph.DirectDependentsOfView(database.Schema, viewId);

            if (dependents.Count > 0 && !cascade)
                throw new CamusDBException(
                    CamusDBErrorCodes.DependentObjectsExist,
                    $"Cannot drop view '{name}' because other objects depend on it: " +
                    $"{string.Join(", ", dependents)}. Use DROP VIEW ... CASCADE to drop them too.");

            if (cascade)
            {
                foreach (string dependent in ViewDependencyGraph.TransitiveDependentsOfView(database.Schema, viewId))
                {
                    if (CatalogsManager.ViewExists(database, dependent))
                        await catalogs.DropViewAsync(database, dependent).ConfigureAwait(false);
                }
            }

            await catalogs.DropViewAsync(database, name).ConfigureAwait(false);
            droppedAny = true;
        }

        return droppedAny;
    }

    /// <summary>
    /// Binds the body and turns it into a storable definition. Split out so <c>CREATE MATERIALIZED
    /// VIEW</c> can reuse the identical derivation — the two statements differ in what they do with
    /// the result, not in how the body is understood.
    /// </summary>
    internal static async Task<ViewDefinition> BuildDefinitionAsync(
        CommandExecutor executor,
        DatabaseDescriptor database,
        NodeAst bodyAst,
        ExecuteSQLTicket ticket,
        NodeAst? columnAliasList,
        CheckOptionKind checkOption)
    {
        await using SelectRowSource source = await executor
            .BuildViewSourceAsync(database, bodyAst, ticket).ConfigureAwait(false);

        return ViewDefinitionBuilder.Build(
            database.Schema,
            bodyAst,
            source.Projections,
            source.Columns,
            columnAliasList,
            checkOption,
            // Null when authentication is off, in which case no base-relation check applies anyway.
            owner: ticket.Principal?.UserName);
    }

    private static CheckOptionKind ParseCheckOption(string? text) => text switch
    {
        "local" => CheckOptionKind.Local,
        "cascaded" => CheckOptionKind.Cascaded,
        _ => CheckOptionKind.None,
    };

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
