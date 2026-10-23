/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.Extensions;
using CamusDB.Core.SQLParser;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Runs <c>CREATE TRIGGER</c> and <c>DROP TRIGGER</c>: validates the statement against this node's
/// schema and loaded extensions, then proposes the replicated delta through
/// <see cref="CatalogsManager"/>.
///
/// <para><b>Typed triggers.</b> For a type of <c>ICamusTrigger&lt;TRow&gt;</c> the proposer also checks
/// the row class against the table and records the mapped columns on the trigger
/// (<see cref="TriggerDefinition.RowColumns"/>). The schema-change validation then refuses to drop or
/// rename those columns, and refuses the trigger itself if one of them went away before the proposal
/// (<see cref="TypedTriggerColumnGuard"/>).</para>
///
/// <para><b>The proposer resolves the type.</b> <c>CREATE TRIGGER</c> succeeds only when the type
/// named by <c>EXTERNAL NAME</c> is a trigger of an extension installed in the database, as this node
/// loads the installed build, and the record it proposes names that extension. The extension's own
/// record fixes the version and the hash, so an extension update rebinds the trigger without a change
/// to it. Followers apply the record without looking at their own extensions; they bind it at the
/// first DML that fires it, and refuse the DML when they do not load the installed build. That keeps
/// the schema apply infallible and still stops a node from firing other code.</para>
///
/// <para><b>Runs on the schema leader.</b> In cluster mode the statement must reach the database's
/// schema leader, the same as <c>CREATE VIEW</c> and <c>CREATE SEQUENCE</c>: trigger DDL has no
/// ticket forwarder. A follower refuses with an error that names the leader.</para>
///
/// <para><b>Privileges.</b> The table is opened through <see cref="TableOpener"/>, which is where the
/// per-table check of the ambient required privilege (<c>Alter</c>, mapped by the statement
/// authorizer) runs, so trigger DDL needs the same grant as <c>ALTER TABLE</c>.</para>
/// </summary>
internal sealed class TriggerDdlService
{
    private readonly ExecutorContext context;

    private readonly CatalogsManager catalogs;

    public TriggerDdlService(ExecutorContext context, CatalogsManager catalogs)
    {
        this.context = context;
        this.catalogs = catalogs;
    }

    public async Task CreateAsync(DatabaseDescriptor database, NodeAst ast)
    {
        string triggerName = RequireSimpleName(ast.leftAst?.yytext, "trigger");
        string tableName = ast.rightAst?.yytext ?? "";
        string typeName = SqlStringLiteral.Decode(ast.extendedTwo?.yytext ?? "");

        TriggerFireTiming timing = ast.yytext == "before" ? TriggerFireTiming.Before : TriggerFireTiming.After;
        TriggerEventSet events = ParseEvents(ast.extendedOne?.yytext ?? "");

        TableDescriptor table = await OpenTargetAsync(database, tableName).ConfigureAwait(false);

        ExtensionTriggerType type = FindInstalledTriggerType(database, typeName);

        List<string>? rowColumns = type.RowType is null ? null : CheckRowMapping(type, table);

        await RequireSchemaLeaderAsync(database, "CREATE TRIGGER").ConfigureAwait(false);

        TriggerDefinition trigger = new()
        {
            Name = triggerName,
            Timing = timing,
            Events = events,
            TypeName = type.FullName,
            ExtensionName = type.Source.Name,
            RowColumns = rowColumns,
            CreatedAt = Now(),
        };

        await catalogs.ReplicateCreateTriggerAsync(database, table.Name, table.Id, trigger).ConfigureAwait(false);
    }

    public async Task DropAsync(DatabaseDescriptor database, NodeAst ast)
    {
        string triggerName = ast.leftAst?.yytext ?? "";
        string tableName = ast.rightAst?.yytext ?? "";

        TableDescriptor table = await OpenTargetAsync(database, tableName).ConfigureAwait(false);

        await RequireSchemaLeaderAsync(database, "DROP TRIGGER").ConfigureAwait(false);
        await catalogs.ReplicateDropTriggerAsync(database, table.Name, triggerName).ConfigureAwait(false);
    }

    /// <summary>
    /// Finds the trigger type among the extensions installed in the database. A type defined by two
    /// installed extensions is ambiguous. A type of an extension this node loads but the database has
    /// not installed is refused with <see cref="CamusDBErrorCodes.ExtensionNotInstalled"/>, which names
    /// the extension to install.
    /// </summary>
    private ExtensionTriggerType FindInstalledTriggerType(DatabaseDescriptor database, string typeName)
    {
        InstalledExtensionSet installed = context.ExtensionScopes.Resolve(database);

        // A trigger of an installed extension that this node cannot bind cannot be checked here; the
        // statement fails with the missing-build error rather than "type not found".
        foreach (string extensionName in installed.Missing.Keys)
        {
            if (database.Schema.Extensions.TryGetValue(extensionName, out ExtensionDefinition? record) &&
                record.HasMember(ExtensionMemberKind.Trigger, typeName))
                installed.GetBound(extensionName, "trigger type '" + typeName + "'");
        }

        ExtensionTriggerType? found = null;
        bool notATrigger = false;

        foreach (LoadedExtension extension in installed.Bound.Values)
        {
            switch (extension.FindTriggerType(typeName, out ExtensionTriggerType? candidate))
            {
                case TriggerTypeLookup.Found when found is not null:
                    throw new CamusDBException(
                        CamusDBErrorCodes.TriggerTypeAmbiguous,
                        $"Trigger type '{typeName}' is defined by installed extensions '{found.Source.Name}' and '{extension.Name}'");

                case TriggerTypeLookup.Found:
                    found = candidate;
                    break;

                case TriggerTypeLookup.NotATrigger:
                    notATrigger = true;
                    break;
            }
        }

        if (found is not null)
            return found;

        if (notATrigger)
            throw new CamusDBException(
                CamusDBErrorCodes.TriggerTypeInvalid,
                $"Type '{typeName}' does not implement ICamusTrigger or ICamusTrigger<TRow>, or is abstract");

        foreach (LoadedExtension available in context.Extensions.Extensions)
        {
            if (available.FindTriggerType(typeName, out _) == TriggerTypeLookup.Found)
                throw new CamusDBException(
                    CamusDBErrorCodes.ExtensionNotInstalled,
                    $"Trigger type '{typeName}' belongs to extension '{available.Name}', which is not installed in database '{database.Name}'; run CREATE EXTENSION {available.Name} first");
        }

        throw new CamusDBException(
            CamusDBErrorCodes.TriggerTypeNotFound,
            $"No extension installed in database '{database.Name}' defines the trigger type '{typeName}'");
    }

    /// <summary>
    /// Checks a typed trigger's row class against the table and returns the columns it maps, which the
    /// record keeps so that a later <c>DROP COLUMN</c> or <c>RENAME COLUMN</c> of one is refused. A
    /// property with no column, or of a type that cannot hold its column, is refused with
    /// <see cref="CamusDBErrorCodes.TriggerRowMappingInvalid"/>, and the message lists every problem.
    /// </summary>
    private static List<string> CheckRowMapping(ExtensionTriggerType type, TableDescriptor table)
    {
        IReadOnlyList<TableColumnSchema> columns = table.Schema.Columns ?? [];
        TypedTriggerAdapter adapter = type.Typed!;
        List<string> problems = adapter.Validate(columns);

        if (problems.Count > 0)
            throw new CamusDBException(
                CamusDBErrorCodes.TriggerRowMappingInvalid,
                $"Trigger type '{type.FullName}' cannot fire on table '{table.Name}': its row class {type.RowType!.FullName} does not fit the table ({string.Join("; ", problems)})");

        return adapter.MappedColumnNames(columns);
    }

    /// <summary>
    /// Opens the target as a plain table. A view is refused before the open (the opener would call it
    /// "not updatable", which is the wrong reason here), and a materialized view after it.
    /// </summary>
    private async Task<TableDescriptor> OpenTargetAsync(DatabaseDescriptor database, string tableName)
    {
        if (database.Schema.Views.ContainsKey(tableName))
            throw new CamusDBException(
                CamusDBErrorCodes.TriggerTargetInvalid,
                $"'{tableName}' is a view; triggers can be created only on tables");

        TableDescriptor table = await context.TableOpener.Open(database, tableName).ConfigureAwait(false);

        if (table.Schema.Kind != RelationKind.Table)
            throw new CamusDBException(
                CamusDBErrorCodes.TriggerTargetInvalid,
                $"'{tableName}' is a materialized view; triggers can be created only on tables");

        return table;
    }

    private async Task RequireSchemaLeaderAsync(DatabaseDescriptor database, string statement)
    {
        if (!context.IsClusterMode)
            return;

        if (await database.Kahuna.AmISchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false))
            return;

        string leader = await database.Kahuna.WaitForSchemaLeaderAsync(database.Id, CancellationToken.None).ConfigureAwait(false);
        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"{statement} must be executed by schema leader '{leader}' for database '{database.Name}'");
    }

    private HLCTimestamp Now() =>
        context.SharedNode is { } node
            ? node.Raft.HybridLogicalClock.SendOrLocalEvent(node.Raft.GetLocalNodeId())
            : default;

    private static TriggerEventSet ParseEvents(string events)
    {
        TriggerEventSet set = TriggerEventSet.None;

        foreach (string part in events.Split(','))
        {
            set |= part switch
            {
                "insert" => TriggerEventSet.Insert,
                "update" => TriggerEventSet.Update,
                "delete" => TriggerEventSet.Delete,
                _ => throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Unknown trigger event '{part}'"),
            };
        }

        return set;
    }

    /// <summary>A trigger name is a plain identifier: a dotted name would read as schema-qualified.</summary>
    private static string RequireSimpleName(string? name, string what)
    {
        if (string.IsNullOrWhiteSpace(name) || name.Contains('.'))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid {what} name '{name}'");

        return name;
    }
}
