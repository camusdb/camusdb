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
/// Runs <c>CREATE EXTENSION</c>, <c>ALTER EXTENSION ... UPDATE</c> and <c>DROP EXTENSION</c>: picks the
/// loaded build the statement names, checks the cluster, and proposes the replicated delta through
/// <see cref="CatalogsManager"/>.
///
/// <para><b>The proposer resolves the build.</b> The statement succeeds only when this node loads the
/// named version (the highest loaded version when the statement names none). The record it proposes
/// carries that build's version, SHA-256, requirements and member list, so every node binds the same
/// build and a node with another build of the same version refuses to run it.</para>
///
/// <para><b>Cluster check.</b> In cluster mode, before it proposes an install or an update, the
/// statement asks every other node in the Raft roster which builds it loaded and refuses with
/// <see cref="CamusDBErrorCodes.ExtensionClusterMismatch"/> when a node that answers lacks the target
/// build. A node that does not answer is not live for this purpose and is skipped; if it comes back
/// without the file it reports <see cref="CamusDBErrorCodes.ExtensionNotLoadedOnNode"/> for the
/// statements that use the extension. This is what makes the rolling upgrade order (deploy the new
/// file everywhere, then update) enforced rather than advised.</para>
///
/// <para><b>Runs on the schema leader.</b> Like trigger, view and sequence DDL, extension DDL has no
/// ticket forwarder: in cluster mode it must reach the database's schema leader, and a follower
/// refuses with the leader's name.</para>
///
/// <para><b>Privileges.</b> Superuser only; the statement authorizer enforces it before this runs.</para>
/// </summary>
internal sealed class ExtensionDdlService
{
    /// <summary>How long one peer may take to answer the loaded-extensions probe.</summary>
    private static readonly TimeSpan PeerProbeTimeout = TimeSpan.FromSeconds(2);

    private readonly ExecutorContext context;

    private readonly CatalogsManager catalogs;

    private readonly SqlParserCache sqlParserCache;

    public ExtensionDdlService(ExecutorContext context, CatalogsManager catalogs, SqlParserCache sqlParserCache)
    {
        this.context = context;
        this.catalogs = catalogs;
        this.sqlParserCache = sqlParserCache;
    }

    public async Task CreateAsync(DatabaseDescriptor database, NodeAst ast)
    {
        string name = RequireName(ast);
        LoadedExtension build = PickBuild(name, TargetVersion(ast));

        await RequireSchemaLeaderAsync(database, "CREATE EXTENSION").ConfigureAwait(false);
        await RequireBuildOnPeersAsync(build).ConfigureAwait(false);

        HLCTimestamp now = Now();

        await InDdlGateAsync(database, () => catalogs.CreateExtensionAsync(database, BuildRecord(build, now, now))).ConfigureAwait(false);
    }

    public async Task UpdateAsync(DatabaseDescriptor database, NodeAst ast)
    {
        string name = RequireName(ast);

        if (!database.Schema.Extensions.ContainsKey(name))
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionNotInstalled,
                $"Extension '{name}' is not installed in database '{database.Name}'");

        LoadedExtension build = PickBuild(name, TargetVersion(ast));

        await RequireSchemaLeaderAsync(database, "ALTER EXTENSION").ConfigureAwait(false);
        await RequireBuildOnPeersAsync(build).ConfigureAwait(false);

        HLCTimestamp now = Now();

        await InDdlGateAsync(database, () => catalogs.UpdateExtensionAsync(database, BuildRecord(build, now, now), (schema, installed, target) =>
        {
            List<ExtensionMemberDefinition> removed = ExtensionDependencyFinder.MembersMissingFrom(installed, target);
            List<string> dependents = ExtensionDependencyFinder.Find(schema, installed, removed, Parse);

            if (dependents.Count > 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.ExtensionHasDependents,
                    $"Cannot update extension '{installed.Name}' from {installed.Version} to {target.Version}: version {target.Version} " +
                    $"does not define members that these objects use: {string.Join(", ", dependents)}");
        })).ConfigureAwait(false);
    }

    public async Task DropAsync(DatabaseDescriptor database, NodeAst ast)
    {
        string name = RequireName(ast);

        await RequireSchemaLeaderAsync(database, "DROP EXTENSION").ConfigureAwait(false);

        await InDdlGateAsync(database, () => catalogs.DropExtensionAsync(database, name, (schema, installed) =>
        {
            List<string> dependents = [];

            foreach (ExtensionDefinition other in schema.Extensions.Values)
            {
                if (!ReferenceEquals(other, installed) && other.Requires.Contains(installed.Name, StringComparer.OrdinalIgnoreCase))
                    dependents.Add($"extension {other.Name}");
            }

            dependents.AddRange(ExtensionDependencyFinder.Find(schema, installed, installed.Members, Parse));

            if (dependents.Count > 0)
                throw new CamusDBException(
                    CamusDBErrorCodes.ExtensionHasDependents,
                    $"Cannot drop extension '{installed.Name}' because these objects depend on it: {string.Join(", ", dependents)}");
        })).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs a proposal under the database's DDL semaphore, as the table DDL does, so the dependency
    /// check of a drop or an update and a concurrent <c>CREATE TABLE</c> or <c>ALTER TABLE</c> whose
    /// default or CHECK calls the extension are not built against the same schema.
    /// </summary>
    private static async Task InDdlGateAsync(DatabaseDescriptor database, Func<Task> proposal)
    {
        await database.SchemaDdlSemaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            await proposal().ConfigureAwait(false);
        }
        finally
        {
            database.SchemaDdlSemaphore.Release();
        }
    }

    private NodeAst Parse(string sql) => SQLParserProcessor.Parse(sql, sqlParserCache);

    /// <summary>
    /// The loaded build of <paramref name="name"/> with <paramref name="version"/>, or the highest loaded
    /// version when <paramref name="version"/> is null. Refused with
    /// <see cref="CamusDBErrorCodes.ExtensionNotAvailable"/> when this node does not load it.
    /// </summary>
    private LoadedExtension PickBuild(string name, string? version)
    {
        IReadOnlyList<LoadedExtension> versions = context.Extensions.GetVersions(name);

        if (versions.Count == 0)
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionNotAvailable,
                $"Extension '{name}' is not available on this node; deploy its file and list it in extension_assemblies");

        if (version is null)
            return versions[0];

        return context.Extensions.Find(name, version)
            ?? throw new CamusDBException(
                CamusDBErrorCodes.ExtensionNotAvailable,
                $"Extension '{name}' version {version} is not available on this node (loaded: {string.Join(", ", versions.Select(v => v.Version))})");
    }

    private static ExtensionDefinition BuildRecord(LoadedExtension build, HLCTimestamp installedAt, HLCTimestamp updatedAt) => new()
    {
        Name = build.Name,
        Version = build.Version,
        Sha256 = build.Sha256,
        Requires = [.. build.Requires],
        Members = build.BuildMembers(),
        InstalledAt = installedAt,
        UpdatedAt = updatedAt,
    };

    /// <summary>
    /// In cluster mode, asks every other node in the roster which builds it loaded, in parallel and
    /// bounded by one probe timeout, and refuses when a node that answers lacks the target build.
    /// </summary>
    private async Task RequireBuildOnPeersAsync(LoadedExtension build)
    {
        if (!context.IsClusterMode || context.SharedNode is not { } node || node.ExtensionPeerProbe is not { } probe)
            return;

        IReadOnlyList<string> peers = node.GetRosterPeerEndpoints();

        if (peers.Count == 0)
            return;

        using CancellationTokenSource timeout = new(PeerProbeTimeout);

        IReadOnlyList<LoadedExtensionProbeEntry>?[] answers = await Task.WhenAll(
            peers.Select(peer => probe.ProbeLoadedExtensionsAsync(peer, timeout.Token))).ConfigureAwait(false);

        List<string> lacking = [];

        for (int i = 0; i < peers.Count; i++)
        {
            if (answers[i] is not { } loaded)
                continue;

            bool hasBuild = loaded.Any(entry =>
                string.Equals(entry.Name, build.Name, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(entry.Version, build.Version, StringComparison.Ordinal) &&
                string.Equals(entry.Sha256, build.Sha256, StringComparison.OrdinalIgnoreCase));

            if (!hasBuild)
                lacking.Add(peers[i]);
        }

        if (lacking.Count > 0)
            throw new CamusDBException(
                CamusDBErrorCodes.ExtensionClusterMismatch,
                $"Extension '{build.Name}' {build.Version} (sha256 {build.Sha256}) is not loaded on nodes {string.Join(", ", lacking)}; " +
                "deploy the same file there, add it to extension_assemblies and restart them, then run the statement again");
    }

    private static string RequireName(NodeAst ast)
    {
        string? name = ast.leftAst?.yytext;

        if (!ExtensionLoader.IsExtensionName(name))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, $"Invalid extension name '{name}'");

        return name!;
    }

    private static string? TargetVersion(NodeAst ast) =>
        ast.rightAst?.yytext is { } literal ? SqlStringLiteral.Decode(literal) : null;

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
}
