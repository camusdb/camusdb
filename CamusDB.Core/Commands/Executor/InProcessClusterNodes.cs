/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

using CamusDB.Core.Storage.Kv;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// One member of an in-process cluster: the node's Raft endpoint, its embedded Kahuna engine, and
/// the <see cref="CommandExecutor"/> that serves statements on it.
/// </summary>
/// <param name="RaftEndpoint">
/// The endpoint the engine identifies every node by. The schema-DDL forwarder, the ack sender and
/// the fragment transport all address a peer with it, exactly as the HTTP versions do.
/// </param>
public sealed record InProcessClusterNode(string RaftEndpoint, EmbeddedKahuna Node, CommandExecutor Executor);

/// <summary>
/// The address book of a cluster whose members all live in one process: it maps a Raft endpoint to
/// the member that owns it. <see cref="InProcessSchemaDdlForwarder"/> and
/// <see cref="InProcessQueryFragmentTransport"/> resolve their target through it, which is the job
/// the HTTP transports do with a base URI and a socket.
///
/// <para>Registration is deliberately late and mutable. A member's <see cref="CommandExecutor"/>
/// can only be built after its node runs, and the transports have to exist before that, because a
/// node is constructed with them. A member that stops is removed, so a call addressed to it fails
/// the way a call to a crashed host fails, instead of reaching a disposed executor.</para>
///
/// <para>Two callers may register and resolve at the same time, so the map is concurrent. The
/// members themselves are not guarded: a caller that stops a member must not let another caller
/// keep using the executor it removed.</para>
/// </summary>
public sealed class InProcessClusterNodes
{
    private readonly ConcurrentDictionary<string, InProcessClusterNode> byEndpoint = new(StringComparer.Ordinal);

    /// <summary>The members that are registered now, in no particular order.</summary>
    public IReadOnlyCollection<InProcessClusterNode> Nodes => (IReadOnlyCollection<InProcessClusterNode>)byEndpoint.Values;

    /// <summary>
    /// Adds a member, or replaces the member that holds the same endpoint. A restarted node reuses
    /// its endpoint and carries a new engine and a new executor, so replacement is the normal case.
    /// </summary>
    public InProcessClusterNode Register(EmbeddedKahuna node, CommandExecutor executor)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(executor);

        InProcessClusterNode member = new(node.Raft.GetLocalEndpoint(), node, executor);
        byEndpoint[member.RaftEndpoint] = member;
        return member;
    }

    /// <summary>
    /// Removes the member at <paramref name="raftEndpoint"/>. Call it before the member is disposed:
    /// a peer must see the endpoint as unreachable rather than reach a disposed executor.
    /// </summary>
    public bool Unregister(string raftEndpoint) => byEndpoint.TryRemove(raftEndpoint, out _);

    /// <summary>The member at <paramref name="raftEndpoint"/>, or false when no member holds it.</summary>
    public bool TryGet(string raftEndpoint, [NotNullWhen(true)] out InProcessClusterNode? node)
        => byEndpoint.TryGetValue(raftEndpoint, out node);

    /// <summary>
    /// The member at <paramref name="raftEndpoint"/>.
    /// </summary>
    /// <exception cref="CamusDBException">
    /// No member holds the endpoint. A stopped member is the ordinary cause, and the caller must
    /// treat this as a transport failure, not as a wrong answer.
    /// </exception>
    public InProcessClusterNode Get(string raftEndpoint)
    {
        if (byEndpoint.TryGetValue(raftEndpoint, out InProcessClusterNode? node))
            return node;

        throw new CamusDBException(
            CamusDBErrorCodes.InvalidInternalOperation,
            $"No in-process cluster node listens on '{raftEndpoint}'"
        );
    }
}
