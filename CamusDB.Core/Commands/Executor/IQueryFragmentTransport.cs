
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models.Queries;

namespace CamusDB.Core.CommandsExecutor;

/// <summary>
/// Node-to-node channel that executes one span-scan <see cref="QueryFragmentRequest"/> on a
/// peer and streams back the surviving rows' raw bytes. The target is addressed by its Raft
/// endpoint (the engine's canonical node identity); each implementation owns the mapping to
/// its own wire address, exactly as the schema-DDL and cluster-settings forwarders do.
///
/// <para>Contract for implementations: NO transparent retries — a fragment is not idempotent
/// mid-stream, and the coordinator owns retry/fallback (it resumes the span locally from the
/// last row it consumed). Cancelling the enumeration must promptly cancel the remote
/// execution. Any transport or remote failure surfaces as a thrown exception from the
/// stream; the coordinator treats every such failure identically (fall back to a local
/// scan of the remainder of the span).</para>
/// </summary>
public interface IQueryFragmentTransport
{
    IAsyncEnumerable<QueryFragmentRow> ExecuteFragmentAsync(
        string targetRaftEndpoint,
        QueryFragmentRequest request,
        CancellationToken cancellationToken);
}
