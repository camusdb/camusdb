/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Diagnostics;
using Microsoft.Extensions.Hosting;

namespace CamusDB.App.Services;

/// <summary>
/// Runs the node's <see cref="NodeResourceSampler"/> for the life of the host, so the node stats call
/// (<c>GetNodeStats</c>, <c>GET /v1/admin/stats</c>) can report a CPU rate, load, host memory and the
/// engine's on-disk size without measuring any of them on the request path.
///
/// <para>The first sample is taken as the host starts, so a monitor's first call finds every figure
/// populated. The loop stops when the host stops; the sampler itself never lets a failed probe end
/// it.</para>
/// </summary>
public sealed class NodeResourceSamplerService : BackgroundService
{
    private readonly NodeResourceSampler sampler;

    public NodeResourceSamplerService(NodeResourceSampler sampler) => this.sampler = sampler;

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => sampler.RunAsync(stoppingToken);
}
