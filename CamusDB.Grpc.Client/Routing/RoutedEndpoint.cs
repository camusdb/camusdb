
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Net.Client;
using CamusDB.Grpc.Client.Batching;

namespace CamusDB.Grpc.Client.Routing;

/// <summary>
/// One configured endpoint of a multi-endpoint connection: its address, its own batcher (a pool of
/// duplex streams to that address), the channel the connection owns for it, and its cooldown state.
///
/// <para><b>The batcher is per-endpoint by design.</b> A long-lived duplex stream cannot redirect
/// an individual request, so routing means choosing the endpoint — and therefore the batcher —
/// <em>before</em> enqueueing. Everything the batcher already guarantees (stream incarnations,
/// per-slot prepared registrations, transaction pinning to a slot) then holds per endpoint with no
/// change to the batcher itself.</para>
///
/// <para><b>Cooldown affects only future unpinned selection.</b> Suppression is a routing bias,
/// not a circuit breaker: pinned transactions keep their endpoint, in-flight work is untouched,
/// and when every endpoint is suppressed selection proceeds anyway rather than failing.</para>
/// </summary>
internal sealed class RoutedEndpoint : IAsyncDisposable
{
    private long suppressedUntil;

    public RoutedEndpoint(string address, GrpcBatcher batcher, GrpcChannel? ownedChannel)
    {
        Address = address;
        Batcher = batcher;
        OwnedChannel = ownedChannel;
    }

    public string Address { get; }

    public GrpcBatcher Batcher { get; }

    public GrpcChannel? OwnedChannel { get; }

    /// <summary>True while the endpoint is skipped for new unpinned work.</summary>
    public bool IsSuppressed(long now) => now < Volatile.Read(ref suppressedUntil);

    /// <summary>Suppresses the endpoint until the given monotonic instant; a later deadline wins.</summary>
    public void SuppressUntil(long until)
    {
        long current = Volatile.Read(ref suppressedUntil);
        while (until > current)
        {
            long seen = Interlocked.CompareExchange(ref suppressedUntil, until, current);
            if (seen == current)
                return;
            current = seen;
        }
    }

    public async ValueTask DisposeAsync()
    {
        await Batcher.DisposeAsync().ConfigureAwait(false);
        OwnedChannel?.Dispose();
    }
}
