/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Elle;

/// <summary>
/// The pause an Elle operation takes after a transport failure before it reports it.
///
/// <para>A closed-loop worker issues its next attempt the moment the previous one returns. When the
/// only node of a cluster is down, every attempt fails in well under a millisecond ("could not be
/// reached"), so sixteen workers recorded 2.4 million failed attempts in a 90 s run: each one an
/// invocation and a failure line in the history, each write a fresh value and a rotated key. The
/// checker crashed reading the 5 million lines, and the final-state check read 200,000 keys that no
/// write ever reached. A client that cannot reach the server does not retry 27,000 times a second;
/// a short pause keeps the history about the transactions that ran, and still lets the error-rate
/// series show the outage.</para>
///
/// <para>Only a transient failure pauses. A conflict is retried at once by the operation's own loop,
/// and a definite abort or an unresolved commit says nothing about the server being gone.</para>
/// </summary>
public static class TransientBackoff
{
    public static readonly TimeSpan Pause = TimeSpan.FromMilliseconds(100);

    /// <summary>Pauses, unless the run is shutting down; a cancelled pause is not an error.</summary>
    public static async Task PauseAsync(CancellationToken ct)
    {
        try
        {
            await Task.Delay(Pause, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }
}
