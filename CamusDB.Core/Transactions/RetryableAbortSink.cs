/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Diagnostics;

namespace CamusDB.Core.Transactions;

/// <summary>
/// Carries a statement's retryable abort up to the transport as a value instead of an exception, for
/// the callers that opt in by passing a sink down.
///
/// <para><b>Why it exists.</b> Under write contention Kahuna answers a transaction's read or write with
/// <c>Aborted</c> when another transaction committed the key after this one first read it: first
/// committer wins. Nothing in the server can retry that. The client restarts the transaction from
/// BeginAsync, so the abort has to reach the transport intact. Thrown, it is rethrown at every async
/// frame between the Kahuna call and the transport, about fifteen on the UPDATE path, and each rethrow
/// costs about as much as the first throw. A contended workload pays that on every conflict.</para>
///
/// <para><b>Contract.</b> A method that accepts a sink records a retryable abort in it and returns
/// early with a neutral result (a miss, an empty batch, no write) instead of throwing. Its caller must
/// test <see cref="HasAbort"/> before it uses that result or does anything else, and return early in
/// turn. Only a retryable code (<see cref="SerializableRetryHelper.IsRetryable"/>) is ever recorded;
/// anything else is thrown as before. A null sink means the caller did not opt in, and every abort is
/// thrown exactly as it always was. The first abort recorded wins, because every later answer on the
/// same transaction is a consequence of it.</para>
///
/// <para><b>Ownership.</b> The transport that creates the sink owns it: after the statement returns it
/// reports <see cref="Abort"/> as the statement's error, and it never commits a transaction whose sink
/// holds one. The neutral results left behind by an abort are meaningless, which is why no code
/// between the site and the owner may act on them.</para>
/// </summary>
public sealed class RetryableAbortSink
{
    /// <summary>The first retryable abort recorded, or null when the statement has not been aborted.</summary>
    public CamusDBException? Abort { get; private set; }

    /// <summary>True once a retryable abort has been recorded.</summary>
    public bool HasAbort => Abort is not null;

    /// <summary>
    /// Records <paramref name="abort"/> in <paramref name="sink"/>. Throws it instead when there is no
    /// sink or its code is not retryable, which is what the site did before sinks existed.
    /// <paramref name="site"/> is a <see cref="ServerDiagnostics.Tags.AbortSite"/> value.
    /// </summary>
    internal static void Raise(RetryableAbortSink? sink, CamusDBException abort, string site)
    {
        if (sink is null || !SerializableRetryHelper.IsRetryable(abort))
        {
            ServerDiagnostics.AddKvRetryableAbort(site, ServerDiagnostics.Tags.AbortDelivery.Thrown);
            throw abort;
        }

        ServerDiagnostics.AddKvRetryableAbort(site, ServerDiagnostics.Tags.AbortDelivery.Value);
        sink.Abort ??= abort;
    }
}
