/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.Core.Extensions.Inline;

/// <summary>
/// Limits how many statements that compile inline C# one user runs per minute on this node:
/// <c>CREATE [OR REPLACE] FUNCTION ... LANGUAGE csharp</c> and <c>CREATE TRIGGER ... LANGUAGE csharp</c>.
///
/// <para><b>The cost it bounds.</b> Each of these statements compiles the body on the node that runs
/// it, holding one of the <c>inline_function_compile_concurrency</c> slots for up to
/// <c>inline_function_compile_timeout_ms</c>. A function or trigger that the statement creates or
/// replaces is compiled again by every node at its next call or fire. The compile limits bound one
/// compile, so a loop of <c>CREATE OR REPLACE FUNCTION</c> could keep the compile slots of the cluster
/// busy and change the schema without end. This limit bounds how often one user can start that work.</para>
///
/// <para><b>A count that drains.</b> Each user has a count of recent statements, and the count drains
/// by <c>max_inline_definitions_per_user_per_minute</c> each minute. A statement runs when one more keeps
/// the count within the limit, and adds one to it before it compiles. A user can run that many statements at once,
/// and then one every <c>60 / limit</c> seconds. The count is not given back when the statement fails:
/// a body that does not compile costs the node the same work. Because the limiter keeps a count, a
/// raised limit applies to the next statement, and a lowered one applies to the count already spent.</para>
///
/// <para><b>Who is limited.</b> An authenticated user who is not a superuser. A superuser can already
/// drop any database, and with authentication off every client can, so a limit there protects nothing;
/// it would only slow a reimport. Users are matched by their immutable id, so a rename keeps the
/// count, and a user that predates ids is matched by name.</para>
///
/// <para><b>Per node.</b> The count lives in this process. <c>CREATE FUNCTION</c> runs on the schema
/// leader of its database, so in a cluster a user who creates functions in databases with different
/// schema leaders has a count on each of those nodes, and a leader change starts a new count. The
/// setting is still a cluster setting, so every node applies the same limit. It is read for each
/// statement, so a change applies to the next one.</para>
///
/// <para><b>Memory.</b> A missing user has a count of zero, so dropping a user whose count has drained
/// changes nothing. The map drops those once it holds <see cref="PruneThreshold"/> users. A key is a
/// user that exists, and only a superuser creates users, so a client cannot grow the map by itself.</para>
/// </summary>
internal sealed class InlineDefinitionRateLimiter
{
    /// <summary>How many users the map holds before it drops the users whose count has drained.</summary>
    internal const int PruneThreshold = 4096;

    private const long WindowMs = 60_000;

    private const double Tolerance = 1e-9;

    private readonly object gate = new();

    private readonly Dictionary<string, Bucket> buckets = new(StringComparer.Ordinal);

    private readonly Func<long> clock;

    public InlineDefinitionRateLimiter() : this(static () => Environment.TickCount64)
    {
    }

    /// <summary>Builds a limiter that reads the time, in milliseconds, from <paramref name="clock"/>. Test-only; the engine uses the monotonic tick count.</summary>
    internal InlineDefinitionRateLimiter(Func<long> clock)
    {
        this.clock = clock;
    }

    /// <summary>How many users the limiter tracks now. For tests.</summary>
    internal int TrackedUsers
    {
        get
        {
            lock (gate)
                return buckets.Count;
        }
    }

    /// <summary>
    /// Counts one statement for <paramref name="caller"/>, or throws
    /// <see cref="CamusDBErrorCodes.InlineDefinitionRateExceeded"/> with the time until the next one may
    /// run. Call it right before the compile, after the checks that refuse the statement without
    /// compiling.
    /// </summary>
    /// <param name="options">The engine's current options; the limit is read from them for each statement.</param>
    /// <param name="caller">The user who runs the statement, or null for engine work.</param>
    /// <param name="statement">The statement, for the message, such as <c>CREATE FUNCTION</c>.</param>
    public void Admit(CamusDBOptions options, Principal? caller, string statement)
    {
        int perMinute = options.MaxInlineDefinitionsPerUserPerMinute;

        if (perMinute <= 0 || !options.AuthenticationEnabled || caller is null || caller.IsSuperuser)
            return;

        string key = caller.UserId is { Length: > 0 } id ? "id:" + id : "name:" + caller.UserName;
        long waitMs;

        lock (gate)
        {
            long now = clock();

            if (!buckets.TryGetValue(key, out Bucket? bucket))
            {
                if (buckets.Count >= PruneThreshold)
                    PruneDrained(now, perMinute);

                bucket = new Bucket { Updated = now };
                buckets[key] = bucket;
            }

            bucket.Spent = Drained(bucket, now, perMinute);
            bucket.Updated = now;

            // The tolerance absorbs the rounding of the drain, so a statement at the exact moment its
            // turn comes is not refused for a remainder of 1e-12.
            if (bucket.Spent + 1 <= perMinute + Tolerance)
            {
                bucket.Spent += 1;
                return;
            }

            waitMs = (long)Math.Ceiling((bucket.Spent + 1 - perMinute) * WindowMs / perMinute);
        }

        throw new CamusDBException(
            CamusDBErrorCodes.InlineDefinitionRateExceeded,
            $"User '{caller.UserName}' reached the limit of {perMinute} statements per minute that compile inline C# " +
            $"(max_inline_definitions_per_user_per_minute); {statement} can run again in {Math.Max(1, (waitMs + 999) / 1000)} s");
    }

    /// <summary>The count of <paramref name="bucket"/> at <paramref name="now"/>, after the drain since its last update.</summary>
    private static double Drained(Bucket bucket, long now, int perMinute) =>
        Math.Max(0, bucket.Spent - (double)Math.Max(0, now - bucket.Updated) * perMinute / WindowMs);

    private void PruneDrained(long now, int perMinute)
    {
        List<string>? drained = null;

        foreach (KeyValuePair<string, Bucket> entry in buckets)
        {
            if (Drained(entry.Value, now, perMinute) <= Tolerance)
                (drained ??= []).Add(entry.Key);
        }

        if (drained is null)
            return;

        foreach (string key in drained)
            buckets.Remove(key);
    }

    private sealed class Bucket
    {
        /// <summary>The statements counted at <see cref="Updated"/>, before the drain since then.</summary>
        public double Spent;

        public long Updated;
    }
}
