
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Security.Cryptography;
using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;

namespace CamusDB.App.Services;

/// <summary>
/// Node-local registry of prepared statements for the REST surface.
///
/// <para>The gRPC surface scopes a handle to its duplex stream, which answers ownership and lifetime
/// for free. REST has no equivalent: connections are pooled, HTTP/2-multiplexed, and load-balanced,
/// so nothing about a request identifies a session the server can trust. This registry supplies the
/// three things the stream would otherwise have given — an unguessable handle so it cannot be
/// forged, an owner recorded at registration so a leaked handle is useless to anyone else, and an
/// idle timeout so a client that never closes what it prepared cannot leak memory forever.</para>
///
/// <para>A handle is therefore <b>local to this process</b>. Behind a load balancer a client will
/// meet <see cref="CamusDBErrorCodes.UnknownPreparedStatement"/> as a matter of course — after an
/// idle period, a restart, or simply a request routed elsewhere — and is expected to prepare again
/// and replay. That is a normal part of the contract, not an error condition.</para>
///
/// <para>Idle time is measured with a monotonic local timer rather than an HLC: expiry is a
/// node-local memory-management decision about this process's own dictionary, not an ordering
/// decision between distributed events.</para>
/// </summary>
public sealed class PreparedStatementRegistry
{
    /// <summary>
    /// A registered statement plus the two facts the stream model gives gRPC for free: who may use
    /// it, and when it was last used. <see cref="LastUsedTicks"/> is written on every successful
    /// resolve and read by the reaper, both without locking — a torn or slightly stale read only
    /// shifts an expiry by one sweep, which is well within what an idle timeout promises.
    /// </summary>
    private sealed class Entry
    {
        public Entry(PreparedStatement statement, string ownerKey, long retainedBytes)
        {
            Statement = statement;
            OwnerKey = ownerKey;
            RetainedBytes = retainedBytes;
            LastUsedTicks = Stopwatch.GetTimestamp();
        }

        public PreparedStatement Statement { get; }

        public string OwnerKey { get; }

        /// <summary>Quota this entry holds, remembered so release returns exactly what admission took.</summary>
        public long RetainedBytes { get; }

        public long LastUsedTicks;

        public void Touch() => Volatile.Write(ref LastUsedTicks, Stopwatch.GetTimestamp());

        public TimeSpan IdleTime => Stopwatch.GetElapsedTime(Volatile.Read(ref LastUsedTicks));
    }

    private readonly ConcurrentDictionary<string, Entry> statements = new(StringComparer.Ordinal);

    /// <summary>
    /// What one principal currently holds. Count and bytes live together because they are admitted and
    /// released together; splitting them across two dictionaries is what previously let a decrement
    /// create a counter entry out of thin air and drift the quota upward.
    /// </summary>
    private sealed class OwnerQuota
    {
        public int Statements;
        public long Bytes;
    }

    private readonly Dictionary<string, OwnerQuota> quotas = new(StringComparer.Ordinal);

    /// <summary>
    /// Guards every quota transition — the limit checks, the counters, and the decision to publish.
    ///
    /// <para>A lock rather than interlocked counters, deliberately. Admission has to weigh four
    /// limits at once (node count, node bytes, owner count, owner bytes) and either take all four or
    /// none; done with separate atomics, concurrent callers can each observe the last free slot and
    /// all publish, which is exactly the over-admission the caps exist to prevent. This runs once per
    /// registration — never per execution — so its cost is irrelevant next to the parse it guards.</para>
    /// </summary>
    private readonly Lock admission = new();

    private long totalBytes;

    /// <summary>
    /// Identifies this process in every handle it mints. Its only job is diagnostics: a handle that
    /// does not start with this prefix was minted by another node or before a restart, which lets
    /// the error say so without ever revealing whether some other handle exists.
    /// </summary>
    private readonly string instanceId = NewToken(6);

    /// <summary>Live statement count across all principals; for tests and diagnostics.</summary>
    public int Count => statements.Count;

    /// <summary>Retained bytes across all principals; for tests and diagnostics.</summary>
    public long RetainedBytes
    {
        get { lock (admission) return totalBytes; }
    }

    /// <summary>
    /// Parses and registers a statement, returning its handle.
    ///
    /// <para>No privilege check happens here — authorization is enforced on execution against the
    /// executing request's principal, exactly as for an inline statement. Registering therefore
    /// reveals nothing beyond whether the SQL parses.</para>
    ///
    /// <para>Quota is <b>reserved before the entry is published</b> and released on every failure
    /// path. Publishing first and accounting afterwards leaves a window in which a concurrent close
    /// sees the entry, decrements a counter that does not exist yet, and permanently inflates the
    /// owner's usage.</para>
    /// </summary>
    public (string Handle, PreparedStatement Statement) Prepare(Principal? principal, string database, string sql)
    {
        PreparedStatement statement = PreparedStatementBinder.Create(database, sql);
        string ownerKey = OwnerKey(principal);
        long bytes = RetainedBytesOf(statement);

        int statementLimit = CamusDBConfig.MaxPreparedStatementBytes;
        if (statementLimit > 0 && bytes > statementLimit)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Statement is too large to prepare ({bytes} bytes; the limit is {statementLimit})");

        Reserve(ownerKey, bytes);

        try
        {
            string handle = $"{instanceId}.{NewToken(16)}";
            if (!statements.TryAdd(handle, new Entry(statement, ownerKey, bytes)))
                throw new CamusDBException(
                    CamusDBErrorCodes.InvalidInternalOperation, "Prepared statement handle collision");

            return (handle, statement);
        }
        catch
        {
            Release(ownerKey, bytes);
            throw;
        }
    }

    /// <summary>
    /// Takes one statement's worth of every quota, or takes none and throws.
    ///
    /// <para>Expired entries are reclaimed first, so a caller is only refused when its statements are
    /// genuinely all live — being told to close statements that timed out ten minutes ago would be
    /// an unhelpful error.</para>
    /// </summary>
    private void Reserve(string ownerKey, long bytes)
    {
        int nodeCountLimit = CamusDBConfig.RestMaxPreparedStatements;
        int ownerCountLimit = CamusDBConfig.RestMaxPreparedStatementsPerPrincipal;
        long nodeByteLimit = CamusDBConfig.RestMaxPreparedStatementBytes;
        long ownerByteLimit = CamusDBConfig.RestMaxPreparedStatementBytesPerPrincipal;

        for (int attempt = 0; ; attempt++)
        {
            lock (admission)
            {
                OwnerQuota quota = quotas.TryGetValue(ownerKey, out OwnerQuota? existing)
                    ? existing
                    : new OwnerQuota();

                bool overOwnerCount = ownerCountLimit > 0 && quota.Statements >= ownerCountLimit;
                bool overNodeCount = nodeCountLimit > 0 && statements.Count >= nodeCountLimit;
                bool overOwnerBytes = ownerByteLimit > 0 && quota.Bytes + bytes > ownerByteLimit;
                bool overNodeBytes = nodeByteLimit > 0 && totalBytes + bytes > nodeByteLimit;

                if (!overOwnerCount && !overNodeCount && !overOwnerBytes && !overNodeBytes)
                {
                    quota.Statements++;
                    quota.Bytes += bytes;
                    quotas[ownerKey] = quota;
                    totalBytes += bytes;
                    return;
                }

                if (attempt > 0)
                {
                    throw (overOwnerCount, overNodeCount, overOwnerBytes) switch
                    {
                        (true, _, _) => PreparedStatementBinder.LimitExceeded(ownerCountLimit, "per-principal"),
                        (_, true, _) => PreparedStatementBinder.LimitExceeded(nodeCountLimit, "node-wide"),
                        (_, _, true) => PreparedStatementBinder.LimitExceeded(ownerByteLimit, "per-principal retained-byte"),
                        _            => PreparedStatementBinder.LimitExceeded(nodeByteLimit, "node-wide retained-byte"),
                    };
                }
            }

            // Refused once: reclaim expired entries outside the lock, then re-weigh. Reaping takes the
            // same lock to release quota, so it must not run while it is held.
            ReapExpired();
        }
    }

    /// <summary>Returns one statement's quota. Never creates an owner entry — only a reservation can.</summary>
    private void Release(string ownerKey, long bytes)
    {
        lock (admission)
        {
            if (quotas.TryGetValue(ownerKey, out OwnerQuota? quota))
            {
                quota.Statements--;
                quota.Bytes -= bytes;
                if (quota.Statements <= 0)
                    quotas.Remove(ownerKey);
            }

            totalBytes -= bytes;
        }
    }

    /// <summary>
    /// Bytes an entry keeps alive: the database name, the SQL, and the parameter names, all UTF-16.
    /// Approximate by design — it bounds growth, it is not an allocator accounting.
    /// </summary>
    private static long RetainedBytesOf(PreparedStatement statement)
    {
        long chars = statement.Database.Length + statement.Sql.Length;
        foreach (string name in statement.ParameterNames)
            chars += name.Length;
        return chars * sizeof(char);
    }

    /// <summary>
    /// Resolves a handle for <paramref name="principal"/>, refreshing its idle timer.
    ///
    /// <para>Every failure — unknown, expired, or owned by someone else — raises the one shared
    /// unknown-statement error. An ownership-specific error would confirm to a caller that a handle
    /// it does not own nevertheless exists, so the cases are deliberately indistinguishable.</para>
    /// </summary>
    public PreparedStatement Resolve(Principal? principal, string handle)
    {
        if (string.IsNullOrEmpty(handle))
            throw PreparedStatementBinder.UnknownStatement();

        if (!statements.TryGetValue(handle, out Entry? entry))
            throw PreparedStatementBinder.UnknownStatement(
                handle.StartsWith(instanceId, StringComparison.Ordinal)
                    ? null
                    : "prepared on a different node, or before this node restarted");

        if (!string.Equals(entry.OwnerKey, OwnerKey(principal), StringComparison.Ordinal))
            throw PreparedStatementBinder.UnknownStatement();

        // Expiry is enforced here as well as in the reaper, so an entry that outlives its timeout
        // between sweeps is never executed.
        if (IsExpired(entry))
        {
            Remove(handle, entry);
            throw PreparedStatementBinder.UnknownStatement("expired after sitting idle");
        }

        entry.Touch();
        return entry.Statement;
    }

    /// <summary>
    /// Releases a handle. Idempotent and silent about unknown handles for the same reason the gRPC
    /// CLOSE is: the caller asked for the statement to be gone, and it is gone. Returns whether an
    /// entry was actually removed, which is only of interest to tests.
    /// </summary>
    public bool Close(Principal? principal, string handle)
    {
        if (string.IsNullOrEmpty(handle) || !statements.TryGetValue(handle, out Entry? entry))
            return false;

        if (!string.Equals(entry.OwnerKey, OwnerKey(principal), StringComparison.Ordinal))
            return false;

        return Remove(handle, entry);
    }

    /// <summary>
    /// Drops every entry idle for longer than <see cref="CamusDBConfig.PreparedStatementIdleTimeoutMs"/>
    /// and returns how many went. Called by the background reaper, and inline when a cap is hit.
    /// </summary>
    public int ReapExpired()
    {
        if (CamusDBConfig.PreparedStatementIdleTimeoutMs <= 0)
            return 0;

        int reaped = 0;
        foreach (KeyValuePair<string, Entry> pair in statements)
        {
            if (IsExpired(pair.Value) && Remove(pair.Key, pair.Value))
                reaped++;
        }

        return reaped;
    }

    private bool Remove(string handle, Entry expected)
    {
        // Remove-if-still-this-entry: a handle is never reused, so this only guards against two
        // removers racing (a resolve that found it expired and the reaper). Exactly one wins, and
        // only the winner releases quota — which is what keeps the accounting exact.
        if (!statements.TryRemove(new KeyValuePair<string, Entry>(handle, expected)))
            return false;

        Release(expected.OwnerKey, expected.RetainedBytes);
        return true;
    }

    private static bool IsExpired(Entry entry)
    {
        int timeoutMs = CamusDBConfig.PreparedStatementIdleTimeoutMs;
        return timeoutMs > 0 && entry.IdleTime.TotalMilliseconds >= timeoutMs;
    }

    /// <summary>
    /// The identity a handle belongs to. With authentication disabled every caller shares one
    /// anonymous owner, which matches the rest of the server's behavior in that mode.
    /// </summary>
    private static string OwnerKey(Principal? principal) => principal?.UserName ?? "";

    /// <summary>
    /// A URL-safe random token. Cryptographic randomness rather than <c>Random</c> because a handle
    /// is a capability: guessing one would let a caller run SQL it never sent.
    /// </summary>
    private static string NewToken(int bytes)
    {
        Span<byte> raw = stackalloc byte[bytes];
        RandomNumberGenerator.Fill(raw);
        return Base64Url.EncodeToString(raw);
    }
}
