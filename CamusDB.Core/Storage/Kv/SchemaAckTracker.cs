/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Storage.Kv;

internal sealed class SchemaAckTracker
{
    private readonly object sync = new();
    private readonly Dictionary<string, DatabaseAcks> databases = new(StringComparer.Ordinal);

    public void Register(string database, string node)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(node);

        lock (sync)
        {
            DatabaseAcks acks = GetOrCreate(database);
            acks.LiveNodes[node] = DateTime.UtcNow;
        }
    }

    public void Unregister(string database, string node)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(node);

        lock (sync)
        {
            if (!databases.TryGetValue(database, out DatabaseAcks? acks))
                return;

            acks.LiveNodes.Remove(node);
            acks.AppliedVersions.Remove(node);

            if (acks.LiveNodes.Count == 0)
                databases.Remove(database);
        }
    }

    public void RecordApplied(string database, string node, long schemaVersion)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(database);
        ArgumentException.ThrowIfNullOrWhiteSpace(node);

        lock (sync)
        {
            DatabaseAcks acks = GetOrCreate(database);
            acks.LiveNodes[node] = DateTime.UtcNow;

            if (!acks.AppliedVersions.TryGetValue(node, out long current) || current < schemaVersion)
                acks.AppliedVersions[node] = schemaVersion;
        }
    }

    public async Task<bool> WaitForAllLiveAsync(
        string database,
        long schemaVersion,
        TimeSpan timeout,
        TimeSpan liveNodeLease,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(database);

        DateTime deadline = DateTime.UtcNow.Add(timeout);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            lock (sync)
            {
                if (!databases.TryGetValue(database, out DatabaseAcks? acks) ||
                    HasEveryLiveNodeAcked(acks, schemaVersion, liveNodeLease))
                    return true;
            }

            DateTime now = DateTime.UtcNow;
            if (now >= deadline)
                return false;

            TimeSpan remaining = deadline - now;
            TimeSpan delay = remaining < TimeSpan.FromMilliseconds(25)
                ? remaining
                : TimeSpan.FromMilliseconds(25);

            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
    }

    private DatabaseAcks GetOrCreate(string database)
    {
        if (!databases.TryGetValue(database, out DatabaseAcks? acks))
        {
            acks = new();
            databases[database] = acks;
        }

        return acks;
    }

    private static bool HasEveryLiveNodeAcked(DatabaseAcks acks, long schemaVersion, TimeSpan liveNodeLease)
    {
        DateTime now = DateTime.UtcNow;

        foreach ((string node, DateTime lastSeen) in acks.LiveNodes)
        {
            if (liveNodeLease != Timeout.InfiniteTimeSpan && now - lastSeen > liveNodeLease)
                continue;

            if (!acks.AppliedVersions.TryGetValue(node, out long appliedVersion) || appliedVersion < schemaVersion)
                return false;
        }

        return true;
    }

    private sealed class DatabaseAcks
    {
        public Dictionary<string, DateTime> LiveNodes { get; } = new(StringComparer.Ordinal);

        public Dictionary<string, long> AppliedVersions { get; } = new(StringComparer.Ordinal);
    }
}
