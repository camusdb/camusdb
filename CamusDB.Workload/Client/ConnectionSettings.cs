/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;

namespace CamusDB.Workload.Client;

/// <summary>
/// Connection-string knobs the CLI can vary per run. Locking/isolation shape only the write
/// connections (and must match the explicit <c>CamusTransactionOptions</c> used by the write path, or
/// the connection-string default and the per-transaction options would silently disagree);
/// auto-prepare and the request timeout apply to every connection — read, write, and setup — because a
/// chaos run that kills nodes needs a bounded round-trip everywhere, not just on the measured path.
/// The defaults reproduce the historical hardcoded strings byte-for-byte, so a run with no new flags
/// is comparable to older results.
/// </summary>
public sealed record ConnectionSettings(
    CamusLocking Locking = CamusLocking.Optimistic,
    CamusIsolationLevel IsolationLevel = CamusIsolationLevel.ReadCommitted,
    bool NoAutoPrepare = false,
    int? RequestTimeoutSeconds = null)
{
    public static ConnectionSettings Default { get; } = new();

    /// <summary>Suffix appended to every connection string (empty for the defaults).</summary>
    public string CommonSuffix()
    {
        string suffix = "";
        if (NoAutoPrepare)
            suffix += ";MaxAutoPrepare=0";
        if (RequestTimeoutSeconds is int seconds)
            suffix += $";Timeout={seconds}";
        return suffix;
    }
}
