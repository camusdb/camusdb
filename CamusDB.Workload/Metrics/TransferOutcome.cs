/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Metrics;

/// <summary>
/// How one transfer attempt ended. The journal and the per-row attribution both key off this, so it is
/// an enum rather than the free-form string it used to be: the attribution decides whether an attempt
/// contributes a durable effect, and a typo in a string literal would have silently changed the
/// expected balance of every row that attempt touched.
/// </summary>
public enum TransferOutcome
{
    /// <summary>The server acknowledged the commit. Both legs are durably applied, so each of the two
    /// rows moved by its delta and gained exactly one version increment.</summary>
    Committed,

    /// <summary>The commit round trip produced no server verdict. The transaction may have applied both
    /// legs or neither, and the client cannot tell which — the rows it touched carry a known ambiguity
    /// into reconciliation rather than a known effect.</summary>
    Indeterminate,

    /// <summary>The server aborted the attempt on a retryable conflict; the caller retries from a fresh
    /// BEGIN. Nothing was applied.</summary>
    ConflictRetry,

    /// <summary>The retry budget ran out with the attempt still conflicting. Nothing was applied.</summary>
    ConflictFinal,

    /// <summary>The server definitely aborted the attempt for a non-conflict reason. Nothing was
    /// applied.</summary>
    Error,
}

/// <summary>Conversions for <see cref="TransferOutcome"/>.</summary>
public static class TransferOutcomeExtensions
{
    /// <summary>
    /// The token written to <c>transfer-ledger.csv</c>. Fixed strings, not <c>ToString()</c>, because
    /// the CSV is a published artifact that offline analysis already parses — renaming an enum member
    /// must not change the file's contents.
    /// </summary>
    public static string ToLedgerToken(this TransferOutcome outcome) => outcome switch
    {
        TransferOutcome.Committed => "committed",
        TransferOutcome.Indeterminate => "indeterminate",
        TransferOutcome.ConflictRetry => "conflict-retry",
        TransferOutcome.ConflictFinal => "conflict-final",
        _ => "error",
    };
}
