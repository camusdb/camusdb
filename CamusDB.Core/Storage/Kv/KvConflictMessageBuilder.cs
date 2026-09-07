/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using CamusDB.Core.Transactions;

namespace CamusDB.Core.Storage.Kv;

/// <summary>
/// Builds the user-facing text for a lock conflict raised by one table's KV access paths.
///
/// <para>A bare "deadline exceeded" or "write conflict" line is useless in a production log: it says
/// a conflict happened but not <em>where</em>. Every message this class builds therefore names the
/// object a user can act on (database and table, plus the raw <c>{dbId}:{tableId}</c> so the key
/// prefix in a KV trace can be matched), a bounded sample of the contended keys decoded to index
/// names, and the identity, isolation mode and age of the waiting transaction — which is what
/// separates "someone else holds a lock" from "this transaction has been open far too long".</para>
///
/// <para>Rendered key text is a locator for a log reader, not a round-trippable value: encoded index
/// keys carry control characters from <see cref="KeyEncoder"/>'s ordered encoding, so the tail is
/// escaped and truncated.</para>
/// </summary>
internal sealed class KvConflictMessageBuilder
{
    // Cap on how many contended keys a single lock-wait message spells out. A mass UPDATE can
    // have thousands pending; the count is reported in full, the keys only as a sample.
    private const int MaxReportedConflictKeys = 3;

    private readonly KvKeyBuilder keys;

    /// <summary>Configuration snapshot; swapped atomically by <see cref="ApplyOptions"/>.</summary>
    private CamusDBOptions options;

    internal KvConflictMessageBuilder(KvKeyBuilder keys, CamusDBOptions options)
    {
        this.keys = keys;
        this.options = options;
    }

    /// <summary>Swaps in a newly published configuration snapshot. See <see cref="KvTableStore.ApplyOptions"/>.</summary>
    internal void ApplyOptions(CamusDBOptions next) => options = next;

    /// <summary>
    /// Builds the lock-wait-deadline error message for <paramref name="operation"/>: the operation
    /// waited past the configured wall-clock deadline for a lock that never became available.
    ///
    /// <paramref name="conflictingKeys"/> is the total count still unresolved when the deadline
    /// fired; <paramref name="conflictKeys"/> is a sample of them (only the first few are rendered,
    /// so a 20k-row batch does not produce a 20k-key message).
    /// </summary>
    internal string LockWaitDeadlineMessage(
        KvTransaction? tx,
        string operation,
        IReadOnlyList<string>? conflictKeys = null,
        int conflictingKeys = 0)
    {
        StringBuilder message = new();

        message.Append("Lock-wait deadline exceeded after ")
               .Append(options.LockWaitDeadlineMs)
               .Append(" ms on ")
               .Append(operation);

        AppendConflictContext(message, tx, conflictKeys, conflictingKeys);

        message.Append("; the operation conflicts with a long-held lock or is in a deadlock — retry the transaction from BeginAsync");

        return message.ToString();
    }

    /// <summary>
    /// Builds the message for a retry-budget exhaustion (as opposed to a wall-clock deadline):
    /// the operation kept receiving transient conflict responses for the whole retry budget.
    /// Carries the same diagnostic context as <see cref="LockWaitDeadlineMessage"/> — which
    /// database/table, which keys, and which transaction was waiting.
    /// </summary>
    internal string WriteConflictMessage(
        KvTransaction? tx,
        string operation,
        IReadOnlyList<string>? conflictKeys = null,
        int conflictingKeys = 0)
    {
        StringBuilder message = new();

        message.Append("Write conflict on ")
               .Append(operation)
               .Append(" after ")
               .Append(KahunaRetryPolicy.MaxKahunaRetries)
               .Append(" attempts");

        AppendConflictContext(message, tx, conflictKeys, conflictingKeys);

        message.Append("; a concurrent transaction holds a lock — retry the operation from BeginAsync");

        return message.ToString();
    }

    /// <summary>
    /// Appends the shared "what was contended and who was waiting" tail used by every conflict
    /// message: the database and table by user-facing name (falling back to their ids) plus the
    /// raw <c>{dbId}:{tableId}</c> key prefix so a message can be matched against KV traces, a
    /// bounded sample of the contended keys, and the waiting transaction's id, isolation/locking
    /// mode and age.
    /// </summary>
    private void AppendConflictContext(
        StringBuilder message,
        KvTransaction? tx,
        IReadOnlyList<string>? conflictKeys,
        int conflictingKeys)
    {
        message.Append(" for table '")
               .Append(string.IsNullOrEmpty(keys.TableName) ? keys.TableId : keys.TableName)
               .Append("' in database '")
               .Append(string.IsNullOrEmpty(keys.DbName) ? keys.DbId : keys.DbName)
               .Append("' (")
               .Append(keys.TableKeyPrefix)
               .Append(')');

        if (conflictingKeys > 0)
            message.Append("; ").Append(conflictingKeys).Append(" key(s) still conflicting");

        if (conflictKeys is { Count: > 0 })
        {
            message.Append(conflictKeys.Count == 1 ? "; key " : "; keys ");
            for (int i = 0; i < conflictKeys.Count && i < MaxReportedConflictKeys; i++)
            {
                if (i > 0)
                    message.Append(", ");
                message.Append('\'').Append(DescribeKey(conflictKeys[i])).Append('\'');
            }
            if (conflictKeys.Count > MaxReportedConflictKeys)
                message.Append(", … (").Append(conflictKeys.Count - MaxReportedConflictKeys).Append(" more)");
        }

        if (tx is not null)
        {
            message.Append("; transaction ").Append(tx.UniqueId)
                   .Append(" (").Append(tx.IsolationLevel).Append('/').Append(tx.Locking);
            if (tx.AgeMs is long age)
                message.Append(", open ").Append(age).Append(" ms");
            message.Append(')');
        }
    }

    /// <summary>
    /// Renders a KV key for a human: a secondary-index key becomes
    /// <c>index &lt;name&gt; entry &lt;encoded key&gt;</c> so the message names the index a user
    /// declared rather than its opaque immutable id; a row key becomes <c>row &lt;rowId&gt;</c>.
    /// Anything unrecognized is returned verbatim.
    /// </summary>
    internal string DescribeKey(string key)
    {
        if (string.IsNullOrEmpty(key))
            return key;

        if (key.StartsWith(keys.RowKeyPrefix, StringComparison.Ordinal))
            return $"row {key[keys.RowKeyPrefix.Length..]}";

        string indexPrefix = $"{keys.TableKeyPrefix}:i:";
        if (key.StartsWith(indexPrefix, StringComparison.Ordinal))
        {
            int slash = key.IndexOf('/', indexPrefix.Length);
            if (slash > 0)
            {
                string indexId = key[indexPrefix.Length..slash];
                return $"index {keys.DisplayNameOf(indexId)} entry {Printable(key[(slash + 1)..])}";
            }
        }

        return Printable(key);
    }

    /// <summary>
    /// Renders a bucket prefix for a human: the row space becomes <c>rows</c> and an index space
    /// becomes <c>index &lt;name&gt;</c>. Unrecognized prefixes are returned verbatim.
    /// </summary>
    internal string DescribeBucket(string bucketPrefix)
    {
        if (string.Equals(bucketPrefix, keys.RowBucketPrefix, StringComparison.Ordinal))
            return "rows";

        string indexPrefix = $"{keys.TableKeyPrefix}:i:";
        if (bucketPrefix.StartsWith(indexPrefix, StringComparison.Ordinal))
            return $"index {keys.DisplayNameOf(bucketPrefix[indexPrefix.Length..])}";

        return bucketPrefix;
    }

    // Escapes control characters and clips the result so one contended key stays readable on a log line.
    internal static string Printable(string value)
    {
        const int maxLength = 64;

        StringBuilder printable = new(Math.Min(value.Length, maxLength) + 4);

        for (int i = 0; i < value.Length && i < maxLength; i++)
        {
            char c = value[i];
            printable.Append(char.IsControl(c) || c > '~' ? '·' : c);
        }

        if (value.Length > maxLength)
            printable.Append('…');

        return printable.ToString();
    }
}
