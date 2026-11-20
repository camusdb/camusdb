/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.Queries;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Plans;
using CamusDB.Core.Diagnostics;
using CamusDB.Core.SQLParser;
using CamusDB.Core.Storage.Kv;
using CamusDB.Core.Transactions;
using CamusDB.Core.Util.ObjectIds;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace CamusDB.Core.CommandsExecutor.Controllers;

/// <summary>
/// The row an UPDATE or DELETE by primary key names, taken from the row-id cache
/// (<see cref="UniqueRowIdCache"/>), so the statement skips its locate read.
///
/// <para><b>What it replaces.</b> A write statement finds its rows with a query, keeps only their ids,
/// and its write phase then locks each row, reads it again at the latest value and re-checks the
/// predicate on that read (<see cref="MutationRowRecheck"/>). When the locate plan is a point lookup
/// on the primary key and the cache holds the key, that query is one read of the row the write phase
/// reads next. The statement takes the id from the cache and goes to the write phase.</para>
///
/// <para><b>Which statements.</b> <see cref="StatementQualifies"/> admits a statement only when the
/// write phase's lock and read are the ones the locate read would have made:
/// <list type="bullet">
///   <item>The transaction is optimistic or Serializable. Its locate step reads the latest value, and
///   the write phase repeats that read on the same row key: with the same shared point lock for a
///   Serializable transaction, and the same read registration for an optimistic one. A pessimistic
///   Read Committed transaction is left out: its locate reads at the statement timestamp, and its
///   write phase takes the exclusive row lock before the read, which would lock a row whose
///   predicate then fails.</item>
///   <item>The predicate holds no subquery and the statement has no LIMIT, so the re-check on the
///   locked read is the whole predicate. A conjunct with a subquery keeps the locate scan's answer,
///   which a skipped scan cannot give.</item>
/// </list></para>
///
/// <para><b>A remembered id is a hint.</b> A primary key never changes and a row id is never reused, so
/// the id is wrong only when its row is gone. The write phase reports that
/// (<see cref="RowGone"/>), and the statement then forgets the id and runs <see cref="Plan"/>, whose
/// index read is authoritative: the key may be absent, or live under a new row id. A row that is
/// present and fails the re-check is the statement's answer, as it is after a locate read.</para>
/// </summary>
internal sealed class RememberedRowLocate
{
    private readonly string cacheKey;
    private readonly string indexId;

    /// <summary>The row id the cache holds for the statement's primary-key value.</summary>
    public ObjectIdValue RowId { get; }

    /// <summary>
    /// The locate plan the statement would have run. It has not been executed, and it is the plan to
    /// run when the remembered row is gone.
    /// </summary>
    public QueryPlan Plan { get; }

    /// <summary>Set by the write phase when it found no row under <see cref="RowId"/>.</summary>
    public bool RowGone { get; set; }

    private RememberedRowLocate(QueryPlan plan, ObjectIdValue rowId, string cacheKey, string indexId)
    {
        Plan = plan;
        RowId = rowId;
        this.cacheKey = cacheKey;
        this.indexId = indexId;
    }

    /// <summary>
    /// True when a statement of <paramref name="tx"/> with this predicate may take its row from the
    /// cache; see the class summary. <paramref name="statementTimestamp"/> is the timestamp the locate
    /// scan would read at, or null when it reads the latest value. The transaction test is the one
    /// <see cref="KvBatchWriter.AcquireRowLocksForMutationAsync"/> uses to take no lock before the
    /// write phase's read.
    /// </summary>
    public static bool StatementQualifies(
        KvTransaction tx,
        HLCTimestamp? statementTimestamp,
        NodeAst? where,
        NodeAst? limit,
        ExistsSubqueryRegistry? existsSubqueries)
    {
        if (statementTimestamp is not null || limit is not null || existsSubqueries is not null)
            return false;

        if (tx.Locking != KeyValueTransactionLocking.Optimistic && !KvRangeLockManager.IsSerializableReadWrite(tx))
            return false;

        return where is null || !RequiredColumnAnalyzer.ContainsSubqueryNode(where);
    }

    /// <summary>
    /// Answers the remembered row of <paramref name="plan"/>, when the plan is a point lookup on the
    /// primary key, alone or under its filter, and the cache holds the key. The plan must not have been
    /// executed. Any other plan shape answers false, and the caller runs the plan.
    /// </summary>
    public static bool TryFrom(QueryPlan plan, out RememberedRowLocate? remembered)
    {
        remembered = null;

        KvTableStore store = plan.Table.Store;
        if (!store.RemembersUniqueRowIds)
            return false;

        PhysicalPlanNode? node = plan.Root;
        while (node is FilterNode)
            node = node.Input;

        if (node is not IndexLookupNode lookup
            || lookup.Index.Type != IndexType.Unique
            || lookup.Index.Name != CamusDBConstants.PrimaryKeyInternalName)
            return false;

        if (!store.TryGetCachedUniqueRowId(lookup.Index.KvId, lookup.LookupKey, out ObjectIdValue rowId, out string? cacheKey))
            return false;

        remembered = new(plan, rowId, cacheKey!, lookup.Index.KvId);
        return true;
    }

    /// <summary>
    /// Records what the write phase found, once, after its read. A row that is there counts as the
    /// index read and the row read the locate lookup would have counted. A row that is gone drops the
    /// cache entry, so the caller's run of <see cref="Plan"/> resolves the key through the index.
    /// </summary>
    public void Settle(StatementProbe? probe)
    {
        KvTableStore store = Plan.Table.Store;

        if (RowGone)
        {
            store.EvictCachedUniqueRowId(cacheKey);
            ServerDiagnostics.RecordRowIdCache("stale");
            return;
        }

        ServerDiagnostics.RecordRowIdCache("hit");
        store.IndexUsage?.BeginRead(indexId)?.AddEntriesRead(1);
        probe?.AddRowRead();
    }
}
