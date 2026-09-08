
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.Routing;

/// <summary>
/// The single physical table dependency one eligible statement resolved to, captured at record
/// time so a later schema or storage change cannot alter what the statement actually touched.
/// <see cref="RowKeySpace"/> is the exact bucket prefix the placement lookup uses; the identity
/// fields feed the opaque dependency token (see <see cref="StatementRoutingResolver"/>).
/// </summary>
public sealed record StatementRoutingCandidate(
    string RowKeySpace,
    string TableId,
    string StorageId,
    long ContentsGeneration,
    int SchemaVersion);

/// <summary>
/// Per-statement accumulator for advisory routing metadata, modeled on
/// <see cref="Diagnostics.StatementProbe"/> and <see cref="Cache.CacheMetadataHolder"/>.
///
/// <para><b>It exists only when the client negotiated routing metadata.</b> The transport creates
/// one per statement and passes it on <c>ExecuteSQLTicket.Routing</c>; every engine write site is
/// a null-conditional call, so an un-negotiated request pays one null check and nothing more.</para>
///
/// <para><b>Last write wins, deliberately.</b> A transport-level serializable retry and the
/// engine's schema-fence retry both re-run the statement against the same collector; each attempt
/// overwrites the previous state, so what the transport reads after success describes the final
/// successful execution only. A statement that fails never has its collector read, because the
/// transport builds no success response for it.</para>
///
/// <para><b>Fail closed.</b> A statement kind the recorder does not understand leaves the
/// collector untouched, which the resolver turns into "no metadata". A recognized statement whose
/// footprint cannot be proven to be one ordinary hash-routed table is recorded ineligible, which
/// the resolver turns into a clear disposition. A future feature that widens a DML statement's
/// footprint beyond its own table (for example enforced foreign keys or triggers) must mark
/// ineligibility here before it ships, because unknown behavior must never advertise locality.</para>
///
/// <para><b>Not thread-safe by design.</b> One instance serves one statement; the record sites run
/// on the statement's own await chain, and the transport reads only after the statement finished.
/// The single mutable field is written with <see cref="Volatile"/> so the read after an await chain
/// crossing threads observes the final state.</para>
/// </summary>
public sealed class StatementRoutingCollector
{
    private sealed record State(StatementRoutingCandidate? Candidate, string? IneligibleReason);

    private static readonly State Untouched = new(null, null);

    private State state = Untouched;

    /// <summary>The eligible statement's single table dependency, or null.</summary>
    public StatementRoutingCandidate? Candidate => Volatile.Read(ref state).Candidate;

    /// <summary>The recorded ineligibility reason, or null when eligible or untouched.</summary>
    public string? IneligibleReason => Volatile.Read(ref state).IneligibleReason;

    /// <summary>True when no record site classified the statement — the resolver emits nothing.</summary>
    public bool IsUntouched => ReferenceEquals(Volatile.Read(ref state), Untouched);

    /// <summary>
    /// Classifies a bound SELECT. Called once per execution attempt, after binding and before the
    /// cursor runs; the shape decision needs no rows. Eligibility requires exactly one ordinary
    /// table source with no derived source, no subquery anywhere in the statement, no time-travel
    /// clause, and no result-cache hint (cache locality and data locality can prefer different
    /// nodes, and oscillating between them would make both caches cold).
    /// </summary>
    internal void RecordSelect(BoundSelectQuery bound, bool hasTimeTravel, bool hasSubquery, bool hasCacheHint)
    {
        if (hasCacheHint)
        {
            MarkIneligible(StatementRoutingAdvice.ReasonCacheAffinity);
            return;
        }

        if (bound.IsMultiSource || hasTimeTravel || hasSubquery)
        {
            MarkIneligible(StatementRoutingAdvice.ReasonIneligible);
            return;
        }

        NoteSingleTable(bound.PrimaryTable);
    }

    /// <summary>
    /// Classifies a mutation after it succeeded. Only plain INSERT, UPDATE and DELETE are
    /// candidates; INSERT … SELECT, CTAS and REFRESH involve a second relation or a storage swap
    /// and never reach this method. A subquery anywhere in the statement widens the footprint
    /// beyond the target table, so it forces ineligibility even though the rewrite already
    /// materialized part of it.
    /// </summary>
    internal void RecordDml(TableDescriptor? table, bool hasSubquery)
    {
        if (table is null || hasSubquery)
        {
            MarkIneligible(StatementRoutingAdvice.ReasonIneligible);
            return;
        }

        NoteSingleTable(table);
    }

    /// <summary>
    /// Records the single table an eligible statement touched, or marks the statement ineligible
    /// when the table itself disqualifies it: a materialized view can have its storage swapped by
    /// REFRESH mid-advice, and a branch table reads through ancestor key spaces whose placement
    /// this table's own row space does not describe.
    /// </summary>
    internal void NoteSingleTable(TableDescriptor table)
    {
        if (table.Schema.Kind != RelationKind.Table || table.Store.LineageDepth > 0)
        {
            MarkIneligible(StatementRoutingAdvice.ReasonIneligible);
            return;
        }

        StatementRoutingCandidate candidate = new(
            RowKeySpace: table.Store.RowKeySpace,
            TableId: table.Id,
            StorageId: table.Schema.EffectiveStorageId,
            ContentsGeneration: table.Schema.ContentsGeneration,
            SchemaVersion: table.Schema.Version);

        Volatile.Write(ref state, new State(candidate, null));
    }

    /// <summary>Marks the statement ineligible; the resolver answers with a clear disposition.</summary>
    internal void MarkIneligible(string reason) => Volatile.Write(ref state, new State(null, reason));

    /// <summary>
    /// True when the tree contains any subquery expression node. A whole-tree walk rather than a
    /// WHERE-only check, so a subquery in a projection or a SET expression fails closed too. The
    /// EXISTS forms are included even though DML rewrites leave them in place: they execute
    /// against another table during the scan, which is exactly the hidden footprint the
    /// eligibility rule exists to exclude.
    /// </summary>
    internal static bool ContainsSubqueryNodes(NodeAst? ast)
    {
        if (ast is null)
            return false;

        switch (ast.nodeType)
        {
            case NodeType.ExprScalarSubquery:
            case NodeType.ExprInSubquery:
            case NodeType.ExprNotInSubquery:
            case NodeType.ExprExistsSubquery:
            case NodeType.ExprExistsCorrelated:
                return true;
        }

        return ContainsSubqueryNodes(ast.leftAst)
            || ContainsSubqueryNodes(ast.rightAst)
            || ContainsSubqueryNodes(ast.extendedOne)
            || ContainsSubqueryNodes(ast.extendedTwo)
            || ContainsSubqueryNodes(ast.extendedThree)
            || ContainsSubqueryNodes(ast.extendedFour)
            || ContainsSubqueryNodes(ast.extendedFive)
            || ContainsSubqueryNodes(ast.extendedSix)
            || ContainsSubqueryNodes(ast.extendedSeven);
    }
}
