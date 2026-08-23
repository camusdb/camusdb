
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Models.Queries;

/// <summary>
/// Per-database cache of bound SELECT statements, keyed by the statement's root <see cref="NodeAst"/>.
///
/// <para><b>Why the AST is the key.</b> The SQL parser cache returns the same <see cref="NodeAst"/>
/// instance for repeated identical statement text, and every artifact this cache stores is derived
/// from that instance alone (plus catalog state, which the stamps below guard). Keying by the AST
/// reference makes the lookup one weak-table probe, and it bounds each entry's lifetime by the parse
/// cache: a statement evicted from the parse cache drops its bound artifacts with it. An AST that is
/// not reference-stable (for example, one produced by view expansion) simply never hits.</para>
///
/// <para><b>Why the table lives on the <see cref="DatabaseDescriptor"/>.</b> A slot holds strong
/// references to a <see cref="TableDescriptor"/> and the bound sources built over it. Hanging the
/// weak table off the database descriptor makes the descriptor's lifetime an upper bound for every
/// slot: when a database is closed, dropped, or idle-evicted, its whole bound-query cache becomes
/// unreachable with it, so a slot can never keep a closed database's objects alive. It also makes
/// database identity implicit — the same SQL text executed against two databases uses two
/// independent caches, and a dropped-and-recreated database starts empty because it starts with a
/// fresh descriptor.</para>
///
/// <para><b>Validation contract.</b> A slot must never be served without the consumer re-validating
/// it: the current table descriptor must be the same instance the slot was built over, and the
/// schema stamp (<see cref="Catalogs.Models.TableSchema.Version"/> plus
/// <see cref="Catalogs.Models.TableSchema.ContentsGeneration"/>) must be unchanged. A rename
/// rewrites schema history in place and a materialized-view refresh swaps columns while reusing
/// version numbers — each bumps one of the two stamp fields, which is the same pair the row decode
/// plan cache relies on. On any mismatch the consumer rebinds and replaces the slot wholesale;
/// over-invalidation is the simple safe choice because DDL is rare.</para>
///
/// <para><b>Concurrency.</b> Slot creation is a benign race: two first executions may both bind and
/// both store, and the last writer wins. Both slots are correct for their stamps, so either result
/// is safe to serve. Reads are lock-free.</para>
/// </summary>
public sealed class BoundQueryCache
{
    private readonly ConditionalWeakTable<NodeAst, BoundQuerySlot> slots = new();

    /// <summary>Returns the slot stored for <paramref name="ast"/>, or null when none exists.</summary>
    internal BoundQuerySlot? TryGet(NodeAst ast) =>
        slots.TryGetValue(ast, out BoundQuerySlot? slot) ? slot : null;

    /// <summary>Stores or replaces the slot for <paramref name="ast"/>. Last writer wins.</summary>
    internal void Store(NodeAst ast, BoundQuerySlot slot) => slots.AddOrUpdate(ast, slot);
}

/// <summary>
/// One cached bound statement, or a permanent bypass marker for a statement shape the cache does not
/// support. The supported shape is a single-table, no-subquery SELECT with no cache hint, no
/// <c>AS OF SYSTEM TIME</c> clause, and no session-scoped function call: exactly the shape whose
/// binding is a pure function of the AST and the stamped schema. Everything the shape excludes either
/// bakes per-execution state into the query record (a rewritten subquery materializes data values) or
/// is a multi-source form (joins, views, derived tables) whose binding this cache does not reuse yet.
/// </summary>
internal sealed class BoundQuerySlot
{
    /// <summary>
    /// The single shared marker for permanently ineligible statement shapes. Shape eligibility is a
    /// function of the AST alone, so one immutable instance serves every ineligible statement and a
    /// later execution skips the shape analysis entirely.
    /// </summary>
    internal static readonly BoundQuerySlot Ineligible = new();

    private BoundQuerySlot() { }

    internal BoundQuerySlot(
        TableDescriptor table,
        BoundSelectQuery bound,
        int schemaVersion,
        long contentsGeneration)
    {
        Table = table;
        Bound = bound;
        SchemaVersion = schemaVersion;
        ContentsGeneration = contentsGeneration;
        RequiredColumns = new SingleTableRequiredColumnsMemo();
    }

    /// <summary>False only on the shared <see cref="Ineligible"/> marker.</summary>
    internal bool Eligible => Bound is not null;

    /// <summary>
    /// The table descriptor the statement was bound over. A hit requires the freshly opened
    /// descriptor to be this same instance: descriptor eviction (drop, index DDL, element-state
    /// changes) replaces the instance, so a reference mismatch fails closed and forces a rebind.
    /// </summary>
    internal TableDescriptor? Table { get; }

    /// <summary>Schema version stamp captured at bind time; see the class contract.</summary>
    internal int SchemaVersion { get; }

    /// <summary>Contents-generation stamp captured at bind time; see the class contract.</summary>
    internal long ContentsGeneration { get; }

    /// <summary>
    /// The bound statement. Its <see cref="BoundSelectQuery.RowNames"/> resolver is frozen before the
    /// slot is published, so concurrent executions share it read-only.
    /// </summary>
    internal BoundSelectQuery? Bound { get; }

    /// <summary>
    /// Cross-execution memo for the single-table required-column analysis. Safe to share because the
    /// eligible shape carries no API filters, no semi-join specs, and no prepared EXISTS subqueries —
    /// the only ticket inputs that could make the analysis differ between executions.
    /// </summary>
    internal SingleTableRequiredColumnsMemo? RequiredColumns { get; }
}

/// <summary>
/// A write-once memo for the result of the single-table required-column analysis. The analysis
/// result may legitimately be null ("decode all columns"), so presence is tracked by a separate
/// flag rather than by a null check. The value is written before the volatile flag, so a reader
/// that observes the flag also observes the value.
/// </summary>
public sealed class SingleTableRequiredColumnsMemo
{
    private IReadOnlySet<string>? value;

    private volatile bool computed;

    /// <summary>True when a result was stored; <paramref name="required"/> then carries it (possibly null).</summary>
    internal bool TryGet(out IReadOnlySet<string>? required)
    {
        if (computed)
        {
            required = value;
            return true;
        }

        required = null;
        return false;
    }

    /// <summary>
    /// Stores the analysis result. The analysis is deterministic for one bound statement, so a
    /// concurrent duplicate store writes an equivalent value and the race is benign.
    /// </summary>
    internal void Set(IReadOnlySet<string>? required)
    {
        value = required;
        computed = true;
    }
}
