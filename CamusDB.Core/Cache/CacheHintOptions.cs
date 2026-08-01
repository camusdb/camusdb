
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Cache;

/// <summary>
/// Parsed options extracted from a <c>{cache=name}</c> table-reference hint.
///
/// <para>The hint is placed immediately after the table name (and optional alias) in a
/// <c>FROM</c> clause: <c>FROM orders {cache=recent_orders}</c>. It opts the enclosing
/// autocommit SELECT into the query-result cache under the given family name.</para>
///
/// <para><b>CacheName</b> is the logical cache family name. All queries that share the same
/// name, database, and result fingerprint compete for the same cached entry. Names are
/// lowercased at parse time by the grammar's <c>identifier</c> production (via
/// <c>ToLowerInvariant</c>), so comparisons are case-insensitive and locale-independent
/// at the SQL level.</para>
///
/// <para><b>TtlMs</b> overrides the server-default TTL for this specific entry. When null,
/// <see cref="CamusDB.Core.CamusDBOptions.QueryResultCacheDefaultTtlMs"/> applies.</para>
///
/// <para><b>IsStrict</b>: when true, a cache hit is only served if the entry's
/// <c>CachedAt</c> HLC timestamp is causally after the current session's most-recent
/// committed write. Cross-node staleness is bounded by TTL when strict is false.</para>
/// </summary>
public sealed record CacheHintOptions(
    string CacheName,
    int? TtlMs = null,
    bool IsStrict = false);
