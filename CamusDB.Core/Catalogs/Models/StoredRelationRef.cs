
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Catalogs.Models;

/// <summary>
/// The form a stored view body uses to name a relation it reads: the relation's immutable id
/// wrapped in a reserved identifier, <c>__camus_rel_{id}</c>.
///
/// <para><b>Why an identifier rather than a new syntax.</b> A body is stored as SQL text and
/// re-parsed on every use, so whatever names a relation has to survive the lexer. Its identifier
/// rule is <c>[a-zA-Z_][a-zA-Z0-9_]*</c>, and every relation id in this codebase is
/// alphanumeric — a short base-62 id, or a legacy 24-character hex one — so a prefixed id lexes as
/// an ordinary identifier. Binding by id therefore needs no grammar change, no lexer change, and no
/// second storage format.</para>
///
/// <para>One token form covers tables, views and materialized views because all three draw their
/// ids from the same per-store sequence, so an id identifies a relation without saying which kind
/// it is.</para>
///
/// <para><b>The prefix is reserved.</b> A user relation actually named <c>__camus_rel_A0</c> would
/// shadow the token for relation <c>A0</c>, so <see cref="IsReservedRelationName"/> is checked
/// wherever a relation name enters the catalog. It is deliberately <i>not</i> checked for users,
/// databases, columns or indexes: those live in different namespaces and cannot collide with a
/// relation reference in a FROM clause.</para>
/// </summary>
public static class StoredRelationRef
{
    /// <summary>The reserved identifier prefix. Changing it invalidates every stored body.</summary>
    public const string Prefix = "__camus_rel_";

    /// <summary>The identifier a stored body uses for <paramref name="relationId"/>.</summary>
    public static string Format(string relationId) => Prefix + relationId;

    /// <summary>
    /// The relation id carried by <paramref name="identifier"/>, when it is one of these tokens.
    /// </summary>
    /// <remarks>
    /// The prefix is matched case-insensitively — SQL identifiers are — but the id is returned
    /// verbatim, because ids are case-sensitive: base-62 distinguishes <c>A0</c> from <c>a0</c>.
    /// Nothing but the engine writes these tokens, so the case is always the case it stored.
    /// </remarks>
    public static bool TryGetRelationId(string? identifier, out string relationId)
    {
        if (identifier is not null &&
            identifier.Length > Prefix.Length &&
            identifier.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase))
        {
            relationId = identifier[Prefix.Length..];
            return true;
        }

        relationId = "";
        return false;
    }

    /// <summary>
    /// Whether <paramref name="name"/> is a relation name the engine must refuse, because it would
    /// shadow a stored reference.
    /// </summary>
    public static bool IsReservedRelationName(string? name) =>
        name is not null && name.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Whether a stored body could contain a token at all — a substring test that decides whether
    /// re-binding is worth a tree walk.
    /// </summary>
    /// <remarks>
    /// A body written before bodies were bound by id contains no token, and the overwhelming
    /// majority of stored bodies name one or two relations, so this keeps expansion off a walk that
    /// would find nothing. It may say yes when the prefix appears somewhere that is not a FROM
    /// position (a string literal, say); the walk is what decides, and it only touches FROM.
    /// </remarks>
    public static bool MayContainReference(string? sql) =>
        sql is not null && sql.Contains(Prefix, StringComparison.OrdinalIgnoreCase);
}
