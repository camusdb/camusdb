
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Creates a new parser instance
/// </summary>
public static class SQLParserProcessor
{
    /// <summary>
    /// Parses <paramref name="sql"/> and returns the root <see cref="NodeAst"/> of the parse tree.
    /// </summary>
    /// <remarks>
    /// <b>Immutability invariant:</b> the returned <see cref="NodeAst"/> must be treated as
    /// immutable by all callers. Identifiers are preserved verbatim, in the exact case the user
    /// wrote them; case-insensitive matching happens at lookup time via case-insensitive
    /// comparers, not by folding the identifier text here. Any transformation of
    /// the tree downstream must construct new <see cref="NodeAst"/> nodes; it must not assign into
    /// the fields of a node that was returned by this method. This invariant is the prerequisite
    /// for the SQL parser AST cache: a single cached instance may be shared across many
    /// concurrent query executions of the same SQL text.
    /// <para>
    /// When <see cref="CamusDBConfig.SqlParserCacheTtlSeconds"/> is positive the result of a
    /// successful parse is stored in <see cref="SqlParserCache"/>. Subsequent calls with the same
    /// SQL text return the <b>same <see cref="NodeAst"/> reference</b> (cache hit) and extend the
    /// sliding TTL. A parse that throws is never cached; the exception propagates unchanged.
    /// When TTL is zero or negative the cache is bypassed.
    /// </para>
    /// </remarks>
    /// <summary>
    /// Parses <paramref name="sql"/> without any caching. Each call produces a fresh
    /// <see cref="NodeAst"/>. Used directly by tests and internal utilities that do not
    /// go through <see cref="CommandsExecutor.CommandExecutor"/>.
    /// </summary>
    public static NodeAst Parse(string sql)
    {
        sqlParser sqlParser = new();
        NodeAst ast = sqlParser.Parse(sql);
        return ast;
    }

    /// <summary>
    /// Parses a bare SQL condition expression (e.g. <c>price &gt; 0</c>) and returns its
    /// <see cref="NodeAst"/>. Used to rebuild the <c>ParsedCondition</c> cache on a
    /// <see cref="CamusDB.Core.Catalogs.Models.CheckConstraintSchema"/> after the stored
    /// SQL text is loaded from the KV checkpoint.
    /// <para>
    /// Internally wraps the expression as <c>SELECT 1 FROM t WHERE (&lt;expr&gt;)</c> and
    /// extracts the WHERE subtree, so any condition the engine can evaluate in a WHERE clause
    /// is also valid here.
    /// </para>
    /// </summary>
    public static NodeAst ParseCondition(string expression)
    {
        NodeAst ast = Parse($"SELECT 1 FROM t WHERE ({expression})");
        return ast.extendedOne
            ?? throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Could not parse CHECK condition expression: {expression}");
    }

    /// <summary>
    /// Parses <paramref name="sql"/> through the supplied <paramref name="cache"/>.
    /// Returns the cached <see cref="NodeAst"/> on a hit (same reference, TTL slid);
    /// parses, stores, and returns a fresh instance on a miss.
    /// When the cache is disabled (<see cref="SqlParserCache.IsEnabled"/> is
    /// <see langword="false"/>) behaves identically to <see cref="Parse(string)"/>.
    /// </summary>
    public static NodeAst Parse(string sql, SqlParserCache cache)
    {
        if (cache.TryGet(sql, out NodeAst? cached))
            return cached!;

        sqlParser sqlParser = new();
        NodeAst ast = sqlParser.Parse(sql);

        cache.Store(sql, ast);
        return ast;
    }
}

