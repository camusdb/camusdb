
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
    /// immutable by all callers. <see cref="IdentifierNormalizer.Normalize"/> is applied here,
    /// before the tree is returned, so identifiers are already lower-cased. Any transformation of
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
        IdentifierNormalizer.Normalize(ast);
        return ast;
    }

    /// <summary>
    /// Parses <paramref name="sql"/> through the supplied <paramref name="cache"/>.
    /// Returns the cached <see cref="NodeAst"/> on a hit (same reference, TTL slid);
    /// parses, normalises, stores, and returns a fresh instance on a miss.
    /// When the cache is disabled (<see cref="SqlParserCache.IsEnabled"/> is
    /// <see langword="false"/>) behaves identically to <see cref="Parse(string)"/>.
    /// </summary>
    public static NodeAst Parse(string sql, SqlParserCache cache)
    {
        if (cache.TryGet(sql, out NodeAst? cached))
            return cached!;

        sqlParser sqlParser = new();
        NodeAst ast = sqlParser.Parse(sql);
        IdentifierNormalizer.Normalize(ast);

        cache.Store(sql, ast);
        return ast;
    }
}

