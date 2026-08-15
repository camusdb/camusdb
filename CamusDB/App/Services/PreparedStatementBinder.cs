
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.SQLParser;

namespace CamusDB.App.Services;

/// <summary>
/// The rules a prepared statement is registered and executed by, shared verbatim by the gRPC and
/// REST surfaces.
///
/// <para>Both transports route through this type so their behavior cannot drift: a client that
/// switches transports must get the same acceptance decisions, the same arity checks, and the same
/// error codes and wording. The two surfaces differ only in what owns a handle (a duplex stream vs
/// a node-local registry), never in what a handle <em>means</em>.</para>
/// </summary>
public static class PreparedStatementBinder
{
    /// <summary>
    /// True for the statements PREPARE accepts. This is deliberately an allow-list of the repeatable
    /// data statements — the ones whose whole point is being executed many times with different
    /// values — rather than a deny-list of DDL. A statement type added to the grammar later is then
    /// refused by default instead of silently becoming preparable before anyone has considered what
    /// caching its parsed form across executions would mean.
    ///
    /// <para>Schema and database/user administration are excluded: they are one-shot, they are
    /// routed by endpoint rather than by handle, and several of them return no database descriptor,
    /// so nothing about them benefits from a handle.</para>
    /// </summary>
    public static bool IsPreparable(NodeType rootType) => rootType is
        NodeType.Select or
        NodeType.Insert or
        NodeType.Update or
        NodeType.Delete or
        NodeType.ShowColumns or
        NodeType.ShowTables or
        NodeType.ShowCreateTable or
        NodeType.ShowDatabase or
        NodeType.ShowDatabases or
        NodeType.ShowBranches or
        NodeType.ShowAncestors or
        NodeType.ShowOrphanTables or
        NodeType.ShowOrphanDatabases or
        NodeType.ShowIndexes or
        NodeType.ShowStatistics or
        NodeType.ShowGrants;

    /// <summary>
    /// Parses <paramref name="sql"/>, checks it may be prepared, and builds the entry that both
    /// registries store. Parse failures surface here, at prepare time, rather than being deferred to
    /// whichever execution happens to be the first — a client learns its statement is broken when it
    /// registers it.
    /// </summary>
    public static PreparedStatement Create(string database, string sql)
    {
        if (string.IsNullOrEmpty(sql))
            throw new CamusDBException(CamusDBErrorCodes.InvalidInput, "PREPARE requires a 'sql' statement");

        NodeAst ast = SQLParserProcessor.Parse(sql);

        if (!IsPreparable(ast.nodeType))
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Only SELECT, INSERT, UPDATE, DELETE and SHOW statements can be prepared; " +
                "schema and database administration statements must be sent inline");

        return new PreparedStatement(database, sql, ast.nodeType, PlaceholderCollector.Collect(ast));
    }

    /// <summary>
    /// Rejects an execution that names a prepared statement and <em>also</em> carries the inline
    /// fields the handle already stands for. Silently preferring one over the other would let a
    /// client believe it was running SQL that the server never looked at, so the ambiguity is an
    /// error rather than a precedence rule.
    /// </summary>
    public static void ValidateNoInlineFields(bool hasSql, bool hasDatabase, bool hasParameters)
    {
        if (hasSql || hasDatabase || hasParameters)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "A prepared execution must not carry 'sql', 'database' or named 'parameters' — " +
                "the statement id already names all three");
    }

    /// <summary>
    /// Rehydrates the engine-facing named parameter dictionary by zipping the caller's ordinal
    /// values with the statement's published name order, converting each value through
    /// <paramref name="convert"/> so a transport can pass its own wire value type without
    /// materializing an intermediate list.
    ///
    /// <para>Returns <see langword="null"/> for a statement that declares no parameters, which is
    /// exactly what the inline path passes for an empty parameter map — so everything downstream of
    /// here cannot tell a prepared execution from an inline one.</para>
    /// </summary>
    public static Dictionary<string, ColumnValue>? Bind<TValue>(
        PreparedStatement statement,
        IReadOnlyList<TValue>? values,
        Func<TValue, ColumnValue> convert)
    {
        int supplied = values?.Count ?? 0;
        if (supplied != statement.ParameterCount)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"Prepared statement declares {statement.ParameterCount} parameter(s) " +
                $"({string.Join(", ", statement.ParameterNames)}) but {supplied} value(s) were supplied");

        if (statement.ParameterCount == 0)
            return null;

        Dictionary<string, ColumnValue> parameters = new(statement.ParameterCount, StringComparer.Ordinal);
        for (int i = 0; i < statement.ParameterCount; i++)
            parameters[statement.ParameterNames[i]] = convert(values![i]);

        return parameters;
    }

    /// <summary>
    /// The one error raised for every way a handle can fail to resolve — closed, expired, prepared
    /// on another stream or node, or owned by another principal. They share a code and a message on
    /// purpose: an ownership-specific error would tell a caller that a handle it does not own
    /// nevertheless exists.
    /// </summary>
    /// <param name="diagnostic">
    /// Optional non-identifying context appended to the message (for example that the handle was
    /// minted by a different process). Must never reveal whether some other principal holds it.
    /// </param>
    public static CamusDBException UnknownStatement(string? diagnostic = null)
        => new(
            CamusDBErrorCodes.UnknownPreparedStatement,
            diagnostic is null
                ? "Unknown prepared statement id; prepare the statement again and retry"
                : $"Unknown prepared statement id ({diagnostic}); prepare the statement again and retry");

    /// <summary>Raised when a caller is over its prepared-statement cap; never evicts a live handle.</summary>
    public static CamusDBException LimitExceeded(long limit, string scope)
        => new(
            CamusDBErrorCodes.PreparedStatementLimitExceeded,
            $"Too many prepared statements ({scope} limit is {limit}); close the ones no longer in use");

    /// <summary>Raised when a handle arrives on a path that has no session to scope it to.</summary>
    public static CamusDBException NotSupportedHere(string path)
        => new(
            CamusDBErrorCodes.InvalidInput,
            $"Prepared statements are not supported on {path}");
}
