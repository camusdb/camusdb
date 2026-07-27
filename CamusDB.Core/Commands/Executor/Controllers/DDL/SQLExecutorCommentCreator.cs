/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Translates the four <c>COMMENT ON</c> AST nodes into <see cref="CommentTicket"/> instances.
///
/// <para>Two decoding responsibilities live here rather than in the grammar. First, the lexer keeps
/// the surrounding quotes in the literal's <c>yytext</c>, so the text is unquoted and un-doubled
/// here; a missing <c>rightAst</c> (the parser's encoding for <c>IS NULL</c>) becomes a null
/// <see cref="CommentTicket.Comment"/>, which downstream means "remove". Second, the grammar folds a
/// qualified name into a single identifier whose <c>yytext</c> is the dotted text, so the
/// <c>table.column</c> / <c>table.index</c> forms are split here — and the unqualified form is
/// rejected here, since CamusDB has no global column or index namespace to resolve a bare name
/// against.</para>
/// </summary>
internal sealed class SQLExecutorCommentCreator : SQLExecutorBaseCreator
{
    internal CommentTicket CreateCommentTicket(ExecuteSQLTicket sqlTicket, NodeAst ast)
    {
        string? comment = DecodeComment(ast.rightAst);
        string name = ast.leftAst!.yytext!;

        switch (ast.nodeType)
        {
            case NodeType.CommentOnTable:
                return new CommentTicket(CommentTarget.Table, sqlTicket.DatabaseName, name, null, comment);

            case NodeType.CommentOnDatabase:
                return new CommentTicket(CommentTarget.Database, name, null, null, comment);

            case NodeType.CommentOnColumn:
                {
                    (string tableName, string columnName) = SplitQualified(name, "column");
                    return new CommentTicket(CommentTarget.Column, sqlTicket.DatabaseName, tableName, columnName, comment);
                }

            case NodeType.CommentOnIndex:
                {
                    (string tableName, string indexName) = SplitQualified(name, "index");
                    return new CommentTicket(CommentTarget.Index, sqlTicket.DatabaseName, tableName, indexName, comment);
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown COMMENT ON AST stmt: " + ast.nodeType);
        }
    }

    /// <summary>
    /// Returns the literal's decoded text, or null when the statement said <c>IS NULL</c> — which the
    /// grammar encodes as an absent node so that it stays distinguishable from <c>IS ''</c>.
    /// </summary>
    private static string? DecodeComment(NodeAst? valueAst)
    {
        if (valueAst is null)
            return null;

        return UnquoteStringLiteral(valueAst.yytext ?? "");
    }

    /// <summary>
    /// Splits a <c>table.element</c> reference. Anything else — a bare name, or more than one dot —
    /// is a caller mistake rather than a lookup failure, so it is rejected before execution.
    /// </summary>
    private static (string TableName, string ElementName) SplitQualified(string name, string elementKind)
    {
        int dot = name.IndexOf('.');

        if (dot <= 0 || dot == name.Length - 1 || name.IndexOf('.', dot + 1) >= 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"COMMENT ON {elementKind.ToUpperInvariant()} requires a table-qualified name " +
                $"('<table>.<{elementKind}>'), got '{name}'");

        return (name[..dot], name[(dot + 1)..]);
    }
}
