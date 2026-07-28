/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Translates the <c>CREATE USER</c> / <c>ALTER USER</c> / <c>DROP USER</c> AST nodes into their
/// tickets. Password decoding lives here, not in the grammar: the secret arrives as a
/// <see cref="NodeType.String"/> literal (unquoted here) or a <see cref="NodeType.Placeholder"/>
/// (resolved against the request's bound parameters via <see cref="SQLExecutorBaseCreator.EvalExpr"/>).
/// The cleartext is placed on the ticket and goes no further than the executor, which hashes it.
/// </summary>
internal sealed class SQLExecutorUserCreator : SQLExecutorBaseCreator
{
    private const string DefaultPlugin = "sha256_password";

    private static readonly Dictionary<string, ColumnValue> EmptyRow = new();

    internal CreateUserTicket CreateCreateUserTicket(ExecuteSQLTicket sqlTicket, NodeAst ast)
    {
        string userName = ast.leftAst!.yytext!;
        bool ifNotExists = ast.nodeType == NodeType.CreateUserIfNotExists;
        (string? plugin, string? password) = DecodeAuthClause(sqlTicket, ast);
        return new CreateUserTicket(userName, ifNotExists, plugin, password);
    }

    internal AlterUserTicket CreateAlterUserTicket(ExecuteSQLTicket sqlTicket, NodeAst ast)
    {
        string userName = ast.leftAst!.yytext!;
        (string? plugin, string? password) = DecodeAuthClause(sqlTicket, ast);

        // The grammar only admits ALTER USER with a password clause, so both are always present.
        return new AlterUserTicket(userName, plugin ?? DefaultPlugin, password ?? "");
    }

    internal DropUserTicket CreateDropUserTicket(NodeAst ast)
    {
        return new DropUserTicket(ast.leftAst!.yytext!, ast.nodeType == NodeType.DropUserIfExists);
    }

    /// <summary>
    /// Decodes the optional <c>IDENTIFIED …</c> clause. Returns <c>(null, null)</c> when there is no
    /// clause (passwordless user). The plugin name defaults to <c>sha256_password</c> for the
    /// <c>IDENTIFIED BY</c> form (no explicit <c>WITH</c>). The secret must resolve to a string value.
    /// </summary>
    private static (string? Plugin, string? Password) DecodeAuthClause(ExecuteSQLTicket sqlTicket, NodeAst ast)
    {
        if (ast.rightAst is null)
            return (null, null);

        string plugin = (ast.extendedOne?.yytext ?? DefaultPlugin).ToLowerInvariant();

        ColumnValue secret = EvalExpr(ast.rightAst, EmptyRow, sqlTicket.Parameters);
        if (secret.Type != ColumnType.String)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                "Password must be a string value");

        return (plugin, secret.StrValue ?? "");
    }
}
