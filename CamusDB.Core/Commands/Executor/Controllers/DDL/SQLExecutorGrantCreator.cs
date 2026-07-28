/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.CommandsExecutor.Controllers.DML;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.DDL;

/// <summary>
/// Translates the <c>GRANT</c> / <c>REVOKE</c> AST into a <see cref="GrantTicket"/>: folds the
/// privilege-list chain into a <see cref="Privilege"/> bitmask and decodes the scope from the node's
/// marker (<c>global</c>/<c>database</c>/<c>table</c>) and object identifier. The object id resolution
/// (name → immutable database/table id) is deferred to the executor, which can open the target
/// database's catalog; the creator only carries names.
/// </summary>
internal sealed class SQLExecutorGrantCreator : SQLExecutorBaseCreator
{
    internal GrantTicket CreateGrantTicket(NodeAst ast)
    {
        bool revoke = ast.nodeType == NodeType.Revoke;
        string userName = ast.rightAst!.yytext!;
        Privilege privileges = CollectPrivileges(ast.leftAst!);

        switch (ast.yytext)
        {
            case "global":
                return new GrantTicket(userName, privileges, GrantScopeKind.Global, "", "", revoke);

            case "database":
                return new GrantTicket(userName, privileges, GrantScopeKind.Database, ast.extendedOne!.yytext!, "", revoke);

            case "table":
                {
                    (string databaseName, string tableName) = SplitQualified(ast.extendedOne!.yytext!);
                    return new GrantTicket(userName, privileges, GrantScopeKind.Table, databaseName, tableName, revoke);
                }

            default:
                throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unknown grant scope marker: " + ast.yytext);
        }
    }

    /// <summary>Walks the left-recursive privilege list and unions each leaf into one bitmask.</summary>
    private static Privilege CollectPrivileges(NodeAst node)
    {
        if (node.nodeType == NodeType.GrantPrivilegeList)
            return CollectPrivileges(node.leftAst!) | CollectPrivileges(node.rightAst!);

        if (node.nodeType == NodeType.GrantPrivilege)
            return MapPrivilege(node.yytext!);

        throw new CamusDBException(CamusDBErrorCodes.InvalidAstStmt, "Unexpected privilege node: " + node.nodeType);
    }

    private static Privilege MapPrivilege(string token) => token switch
    {
        "select" => Privilege.Select,
        "insert" => Privilege.Insert,
        "update" => Privilege.Update,
        "delete" => Privilege.Delete,
        "create table" => Privilege.CreateTable,
        "drop" => Privilege.Drop,
        "alter" => Privilege.Alter,
        "index" => Privilege.Index,
        "create" => Privilege.Create,
        "all" => Privilege.All,
        _ => throw new CamusDBException(CamusDBErrorCodes.InvalidPrivilege, "Unknown privilege: " + token),
    };

    /// <summary>Splits a required <c>db.table</c> reference; rejects a bare or over-qualified name.</summary>
    private static (string DatabaseName, string TableName) SplitQualified(string name)
    {
        int dot = name.IndexOf('.');

        if (dot <= 0 || dot == name.Length - 1 || name.IndexOf('.', dot + 1) >= 0)
            throw new CamusDBException(
                CamusDBErrorCodes.InvalidInput,
                $"GRANT/REVOKE on a table requires a database-qualified name ('<database>.<table>'), got '{name}'");

        return (name[..dot], name[(dot + 1)..]);
    }
}
