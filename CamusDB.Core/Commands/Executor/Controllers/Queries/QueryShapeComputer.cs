/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO.Hashing;
using System.Text;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Queries;
using CamusDB.Core.SQLParser;

namespace CamusDB.Core.CommandsExecutor.Controllers.Queries;

/// <summary>
/// Computes a stable, literal-independent query-shape identifier for plan-cache hooks.
///
/// The shape ID is derived from the logical structure of a <see cref="SelectQuery"/> — column
/// names, operators, clause presence, and join topology — with all literal and parameter
/// values replaced by a generic "?" placeholder. Two queries that differ only in constant
/// values (<c>WHERE id = 1</c> vs <c>WHERE id = 2</c>) produce the same shape; queries that
/// reference different columns or use different operators produce different shapes.
///
/// The ID is the 16-hex-character (64-bit) XxHash64 digest of the canonical form. XxHash is
/// non-cryptographic, which is appropriate here: the ID keys a local plan cache, not a security
/// boundary, so only a low accidental-collision probability is required. The exhaustive canonical
/// form above (every child slot rendered) is what prevents structurally distinct queries from
/// sharing a shape; the hash only guards against digest collisions.
/// </summary>
internal static class QueryShapeComputer
{
    public static string Compute(SelectQuery query)
    {
        var sb = new StringBuilder(256);
        AppendQuery(query, sb);
        return HashHex16(sb.ToString());
    }

    private static void AppendQuery(SelectQuery q, StringBuilder sb)
    {
        if (q.IsDistinct)
            sb.Append("DISTINCT ");

        AppendSource(q.Source, sb);
        sb.Append(" SELECT ");

        foreach (ProjectionItem p in q.Projections)
        {
            AppendExpr(p.Expression, sb);
            sb.Append(',');
        }

        if (q.Where is not null)
        {
            sb.Append(" WHERE ");
            AppendExpr(q.Where.Expression, sb);
        }

        if (q.GroupBy is { Count: > 0 })
        {
            sb.Append(" GROUP_BY ");
            foreach (NodeAst e in q.GroupBy)
            {
                AppendExpr(e, sb);
                sb.Append(',');
            }
        }

        if (q.Having is not null)
        {
            sb.Append(" HAVING ");
            AppendExpr(q.Having.Expression, sb);
        }

        if (q.OrderBy is { Count: > 0 })
        {
            sb.Append(" ORDER_BY ");
            foreach (OrderByItem o in q.OrderBy)
            {
                AppendExpr(o.Expression, sb);
                sb.Append(o.Direction == OrderType.Ascending ? " ASC," : " DESC,");
            }
        }

        if (q.Limit is not null)
            sb.Append(" LIMIT");

        if (q.Offset is not null)
            sb.Append(" OFFSET");
    }

    private static void AppendSource(QuerySource source, StringBuilder sb)
    {
        switch (source)
        {
            case TableSource ts:
                sb.Append("TABLE:").Append(ts.TableName);
                if (ts.Alias is not null)
                    sb.Append(" AS ").Append(ts.Alias);
                if (ts.ForcedIndexName is not null)
                    sb.Append(" FORCE_INDEX:").Append(ts.ForcedIndexName);
                break;

            case JoinSource js:
                AppendSource(js.Left, sb);
                sb.Append(" INNER_JOIN ");
                AppendSource(js.Right, sb);
                sb.Append(" ON ");
                AppendExpr(js.OnPredicate, sb);
                break;

            case DerivedTableSource dt:
                sb.Append("DERIVED(");
                AppendQuery(dt.Query, sb);
                sb.Append(") AS ").Append(dt.Alias);
                break;

            default:
                sb.Append(source.GetType().Name);
                break;
        }
    }

    // Walks an AST node, abstracting literal values to "?" and keeping structure intact.
    private static void AppendExpr(NodeAst expr, StringBuilder sb)
    {
        switch (expr.nodeType)
        {
            // Literal / parameter values → abstract to a single placeholder.
            case NodeType.Integer:
            case NodeType.Float:
            case NodeType.String:
            case NodeType.Bool:
            case NodeType.Null:
            case NodeType.ObjectIdLiteral:
            case NodeType.Placeholder:
                sb.Append('?');
                return;

            case NodeType.Identifier:
                sb.Append(expr.yytext);
                return;

            case NodeType.ExprAllFields:
                sb.Append('*');
                return;

            case NodeType.ExprAlias:
                AppendExpr(expr.leftAst!, sb);
                sb.Append(" AS ");
                // Alias name is part of shape (it affects column naming, not selectivity).
                sb.Append(expr.yytext ?? (expr.rightAst is not null ? RenderAlias(expr.rightAst) : "?"));
                return;

            case NodeType.ExprFuncCall:
                sb.Append(expr.leftAst?.yytext ?? "?").Append('(');
                if (expr.rightAst is null || expr.rightAst.nodeType == NodeType.ExprAllFields)
                    sb.Append('*');
                else
                    AppendExpr(expr.rightAst, sb);
                sb.Append(')');
                return;

            default:
                // For all binary/unary structural nodes emit type + recursive children.
                // All seven child slots are covered so no two structurally distinct nodes
                // can collapse to the same canonical string (shape-collision safety).
                sb.Append(expr.nodeType).Append('(');
                if (expr.leftAst      is not null) AppendExpr(expr.leftAst,      sb);
                if (expr.rightAst     is not null) { sb.Append(','); AppendExpr(expr.rightAst,     sb); }
                if (expr.extendedOne  is not null) { sb.Append(','); AppendExpr(expr.extendedOne,  sb); }
                if (expr.extendedTwo  is not null) { sb.Append(','); AppendExpr(expr.extendedTwo,  sb); }
                if (expr.extendedThree is not null) { sb.Append(','); AppendExpr(expr.extendedThree, sb); }
                if (expr.extendedFour  is not null) { sb.Append(','); AppendExpr(expr.extendedFour,  sb); }
                if (expr.extendedFive  is not null) { sb.Append(','); AppendExpr(expr.extendedFive,  sb); }
                sb.Append(')');
                return;
        }
    }

    private static string RenderAlias(NodeAst ast) => ast.yytext ?? ast.nodeType.ToString();

    private static string HashHex16(string canonical)
    {
        // XxHash64 produces 8 bytes → exactly 16 lowercase hex characters, matching the
        // historical 64-bit shape-ID width without the truncation the SHA-256 path needed.
        byte[] hash = XxHash64.Hash(Encoding.UTF8.GetBytes(canonical));
        return Convert.ToHexStringLower(hash);
    }
}
