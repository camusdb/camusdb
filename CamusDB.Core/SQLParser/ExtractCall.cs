
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// The SQL-standard <c>EXTRACT(field FROM value)</c> form, as a call to the scalar function
/// <c>extract('field', value)</c>, which is an alias of <c>date_part</c>.
///
/// <para><b>Why a plain call node and not a node type of its own.</b> Every expression walker — the
/// evaluators, the type inference, the planner analyzers, the query-fragment wire codec — already
/// handles <see cref="NodeType.ExprFuncCall"/>. A call node gives EXTRACT all of them at once, and
/// <c>extract('year', x)</c> written as a call means the same thing.</para>
///
/// <para><b>Why EXTRACT is not a keyword.</b> The grammar matches the name as an identifier, so a
/// table or column named <c>extract</c> still parses. <see cref="Build"/> therefore receives any name
/// that is followed by <c>(word FROM</c> and must reject every name other than <c>extract</c>.</para>
///
/// <para>The renderers use <see cref="TryDeconstruct"/> to print the call back in the
/// <c>EXTRACT(field FROM value)</c> form, which re-parses to the same node.</para>
/// </summary>
internal static class ExtractCall
{
    public const string FunctionName = "extract";

    /// <summary>
    /// Builds the call node for <c>name(field FROM source)</c>. The field is kept as a string literal
    /// argument; whether it names a supported unit is checked when the call is evaluated, with the
    /// same error as <c>date_part</c>.
    /// </summary>
    public static NodeAst Build(NodeAst name, string? field, NodeAst source)
    {
        if (!string.Equals(name.yytext, FunctionName, StringComparison.OrdinalIgnoreCase))
            throw new CamusDBException(
                CamusDBErrorCodes.SqlSyntaxError,
                $"FROM is not valid in the arguments of '{name.yytext}'. Only EXTRACT(field FROM value) takes this form");

        if (string.IsNullOrWhiteSpace(field))
            throw new CamusDBException(
                CamusDBErrorCodes.SqlSyntaxError,
                "EXTRACT expects a field name, as in EXTRACT(year FROM value)");

        NodeAst fieldLiteral = new(NodeType.String, null, null, null, null, null, null, null, SqlStringLiteral.Quote(field));
        NodeAst arguments = new(NodeType.ExprArgumentList, fieldLiteral, source, null, null, null, null, null, null);

        return new NodeAst(NodeType.ExprFuncCall, name, arguments, null, null, null, null, null, null);
    }

    /// <summary>
    /// True when <paramref name="call"/> is a two-argument <c>extract</c> call whose first argument is
    /// a string literal, which is every node <see cref="Build"/> makes and the same call written as
    /// <c>extract('field', value)</c>. <paramref name="field"/> is the decoded literal.
    /// </summary>
    public static bool TryDeconstruct(NodeAst call, out string field, out NodeAst source)
    {
        field = "";
        source = null!;

        if (call.nodeType != NodeType.ExprFuncCall
            || !string.Equals(call.leftAst?.yytext, FunctionName, StringComparison.OrdinalIgnoreCase))
            return false;

        NodeAst? arguments = call.rightAst;
        if (arguments is not { nodeType: NodeType.ExprArgumentList, leftAst.nodeType: NodeType.String, rightAst: not null })
            return false;

        field = SqlStringLiteral.Decode(arguments.leftAst.yytext ?? "");
        source = arguments.rightAst;
        return true;
    }

    /// <summary>
    /// True when <paramref name="field"/> can be printed as a bare word after <c>EXTRACT(</c>: it must
    /// lex as exactly one plain identifier. A keyword (<c>EXTRACT('from' FROM x)</c>) or any other
    /// text would not re-parse as a bare word, so the renderers print it as a string literal instead,
    /// which the grammar also accepts.
    /// </summary>
    public static bool IsBareWord(string field)
    {
        if (field.Length == 0)
            return false;

        sqlScanner scanner = new();
        scanner.SetSource(field, 0);

        return scanner.yylex() == (int)Token.TIDENTIFIER
               && scanner.yylex() == (int)Token.EOF
               && string.IsNullOrEmpty(scanner.YYError);
    }
}
