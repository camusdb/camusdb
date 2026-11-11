/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

#if !CAMUSDB_BROWSER
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
#endif

namespace CamusDB.Core.Extensions.Inline;

/// <summary>
/// Tells whether the C# body of an inline trigger names a column or an inline function, so that
/// <c>DROP COLUMN</c>, <c>RENAME COLUMN</c>, <c>DROP FUNCTION</c> and a replace that changes a
/// function's types can refuse while a trigger body uses the name. Without the check, those statements
/// succeed and every later write to the table fails, because the body no longer compiles.
///
/// <para><b>Syntax only, and on the safe side.</b> The check reads the parsed body and no semantic
/// model, so it runs under the schema lock in microseconds and gives the same answer on every node. A
/// column matches an identifier of that name in any position (<c>NEW.total</c>, <c>OLD.@class</c>) and
/// a string literal that is exactly the name (<c>Context.New.GetString("total")</c>). A function
/// matches an identifier of that name that is not the member part of <c>a.b</c>: a body calls an
/// inline function by its plain name. Both compare the names without case, as SQL names compare. A
/// local variable with the name of a column or a function therefore also matches; the refusal message
/// names the trigger, and the user drops it or renames the local.</para>
///
/// <para><b>Not covered.</b> A change of a column's type can also stop a body from compiling, and is
/// not checked. A body that does not parse matches nothing: such a body was refused at
/// <c>CREATE TRIGGER</c>.</para>
///
/// <para>The browser build has no inline triggers, so there the check never matches.</para>
/// </summary>
internal static class InlineTriggerBodyReferences
{
    /// <summary>True when <paramref name="body"/> names the column <paramref name="column"/>; see the class summary.</summary>
    public static bool NamesColumn(string body, string column)
    {
#if CAMUSDB_BROWSER
        return false;
#else
        if (Parse(body) is not { } root)
            return false;

        foreach (SyntaxToken token in root.DescendantTokens())
        {
            if ((token.IsKind(SyntaxKind.IdentifierToken) || token.IsKind(SyntaxKind.StringLiteralToken)) &&
                string.Equals(token.ValueText, column, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
#endif
    }

    /// <summary>True when <paramref name="body"/> names the inline function <paramref name="function"/>; see the class summary.</summary>
    public static bool NamesFunction(string body, string function)
    {
#if CAMUSDB_BROWSER
        return false;
#else
        if (Parse(body) is not { } root)
            return false;

        foreach (IdentifierNameSyntax name in root.DescendantNodes().OfType<IdentifierNameSyntax>())
        {
            if (name.Parent is MemberAccessExpressionSyntax access && access.Name == name)
                continue;

            if (string.Equals(name.Identifier.ValueText, function, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
#endif
    }

#if !CAMUSDB_BROWSER
    private static readonly CSharpParseOptions ParseOptions = new(InlineFunctionCompiler.Language, DocumentationMode.None, SourceCodeKind.Regular);

    /// <summary>The body as the compiler parses it: the block of an async method. Null when it does not parse.</summary>
    private static BlockSyntax? Parse(string body)
    {
        List<InlineFunctionFinding> errors = [];
        BlockSyntax? block = InlineFunctionSyntaxChecker.Parse(body, ParseOptions, errors, async: true);
        return errors.Count == 0 ? block : null;
    }
#endif
}
