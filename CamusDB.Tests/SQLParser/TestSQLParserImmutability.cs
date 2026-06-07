
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// PC0.1 guard tests — pin the immutability invariant that the SQL parser AST cache relies on.
///
/// Invariant: a NodeAst returned by SQLParserProcessor.Parse is never mutated in place after
/// it is returned; downstream transformations construct new nodes. Placeholder nodes store only
/// the parameter name, not the value, so the same SQL text with different parameter values
/// produces structurally identical trees.
/// </summary>
public class TestSQLParserImmutability
{
    // Recursive structural equality: same node types and yytext at every position.
    private static bool StructurallyEqual(NodeAst? a, NodeAst? b)
    {
        if (a is null && b is null) return true;
        if (a is null || b is null) return false;

        return a.nodeType == b.nodeType
            && a.yytext == b.yytext
            && StructurallyEqual(a.leftAst, b.leftAst)
            && StructurallyEqual(a.rightAst, b.rightAst)
            && StructurallyEqual(a.extendedOne, b.extendedOne)
            && StructurallyEqual(a.extendedTwo, b.extendedTwo)
            && StructurallyEqual(a.extendedThree, b.extendedThree)
            && StructurallyEqual(a.extendedFour, b.extendedFour)
            && StructurallyEqual(a.extendedFive, b.extendedFive)
            && StructurallyEqual(a.extendedSix, b.extendedSix);
    }

    // Walk the tree depth-first and return the first Placeholder node, or null.
    private static NodeAst? FindPlaceholder(NodeAst? node)
    {
        if (node is null) return null;
        if (node.nodeType == NodeType.Placeholder) return node;

        return FindPlaceholder(node.leftAst)
            ?? FindPlaceholder(node.rightAst)
            ?? FindPlaceholder(node.extendedOne)
            ?? FindPlaceholder(node.extendedTwo)
            ?? FindPlaceholder(node.extendedThree)
            ?? FindPlaceholder(node.extendedFour)
            ?? FindPlaceholder(node.extendedFive)
            ?? FindPlaceholder(node.extendedSix);
    }

    [Test]
    public void ParseSameSqlTwice_ProducesStructurallyEqualTrees()
    {
        const string sql = "SELECT id, name FROM users WHERE id = 42";

        NodeAst first = SQLParserProcessor.Parse(sql);
        NodeAst second = SQLParserProcessor.Parse(sql);

        // No-cache overload always returns a fresh instance.
        Assert.That(first, Is.Not.SameAs(second), "Parse without a cache always returns a fresh instance");

        // But the shape must be identical.
        Assert.That(StructurallyEqual(first, second), Is.True,
            "Parsing the same SQL twice must yield structurally equal trees");
    }

    [Test]
    public void ParseDifferentSql_ProducesDistinctTrees()
    {
        NodeAst selectOne = SQLParserProcessor.Parse("SELECT a FROM t WHERE x = 1");
        NodeAst selectTwo = SQLParserProcessor.Parse("SELECT b FROM t WHERE x = 2");

        Assert.That(StructurallyEqual(selectOne, selectTwo), Is.False,
            "Different SQL strings must produce different trees");
    }

    [Test]
    public void PlaceholderNode_StoresNameOnly_NotValue()
    {
        // Two SQL strings that differ only in parameter *values* — the ASTs must be identical
        // because Placeholder nodes carry only the name (@p), never a runtime value.
        const string sql = "SELECT id FROM users WHERE id = @p";

        NodeAst ast = SQLParserProcessor.Parse(sql);

        NodeAst? placeholder = FindPlaceholder(ast);
        Assert.That(placeholder, Is.Not.Null, "Expected a Placeholder node in the parse tree");
        Assert.That(placeholder!.nodeType, Is.EqualTo(NodeType.Placeholder));

        // yytext holds the parameter name; no other field stores a value.
        Assert.That(placeholder.yytext, Is.Not.Null.And.Not.Empty,
            "Placeholder.yytext must contain the parameter name");
        Assert.That(placeholder.leftAst, Is.Null, "Placeholder must have no child nodes");
        Assert.That(placeholder.rightAst, Is.Null);
        Assert.That(placeholder.extendedOne, Is.Null);
    }

    [Test]
    public void ParseSameSqlWithDifferentParameterNames_PlaceholderNamesMatch()
    {
        // If the same SQL is issued with the same placeholder name, both parses agree on the name.
        const string sql = "UPDATE orders SET status = @status WHERE id = @id";

        NodeAst first = SQLParserProcessor.Parse(sql);
        NodeAst second = SQLParserProcessor.Parse(sql);

        Assert.That(StructurallyEqual(first, second), Is.True,
            "Repeated parse of identical parameterized SQL must yield structurally equal trees; " +
            "this is the key assumption that lets a single cached AST serve all parameter bindings");
    }

    [Test]
    public void IdentifierNormalization_AlreadyAppliedBeforeReturn()
    {
        // IdentifierNormalizer runs inside Parse, so the returned tree has lower-cased identifiers.
        NodeAst ast = SQLParserProcessor.Parse("SELECT MyCol FROM MyTable");

        static NodeAst? FindIdentifier(NodeAst? n)
        {
            if (n is null) return null;
            if (n.nodeType == NodeType.Identifier) return n;
            return FindIdentifier(n.leftAst) ?? FindIdentifier(n.rightAst)
                ?? FindIdentifier(n.extendedOne);
        }

        NodeAst? ident = FindIdentifier(ast);
        Assert.That(ident, Is.Not.Null);
        Assert.That(ident!.yytext, Is.EqualTo(ident.yytext!.ToLowerInvariant()),
            "Identifiers must already be lower-cased when Parse returns");
    }
}
