/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Generic;

using NUnit.Framework;

using CamusDB.Core.SQLParser;

namespace CamusDB.Tests.SQLParser;

/// <summary>
/// Coverage for <see cref="NodeAstWalk.Replace"/>, which a correlated subquery uses to put a hidden
/// placeholder in the place of each outer name. A parsed tree is shared through the parser cache, so the
/// copy must not change it. A subtree with no replaced node is shared, a node that occurs twice is replaced
/// in both places, and a tree deeper than the call stack does not overflow it.
/// </summary>
public sealed class TestNodeAstWalkReplace
{
    private static NodeAst Identifier(string name) => new(NodeType.Identifier, null, null, null, null, null, null, null, name);

    private static NodeAst Placeholder(string name) => new(NodeType.Placeholder, null, null, null, null, null, null, null, name);

    private static NodeAst Equal(NodeAst left, NodeAst right) => new(NodeType.ExprEquals, left, right, null, null, null, null, null, null);

    private static NodeAst And(NodeAst left, NodeAst right) => new(NodeType.ExprAnd, left, right, null, null, null, null, null, null);

    private static Dictionary<NodeAst, NodeAst> Replacements(NodeAst from, NodeAst to) =>
        new(System.Collections.Generic.ReferenceEqualityComparer.Instance) { [from] = to };

    [Test]
    public void ReplacesTheNode_AndLeavesTheOriginalTreeAsItWas()
    {
        NodeAst b = Identifier("b");
        NodeAst left = Equal(b, NodeAst.FromLong(1));
        NodeAst right = Equal(Identifier("c"), NodeAst.FromLong(2));
        NodeAst root = And(left, right);
        NodeAst placeholder = Placeholder("@#outer0");

        NodeAst copy = NodeAstWalk.Replace(root, Replacements(b, placeholder));

        Assert.That(copy, Is.Not.SameAs(root));
        Assert.That(copy.leftAst!.leftAst, Is.SameAs(placeholder));
        Assert.That(root.leftAst!.leftAst, Is.SameAs(b), "the original tree must not change");
        Assert.That(copy.rightAst, Is.SameAs(right), "a subtree with no replaced node is shared");
        Assert.That(copy.leftAst.rightAst, Is.SameAs(left.rightAst));
    }

    [Test]
    public void ReplacesASharedNode_InEachPlace()
    {
        NodeAst shared = Identifier("v");
        NodeAst root = And(Equal(shared, NodeAst.FromLong(1)), Equal(NodeAst.FromLong(2), shared));
        NodeAst placeholder = Placeholder("@#outer0");

        NodeAst copy = NodeAstWalk.Replace(root, Replacements(shared, placeholder));

        Assert.That(copy.leftAst!.leftAst, Is.SameAs(placeholder));
        Assert.That(copy.rightAst!.rightAst, Is.SameAs(placeholder));
    }

    [Test]
    public void ReturnsTheSameTree_WhenNoNodeMatches()
    {
        NodeAst root = And(Equal(Identifier("a"), NodeAst.FromLong(1)), Equal(Identifier("b"), NodeAst.FromLong(2)));

        Assert.That(NodeAstWalk.Replace(root, Replacements(Identifier("a"), Placeholder("@#outer0"))), Is.SameAs(root));
    }

    [Test]
    public void ADeepTree_DoesNotOverflowTheStack()
    {
        // A left-deep spine, as a long list in the grammar builds, far deeper than a recursive walk survives.
        NodeAst leaf = Identifier("v");
        NodeAst root = leaf;

        for (int i = 0; i < 200_000; i++)
            root = And(root, NodeAst.FromLong(i));

        NodeAst placeholder = Placeholder("@#outer0");
        NodeAst copy = NodeAstWalk.Replace(root, Replacements(leaf, placeholder));

        NodeAst node = copy;
        while (node.nodeType == NodeType.ExprAnd)
            node = node.leftAst!;

        Assert.That(node, Is.SameAs(placeholder));
    }
}
