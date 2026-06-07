
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

/// <summary>
/// Represents a node in the SQL parse tree produced by <see cref="SQLParserProcessor.Parse"/>.
/// </summary>
/// <remarks>
/// <b>Immutability invariant:</b> a <see cref="NodeAst"/> returned by
/// <see cref="SQLParserProcessor.Parse"/> must be treated as immutable after it is returned.
/// <see cref="SQLParser.SQLParserProcessor.Parse"/> applies <see cref="IdentifierNormalizer"/> before
/// returning, so all identifier text is already normalized. Downstream transformations
/// (<c>SubqueryRewriter</c>, binders, planners) must <b>construct new nodes</b> rather than
/// modifying fields of an existing node. This invariant is what makes sharing a single cached
/// <see cref="NodeAst"/> across concurrent executions of the same SQL text safe.
/// </remarks>
public sealed class NodeAst
{
	public NodeType nodeType;

    public NodeAst? leftAst;

    public NodeAst? rightAst;

    public NodeAst? extendedOne;

	public NodeAst? extendedTwo;

    public NodeAst? extendedThree;

    public NodeAst? extendedFour;

    public NodeAst? extendedFive;

    public NodeAst? extendedSix;

    public string? yytext;

    public NodeAst(
        NodeType nodeType,
        NodeAst? leftAst,
        NodeAst? rightAst,
        NodeAst? extendedOne,
        NodeAst? extendedTwo,
        NodeAst? extendedThree,
        NodeAst? extendedFour,
        NodeAst? extendedFive,
        string? yytext,
        NodeAst? extendedSix = null
    )
	{
		this.nodeType = nodeType;
		this.leftAst = leftAst;
		this.rightAst = rightAst;
		this.extendedOne = extendedOne;
		this.extendedTwo = extendedTwo;
        this.extendedThree = extendedThree;
        this.extendedFour = extendedFour;
        this.extendedFive = extendedFive;
        this.extendedSix = extendedSix;
        this.yytext = yytext;

        //if (leftAst is not null)
        //	Console.WriteLine("left={0}/{1}", leftAst.nodeType, leftAst.yytext);

        //if (rightAst is not null)
        //Console.WriteLine("right={0}/{1}", rightAst.nodeType, rightAst.yytext);

        //if (!string.IsNullOrEmpty(yytext))
		//	Console.WriteLine("{0}: {1}", nodeType, yytext);
	}

    public static NodeAst FromLong(long value) =>
        new(NodeType.Integer, null, null, null, null, null, null, null, value.ToString());
}
