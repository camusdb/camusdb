﻿
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.SQLParser;

public sealed class NodeAst
{
	public NodeType nodeType;

    public NodeAst? leftAst;

    public NodeAst? rightAst;

    public NodeAst? extendedOne;

	public NodeAst? extendedTwo;

    public NodeAst? extendedThree;

    public NodeAst? extendedFour;

    public string? yytext;

    public NodeAst(
        NodeType nodeType,
        NodeAst? leftAst,
        NodeAst? rightAst,
        NodeAst? extendedOne,
        NodeAst? extendedTwo,
        NodeAst? extendedThree,
        NodeAst? extendedFour,
        string? yytext
    )
	{
		this.nodeType = nodeType;
		this.leftAst = leftAst;
		this.rightAst = rightAst;
		this.extendedOne = extendedOne;
		this.extendedTwo = extendedTwo;
        this.extendedThree = extendedThree;
        this.extendedFour = extendedFour;
        this.yytext = yytext;

        //if (leftAst is not null)
        //	Console.WriteLine("left={0}/{1}", leftAst.nodeType, leftAst.yytext);

        //if (rightAst is not null)
        //Console.WriteLine("right={0}/{1}", rightAst.nodeType, rightAst.yytext);

        //if (!string.IsNullOrEmpty(yytext))		
		//	Console.WriteLine("{0}: {1}", nodeType, yytext);
	}
}
