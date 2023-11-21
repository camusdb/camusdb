// This code was generated by the Gardens Point Parser Generator
// Copyright (c) Wayne Kelly, John Gough, QUT 2005-2014
// (see accompanying GPPGcopyright.rtf)

// GPPG version 1.5.2
// DateTime: 11/21/2023 9:15:09 AM
// Input file <SQLParser\SQLParser.Language.grammar.y - 11/21/2023 9:15:04 AM>

// options: no-lines gplex

using System;
using System.Collections.Generic;
using System.CodeDom.Compiler;
using System.Globalization;
using System.Text;
using QUT.Gppg;

namespace CamusDB.Core.SQLParser
{
internal enum Token {error=2,EOF=3,TOR=4,TAND=5,TEQUALS=6,
    TNOTEQUALS=7,TLESSTHAN=8,TGREATERTHAN=9,TLESSTHANEQUALS=10,TGREATERTHANEQUALS=11,TADD=12,
    TMINUS=13,TDIGIT=14,TSTRING=15,IDENTIFIER=16,LPAREN=17,RPAREN=18,
    TCOMMA=19,TMULT=20,TDIV=21,TSELECT=22,TFROM=23,TWHERE=24,
    TORDER=25,TBY=26};

internal partial struct ValueType
{ 
        public NodeAst n;
        public string s;
}
// Abstract base class for GPLEX scanners
[GeneratedCodeAttribute( "Gardens Point Parser Generator", "1.5.2")]
internal abstract class ScanBase : AbstractScanner<ValueType,LexLocation> {
  private LexLocation __yylloc = new LexLocation();
  public override LexLocation yylloc { get { return __yylloc; } set { __yylloc = value; } }
  protected virtual bool yywrap() { return true; }
}

// Utility class for encapsulating token information
[GeneratedCodeAttribute( "Gardens Point Parser Generator", "1.5.2")]
internal class ScanObj {
  public int token;
  public ValueType yylval;
  public LexLocation yylloc;
  public ScanObj( int t, ValueType val, LexLocation loc ) {
    this.token = t; this.yylval = val; this.yylloc = loc;
  }
}

[GeneratedCodeAttribute( "Gardens Point Parser Generator", "1.5.2")]
internal partial class sqlParser: ShiftReduceParser<ValueType, LexLocation>
{
#pragma warning disable 649
  private static Dictionary<int, string> aliases;
#pragma warning restore 649
  private static Rule[] rules = new Rule[29];
  private static State[] states = new State[45];
  private static string[] nonTerms = new string[] {
      "list", "$accept", "stat", "select_stmt", "identifier_list", "identifier", 
      "condition", "equals_expr", "not_equals_expr", "less_than_expr", "greater_than_expr", 
      "and_expr", "or_expr", "simple_expr", "number", "string", };

  static sqlParser() {
    states[0] = new State(new int[]{22,5},new int[]{-1,1,-3,3,-4,4});
    states[1] = new State(new int[]{3,2});
    states[2] = new State(-1);
    states[3] = new State(-2);
    states[4] = new State(-3);
    states[5] = new State(new int[]{16,16},new int[]{-5,6});
    states[6] = new State(new int[]{23,7,19,14});
    states[7] = new State(new int[]{16,37},new int[]{-6,8});
    states[8] = new State(new int[]{24,9,25,42,3,-4});
    states[9] = new State(new int[]{16,37,14,39,15,41},new int[]{-7,10,-8,29,-9,30,-10,31,-11,32,-12,33,-13,34,-14,35,-6,36,-15,38,-16,40});
    states[10] = new State(new int[]{25,11,6,17,7,19,8,21,9,23,5,25,4,27,3,-5});
    states[11] = new State(new int[]{26,12});
    states[12] = new State(new int[]{16,16},new int[]{-5,13});
    states[13] = new State(new int[]{19,14,3,-7});
    states[14] = new State(new int[]{16,15});
    states[15] = new State(-8);
    states[16] = new State(-9);
    states[17] = new State(new int[]{16,37,14,39,15,41},new int[]{-7,18,-8,29,-9,30,-10,31,-11,32,-12,33,-13,34,-14,35,-6,36,-15,38,-16,40});
    states[18] = new State(new int[]{6,-19,7,-19,8,21,9,23,5,-19,4,-19,25,-19,3,-19});
    states[19] = new State(new int[]{16,37,14,39,15,41},new int[]{-7,20,-8,29,-9,30,-10,31,-11,32,-12,33,-13,34,-14,35,-6,36,-15,38,-16,40});
    states[20] = new State(new int[]{6,-20,7,-20,8,21,9,23,5,-20,4,-20,25,-20,3,-20});
    states[21] = new State(new int[]{16,37,14,39,15,41},new int[]{-7,22,-8,29,-9,30,-10,31,-11,32,-12,33,-13,34,-14,35,-6,36,-15,38,-16,40});
    states[22] = new State(-21);
    states[23] = new State(new int[]{16,37,14,39,15,41},new int[]{-7,24,-8,29,-9,30,-10,31,-11,32,-12,33,-13,34,-14,35,-6,36,-15,38,-16,40});
    states[24] = new State(-22);
    states[25] = new State(new int[]{16,37,14,39,15,41},new int[]{-7,26,-8,29,-9,30,-10,31,-11,32,-12,33,-13,34,-14,35,-6,36,-15,38,-16,40});
    states[26] = new State(new int[]{6,17,7,19,8,21,9,23,5,-17,4,-17,25,-17,3,-17});
    states[27] = new State(new int[]{16,37,14,39,15,41},new int[]{-7,28,-8,29,-9,30,-10,31,-11,32,-12,33,-13,34,-14,35,-6,36,-15,38,-16,40});
    states[28] = new State(new int[]{6,17,7,19,8,21,9,23,5,25,4,-18,25,-18,3,-18});
    states[29] = new State(-10);
    states[30] = new State(-11);
    states[31] = new State(-12);
    states[32] = new State(-13);
    states[33] = new State(-14);
    states[34] = new State(-15);
    states[35] = new State(-16);
    states[36] = new State(-23);
    states[37] = new State(-26);
    states[38] = new State(-24);
    states[39] = new State(-27);
    states[40] = new State(-25);
    states[41] = new State(-28);
    states[42] = new State(new int[]{26,43});
    states[43] = new State(new int[]{16,16},new int[]{-5,44});
    states[44] = new State(new int[]{19,14,3,-6});

    for (int sNo = 0; sNo < states.Length; sNo++) states[sNo].number = sNo;

    rules[1] = new Rule(-2, new int[]{-1,3});
    rules[2] = new Rule(-1, new int[]{-3});
    rules[3] = new Rule(-3, new int[]{-4});
    rules[4] = new Rule(-4, new int[]{22,-5,23,-6});
    rules[5] = new Rule(-4, new int[]{22,-5,23,-6,24,-7});
    rules[6] = new Rule(-4, new int[]{22,-5,23,-6,25,26,-5});
    rules[7] = new Rule(-4, new int[]{22,-5,23,-6,24,-7,25,26,-5});
    rules[8] = new Rule(-5, new int[]{-5,19,16});
    rules[9] = new Rule(-5, new int[]{16});
    rules[10] = new Rule(-7, new int[]{-8});
    rules[11] = new Rule(-7, new int[]{-9});
    rules[12] = new Rule(-7, new int[]{-10});
    rules[13] = new Rule(-7, new int[]{-11});
    rules[14] = new Rule(-7, new int[]{-12});
    rules[15] = new Rule(-7, new int[]{-13});
    rules[16] = new Rule(-7, new int[]{-14});
    rules[17] = new Rule(-12, new int[]{-7,5,-7});
    rules[18] = new Rule(-13, new int[]{-7,4,-7});
    rules[19] = new Rule(-8, new int[]{-7,6,-7});
    rules[20] = new Rule(-9, new int[]{-7,7,-7});
    rules[21] = new Rule(-10, new int[]{-7,8,-7});
    rules[22] = new Rule(-11, new int[]{-7,9,-7});
    rules[23] = new Rule(-14, new int[]{-6});
    rules[24] = new Rule(-14, new int[]{-15});
    rules[25] = new Rule(-14, new int[]{-16});
    rules[26] = new Rule(-6, new int[]{16});
    rules[27] = new Rule(-15, new int[]{14});
    rules[28] = new Rule(-16, new int[]{15});
  }

  protected override void Initialize() {
    this.InitSpecialTokens((int)Token.error, (int)Token.EOF);
    this.InitStates(states);
    this.InitRules(rules);
    this.InitNonTerminals(nonTerms);
  }

  protected override void DoAction(int action)
  {
#pragma warning disable 162, 1522
    switch (action)
    {
      case 2: // list -> stat
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 3: // stat -> select_stmt
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 4: // select_stmt -> TSELECT, identifier_list, TFROM, identifier
{ CurrentSemanticValue.n = new(NodeType.Select, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null); }
        break;
      case 5: // select_stmt -> TSELECT, identifier_list, TFROM, identifier, TWHERE, condition
{ CurrentSemanticValue.n = new(NodeType.Select, ValueStack[ValueStack.Depth-5].n, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null); }
        break;
      case 6: // select_stmt -> TSELECT, identifier_list, TFROM, identifier, TORDER, TBY, 
              //                identifier_list
{ CurrentSemanticValue.n = new(NodeType.Select, ValueStack[ValueStack.Depth-6].n, ValueStack[ValueStack.Depth-4].n, ValueStack[ValueStack.Depth-1].n, null, null); }
        break;
      case 7: // select_stmt -> TSELECT, identifier_list, TFROM, identifier, TWHERE, condition, 
              //                TORDER, TBY, identifier_list
{ CurrentSemanticValue.n = new(NodeType.Select, ValueStack[ValueStack.Depth-8].n, ValueStack[ValueStack.Depth-6].n, ValueStack[ValueStack.Depth-4].n, ValueStack[ValueStack.Depth-1].n, null); }
        break;
      case 8: // identifier_list -> identifier_list, TCOMMA, IDENTIFIER
{ CurrentSemanticValue.n = new(NodeType.IdentifierList, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null); }
        break;
      case 9: // identifier_list -> IDENTIFIER
{ CurrentSemanticValue.n = new(NodeType.Identifier, null, null, null, null, null); }
        break;
      case 10: // condition -> equals_expr
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 11: // condition -> not_equals_expr
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 12: // condition -> less_than_expr
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 13: // condition -> greater_than_expr
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 14: // condition -> and_expr
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 15: // condition -> or_expr
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 16: // condition -> simple_expr
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 17: // and_expr -> condition, TAND, condition
{ CurrentSemanticValue.n = new(NodeType.ExprEquals, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null); }
        break;
      case 18: // or_expr -> condition, TOR, condition
{ CurrentSemanticValue.n = new(NodeType.ExprEquals, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null); }
        break;
      case 19: // equals_expr -> condition, TEQUALS, condition
{ CurrentSemanticValue.n = new(NodeType.ExprEquals, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null); }
        break;
      case 20: // not_equals_expr -> condition, TNOTEQUALS, condition
{ CurrentSemanticValue.n = new(NodeType.ExprNotEquals, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null); }
        break;
      case 21: // less_than_expr -> condition, TLESSTHAN, condition
{ CurrentSemanticValue.n = new(NodeType.ExprNotEquals, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null); }
        break;
      case 22: // greater_than_expr -> condition, TGREATERTHAN, condition
{ CurrentSemanticValue.n = new(NodeType.ExprNotEquals, ValueStack[ValueStack.Depth-3].n, ValueStack[ValueStack.Depth-1].n, null, null, null); }
        break;
      case 23: // simple_expr -> identifier
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 24: // simple_expr -> number
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 25: // simple_expr -> string
{ CurrentSemanticValue.n = ValueStack[ValueStack.Depth-1].n; }
        break;
      case 26: // identifier -> IDENTIFIER
{ CurrentSemanticValue.n = new(NodeType.Identifier, null, null, null, null, CurrentSemanticValue.s); }
        break;
      case 27: // number -> TDIGIT
{ CurrentSemanticValue.n = new(NodeType.Number, null, null, null, null, CurrentSemanticValue.s); }
        break;
      case 28: // string -> TSTRING
{ CurrentSemanticValue.n = new(NodeType.String, null, null, null, null, CurrentSemanticValue.s); }
        break;
    }
#pragma warning restore 162, 1522
  }

  protected override string TerminalToString(int terminal)
  {
    if (aliases != null && aliases.ContainsKey(terminal))
        return aliases[terminal];
    else if (((Token)terminal).ToString() != terminal.ToString(CultureInfo.InvariantCulture))
        return ((Token)terminal).ToString();
    else
        return CharToString((char)terminal);
  }

}
}