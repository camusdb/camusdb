%namespace CamusDB.Core.SQLParser
%scannertype sqlScanner
%visibility internal
%tokentype Token

%option stack, minimize, parser, verbose, persistbuffer, noembedbuffers

TSelect         (S|s)(E|e)(L|l)(E|e)(C|c)(T|t)
TFrom           (F|f)(R|r)(O|o)(M|m)
TWhere          (W|w)(H|h)(E|e)(R|r)(E|e)
TOrder          (O|o)(R|r)(D|d)(E|e)(R|r)
TBy             (B|b)(Y|y)
TAnd            (A|a)(N|n)(D|d)
TOr             (O|o)(R|r)
TAsc            (A|a)(S|s)(C|c)
TTrue           (T|t)(R|r)(U|u)(E|e)
TFalse          (F|f)(A|a)(L|l)(S|s)(E|e)
TUpdate         (U|u)(P|p)(D|d)(A|a)(T|t)(E|e)
TSet            (S|s)(E|e)(T|t)
TDelete 	    (D|d)(E|e)(L|l)(E|e)(T|t)(E|e)
TInsert 	    (I|i)(N|n)(S|s)(E|e)(R|r)(T|t)
TInto 		    (I|i)(N|n)(T|t)(O|o)
TValues         (V|v)(A|a)(L|l)(U|u)(E|e)(S|s)
TCreate         (C|c)(R|r)(E|e)(A|a)(T|t)(E|e)
TTable          (T|t)(A|a)(B|b)(L|l)(E|e)
TNot            (N|n)(O|o)(T|t)
TNull           (N|n)(U|u)(L|l)(L|l)
TPrimary 	    (P|p)(R|r)(I|i)(M|m)(A|a)(R|r)(Y|y)
TKey 		    (K|k)(E|e)(Y|y)
TUnique 	    (U|u)(N|n)(I|i)(Q|q)(U|u)(E|e)
TIndex 		    (I|i)(N|n)(D|d)(E|e)(X|x)	
TAlter 		    (A|a)(L|l)(T|t)(E|e)(R|r)
TWAdd 		    (A|a)(D|d)(D|d)
TDrop 		    (D|d)(R|r)(O|o)(P|p)
TColumn 	    (C|c)(O|o)(L|l)(U|u)(M|m)(N|n)
TLimit          (L|l)(I|i)(M|m)(I|i)(T|t)
TOffset         (O|o)(F|f)(F|f)(S|s)(E|e)(T|t)
TAs 		    (A|a)(S|s)
TGroup 		    (G|g)(R|r)(O|o)(U|u)(P|p)
TShow 		    (S|s)(H|h)(O|o)(W|w)
TColumns 	    (C|c)(O|o)(L|l)(U|u)(M|m)(N|n)(S|s)
TTables         (T|t)(A|a)(B|b)(L|l)(E|e)(S|s)
TDesc           (D|d)(E|e)(S|s)(C|c)
TDescribe       (D|d)(E|e)(S|s)(C|c)(R|r)(I|i)(B|b)(E|e)
TTypeString     (S|s)(T|t)(R|r)(I|i)(N|n)(G|g)
TTypeInt64      (I|i)(N|n)(T|t)(6)(4)
TTypeFloat64    (F|f)(L|l)(O|o)(A|a)(T|t)(6)(4)
TTypeObjectId   (O|o)(B|b)(J|j)(E|e)(C|c)(T|t)(_)(I|i)(D|d)
TTypeSObjectId  (O|o)(I|i)(D|d)
LParen          \(
RParen          \)
Eol             (\r\n?|\n)
NotWh           [^ \t\r\n]
Space           [ \t]
Number          ("-"?[0-9]+)|("-"?[0][x][0-9A-Fa-f]+)
StrChs          [^\\\"\a\b\f\n\r\t\v\0]
DotChr          [^\r\n]
EscChr          \\{DotChr}
OctDig          [0-7]
HexDig          [0-9a-fA-F]
OctEsc          \\{OctDig}{3}
HexEsc          \\x{HexDig}{2}
UniEsc          \\u{HexDig}{4}
UNIESC          \\U{HexDig}{8}
String          \"({StrChs}|{EscChr}|{OctEsc}|{HexEsc}|{UniEsc}|{UNIESC})*\"
Identifier      [a-zA-Z_][a-zA-Z0-9_]*
EscIdentifier   (`)[a-zA-Z_][a-zA-Z0-9_]*(`)
Placeholder     (@)[a-zA-Z0-9_]*
TAdd            \+
TMult           \*
TMinus          \-
TDiv            /
TComma          ,
TEquals         =
TNotEquals      <>
TNotEquals2     !=
TLess           <
TGreater        >
TLessEquals     <=
TGreaterEquals  >=

%{

%}

%%

/* Scanner body */

{Number}		{ yylval.s = yytext; return (int)Token.TDIGIT; }

{String}		{ yylval.s = yytext; return (int)Token.TSTRING; }

{Space}+		/* skip */

{LParen} { return (int)Token.LPAREN; }

{RParen} { return (int)Token.RPAREN; }

{TSelect} { return (int)Token.TSELECT; }

{TFrom} { return (int)Token.TFROM; }

{TWhere} { return (int)Token.TWHERE; }

{TOrder} { return (int)Token.TORDER; }

{TBy} { return (int)Token.TBY; }

{TAsc} { return (int)Token.TASC; }

{TDesc} { return (int)Token.TDESC; }

{TTrue} { return (int)Token.TTRUE; }

{TFalse} { return (int)Token.TFALSE; }

{TUpdate} { return (int)Token.TUPDATE; }

{TDelete} { return (int)Token.TDELETE; }

{TSet} { return (int)Token.TSET; }

{TInsert} { return (int)Token.TINSERT; }

{TInto} { return (int)Token.TINTO; }

{TValues} { return (int)Token.TVALUES; }

{TCreate} { return (int)Token.TCREATE; }

{TTable} { return (int)Token.TTABLE; }

{TNot} { return (int)Token.TNOT; }

{TNull} { return (int)Token.TNULL; }

{TPrimary} { return (int)Token.TPRIMARY; }

{TKey} { return (int)Token.TKEY; }

{TUnique} { return (int)Token.TUNIQUE; }

{TIndex} { return (int)Token.TINDEX; }

{TAlter} { return (int)Token.TALTER; }

{TWAdd} { return (int)Token.TWADD; }

{TDrop} { return (int)Token.TDROP; }

{TColumn} { return (int)Token.TCOLUMN; }

{TLimit} { return (int)Token.TLIMIT; }

{TOffset} { return (int)Token.TOFFSET; }

{TAs} { return (int)Token.TAS; }

{TGroup} { return (int)Token.TGROUP; }

{TShow} { return (int)Token.TSHOW; }

{TColumns} { return (int)Token.TCOLUMNS; }

{TTables} { return (int)Token.TTABLES; }

{TDescribe} { return (int)Token.TDESCRIBE; }

{TTypeObjectId} { return (int)Token.TTYPE_OBJECT_ID; }

{TTypeSObjectId} { return (int)Token.TTYPE_OBJECT_ID; }

{TTypeString} { return (int)Token.TTYPE_STRING; }

{TTypeInt64} { return (int)Token.TTYPE_INT64; }

{TTypeFloat64} { return (int)Token.TTYPE_FLOAT64; }

{TAdd} { return (int)Token.TADD; }

{TMult} { return (int)Token.TMULT; }

{TDiv} { return (int)Token.TDIV; }

{TMinus} { return (int)Token.TMINUS; }

{TComma} { return (int)Token.TCOMMA; }

{TAnd} { return (int)Token.TAND; }

{TOr} { return (int)Token.TOR; }

{TEquals} { return (int)Token.TEQUALS; }

{TGreater} { return (int)Token.TGREATERTHAN; }

{TGreaterEquals} { return (int)Token.TGREATERTHANEQUALS; }

{TLess} { return (int)Token.TLESSTHAN; }

{TLessEquals} { return (int)Token.TLESSTHANEQUALS; }

{TNotEquals} { return (int)Token.TNOTEQUALS; }

{TNotEquals2} { return (int)Token.TNOTEQUALS; }

{Identifier} { yylval.s = yytext; return (int)Token.TIDENTIFIER; }

{EscIdentifier} { yylval.s = yytext; return (int)Token.TESCAPED_IDENTIFIER; }

{Placeholder} { yylval.s = yytext; return (int)Token.TPLACEHOLDER; }

%%