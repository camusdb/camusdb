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
TDesc           (D|d)(E|e)(S|s)(C|c)
TAsc            (A|a)(S|s)(C|c)
TTrue           (T|t)(R|r)(U|u)(E|e)
TFalse          (F|f)(A|a)(L|l)(S|s)(E|e)
TUpdate         (U|u)(P|p)(D|d)(A|a)(T|t)(E|e)
TSet            (S|s)(E|e)(T|t)
TDelete 	    (D|d)(E|e)(L|l)(E|e)(T|t)(E|e)
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

{Identifier} { yylval.s = yytext; return (int)Token.IDENTIFIER; }

%%