%namespace CamusDB.Core.SQLParser
%scannertype sqlScanner
%visibility internal
%tokentype Token

%option stack, minimize, parser, verbose, persistbuffer, noembedbuffers 

TSelect         "SELECT"
TFrom           "FROM"
LParen          \(
RParen          \)
Eol             (\r\n?|\n)
NotWh           [^ \t\r\n]
Space           [ \t]
Number          [0-9]+
Identifier      [a-zA-Z]+
TAdd            \+
TMult           \*
TMinus          \-
TDiv            /
TComma          ,


%{

%}

%%

/* Scanner body */

{Number}		{ Console.WriteLine("number: {0}", yytext); yylval.s = yytext; return (int)Token.DIGIT; }

{Space}+		/* skip */

{LParen} { return (int)Token.LPAREN; }

{RParen} { return (int)Token.RPAREN; }

{TSelect} { return (int)Token.TSELECT; }

{TFrom} { return (int)Token.TFROM; }

{TAdd} { return (int)Token.TADD; }

{TMult} { return (int)Token.TMULT; }

{TDiv} { return (int)Token.TDIV; }

{TMinus} { return (int)Token.TMINUS; }

{TComma} { return (int)Token.TCOMMA; }

{Identifier} { Console.WriteLine("identifier: {0}", yytext); return (int)Token.IDENTIFIER; }

%%