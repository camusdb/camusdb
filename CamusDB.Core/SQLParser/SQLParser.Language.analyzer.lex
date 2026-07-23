%namespace CamusDB.Core.SQLParser
%scannertype sqlScanner
%visibility internal
%tokentype Token

%option stack, minimize, parser, verbose, persistbuffer, noembedbuffers

%x BLOCKCOMMENT

TDatabases      (D|d)(A|a)(T|t)(A|a)(B|b)(A|a)(S|s)(E|e)(S|s)
TDatabase       (D|d)(A|a)(T|t)(A|a)(B|b)(A|a)(S|s)(E|e)
TSelect         (S|s)(E|e)(L|l)(E|e)(C|c)(T|t)
TFrom           (F|f)(R|r)(O|o)(M|m)
TWhere          (W|w)(H|h)(E|e)(R|r)(E|e)
TOrder          (O|o)(R|r)(D|d)(E|e)(R|r)
TBy             (B|b)(Y|y)
TBetween         (B|b)(E|e)(T|t)(W|w)(E|e)(E|e)(N|n)
TAnd            (A|a)(N|n)(D|d)
TOr             (O|o)(R|r)
TOn             (O|o)(N|n)
TIn             (I|i)(N|n)
TIs             (I|i)(S|s)
TAsc            (A|a)(S|s)(C|c)
TLike           (L|l)(I|i)(K|k)(E|e)
TILike          (I|i)(L|l)(I|i)(K|k)(E|e)
TRegexIMatch    ~\*
TRegexMatch     ~
TRegexNotIMatch !~\*
TRegexNotMatch  !~
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
TIf 		    (I|i)(F|f)
TExists 	    (E|e)(X|x)(I|i)(S|s)(T|t)(S|s)
TNot            (N|n)(O|o)(T|t)
TNull           (N|n)(U|u)(L|l)(L|l)
TPrimary 	    (P|p)(R|r)(I|i)(M|m)(A|a)(R|r)(Y|y)
TKey 		    (K|k)(E|e)(Y|y)
TUnique 	    (U|u)(N|n)(I|i)(Q|q)(U|u)(E|e)
TIndex 		    (I|i)(N|n)(D|d)(E|e)(X|x)
TIndexes 		(I|i)(N|n)(D|d)(E|e)(X|x)(E|e)(S|s)
TDefault 		(D|d)(E|e)(F|f)(A|a)(U|u)(L|l)(T|t)
TAlter 		    (A|a)(L|l)(T|t)(E|e)(R|r)
TWAdd 		    (A|a)(D|d)(D|d)
TDrop 		    (D|d)(R|r)(O|o)(P|p)
TForce 		    (F|f)(O|o)(R|r)(C|c)(E|e)
TRelink 		(R|r)(E|e)(L|l)(I|i)(N|n)(K|k)
TOrphan 		(O|o)(R|r)(P|p)(H|h)(A|a)(N|n)
TColumn 	    (C|c)(O|o)(L|l)(U|u)(M|m)(N|n)
TLimit          (L|l)(I|i)(M|m)(I|i)(T|t)
TOffset         (O|o)(F|f)(F|f)(S|s)(E|e)(T|t)
TAs 		    (A|a)(S|s)
TGroup 		    (G|g)(R|r)(O|o)(U|u)(P|p)
THaving         (H|h)(A|a)(V|v)(I|i)(N|n)(G|g)
TDistinct       (D|d)(I|i)(S|s)(T|t)(I|i)(N|n)(C|c)(T|t)
TJoin           (J|j)(O|o)(I|i)(N|n)
TInner          (I|i)(N|n)(N|n)(E|e)(R|r)
TShow 		    (S|s)(H|h)(O|o)(W|w)
TColumns 	    (C|c)(O|o)(L|l)(U|u)(M|m)(N|n)(S|s)
TTables         (T|t)(A|a)(B|b)(L|l)(E|e)(S|s)
TDesc           (D|d)(E|e)(S|s)(C|c)
TDescribe       (D|d)(E|e)(S|s)(C|c)(R|r)(I|i)(B|b)(E|e)
TTypeString     (S|s)(T|t)(R|r)(I|i)(N|n)(G|g)
TTypeChar       (C|c)(H|h)(A|a)(R|r)
TTypeVarchar    (V|v)(A|a)(R|r)(C|c)(H|h)(A|a)(R|r)
TTypeText       (T|t)(E|e)(X|x)(T|t)
TTypeInt64      (I|i)(N|n)(T|t)(6)(4)
TTypeInt        (I|i)(N|n)(T|t)
TTypeFloat64    (F|f)(L|l)(O|o)(A|a)(T|t)(6)(4)
TTypeFloat32    (F|f)(L|l)(O|o)(A|a)(T|t)(3)(2)
TTypeReal       (R|r)(E|e)(A|a)(L|l)
TTypeObjectId   (O|o)(B|b)(J|j)(E|e)(C|c)(T|t)(_)(I|i)(D|d)
TTypeSObjectId  (O|o)(I|i)(D|d)
TTypeBool       (B|b)(O|o)(O|o)(L|l)
TTypeBoolean    (B|b)(O|o)(O|o)(L|l)(E|e)(A|a)(N|n)
TTypeBytes      (B|b)(Y|y)(T|t)(E|e)(S|s)
TTypeBlob       (B|b)(L|l)(O|o)(B|b)
TTypeDatetime   (D|d)(A|a)(T|t)(E|e)(T|t)(I|i)(M|m)(E|e)
TTypeTimestamp  (T|t)(I|i)(M|m)(E|e)(S|s)(T|t)(A|a)(M|m)(P|p)
TTypeDate       (D|d)(A|a)(T|t)(E|e)
TTypeUuid       (U|u)(U|u)(I|i)(D|d)
TTypeGuid       (G|g)(U|u)(I|i)(D|d)
TTypeArray      (A|a)(R|r)(R|r)(A|a)(Y|y)
TCast           (C|c)(A|a)(S|s)(T|t)
TCase           (C|c)(A|a)(S|s)(E|e)
TWhen           (W|w)(H|h)(E|e)(N|n)
TThen           (T|t)(H|h)(E|e)(N|n)
TElse           (E|e)(L|l)(S|s)(E|e)
TEnd            (E|e)(N|n)(D|d)
TInteger        (I|i)(N|n)(T|t)(E|e)(G|g)(E|e)(R|r)
TDouble         (D|d)(O|o)(U|u)(B|b)(L|l)(E|e)
TConstraint     (C|c)(O|o)(N|n)(S|s)(T|t)(R|r)(A|a)(I|i)(N|n)(T|t)
TCheck          (C|c)(H|h)(E|e)(C|c)(K|k)
TExplain        (E|e)(X|x)(P|p)(L|l)(A|a)(I|i)(N|n)
TBegin          (B|b)(E|e)(G|g)(I|i)(N|n)
TStart          (S|s)(T|t)(A|a)(R|r)(T|t)
TCommit         (C|c)(O|o)(M|m)(M|m)(I|i)(T|t)
TRollback       (R|r)(O|o)(L|l)(L|l)(B|b)(A|a)(C|c)(K|k)
TRename         (R|r)(E|e)(N|n)(A|a)(M|m)(E|e)
TTo             (T|t)(O|o)
TAnalyze        (A|a)(N|n)(A|a)(L|l)(Y|y)(Z|z)(E|e)
TBranch         (B|b)(R|r)(A|a)(N|n)(C|c)(H|h)
TBranches       (B|b)(R|r)(A|a)(N|n)(C|c)(H|h)(E|e)(S|s)
TAncestors      (A|a)(N|n)(C|c)(E|e)(S|s)(T|t)(O|o)(R|r)(S|s)
TTransaction    (T|t)(R|r)(A|a)(N|n)(S|s)(A|a)(C|c)(T|t)(I|i)(O|o)(N|n)
TEvict          (E|e)(V|v)(I|i)(C|c)(T|t)
LParen          \(
RParen          \)
LBrace          \{
RBrace          \}
Eol             (\r\n?|\n)
NotWh           [^ \t\r\n]
Space           [ \t]
Number          ("-"?[0-9]+)|("-"?[0][x][0-9A-Fa-f]+)
Decimal         ("-"?)([0-9]+)(\.)([0-9]+)
StrChs          [^\\\"\a\b\f\n\r\t\v\0]
StrChs2          [^\\\'\a\b\f\n\r\t\v\0]
DotChr          [^\r\n]
LineComment     --{DotChr}*
EscChr          \\{DotChr}
OctDig          [0-7]
HexDig          [0-9a-fA-F]
OctEsc          \\{OctDig}{3}
HexEsc          \\x{HexDig}{2}
UniEsc          \\u{HexDig}{4}
UNIESC          \\U{HexDig}{8}
String          \"({StrChs}|{EscChr}|{OctEsc}|{HexEsc}|{UniEsc}|{UNIESC}|\"\")*\"
StringSingle    \'({StrChs2}|{EscChr}|{OctEsc}|{HexEsc}|{UniEsc}|{UNIESC}|\'\')*\'
Identifier      [a-zA-Z_][a-zA-Z0-9_]*
EscIdentifier   (`)[a-zA-Z_][a-zA-Z0-9_]*(`)
Placeholder     (@)([a-zA-Z0-9_]+)
TAt             @
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
TDot            \.

%{

%}

%%

/* Scanner body */

{Number}		{ yylval.s = yytext; return (int)Token.TDIGIT; }

{Decimal}		{ yylval.s = yytext; return (int)Token.TFLOAT; }

{String}		{ yylval.s = yytext; return (int)Token.TSTRING; }

{StringSingle}  { yylval.s = yytext; return (int)Token.TSTRING; }

{Space}+		/* skip */

{LParen} { return (int)Token.LPAREN; }

{RParen} { return (int)Token.RPAREN; }

{LBrace} { return (int)Token.LBRACE; }

{RBrace} { return (int)Token.RBRACE; }

{TLike} { return (int)Token.TLIKE; }

{TILike} { return (int)Token.TILIKE; }

{TRegexIMatch}    { return (int)Token.TREGEXIMATCH; }
{TRegexMatch}     { return (int)Token.TREGEXMATCH; }
{TRegexNotIMatch} { return (int)Token.TREGEXNOTIMATCH; }
{TRegexNotMatch}  { return (int)Token.TREGEXNOTMATCH; }

{TBegin} { return (int)Token.TBEGIN; }

{TRollback} { return (int)Token.TROLLBACK; }
{TRename} { return (int)Token.TRENAME; }
{TTo} { return (int)Token.TTO; }
{TAnalyze} { return (int)Token.TANALYZE; }
{TBranches} { return (int)Token.TBRANCHES; }
{TAncestors} { return (int)Token.TANCESTORS; }
{TBranch} { return (int)Token.TBRANCH; }
{TEvict} { return (int)Token.TEVICT; }

{TCommit} { return (int)Token.TCOMMIT; }

{TStart} { return (int)Token.TSTART; }

{TTransaction} { return (int)Token.TTRANSACTION; }

{TDatabases} { return (int)Token.TDATABASES; }
{TDatabase} { return (int)Token.TDATABASE; }

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

{TIs} { return (int)Token.TIS; }

{TIf} { return (int)Token.TIF; }

{TExists} { return (int)Token.TEXISTS; }

{TTable} { return (int)Token.TTABLE; }

{TNot} { return (int)Token.TNOT; }

{TNull} { return (int)Token.TNULL; }

{TPrimary} { return (int)Token.TPRIMARY; }

{TKey} { return (int)Token.TKEY; }

{TDefault} { return (int)Token.TDEFAULT; }

{TUnique} { return (int)Token.TUNIQUE; }

{TIndex} { return (int)Token.TINDEX; }

{TIndexes} { return (int)Token.TINDEXES; }

{TAlter} { return (int)Token.TALTER; }

{TWAdd} { return (int)Token.TWADD; }

{TDrop} { return (int)Token.TDROP; }
{TForce} { return (int)Token.TFORCE; }
{TRelink} { return (int)Token.TRELINK; }
{TOrphan} { return (int)Token.TORPHAN; }

{TColumn} { return (int)Token.TCOLUMN; }

{TLimit} { return (int)Token.TLIMIT; }

{TOffset} { return (int)Token.TOFFSET; }

{TAs} { return (int)Token.TAS; }

{TGroup} { return (int)Token.TGROUP; }
{THaving} { return (int)Token.THAVING; }

{TDistinct} { return (int)Token.TDISTINCT; }

{TExplain} { return (int)Token.TEXPLAIN; }

{TJoin} { return (int)Token.TJOIN; }

{TInner} { return (int)Token.TINNER; }

{TShow} { return (int)Token.TSHOW; }

{TColumns} { return (int)Token.TCOLUMNS; }

{TTables} { return (int)Token.TTABLES; }

{TDescribe} { return (int)Token.TDESCRIBE; }

{TTypeObjectId} { return (int)Token.TTYPE_OBJECT_ID; }

{TTypeSObjectId} { return (int)Token.TTYPE_OBJECT_ID; }

{TTypeVarchar} { return (int)Token.TTYPE_STRING; }

{TTypeChar} { return (int)Token.TTYPE_STRING; }

{TTypeText} { return (int)Token.TTYPE_STRING; }

{TTypeString} { return (int)Token.TTYPE_STRING; }

{TTypeInt64} { return (int)Token.TTYPE_INT64; }

{TTypeInt} { return (int)Token.TTYPE_INT64; }

{TTypeFloat64} { return (int)Token.TTYPE_FLOAT64; }

{TTypeFloat32} { return (int)Token.TTYPE_FLOAT32; }

{TTypeReal} { return (int)Token.TTYPE_FLOAT32; }

{TTypeBool} { return (int)Token.TTYPE_BOOL; }

{TTypeBoolean} { return (int)Token.TTYPE_BOOL; }

{TTypeBytes} { return (int)Token.TTYPE_BYTES; }

{TTypeBlob} { return (int)Token.TTYPE_BYTES; }

{TTypeDatetime} { return (int)Token.TTYPE_DATETIME; }

{TTypeTimestamp} { return (int)Token.TTYPE_DATETIME; }

{TTypeDate} { return (int)Token.TTYPE_DATE; }

{TTypeUuid} { return (int)Token.TTYPE_UUID; }

{TTypeGuid} { return (int)Token.TTYPE_UUID; }

{TTypeArray} { return (int)Token.TTYPE_ARRAY; }

{TCast} { return (int)Token.TCAST; }

{TCase} { return (int)Token.TCASE; }

{TWhen} { return (int)Token.TWHEN; }

{TThen} { return (int)Token.TTHEN; }

{TElse} { return (int)Token.TELSE; }

{TEnd} { return (int)Token.TEND; }

{TInteger} { return (int)Token.TINTEGER; }

{TDouble} { return (int)Token.TDOUBLE; }

{TConstraint} { return (int)Token.TCONSTRAINT; }

{TCheck} { return (int)Token.TCHECK; }

{TAt} { return (int)Token.TAT; }

{TAdd} { return (int)Token.TADD; }

"/*"                        { yy_push_state(BLOCKCOMMENT); }
<BLOCKCOMMENT>"*/"          { yy_pop_state(); }
<BLOCKCOMMENT>[^*\n]+       { /* skip block comment body */ }
<BLOCKCOMMENT>"*"           { /* lone star, not followed by slash */ }
<BLOCKCOMMENT>\n            { /* skip newline in block comment */ }

{TMult} { return (int)Token.TMULT; }

{TDiv} { return (int)Token.TDIV; }

{LineComment}               /* skip */

{TMinus} { return (int)Token.TMINUS; }

{TComma} { return (int)Token.TCOMMA; }

{TBetween} { return (int)Token.TBETWEEN; }

{TAnd} { return (int)Token.TAND; }

{TOn} { return (int)Token.TON; }

{TIn} { return (int)Token.TIN; }

{TOr} { return (int)Token.TOR; }

{TEquals} { return (int)Token.TEQUALS; }

{TGreater} { return (int)Token.TGREATERTHAN; }

{TGreaterEquals} { return (int)Token.TGREATERTHANEQUALS; }

{TLess} { return (int)Token.TLESSTHAN; }

{TLessEquals} { return (int)Token.TLESSTHANEQUALS; }

{TNotEquals} { return (int)Token.TNOTEQUALS; }

{TNotEquals2} { return (int)Token.TNOTEQUALS; }

{TDot} { return (int)Token.TDOT; }

{Identifier} { yylval.s = yytext; return (int)Token.TIDENTIFIER; }

{EscIdentifier} { yylval.s = yytext; return (int)Token.TESCAPED_IDENTIFIER; }

{Placeholder} { yylval.s = yytext; return (int)Token.TPLACEHOLDER; }

%%