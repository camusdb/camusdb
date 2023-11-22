%namespace CamusDB.Core.SQLParser
%partial
%parsertype sqlParser
%visibility internal
%tokentype Token

%union { 
        public NodeAst n;
        public string s;
}

%start list
%visibility internal

%left TOR
%left TAND
%left TEQUALS TNOTEQUALS
%left TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS
%left TADD TMINUS

%token TDIGIT TSTRING IDENTIFIER LPAREN RPAREN TCOMMA TMULT TADD TMINUS TDIV TSELECT TFROM TWHERE TEQUALS TNOTEQUALS TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS TAND TOR TORDER TBY

%%

list    : stat { $$.n = $1.n; }
        ;

stat    : select_stmt { $$.n = $1.n; }
        ;

select_stmt    : TSELECT identifier_list TFROM identifier { $$.n = new(NodeType.Select, $2.n, $4.n, null, null, null); }
               | TSELECT identifier_list TFROM identifier TWHERE condition { $$.n = new(NodeType.Select, $2.n, $4.n, $6.n, null, null); }
               | TSELECT identifier_list TFROM identifier TORDER TBY identifier_list { $$.n = new(NodeType.Select, $2.n, $4.n, $7.n, null, null); }
               | TSELECT identifier_list TFROM identifier TWHERE condition TORDER TBY identifier_list { $$.n = new(NodeType.Select, $2.n, $4.n, $6.n, $9.n, null); }
               ;

identifier_list  : identifier_list TCOMMA identifier { $$.n = new(NodeType.IdentifierList, $1.n, $3.n, null, null, null); }
                 | identifier { $$.n = $1.n; $$.s = $1.s; }
                 ;

condition  : equals_expr { $$.n = $1.n; }
           | not_equals_expr { $$.n = $1.n; }
           | less_than_expr { $$.n = $1.n; }
           | greater_than_expr { $$.n = $1.n; }
           | and_expr { $$.n = $1.n; }
           | or_expr { $$.n = $1.n; }
           | simple_expr { $$.n = $1.n; }
           ;

and_expr  : condition TAND condition { $$.n = new(NodeType.ExprAnd, $1.n, $3.n, null, null, null); }
          ; 

or_expr   : condition TOR condition { $$.n = new(NodeType.ExprOr, $1.n, $3.n, null, null, null); }
          ;

equals_expr      : condition TEQUALS condition { $$.n = new(NodeType.ExprEquals, $1.n, $3.n, null, null, null); }
                 ;

not_equals_expr  : condition TNOTEQUALS condition { $$.n = new(NodeType.ExprNotEquals, $1.n, $3.n, null, null, null); }
                 ;

less_than_expr   : condition TLESSTHAN condition { $$.n = new(NodeType.ExprLessThan, $1.n, $3.n, null, null, null); }
                 ;

greater_than_expr : condition TGREATERTHAN condition { $$.n = new(NodeType.ExprGreaterThan, $1.n, $3.n, null, null, null); }
                  ;

simple_expr : identifier { $$.n = $1.n; $$.s = $1.s; }
			| number { $$.n = $1.n; $$.s = $1.s; }
            | string { $$.n = $1.n; $$.s = $1.s; }
			;

identifier  : IDENTIFIER { $$.n = new(NodeType.Identifier, null, null, null, null, $$.s); }
            ;

number  : TDIGIT { $$.n = new(NodeType.Number, null, null, null, null, $$.s); }
        ;

string  : TSTRING { $$.n = new(NodeType.String, null, null, null, null, $$.s); }
        ;

%%