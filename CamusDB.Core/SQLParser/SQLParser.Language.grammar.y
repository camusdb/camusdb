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

%token TDIGIT TSTRING IDENTIFIER LPAREN RPAREN TCOMMA TMULT TADD TMINUS TDIV TSELECT TFROM TWHERE TEQUALS TNOTEQUALS TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS TAND TOR

%%

list    : stat { $$.n = $1.n; }
        ;

stat    : select_stmt { $$.n = $1.n; }
        ;

select_stmt    : TSELECT identifier_list TFROM identifier { $$.n = new(NodeType.Select, $2.n, $4.n); }
               | TSELECT identifier_list TFROM identifier TWHERE condition { $$.n = new(NodeType.Select, $2.n, $4.n); }
               ;

identifier_list  : identifier_list TCOMMA IDENTIFIER { $$.n = new(NodeType.IdentifierList, $1.n, $3.n); }
                 | IDENTIFIER { $$.n = new(NodeType.Identifier, null, null); }
                 ;

condition  : equals_expr { $$.n = new(NodeType.Identifier, null, null); }
           | not_equals_expr { $$.n = new(NodeType.Identifier, null, null); }
           | less_than_expr { $$.n = new(NodeType.Identifier, null, null); }
           | greater_than_expr { $$.n = new(NodeType.Identifier, null, null); }
           | and_expr { $$.n = new(NodeType.Identifier, null, null); }
           | or_expr { $$.n = new(NodeType.Identifier, null, null); }
           | simple_expr { $$.n = $1.n; }
           ;

and_expr  : condition TAND condition { $$.n = new(NodeType.ExprEquals, $1.n, $3.n); }
          ; 

or_expr   : condition TOR condition { $$.n = new(NodeType.ExprEquals, $1.n, $3.n); }
          ;

equals_expr      : condition TEQUALS condition { $$.n = new(NodeType.ExprEquals, $1.n, $3.n); }
                 ;

not_equals_expr  : condition TNOTEQUALS condition { $$.n = new(NodeType.ExprNotEquals, $1.n, $3.n); }
                 ;

less_than_expr   : condition TLESSTHAN condition { $$.n = new(NodeType.ExprNotEquals, $1.n, $3.n); }
                 ;

greater_than_expr : condition TGREATERTHAN condition { $$.n = new(NodeType.ExprNotEquals, $1.n, $3.n); }
                  ;

simple_expr : identifier { $$.n = new(NodeType.Identifier, null, null); }
			| number { $$.n = new(NodeType.Number, null, null); }
            | string { $$.n = new(NodeType.String, null, null); }
			;

identifier  : IDENTIFIER { $$.n = new(NodeType.Identifier, null, null); }
            ;

number  : TDIGIT { $$.n = new(NodeType.Number, null, null); }
        ;

string  : TSTRING { $$.n = new(NodeType.String, null, null); }
        ;

%%