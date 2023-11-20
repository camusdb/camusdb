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

%token DIGIT IDENTIFIER LPAREN RPAREN TCOMMA TMULT TADD TMINUS TDIV TSELECT TFROM

%%

list    : stat { $$.n = $1.n; }
        ;

stat    : expr { System.Console.WriteLine("expr={0}", $1.n.nodeType); $$.n = $1.n; }
        ;

expr    : TSELECT identifier_list TFROM identifier { $$.n = new(NodeType.Select, $2.n, null); }        
        ;

identifier_list  : identifier_list TCOMMA IDENTIFIER { $$.n = new(NodeType.IdentifierList, $1.n, $3.n); }
                 | IDENTIFIER { $$.n = new(NodeType.Identifier, null, null); }
                 ;

identifier  : IDENTIFIER { $$.n = new(NodeType.Identifier, null, null); }
            ;

number  : DIGIT { $$.n = new(NodeType.ExprDigit, null, null); }
        ;

%%