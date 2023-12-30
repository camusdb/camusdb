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

%token TDIGIT TSTRING IDENTIFIER LPAREN RPAREN TCOMMA TMULT TADD TMINUS TDIV TSELECT TFROM TWHERE 
%token TEQUALS TNOTEQUALS TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS TAND TOR TORDER TBY TASC TDESC TTRUE TFALSE
%token TUPDATE TSET TDELETE TINSERT TINTO TVALUES TCREATE TTABLE TNOT TNULL TTYPE_STRING TTYPE_INT64 TTYPE_FLOAT64 TTYPE_OBJECT_ID
%token TPRIMARY TKEY TUNIQUE TINDEX TALTER TWADD TDROP TCOLUMN

%%

list    : stat { $$.n = $1.n; }
        ;

stat    : select_stmt { $$.n = $1.n; }
        | update_stmt { $$.n = $1.n; }
        | delete_stmt { $$.n = $1.n; }
        | insert_stmt { $$.n = $1.n; }
        | create_table_stmt { $$.n = $1.n; }
        | drop_table_stmt { $$.n = $1.n; }
        | alter_table_stmt { $$.n = $1.n; }
        ;

select_stmt : TSELECT select_field_list TFROM identifier { $$.n = new(NodeType.Select, $2.n, $4.n, null, null, null); }
            | TSELECT select_field_list TFROM identifier TWHERE condition { $$.n = new(NodeType.Select, $2.n, $4.n, $6.n, null, null); }
            | TSELECT select_field_list TFROM identifier TORDER TBY order_list { $$.n = new(NodeType.Select, $2.n, $4.n, null, $7.n, null); }
            | TSELECT select_field_list TFROM identifier TWHERE condition TORDER TBY order_list { $$.n = new(NodeType.Select, $2.n, $4.n, $6.n, $9.n, null); }
            ;

insert_stmt : TINSERT TINTO identifier LPAREN insert_field_list RPAREN TVALUES LPAREN values_list RPAREN { $$.n = new(NodeType.Insert, $3.n, $5.n, $9.n, null, null); }
			;

update_stmt : TUPDATE identifier TSET update_list TWHERE condition { $$.n = new(NodeType.Update, $2.n, $4.n, $6.n, null, null); }
		    ;

delete_stmt : TDELETE TFROM identifier TWHERE condition { $$.n = new(NodeType.Delete, $3.n, $5.n, null, null, null); }
			;

create_table_stmt : TCREATE TTABLE identifier LPAREN create_table_item_list RPAREN { $$.n = new(NodeType.CreateTable, $3.n, $5.n, null, null, null); }                  
                  ;

drop_table_stmt : TDROP TTABLE identifier { $$.n = new(NodeType.DropTable, $3.n, null, null, null, null); }
				;

alter_table_stmt : TALTER TTABLE identifier TWADD identifier field_type { $$.n = new(NodeType.AlterTableAddColumn, $3.n, $6.n, null, null, null); }
                 | TALTER TTABLE identifier TWADD TCOLUMN identifier field_type { $$.n = new(NodeType.AlterTableAddColumn, $3.n, $6.n, null, null, null); }
				 | TALTER TTABLE identifier TDROP identifier { $$.n = new(NodeType.AlterTableDropColumn, $3.n, $5.n, null, null, null); }
                 | TALTER TTABLE identifier TDROP TCOLUMN identifier { $$.n = new(NodeType.AlterTableDropColumn, $3.n, $5.n, null, null, null); }
				 ;

create_table_item_list : create_table_item_list TCOMMA create_table_item { $$.n = new(NodeType.CreateTableItemList, $1.n, $3.n, null, null, null); }
                       | create_table_item { $$.n = $1.n; $$.s = $1.s; }
                       ;

create_table_item : identifier field_type { $$.n = new(NodeType.CreateTableItem, $1.n, $2.n, null, null, null); }
                  | identifier field_type create_table_constraint_list { $$.n = new(NodeType.CreateTableItem, $1.n, $2.n, $3.n, null, null); }
                  ;

create_table_constraint_list : create_table_constraint_list create_table_constraint { $$.n = new(NodeType.CreateTableConstraintList, $1.n, $2.n, null, null, null); }
							 | create_table_constraint { $$.n = $1.n; $$.s = $1.s; }
							 ;

create_table_constraint : TNULL { $$.n = new(NodeType.ConstraintNull, null, null, null, null, null); }
                        | TNOT TNULL { $$.n = new(NodeType.ConstraintNotNull, null, null, null, null, null); }
						| TPRIMARY TKEY { $$.n = new(NodeType.ConstraintPrimaryKey, null, null, null, null, null); }
                        | TUNIQUE { $$.n = new(NodeType.ConstraintUnique, null, null, null, null, null); }
                        ;

field_type : TTYPE_OBJECT_ID { $$.n = new(NodeType.TypeObjectId, null, null, null, null, null); }
           | TTYPE_STRING { $$.n = new(NodeType.TypeString, null, null, null, null, null); }
           | TTYPE_INT64 { $$.n = new(NodeType.TypeInteger64, null, null, null, null, null); }
           | TTYPE_FLOAT64 { $$.n = new(NodeType.TypeFloat64, null, null, null, null, null); } 
           ;

update_list : update_list TCOMMA update_item { $$.n = new(NodeType.UpdateList, $1.n, $3.n, null, null, null); }
		    | update_item { $$.n = $1.n; $$.s = $1.s; }
		    ;

update_item    : identifier TEQUALS simple_expr { $$.n = new(NodeType.UpdateItem, $1.n, $3.n, null, null, null); }
			   ;

select_field_list  : select_field_list TCOMMA select_field_item { $$.n = new(NodeType.IdentifierList, $1.n, $3.n, null, null, null); }
                   | select_field_item { $$.n = $1.n; $$.s = $1.s; }
                   ;

select_field_item  : identifier { $$.n = $1.n; $$.s = $1.s; }
                   | TMULT { $$.n = new(NodeType.ExprAllFields, null, null, null, null, null); }
                   ;

insert_field_list  : insert_field_list TCOMMA insert_field_item { $$.n = new(NodeType.IdentifierList, $1.n, $3.n, null, null, null); }
                   | insert_field_item { $$.n = $1.n; $$.s = $1.s; }
                   ;

insert_field_item  : identifier { $$.n = $1.n; $$.s = $1.s; }               
                   ;

values_list  : values_list TCOMMA values_item { $$.n = new(NodeType.ExprList, $1.n, $3.n, null, null, null); }
			 | values_item { $$.n = $1.n; $$.s = $1.s; }
			 ;

values_item  : expr { $$.n = $1.n; $$.s = $1.s; }
             ;

order_list  : order_list TCOMMA order_item { $$.n = new(NodeType.IdentifierList, $1.n, $3.n, null, null, null); }
            | order_item { $$.n = $1.n; $$.s = $1.s; }
            ;

order_item  : identifier { $$.n = $1.n; $$.s = $1.s; }
            | identifier TASC { $$.n = $1.n; $$.s = $1.s; }
            | identifier TDESC { $$.n = $1.n; $$.s = $1.s; }
            ;

condition : expr { $$.n = $1.n; $$.s = $1.s; }
		  ;

expr       : equals_expr { $$.n = $1.n; }
           | not_equals_expr { $$.n = $1.n; }
           | less_than_expr { $$.n = $1.n; }
           | greater_than_expr { $$.n = $1.n; }
           | less_equals_than_expr { $$.n = $1.n; }
           | greater_equals_than_expr { $$.n = $1.n; }
           | and_expr { $$.n = $1.n; }
           | or_expr { $$.n = $1.n; }
           | simple_expr { $$.n = $1.n; }
           | group_paren_expr { $$.n = $1.n; } 
           | fcall_expr { $$.n = $1.n; }
           ;

and_expr  : condition TAND condition { $$.n = new(NodeType.ExprAnd, $1.n, $3.n, null, null, null); }
          ; 

or_expr   : condition TOR condition { $$.n = new(NodeType.ExprOr, $1.n, $3.n, null, null, null); }
          ;

equals_expr : condition TEQUALS condition { $$.n = new(NodeType.ExprEquals, $1.n, $3.n, null, null, null); }
            ;

not_equals_expr : condition TNOTEQUALS condition { $$.n = new(NodeType.ExprNotEquals, $1.n, $3.n, null, null, null); }
                ;

less_than_expr : condition TLESSTHAN condition { $$.n = new(NodeType.ExprLessThan, $1.n, $3.n, null, null, null); }
               ;

greater_than_expr : condition TGREATERTHAN condition { $$.n = new(NodeType.ExprGreaterThan, $1.n, $3.n, null, null, null); }
                  ;

greater_equals_than_expr : condition TGREATERTHANEQUALS condition { $$.n = new(NodeType.ExprGreaterEqualsThan, $1.n, $3.n, null, null, null); }
                         ;

less_equals_than_expr : condition TLESSTHANEQUALS condition { $$.n = new(NodeType.ExprLessEqualsThan, $1.n, $3.n, null, null, null); }
                      ;

fcall_expr : IDENTIFIER LPAREN RPAREN { $$.n = new(NodeType.ExprFuncCall, $1.n, null, null, null, null); }
           ;         

group_paren_expr : LPAREN condition RPAREN { $$.n = $2.n; $$.s = $2.s; }
                 ;

simple_expr : identifier { $$.n = $1.n; $$.s = $1.s; }
			| number { $$.n = $1.n; $$.s = $1.s; }
            | string { $$.n = $1.n; $$.s = $1.s; }
            | bool { $$.n = $1.n; $$.s = $1.s; }
            | null { $$.n = $1.n; $$.s = $1.s; }
			;

identifier  : IDENTIFIER { $$.n = new(NodeType.Identifier, null, null, null, null, $$.s); }
            ;

number  : TDIGIT { $$.n = new(NodeType.Number, null, null, null, null, $$.s); }
        ;

string  : TSTRING { $$.n = new(NodeType.String, null, null, null, null, $$.s); }
        ;

bool    : TTRUE { $$.n = new(NodeType.Bool, null, null, null, null, "true"); }
        | TFALSE { $$.n = new(NodeType.Bool, null, null, null, null, "false"); }
        ;

null    : TNULL { $$.n = new(NodeType.Null, null, null, null, null, "null"); }
        ;

%%