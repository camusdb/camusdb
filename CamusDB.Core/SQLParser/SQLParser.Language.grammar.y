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
%left TLIKE TILIKE
%left TEQUALS TNOTEQUALS TBETWEEN
%left TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS
%left TADD TMINUS
%left TMULT

%token TDIGIT TFLOAT TSTRING TIDENTIFIER TPLACEHOLDER LPAREN RPAREN TCOMMA TMULT TADD TMINUS TDIV TSELECT TFROM TWHERE 
%token TEQUALS TNOTEQUALS TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS TAND TOR TORDER TBY TASC TDESC
%token TTRUE TFALSE TUPDATE TSET TDELETE TINSERT TINTO TVALUES TCREATE TTABLE TNOT TNULL
%token TTYPE_STRING TTYPE_INT64 TTYPE_FLOAT64 TTYPE_OBJECT_ID TTYPE_BOOL TCAST TINTEGER TDOUBLE
%token TPRIMARY TKEY TUNIQUE TINDEX TALTER TWADD TDROP TCOLUMN TESCAPED_IDENTIFIER TLIMIT TOFFSET TAS TGROUP TSHOW TCONSTRAINT
%token TCOLUMNS TTABLES TDESCRIBE TDATABASE TAT LBRACE RBRACE TINDEXES TLIKE TILIKE TDEFAULT TIF TEXISTS TON TIN TIS
%token TBEGIN TSTART TTRANSACTION TROLLBACK TCOMMIT TJOIN TINNER TDOT THAVING TDISTINCT TBETWEEN TEXPLAIN

%%

list    : stat { $$.n = $1.n; }
        ;

stat    : select_stmt { $$.n = $1.n; }
        | explain_stmt { $$.n = $1.n; }
        | update_stmt { $$.n = $1.n; }
        | delete_stmt { $$.n = $1.n; }
        | insert_stmt { $$.n = $1.n; }
        | create_table_stmt { $$.n = $1.n; }
        | drop_table_stmt { $$.n = $1.n; }
        | alter_table_stmt { $$.n = $1.n; }
        | show_stmt { $$.n = $1.n; }
        | create_index_stmt { $$.n = $1.n; }
        | begin_stmt { $$.n = $1.n; }
        | commit_stmt { $$.n = $1.n; }
        | rollback_stmt { $$.n = $1.n; }
        ;

opt_distinct : TDISTINCT { $$.s = "1"; }
             | { $$.s = null; }
             ;

select_stmt : TSELECT opt_distinct select_field_list TFROM select_table opt_where opt_group opt_having opt_order opt_limit opt_offset
            { $$.n = new(NodeType.Select, $3.n, $5.n, $6.n, $9.n, $10.n, $11.n, $7.n, $2.s, $8.n); }
            ;

/* EXPLAIN [( LOGICAL | PHYSICAL | ANALYZE )] select_stmt
   LOGICAL, PHYSICAL and ANALYZE are plain identifiers — no new reserved words needed.
   The grammar action dispatches on the identifier text and rejects anything else
   with an InvalidInput error so typos (e.g. ANALYZ, VERBOOSE) don't silently
   degrade to a plain EXPLAIN. */
explain_stmt : TEXPLAIN select_stmt
             { $$.n = new(NodeType.Explain, $2.n, null, null, null, null, null, null, null); }
             | TEXPLAIN LPAREN TIDENTIFIER RPAREN select_stmt
             {
               string opt = $3.s.ToUpperInvariant();
               if (opt == "LOGICAL")
                   $$.n = new(NodeType.ExplainLogical, $5.n, null, null, null, null, null, null, null);
               else if (opt == "PHYSICAL")
                   $$.n = new(NodeType.ExplainPhysical, $5.n, null, null, null, null, null, null, null);
               else if (opt == "ANALYZE")
                   $$.n = new(NodeType.ExplainAnalyze, $5.n, null, null, null, null, null, null, null);
               else
                   throw new CamusDB.Core.CamusDBException(
                       CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                       "Unknown EXPLAIN option '" + $3.s + "'. Valid options are: LOGICAL, PHYSICAL, ANALYZE.");
             }
             ;

opt_having : THAVING condition { $$.n = new(NodeType.Having, $2.n, null, null, null, null, null, null, null); }
           | { $$.n = null; }
           ;

query_expr : LPAREN select_stmt RPAREN { $$.n = $2.n; $$.s = $2.s; }
           ;

opt_where : TWHERE condition { $$.n = $2.n; }
          | { $$.n = null; }
          ;

opt_group : TGROUP TBY group_list { $$.n = new(NodeType.GroupBy, $3.n, null, null, null, null, null, null, null); }
          | { $$.n = null; }
          ;

opt_order : TORDER TBY order_list { $$.n = $3.n; }
          | { $$.n = null; }
          ;

opt_limit : TLIMIT select_limit_offset { $$.n = $2.n; }
          | { $$.n = null; }
          ;

opt_offset : TOFFSET select_limit_offset { $$.n = $2.n; }
           | { $$.n = null; }
           ;

group_list : group_list TCOMMA expr { $$.n = new(NodeType.ExprList, $1.n, $3.n, null, null, null, null, null, null); }
           | expr { $$.n = $1.n; }
           ;

insert_stmt : TINSERT TINTO any_identifier LPAREN insert_field_list RPAREN TVALUES insert_batch_list { $$.n = new(NodeType.Insert, $3.n, $5.n, $8.n, null, null, null, null, null); }            
            | TINSERT TINTO any_identifier TVALUES insert_batch_list { $$.n = new(NodeType.Insert, $3.n, null, $5.n, null, null, null, null, null); }
			;

insert_batch_list : insert_batch_list TCOMMA insert_values { $$.n = new(NodeType.InsertBatchList, $1.n, $3.n, null, null, null, null, null, null); }
                  | insert_values { $$.n = $1.n; $$.s = $1.s; }
                  ;

insert_values : LPAREN values_list RPAREN { $$.n = $2.n; $$.s = $2.s; }
             ;

update_stmt : TUPDATE any_identifier TSET update_list TWHERE condition opt_limit
            { $$.n = new(NodeType.Update, $2.n, $4.n, $6.n, $7.n, null, null, null, null); }
		    ;

delete_stmt : TDELETE TFROM any_identifier TWHERE condition opt_limit
            { $$.n = new(NodeType.Delete, $3.n, $5.n, $6.n, null, null, null, null, null); }
			;

begin_stmt : TBEGIN { $$.n = new(NodeType.Begin, null, null, null, null, null, null, null, null); }
           | TSTART TTRANSACTION { $$.n = new(NodeType.Begin, null, null, null, null, null, null, null, null); }
           ;

commit_stmt : TCOMMIT { $$.n = new(NodeType.Commit, null, null, null, null, null, null, null, null); }             
            ;

rollback_stmt : TROLLBACK { $$.n = new(NodeType.Rollback, null, null, null, null, null, null, null, null); }             
              ;

create_table_stmt : TCREATE TTABLE any_identifier LPAREN create_table_item_list RPAREN { $$.n = new(NodeType.CreateTable, $3.n, $5.n, null, null, null, null, null, null); }
                  | TCREATE TTABLE TIF TNOT TEXISTS any_identifier LPAREN create_table_item_list RPAREN { $$.n = new(NodeType.CreateTableIfNotExists, $6.n, $8.n, null, null, null, null, null, null); }
                  | TCREATE TTABLE any_identifier LPAREN create_table_item_list RPAREN create_table_constraint_list { $$.n = new(NodeType.CreateTable, $3.n, $5.n, $7.n, null, null, null, null, null); }
                  | TCREATE TTABLE TIF TNOT TEXISTS any_identifier LPAREN create_table_item_list RPAREN create_table_constraint_list { $$.n = new(NodeType.CreateTableIfNotExists, $6.n, $8.n, $10.n, null, null, null, null, null); }
                  ;

drop_table_stmt : TDROP TTABLE any_identifier { $$.n = new(NodeType.DropTable, $3.n, null, null, null, null, null, null, null); }
                | TDROP TTABLE TIF TEXISTS any_identifier { $$.n = new(NodeType.DropTableIfExists, $5.n, null, null, null, null, null, null, null); }
				;

alter_table_stmt : TALTER TTABLE any_identifier TWADD any_identifier field_type { $$.n = new(NodeType.AlterTableAddColumn, $3.n, $5.n, $6.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TCOLUMN any_identifier field_type { $$.n = new(NodeType.AlterTableAddColumn, $3.n, $6.n, $7.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TCOLUMN any_identifier field_type create_table_field_constraint { $$.n = new(NodeType.AlterTableAddColumn, $3.n, $6.n, $7.n, $8.n, null, null, null, null); }
				 | TALTER TTABLE any_identifier TDROP any_identifier { $$.n = new(NodeType.AlterTableDropColumn, $3.n, $5.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TDROP TCOLUMN any_identifier { $$.n = new(NodeType.AlterTableDropColumn, $3.n, $6.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TINDEX any_identifier LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddIndex, $3.n, $6.n, $8.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TINDEX any_identifier TON LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddIndex, $3.n, $6.n, $9.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TUNIQUE any_identifier LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddUniqueIndex, $3.n, $6.n, $8.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TUNIQUE any_identifier TON LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddUniqueIndex, $3.n, $6.n, $9.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TUNIQUE TINDEX any_identifier LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddUniqueIndex, $3.n, $7.n, $9.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TUNIQUE TINDEX any_identifier TON LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddUniqueIndex, $3.n, $7.n, $10.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TDROP TINDEX any_identifier { $$.n = new(NodeType.AlterTableDropIndex, $3.n, $6.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TPRIMARY TKEY LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddPrimaryKey, $3.n, $8.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TDROP TPRIMARY TKEY { $$.n = new(NodeType.AlterTableDropPrimaryKey, $3.n, null, null, null, null, null, null, null); }
				 ;

create_index_stmt : TCREATE TINDEX any_identifier TON any_identifier LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddIndex, $5.n, $3.n, $7.n, null, null, null, null, null); }
                  | TCREATE TINDEX TIF TNOT TEXISTS any_identifier TON any_identifier LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddIndexIfNotExists, $8.n, $6.n, $10.n, null, null, null, null, null); }
                  | TCREATE TUNIQUE TINDEX any_identifier TON any_identifier LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddUniqueIndex, $6.n, $4.n, $8.n, null, null, null, null, null); }
                  | TCREATE TUNIQUE TINDEX TIF TNOT TEXISTS any_identifier TON any_identifier LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddUniqueIndexIfNotExists, $9.n, $7.n, $11.n, null, null, null, null, null); }
                  ;

show_stmt : TSHOW TCOLUMNS TFROM any_identifier { $$.n = new(NodeType.ShowColumns, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TTABLES { $$.n = new(NodeType.ShowTables, null, null, null, null, null, null, null, null); }
          | TDESCRIBE any_identifier { $$.n = new(NodeType.ShowColumns, $2.n, null, null, null, null, null, null, null); }
          | TDESC any_identifier { $$.n = new(NodeType.ShowColumns, $2.n, null, null, null, null, null, null, null); }
          | TSHOW TCREATE TTABLE any_identifier { $$.n = new(NodeType.ShowCreateTable, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TDATABASE { $$.n = new(NodeType.ShowDatabase, null, null, null, null, null, null, null, null); }
          | TSHOW TINDEXES TFROM any_identifier { $$.n = new(NodeType.ShowIndexes, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TINDEX TFROM any_identifier { $$.n = new(NodeType.ShowIndexes, $4.n, null, null, null, null, null, null, null); }
          ;

identifier_index_list : identifier_index_list TCOMMA identifier_index { $$.n = new(NodeType.IndexIdentifierList, $1.n, $3.n, null, null, null, null, null, null); }
                      | identifier_index { $$.n = $1.n; $$.s = $1.s; }
                      ;

identifier_index : any_identifier { $$.n = $1.n; $$.s = $1.s; }
                 | any_identifier TASC { $$.n = new(NodeType.IndexIdentifierAsc, $1.n, null, null, null, null, null, null, null); }
                 | any_identifier TDESC { $$.n = new(NodeType.IndexIdentifierDesc, $1.n, null, null, null, null, null, null, null); }
                 ;

select_table : from_clause { $$.n = $1.n; $$.s = $1.s; }
             ;

from_clause : explicit_join_from
            | comma_join_from
            | table_reference { $$.n = $1.n; $$.s = $1.s; }
            ;

explicit_join_from : table_reference join_op table_reference TON condition
                   { $$.n = new(NodeType.Join, $1.n, $3.n, $5.n, null, null, null, null, null); }
                   | explicit_join_from join_op table_reference TON condition
                   { $$.n = new(NodeType.Join, $1.n, $3.n, $5.n, null, null, null, null, null); }
                   ;

comma_join_from : table_reference TCOMMA comma_table_list
                { $$.n = new(NodeType.CommaJoin, $1.n, $3.n, null, null, null, null, null, null); }
                ;

comma_table_list : comma_table_list TCOMMA table_reference
                 { $$.n = new(NodeType.CommaJoinTableList, $1.n, $3.n, null, null, null, null, null, null); }
                 | table_reference { $$.n = $1.n; $$.s = $1.s; }
                 ;

join_op : TJOIN
        | TINNER TJOIN
        ;

table_reference : table_name opt_table_alias opt_table_hint
                { $$.n = new(NodeType.TableReference, $1.n, $2.n, $3.n, null, null, null, null, null); }
                | derived_table_reference
                ;

derived_table_reference : query_expr derived_table_alias
                { $$.n = new(NodeType.DerivedTableReference, $1.n, $2.n, null, null, null, null, null, null); }
                ;

derived_table_alias : TAS any_identifier { $$.n = $2.n; }
                    | any_identifier { $$.n = $1.n; }
                    ;

table_name : any_identifier { $$.n = $1.n; $$.s = $1.s; }
           ;

opt_table_alias : TAS any_identifier { $$.n = $2.n; }
                | any_identifier { $$.n = $1.n; }
                | { $$.n = null; }
                ;

opt_table_hint : TAT LBRACE identifier TEQUALS identifier RBRACE
               { $$.n = new(NodeType.IdentifierWithOpts, null, $3.n, $5.n, null, null, null, null, null); }
               | { $$.n = null; }
               ;

create_table_item_list : create_table_item_list TCOMMA create_table_item { $$.n = new(NodeType.CreateTableItemList, $1.n, $3.n, null, null, null, null, null, null); }
                       | create_table_item_list TCOMMA create_table_inline_constraint { $$.n = new(NodeType.CreateTableItemList, $1.n, $3.n, null, null, null, null, null, null); }
                       | create_table_item { $$.n = $1.n; $$.s = $1.s; }
                       ;

create_table_inline_constraint : TCONSTRAINT any_identifier TPRIMARY TKEY LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.CreateTableConstraintPrimaryKey, $6.n, null, null, null, null, null, null, null); }
                               | TCONSTRAINT TSTRING TPRIMARY TKEY LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.CreateTableConstraintPrimaryKey, $6.n, null, null, null, null, null, null, null); }
                               | TPRIMARY TKEY LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.CreateTableConstraintPrimaryKey, $4.n, null, null, null, null, null, null, null); }
                               | TKEY any_identifier LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.CreateTableConstraintMultiIndex, $2.n, $4.n, null, null, null, null, null, null); }
                               | TUNIQUE TKEY any_identifier LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.CreateTableConstraintUniqueIndex, $3.n, $5.n, null, null, null, null, null, null); }
                               ;

create_table_item : any_identifier field_type { $$.n = new(NodeType.CreateTableItem, $1.n, $2.n, null, null, null, null, null, null); }
                  | any_identifier field_type create_table_field_constraint_list { $$.n = new(NodeType.CreateTableItem, $1.n, $2.n, $3.n, null, null, null, null, null); }
                  ;

create_table_constraint_list : TPRIMARY TKEY LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.CreateTableConstraintPrimaryKey, $4.n, null, null, null, null, null, null, null); }
                             ;

create_table_field_constraint_list : create_table_field_constraint_list create_table_field_constraint { $$.n = new(NodeType.CreateTableFieldConstraintList, $1.n, $2.n, null, null, null, null, null, null); }
                                   | create_table_field_constraint { $$.n = $1.n; $$.s = $1.s; }
                                   ;

create_table_field_constraint : TNULL { $$.n = new(NodeType.ConstraintNull, null, null, null, null, null, null, null, null); }
                        | TNOT TNULL { $$.n = new(NodeType.ConstraintNotNull, null, null, null, null, null, null, null, null); }
						| TPRIMARY TKEY { $$.n = new(NodeType.ConstraintPrimaryKey, null, null, null, null, null, null, null, null); }
                        | TUNIQUE { $$.n = new(NodeType.ConstraintUnique, null, null, null, null, null, null, null, null); }
                        | TDEFAULT LPAREN default_expr RPAREN { $$.n = new(NodeType.ConstraintDefault, $3.n, null, null, null, null, null, null, null); }
                        ;

default_expr : int { $$.n = $1.n; $$.s = $1.s; }
             | float { $$.n = $1.n; $$.s = $1.s; }
             | string { $$.n = $1.n; $$.s = $1.s; }
             | bool { $$.n = $1.n; $$.s = $1.s; }
             | null { $$.n = $1.n; $$.s = $1.s; }             
			 ;

field_type : TTYPE_OBJECT_ID { $$.n = new(NodeType.TypeObjectId, null, null, null, null, null, null, null, null); }
           | TTYPE_STRING { $$.n = new(NodeType.TypeString, null, null, null, null, null, null, null, null); }
           | TTYPE_INT64 { $$.n = new(NodeType.TypeInteger64, null, null, null, null, null, null, null, null); }
           | TTYPE_FLOAT64 { $$.n = new(NodeType.TypeFloat64, null, null, null, null, null, null, null, null); }
           | TTYPE_BOOL { $$.n = new(NodeType.TypeBool, null, null, null, null, null, null, null, null); } 
           ;

cast_target_type : field_type { $$.n = $1.n; $$.s = $1.s; }
                 | TINTEGER { $$.n = new(NodeType.TypeInteger64, null, null, null, null, null, null, null, null); }
                 | TDOUBLE { $$.n = new(NodeType.TypeFloat64, null, null, null, null, null, null, null, null); }
                 | TIDENTIFIER { $$.n = new(NodeType.Identifier, null, null, null, null, null, null, null, $$.s.ToLowerInvariant()); }
                 ;

update_list : update_list TCOMMA update_item { $$.n = new(NodeType.UpdateList, $1.n, $3.n, null, null, null, null, null, null); }
		    | update_item { $$.n = $1.n; $$.s = $1.s; }
		    ;

update_item : any_identifier TEQUALS expr { $$.n = new(NodeType.UpdateItem, $1.n, $3.n, null, null, null, null, null, null); }
			;

select_field_list : select_field_list TCOMMA select_field_item { $$.n = new(NodeType.IdentifierList, $1.n, $3.n, null, null, null, null, null, null); }
                  | select_field_item { $$.n = $1.n; $$.s = $1.s; }
                  ;

select_field_item  : expr { $$.n = $1.n; $$.s = $1.s; }
                   | expr TAS any_identifier { $$.n = new(NodeType.ExprAlias, $1.n, $3.n, null, null, null, null, null, null); }             
                   ;

select_limit_offset : int  { $$.n = $1.n; $$.s = $1.s; }
                    | placeholder { $$.n = $1.n; $$.s = $1.s; }
                    ;

insert_field_list  : insert_field_list TCOMMA insert_field_item { $$.n = new(NodeType.IdentifierList, $1.n, $3.n, null, null, null, null, null, null); }
                   | insert_field_item { $$.n = $1.n; $$.s = $1.s; }
                   ;

insert_field_item  : any_identifier { $$.n = $1.n; $$.s = $1.s; }               
                   ;

values_list  : values_list TCOMMA values_item { $$.n = new(NodeType.ExprList, $1.n, $3.n, null, null, null, null, null, null); }
			 | values_item { $$.n = $1.n; $$.s = $1.s; }
			 ;

values_item  : expr { $$.n = $1.n; $$.s = $1.s; }
             ;

order_list  : order_list TCOMMA order_item { $$.n = new(NodeType.IdentifierList, $1.n, $3.n, null, null, null, null, null, null); }
            | order_item { $$.n = $1.n; $$.s = $1.s; }
            ;

order_item  : expr { $$.n = $1.n; $$.s = $1.s; }
            | expr TASC { $$.n = new(NodeType.SortAsc, $1.n, $2.n, null, null, null, null, null, null); }
            | expr TDESC { $$.n = new(NodeType.SortDesc, $1.n, $2.n, null, null, null, null, null, null); }
            ;

condition : expr { $$.n = $1.n; $$.s = $1.s; }          
		  ;

expr       : equals_expr { $$.n = $1.n; }
           | not_equals_expr { $$.n = $1.n; }
           | less_than_expr { $$.n = $1.n; }
           | greater_than_expr { $$.n = $1.n; }
           | less_equals_than_expr { $$.n = $1.n; }
           | greater_equals_than_expr { $$.n = $1.n; }
           | between_expr { $$.n = $1.n; }
           | and_expr { $$.n = $1.n; }
           | or_expr { $$.n = $1.n; }
           | add_expr { $$.n = $1.n; }
           | sub_expr { $$.n = $1.n; }
           | mult_expr { $$.n = $1.n; }
           | like_expr { $$.n = $1.n; }
           | ilike_expr { $$.n = $1.n; }
           | simple_expr { $$.n = $1.n; }
           | group_paren_expr { $$.n = $1.n; }
           | fcall_expr { $$.n = $1.n; }
           | cast_expr { $$.n = $1.n; }
           | projection_all { $$.n = $1.n; }
           | use_default_expr { $$.n = $1.n; }
           | is_null_expr { $$.n = $1.n; }
           | is_not_null_expr { $$.n = $1.n; }
           | in_subquery_expr { $$.n = $1.n; }
           | not_in_subquery_expr { $$.n = $1.n; }
           | exists_subquery_expr { $$.n = $1.n; }
           | scalar_subquery_expr { $$.n = $1.n; }
           ;

and_expr  : condition TAND condition { $$.n = new(NodeType.ExprAnd, $1.n, $3.n, null, null, null, null, null, null); }
          ; 

or_expr   : condition TOR condition { $$.n = new(NodeType.ExprOr, $1.n, $3.n, null, null, null, null, null, null); }
          ;

equals_expr : condition TEQUALS condition { $$.n = new(NodeType.ExprEquals, $1.n, $3.n, null, null, null, null, null, null); }
            ;

not_equals_expr : condition TNOTEQUALS condition { $$.n = new(NodeType.ExprNotEquals, $1.n, $3.n, null, null, null, null, null, null); }
                ;

less_than_expr : condition TLESSTHAN condition { $$.n = new(NodeType.ExprLessThan, $1.n, $3.n, null, null, null, null, null, null); }
               ;

greater_than_expr : condition TGREATERTHAN condition { $$.n = new(NodeType.ExprGreaterThan, $1.n, $3.n, null, null, null, null, null, null); }
                  ;

greater_equals_than_expr : condition TGREATERTHANEQUALS condition { $$.n = new(NodeType.ExprGreaterEqualsThan, $1.n, $3.n, null, null, null, null, null, null); }
                         ;

less_equals_than_expr : condition TLESSTHANEQUALS condition { $$.n = new(NodeType.ExprLessEqualsThan, $1.n, $3.n, null, null, null, null, null, null); }
                      ;

between_expr : condition TBETWEEN condition TAND condition { $$.n = new(NodeType.ExprBetween, $1.n, null, $3.n, $5.n, null, null, null, null); }
             ;

add_expr  : condition TADD condition { $$.n = new(NodeType.ExprAdd, $1.n, $3.n, null, null, null, null, null, null); }
          ;

sub_expr  : condition TMINUS condition { $$.n = new(NodeType.ExprSub, $1.n, $3.n, null, null, null, null, null, null); }
          ;

mult_expr : condition TMULT condition { $$.n = new(NodeType.ExprMult, $1.n, $3.n, null, null, null, null, null, null); }
          ;

like_expr : condition TLIKE condition { $$.n = new(NodeType.ExprLike, $1.n, $3.n, null, null, null, null, null, null); }
          ;

ilike_expr : condition TILIKE condition { $$.n = new(NodeType.ExprILike, $1.n, $3.n, null, null, null, null, null, null); }
           ;

is_null_expr : condition TIS TNULL { $$.n = new(NodeType.ExprIsNull, $1.n, $3.n, null, null, null, null, null, null); }
             ;

is_not_null_expr : condition TIS TNOT TNULL { $$.n = new(NodeType.ExprIsNotNull, $1.n, $3.n, null, null, null, null, null, null); }
                 ;

in_subquery_expr : condition TIN query_expr { $$.n = new(NodeType.ExprInSubquery, $1.n, $3.n, null, null, null, null, null, null); }
                 ;

not_in_subquery_expr : condition TNOT TIN query_expr { $$.n = new(NodeType.ExprNotInSubquery, $1.n, $4.n, null, null, null, null, null, null); }
                     ;

exists_subquery_expr : TEXISTS query_expr { $$.n = new(NodeType.ExprExistsSubquery, $2.n, null, null, null, null, null, null, null); }
                     ;

scalar_subquery_expr : query_expr { $$.n = new(NodeType.ExprScalarSubquery, $1.n, null, null, null, null, null, null, null); }
                     ;

fcall_expr : identifier LPAREN RPAREN { $$.n = new(NodeType.ExprFuncCall, $1.n, null, null, null, null, null, null, null); }
           | identifier LPAREN fcall_argument_list RPAREN { $$.n = new(NodeType.ExprFuncCall, $1.n, $3.n, null, null, null, null, null, null); }
           ;

cast_expr : TCAST LPAREN condition TAS cast_target_type RPAREN { $$.n = new(NodeType.ExprCast, $3.n, $5.n, null, null, null, null, null, null); }
          ;

fcall_argument_list  : fcall_argument_list TCOMMA fcall_argument_item { $$.n = new(NodeType.ExprArgumentList, $1.n, $3.n, null, null, null, null, null, null); }
                     | fcall_argument_item { $$.n = $1.n; $$.s = $1.s; }
                     ;

fcall_argument_item : expr { $$.n = $1.n; $$.s = $1.s; }
                    ;

group_paren_expr : LPAREN condition RPAREN { $$.n = $2.n; $$.s = $2.s; }
                 ;

simple_expr : any_identifier { $$.n = $1.n; $$.s = $1.s; }
			| int { $$.n = $1.n; $$.s = $1.s; }
            | float { $$.n = $1.n; $$.s = $1.s; }
            | string { $$.n = $1.n; $$.s = $1.s; }
            | bool { $$.n = $1.n; $$.s = $1.s; }
            | null { $$.n = $1.n; $$.s = $1.s; }
            | placeholder { $$.n = $1.n; $$.s = $1.s; }
			;

use_default_expr : TDEFAULT { $$.n = new(NodeType.ExprDefault, null, null, null, null, null, null, null, null); }
                 ;

projection_all : TMULT { $$.n = new(NodeType.ExprAllFields, null, null, null, null, null, null, null, null); }
               ;

any_identifier : qualified_identifier { $$.n = $1.n; $$.s = $1.s; }
               | identifier { $$.n = $1.n; $$.s = $1.s; }
               | escaped_identifier { $$.n = $1.n; $$.s = $1.s; }
               ;

qualified_identifier : any_identifier TDOT any_identifier
                     { $$.n = new(NodeType.Identifier, null, null, null, null, null, null, null, string.Concat($1.n.yytext, ".", $3.n.yytext)); }
                     ;
           
identifier  : TIDENTIFIER { $$.n = new(NodeType.Identifier, null, null, null, null, null, null, null, $$.s.ToLowerInvariant()); }
            ;

escaped_identifier  : TESCAPED_IDENTIFIER { $$.n = new(NodeType.Identifier, null, null, null, null, null, null, null, $$.s.Trim('`').ToLowerInvariant()); }
                    ;

int     : TDIGIT { $$.n = new(NodeType.Integer, null, null, null, null, null, null, null, $$.s); }
        ;

float    : TFLOAT { $$.n = new(NodeType.Float, null, null, null, null, null, null, null, $$.s); }
         ;

string  : TSTRING { $$.n = new(NodeType.String, null, null, null, null, null, null, null, $$.s); }
        ;

bool    : TTRUE { $$.n = new(NodeType.Bool, null, null, null, null, null, null, null, "true"); }
        | TFALSE { $$.n = new(NodeType.Bool, null, null, null, null, null, null, null, "false"); }
        ;

null    : TNULL { $$.n = new(NodeType.Null, null, null, null, null, null, null, null, "null"); }
        ;

placeholder : TPLACEHOLDER { $$.n = new(NodeType.Placeholder, null, null, null, null, null, null, null, $$.s); }
            ;

%%
