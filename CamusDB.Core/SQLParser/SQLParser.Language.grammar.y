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
%right TNOT
%left TLIKE TILIKE
%left TEQUALS TNOTEQUALS TBETWEEN
%left TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS
%left TADD TMINUS
%left TMULT TDIV

%token TDIGIT TFLOAT TSTRING TIDENTIFIER TPLACEHOLDER LPAREN RPAREN TCOMMA TMULT TADD TMINUS TDIV TSELECT TFROM TWHERE 
%token TEQUALS TNOTEQUALS TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS TAND TOR TORDER TBY TASC TDESC
%token TTRUE TFALSE TUPDATE TSET TDELETE TINSERT TINTO TVALUES TCREATE TTABLE TNOT TNULL
%token TTYPE_STRING TTYPE_INT64 TTYPE_FLOAT64 TTYPE_OBJECT_ID TTYPE_BOOL TCAST TINTEGER TDOUBLE
%token TTYPE_FLOAT32 TTYPE_BYTES TTYPE_DATE TTYPE_DATETIME TTYPE_UUID TTYPE_ARRAY
%token TPRIMARY TKEY TUNIQUE TINDEX TALTER TWADD TDROP TCOLUMN TESCAPED_IDENTIFIER TLIMIT TOFFSET TAS TGROUP TSHOW TCONSTRAINT
%token TCOLUMNS TTABLES TDESCRIBE TDATABASES TDATABASE TAT LBRACE RBRACE TINDEXES TLIKE TILIKE TDEFAULT TIF TEXISTS TON TIN TIS
%token TBEGIN TSTART TTRANSACTION TROLLBACK TCOMMIT TJOIN TINNER TDOT THAVING TDISTINCT TBETWEEN TEXPLAIN
%token TRENAME TTO TANALYZE TBRANCH TBRANCHES TANCESTORS TEVICT

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
        | create_database_stmt { $$.n = $1.n; }
        | drop_database_stmt { $$.n = $1.n; }
        | rename_database_stmt { $$.n = $1.n; }
        | alter_table_stmt { $$.n = $1.n; }
        | show_stmt { $$.n = $1.n; }
        | create_index_stmt { $$.n = $1.n; }
        | begin_stmt { $$.n = $1.n; }
        | commit_stmt { $$.n = $1.n; }
        | rollback_stmt { $$.n = $1.n; }
        | set_transaction_stmt { $$.n = $1.n; }
        | analyze_stmt { $$.n = $1.n; }
        | evict_cache_stmt { $$.n = $1.n; }
        ;

opt_distinct : TDISTINCT { $$.s = "1"; }
             | { $$.s = null; }
             ;

select_stmt : TSELECT opt_distinct select_field_list TFROM select_table opt_where opt_group opt_having opt_order opt_limit opt_offset
            { $$.n = new(NodeType.Select, $3.n, $5.n, $6.n, $9.n, $10.n, $11.n, $7.n, $2.s, $8.n); }
            ;

/* EXPLAIN [( LOGICAL | PHYSICAL | ANALYZE )] select_stmt
   LOGICAL and PHYSICAL are plain identifiers dispatched at runtime.
   ANALYZE is a reserved keyword (TANALYZE) since ANALYZE TABLE was added, so it
   gets its own grammar alternative instead of going through the TIDENTIFIER branch. */
explain_stmt : TEXPLAIN select_stmt
             { $$.n = new(NodeType.Explain, $2.n, null, null, null, null, null, null, null); }
             | TEXPLAIN LPAREN TANALYZE RPAREN select_stmt
             { $$.n = new(NodeType.ExplainAnalyze, $5.n, null, null, null, null, null, null, null); }
             | TEXPLAIN LPAREN TIDENTIFIER RPAREN select_stmt
             {
               string opt = $3.s.ToUpperInvariant();
               if (opt == "LOGICAL")
                   $$.n = new(NodeType.ExplainLogical, $5.n, null, null, null, null, null, null, null);
               else if (opt == "PHYSICAL")
                   $$.n = new(NodeType.ExplainPhysical, $5.n, null, null, null, null, null, null, null);
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

begin_stmt : TBEGIN { $$.n = NodeAst.Begin; }
           | TSTART TTRANSACTION { $$.n = NodeAst.Begin; }
           ;

commit_stmt : TCOMMIT { $$.n = NodeAst.Commit; }             
            ;

rollback_stmt : TROLLBACK { $$.n = NodeAst.Rollback; }
              ;

/* SET TRANSACTION ISOLATION LEVEL SERIALIZABLE [READ ONLY | READ WRITE]
 *
 * Layout of the resulting NodeAst:
 *   yytext   = isolation level string ("Serializable" or "ReadCommitted")
 *   leftAst  = NodeType.String whose yytext is the mode ("ReadOnly" or "ReadWrite")
 *
 * The 5-identifier form covers:
 *     SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
 * The 7-identifier form covers:
 *     SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY
 *     SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE
 *
 * gppg resolves the shift/reduce conflict between the two productions by preferring shift,
 * so the 5-identifier rule fires only when the 6th token is NOT an identifier (i.e. end of
 * statement), and the 7-identifier rule fires when it is.
 */
set_transaction_stmt
    : TSET TTRANSACTION TIDENTIFIER TIDENTIFIER TIDENTIFIER
      {
          if (!string.Equals($3.s, "isolation", StringComparison.OrdinalIgnoreCase) ||
              !string.Equals($4.s, "level", StringComparison.OrdinalIgnoreCase))
              throw new CamusDBException(
                  CamusDBErrorCodes.InvalidInput,
                  "Expected: SET TRANSACTION ISOLATION LEVEL {SERIALIZABLE | READ COMMITTED}");
          string level = $5.s.ToUpperInvariant() switch {
              "SERIALIZABLE"   => "Serializable",
              _ => throw new CamusDBException(
                      CamusDBErrorCodes.InvalidInput,
                      "Unknown isolation level '" + $5.s + "'. Expected: SERIALIZABLE or READ COMMITTED")
          };
          $$.n = new(NodeType.SetTransaction,
                     NodeAst.TransactionModeReadWrite,
                     null, null, null, null, null, null, level);
      }
    | TSET TTRANSACTION TIDENTIFIER TIDENTIFIER TIDENTIFIER TIDENTIFIER
      {
          // SET TRANSACTION ISOLATION LEVEL READ COMMITTED — the two-word level opts down from
          // the Serializable default. Read Committed is read-write; there is no READ ONLY|WRITE variant.
          if (!string.Equals($3.s, "isolation", StringComparison.OrdinalIgnoreCase) ||
              !string.Equals($4.s, "level", StringComparison.OrdinalIgnoreCase) ||
              !string.Equals($5.s, "read", StringComparison.OrdinalIgnoreCase) ||
              !string.Equals($6.s, "committed", StringComparison.OrdinalIgnoreCase))
              throw new CamusDBException(
                  CamusDBErrorCodes.InvalidInput,
                  "Expected: SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
          $$.n = new(NodeType.SetTransaction,
                     NodeAst.TransactionModeReadWrite,
                     null, null, null, null, null, null, "ReadCommitted");
      }
    | TSET TTRANSACTION TIDENTIFIER TIDENTIFIER TIDENTIFIER TIDENTIFIER TIDENTIFIER
      {
          if (!string.Equals($3.s, "isolation", StringComparison.OrdinalIgnoreCase) ||
              !string.Equals($4.s, "level", StringComparison.OrdinalIgnoreCase) ||
              !string.Equals($5.s, "serializable", StringComparison.OrdinalIgnoreCase))
              throw new CamusDBException(
                  CamusDBErrorCodes.InvalidInput,
                  "Expected: SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ {ONLY|WRITE}");
          if (!string.Equals($6.s, "read", StringComparison.OrdinalIgnoreCase))
              throw new CamusDBException(
                  CamusDBErrorCodes.InvalidInput,
                  "Expected READ ONLY or READ WRITE after isolation level");
          string mode = $7.s.ToUpperInvariant() switch {
              "ONLY"  => "ReadOnly",
              "WRITE" => "ReadWrite",
              _ => throw new CamusDBException(
                      CamusDBErrorCodes.InvalidInput,
                      "Expected ONLY or WRITE after READ, got '" + $7.s + "'")
          };
          NodeAst modeAst = mode == "ReadOnly"
              ? NodeAst.TransactionModeReadOnly
              : NodeAst.TransactionModeReadWrite;
          $$.n = new(NodeType.SetTransaction,
                     modeAst,
                     null, null, null, null, null, null, "Serializable");
      }
    ;

create_table_stmt : TCREATE TTABLE any_identifier LPAREN create_table_item_list RPAREN { $$.n = new(NodeType.CreateTable, $3.n, $5.n, null, null, null, null, null, null); }
                  | TCREATE TTABLE TIF TNOT TEXISTS any_identifier LPAREN create_table_item_list RPAREN { $$.n = new(NodeType.CreateTableIfNotExists, $6.n, $8.n, null, null, null, null, null, null); }
                  | TCREATE TTABLE any_identifier LPAREN create_table_item_list RPAREN create_table_constraint_list { $$.n = new(NodeType.CreateTable, $3.n, $5.n, $7.n, null, null, null, null, null); }
                  | TCREATE TTABLE TIF TNOT TEXISTS any_identifier LPAREN create_table_item_list RPAREN create_table_constraint_list { $$.n = new(NodeType.CreateTableIfNotExists, $6.n, $8.n, $10.n, null, null, null, null, null); }
                  ;

drop_table_stmt : TDROP TTABLE any_identifier { $$.n = new(NodeType.DropTable, $3.n, null, null, null, null, null, null, null); }
                | TDROP TTABLE TIF TEXISTS any_identifier { $$.n = new(NodeType.DropTableIfExists, $5.n, null, null, null, null, null, null, null); }
                ;

create_database_stmt : TCREATE TDATABASE any_identifier { $$.n = new(NodeType.CreateDatabase, $3.n, null, null, null, null, null, null, null); }
                     | TCREATE TDATABASE TIF TNOT TEXISTS any_identifier { $$.n = new(NodeType.CreateDatabaseIfNotExists, $6.n, null, null, null, null, null, null, null); }
                     | TCREATE TDATABASE any_identifier TBRANCH TFROM any_identifier { $$.n = new(NodeType.CreateDatabaseBranch, $3.n, $6.n, null, null, null, null, null, null); }
                     | TCREATE TDATABASE TIF TNOT TEXISTS any_identifier TBRANCH TFROM any_identifier { $$.n = new(NodeType.CreateDatabaseBranchIfNotExists, $6.n, $9.n, null, null, null, null, null, null); }
                     ;

drop_database_stmt : TDROP TDATABASE any_identifier { $$.n = new(NodeType.DropDatabase, $3.n, null, null, null, null, null, null, null); }
                   | TDROP TDATABASE TIF TEXISTS any_identifier { $$.n = new(NodeType.DropDatabaseIfExists, $5.n, null, null, null, null, null, null, null); }
				;

rename_database_stmt : TRENAME TDATABASE any_identifier TTO any_identifier { $$.n = new(NodeType.RenameDatabase, $3.n, $5.n, null, null, null, null, null, null); }
                     ;

alter_table_stmt : TALTER TTABLE any_identifier TWADD any_identifier field_type { $$.n = new(NodeType.AlterTableAddColumn, $3.n, $5.n, $6.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD any_identifier field_type create_table_field_constraint_list { $$.n = new(NodeType.AlterTableAddColumn, $3.n, $5.n, $6.n, $7.n, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TCOLUMN any_identifier field_type { $$.n = new(NodeType.AlterTableAddColumn, $3.n, $6.n, $7.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TCOLUMN any_identifier field_type create_table_field_constraint_list { $$.n = new(NodeType.AlterTableAddColumn, $3.n, $6.n, $7.n, $8.n, null, null, null, null); }
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
                 | TALTER TTABLE any_identifier TRENAME TTO any_identifier { $$.n = new(NodeType.AlterTableRenameTo, $3.n, $6.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TRENAME TCOLUMN any_identifier TTO any_identifier { $$.n = new(NodeType.AlterTableRenameColumn, $3.n, $6.n, $8.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TRENAME TINDEX any_identifier TTO any_identifier { $$.n = new(NodeType.AlterTableRenameIndex, $3.n, $6.n, $8.n, null, null, null, null, null); }
				 ;

create_index_stmt : TCREATE TINDEX any_identifier TON any_identifier LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddIndex, $5.n, $3.n, $7.n, null, null, null, null, null); }
                  | TCREATE TINDEX TIF TNOT TEXISTS any_identifier TON any_identifier LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddIndexIfNotExists, $8.n, $6.n, $10.n, null, null, null, null, null); }
                  | TCREATE TUNIQUE TINDEX any_identifier TON any_identifier LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddUniqueIndex, $6.n, $4.n, $8.n, null, null, null, null, null); }
                  | TCREATE TUNIQUE TINDEX TIF TNOT TEXISTS any_identifier TON any_identifier LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddUniqueIndexIfNotExists, $9.n, $7.n, $11.n, null, null, null, null, null); }
                  ;

show_stmt : TSHOW TCOLUMNS TFROM any_identifier { $$.n = new(NodeType.ShowColumns, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TTABLES { $$.n = NodeAst.ShowTables; }
          | TSHOW TTABLES TLIKE string { $$.n = new(NodeType.ShowTables, $4.n, null, null, null, null, null, null, null); }
          | TDESCRIBE any_identifier { $$.n = new(NodeType.ShowColumns, $2.n, null, null, null, null, null, null, null); }
          | TDESC any_identifier { $$.n = new(NodeType.ShowColumns, $2.n, null, null, null, null, null, null, null); }
          | TSHOW TCREATE TTABLE any_identifier { $$.n = new(NodeType.ShowCreateTable, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TDATABASE { $$.n = NodeAst.ShowDatabase; }
          | TSHOW TDATABASES { $$.n = NodeAst.ShowDatabases; }
          | TSHOW TDATABASES TLIKE string { $$.n = new(NodeType.ShowDatabases, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TINDEXES TFROM any_identifier { $$.n = new(NodeType.ShowIndexes, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TINDEX TFROM any_identifier { $$.n = new(NodeType.ShowIndexes, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TBRANCHES TFROM any_identifier { $$.n = new(NodeType.ShowBranches, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TANCESTORS TFROM any_identifier { $$.n = new(NodeType.ShowAncestors, $4.n, null, null, null, null, null, null, null); }
          ;

analyze_stmt : TANALYZE any_identifier
             { $$.n = new(NodeType.AnalyzeTable, $2.n, null, null, null, null, null, null, null); }
             | TANALYZE TTABLE any_identifier
             { $$.n = new(NodeType.AnalyzeTable, $3.n, null, null, null, null, null, null, null); }
             ;

evict_cache_stmt : TEVICT TIDENTIFIER TSTRING
                 {
                   if (!string.Equals($2.s, "cache", System.StringComparison.OrdinalIgnoreCase))
                       throw new CamusDB.Core.CamusDBException(
                           CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                           "Expected: EVICT CACHE '<name>' or EVICT CACHE ALL");
                   $$.n = new(NodeType.EvictCache, null, null, null, null, null, null, null, $3.s);
                 }
                 | TEVICT TIDENTIFIER TIDENTIFIER
                 {
                   if (!string.Equals($2.s, "cache", System.StringComparison.OrdinalIgnoreCase) ||
                       !string.Equals($3.s, "all", System.StringComparison.OrdinalIgnoreCase))
                       throw new CamusDB.Core.CamusDBException(
                           CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                           "Expected: EVICT CACHE '<name>' or EVICT CACHE ALL");
                   $$.n = new(NodeType.EvictCacheAll, null, null, null, null, null, null, null, null);
                 }
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
               {
                 // The at-brace hint with a cache key is an accepted alias of the bare cache hint;
                 // any other key (e.g. FORCE_INDEX) stays an index-style table hint resolved later.
                 if ($3.n.yytext!.Equals("cache", System.StringComparison.Ordinal))
                     $$.n = new(NodeType.CacheHint, null, null, null, null, null, null, null, $5.n.yytext);
                 else
                     $$.n = new(NodeType.IdentifierWithOpts, null, $3.n, $5.n, null, null, null, null, null);
               }
               | TAT LBRACE identifier TEQUALS identifier TCOMMA cache_hint_options RBRACE
               {
                 // ttl/strict options are only meaningful for the cache hint, never for FORCE_INDEX.
                 if (!$3.n.yytext!.Equals("cache", System.StringComparison.Ordinal))
                     throw new CamusDB.Core.CamusDBException(
                         CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                         "Table hint options (ttl, strict) are only valid for the cache hint; got '" + $3.n.yytext + "'");
                 $$.n = new(NodeType.CacheHint, $7.n, null, null, null, null, null, null, $5.n.yytext);
               }
               | LBRACE cache_hint_spec RBRACE
               { $$.n = $2.n; }
               | { $$.n = null; }
               ;

/* {cache=name} or {cache=name, ttl=30s} or {cache=name, strict} or combinations.
 *
 * NodeType.CacheHint layout:
 *   yytext      = cache name (lowercased at reduce time by the `identifier` production's ToLowerInvariant)
 *   leftAst     = option list: ExprList tree of option nodes, single option node, or null
 *
 * Option nodes:
 *   NodeType.String  with yytext="strict"  — strict validation flag
 *   NodeType.Integer with yytext=<ms>      — per-hint TTL override, always stored as milliseconds
 *
 * TTL format: ttl=<N><unit> where unit is ms | s | m | h, or bare ttl=<N> (milliseconds).
 * The grammar normalises every form to milliseconds before storing in yytext, so the
 * SelectQueryCreator / CollectCacheHintOptions path always reads plain integer milliseconds.
 * Valid range: 1 … 2147483647 ms. ttl=0 and overflow both produce a CamusDBException.
 */
cache_hint_spec : TIDENTIFIER TEQUALS identifier
               {
                 if (!$1.s.Equals("cache", System.StringComparison.OrdinalIgnoreCase))
                     throw new CamusDB.Core.CamusDBException(
                         CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                         "Unknown table hint '" + $1.s + "'. Supported: {cache=name}");
                 $$.n = new(NodeType.CacheHint, null, null, null, null, null, null, null, $3.n.yytext);
               }
               | TIDENTIFIER TEQUALS identifier TCOMMA cache_hint_options
               {
                 if (!$1.s.Equals("cache", System.StringComparison.OrdinalIgnoreCase))
                     throw new CamusDB.Core.CamusDBException(
                         CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                         "Unknown table hint '" + $1.s + "'. Supported: {cache=name}");
                 $$.n = new(NodeType.CacheHint, $5.n, null, null, null, null, null, null, $3.n.yytext);
               }
               ;

cache_hint_options : cache_hint_option { $$.n = $1.n; }
                   | cache_hint_options TCOMMA cache_hint_option
                   { $$.n = new(NodeType.ExprList, $1.n, $3.n, null, null, null, null, null, null); }
                   ;

cache_hint_option : TIDENTIFIER
                  {
                    if (!$1.s.Equals("strict", System.StringComparison.OrdinalIgnoreCase))
                        throw new CamusDB.Core.CamusDBException(
                            CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                            "Unknown cache hint option '" + $1.s + "'. Supported: ttl=<value><unit>, strict");
                    $$.n = new(NodeType.String, null, null, null, null, null, null, null, "strict");
                  }
                  | TIDENTIFIER TEQUALS TDIGIT
                  {
                    if (!$1.s.Equals("ttl", System.StringComparison.OrdinalIgnoreCase))
                        throw new CamusDB.Core.CamusDBException(
                            CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                            "Unknown cache hint option '" + $1.s + "'. Supported: ttl=<value><unit>, strict");
                    if (!long.TryParse($3.s, out long ttlBareMs) || ttlBareMs <= 0 || ttlBareMs > int.MaxValue)
                        throw new CamusDB.Core.CamusDBException(
                            CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                            "ttl value must be between 1 and " + int.MaxValue + " (milliseconds); got: " + $3.s);
                    $$.n = new(NodeType.Integer, null, null, null, null, null, null, null, ttlBareMs.ToString());
                  }
                  | TIDENTIFIER TEQUALS TDIGIT TIDENTIFIER
                  {
                    if (!$1.s.Equals("ttl", System.StringComparison.OrdinalIgnoreCase))
                        throw new CamusDB.Core.CamusDBException(
                            CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                            "Unknown cache hint option '" + $1.s + "'. Supported: ttl=<value><unit>, strict");
                    if (!long.TryParse($3.s, out long ttlRaw) || ttlRaw <= 0 || ttlRaw > int.MaxValue)
                        throw new CamusDB.Core.CamusDBException(
                            CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                            "ttl value must be a positive integer no greater than " + int.MaxValue + "; got: " + $3.s);
                    // ttlRaw is bounded to int.MaxValue above, so the unit multiplication cannot
                    // overflow Int64 (max int.MaxValue * 3_600_000 stays well within long); the
                    // post-multiply check enforces the true millisecond ceiling.
                    long ttlUnitMs = $4.s.ToLowerInvariant() switch {
                        "ms" => ttlRaw,
                        "s"  => ttlRaw * 1000L,
                        "m"  => ttlRaw * 60_000L,
                        "h"  => ttlRaw * 3_600_000L,
                        _    => throw new CamusDB.Core.CamusDBException(
                                    CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                                    "Unknown ttl unit '" + $4.s + "'. Supported: ms, s, m, h"),
                    };
                    if (ttlUnitMs > int.MaxValue)
                        throw new CamusDB.Core.CamusDBException(
                            CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                            "ttl value exceeds maximum (" + int.MaxValue + " ms); got: " + $3.s + $4.s);
                    $$.n = new(NodeType.Integer, null, null, null, null, null, null, null, ttlUnitMs.ToString());
                  }
                  | TIDENTIFIER TEQUALS identifier
                  {
                    if ($1.s.Equals("ttl", System.StringComparison.OrdinalIgnoreCase))
                        throw new CamusDB.Core.CamusDBException(
                            CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                            "cache hint option 'ttl' requires an integer value, optionally with a unit (ms, s, m, h); got: " + $3.n.yytext);
                    throw new CamusDB.Core.CamusDBException(
                        CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                        "Unknown cache hint option '" + $1.s + "'. Supported: ttl=<value><unit>, strict");
                  }
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

create_table_field_constraint : TNULL { $$.n = NodeAst.ConstraintNull; }
                        | TNOT TNULL { $$.n = NodeAst.ConstraintNotNull; }
						| TPRIMARY TKEY { $$.n = NodeAst.ConstraintPrimaryKey; }
                        | TUNIQUE { $$.n = NodeAst.ConstraintUnique; }
                        | TDEFAULT LPAREN default_expr RPAREN { $$.n = new(NodeType.ConstraintDefault, $3.n, null, null, null, null, null, null, null); }
                        ;

default_expr : int { $$.n = $1.n; $$.s = $1.s; }
             | float { $$.n = $1.n; $$.s = $1.s; }
             | string { $$.n = $1.n; $$.s = $1.s; }
             | bool { $$.n = $1.n; $$.s = $1.s; }
             | null { $$.n = $1.n; $$.s = $1.s; }             
			 ;

field_type : TTYPE_OBJECT_ID { $$.n = NodeAst.TypeObjectId; }
           | TTYPE_STRING { $$.n = NodeAst.TypeString; }
           | TTYPE_STRING LPAREN TDIGIT RPAREN { $$.n = new(NodeType.TypeStringSized, null, null, null, null, null, null, null, $3.s); }
           | TTYPE_INT64 { $$.n = NodeAst.TypeInteger64; }
           | TTYPE_FLOAT64 { $$.n = NodeAst.TypeFloat64; }
           | TTYPE_BOOL { $$.n = NodeAst.TypeBool; }
           | TTYPE_FLOAT32 { $$.n = NodeAst.TypeFloat32; }
           | TTYPE_BYTES { $$.n = NodeAst.TypeBytes; }
           | TTYPE_DATE { $$.n = NodeAst.TypeDate; }
           | TTYPE_DATETIME { $$.n = NodeAst.TypeDateTime; }
           | TTYPE_UUID { $$.n = NodeAst.TypeUuid; }
           | TTYPE_ARRAY LPAREN field_type RPAREN { $$.n = new(NodeType.TypeArray, $3.n, null, null, null, null, null, null, null); }
           ;

cast_target_type : field_type { $$.n = $1.n; $$.s = $1.s; }
                 | TINTEGER { $$.n = NodeAst.TypeInteger64; }
                 | TDOUBLE { $$.n = NodeAst.TypeFloat64; }
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
           | not_expr { $$.n = $1.n; }
           | add_expr { $$.n = $1.n; }
           | sub_expr { $$.n = $1.n; }
           | mult_expr { $$.n = $1.n; }
           | div_expr { $$.n = $1.n; }
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

not_expr  : TNOT condition { $$.n = new(NodeType.ExprNot, $2.n, null, null, null, null, null, null, null); }
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

div_expr  : condition TDIV  condition { $$.n = new(NodeType.ExprDiv,  $1.n, $3.n, null, null, null, null, null, null); }
          ;

like_expr : condition TLIKE condition { $$.n = new(NodeType.ExprLike, $1.n, $3.n, null, null, null, null, null, null); }
          ;

ilike_expr : condition TILIKE condition { $$.n = new(NodeType.ExprILike, $1.n, $3.n, null, null, null, null, null, null); }
           ;

is_null_expr : condition TIS TNULL { $$.n = new(NodeType.ExprIsNull, $1.n, NodeAst.Null, null, null, null, null, null, null); }
             ;

is_not_null_expr : condition TIS TNOT TNULL { $$.n = new(NodeType.ExprIsNotNull, $1.n, $3.n, null, null, null, null, null, null); }
                 ;

in_subquery_expr : condition TIN query_expr { $$.n = new(NodeType.ExprInSubquery, $1.n, $3.n, null, null, null, null, null, null); }
                 | condition TIN LPAREN in_value_list RPAREN { $$.n = new(NodeType.ExprInMembership, $1.n, $4.n, null, null, null, null, null, null); }
                 ;

not_in_subquery_expr : condition TNOT TIN query_expr { $$.n = new(NodeType.ExprNotInSubquery, $1.n, $4.n, null, null, null, null, null, null); }
                     | condition TNOT TIN LPAREN in_value_list RPAREN { $$.n = new(NodeType.ExprNotInMembership, $1.n, $5.n, null, null, null, null, null, null); }
                     ;

in_value_list : in_value_list TCOMMA in_value_item { $$.n = new(NodeType.ExprList, $1.n, $3.n, null, null, null, null, null, null); }
              | in_value_item { $$.n = $1.n; $$.s = $1.s; }
              ;

in_value_item : simple_expr { $$.n = $1.n; $$.s = $1.s; }
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

use_default_expr : TDEFAULT { $$.n = NodeAst.ExprDefault; }
                 ;

projection_all : TMULT { $$.n = NodeAst.ExprAllFields; }
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

bool    : TTRUE { $$.n = NodeAst.True; }
        | TFALSE { $$.n = NodeAst.False; }
        ;

null    : TNULL { $$.n = NodeAst.Null; }
        ;

placeholder : TPLACEHOLDER { $$.n = new(NodeType.Placeholder, null, null, null, null, null, null, null, $$.s); }
            ;

%%
