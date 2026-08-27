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
%left TLIKE TILIKE TREGEXMATCH TREGEXIMATCH TREGEXNOTMATCH TREGEXNOTIMATCH
%left TEQUALS TNOTEQUALS TBETWEEN
%left TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS
%left TADD TMINUS
%left TMULT TDIV
/* IS (IS NULL / IS NOT NULL), IN, and the qualified-name dot bind tighter than any binary operator.
   Without a precedence they produced shift/reduce warnings that gppg resolved by its default shift —
   which is already the intended parse (a AND b IS NULL -> a AND (b IS NULL); a.b as one name). Declaring
   their precedence makes that resolution explicit and removes the warnings without changing any parse. */
%left TIS TIN TDOT

%token TBYTESLIT LBRACKET RBRACKET
%token TDIGIT TFLOAT TSTRING TIDENTIFIER TPLACEHOLDER LPAREN RPAREN TCOMMA TMULT TADD TMINUS TDIV TSELECT TFROM TWHERE 
%token TEQUALS TNOTEQUALS TLESSTHAN TGREATERTHAN TLESSTHANEQUALS TGREATERTHANEQUALS TAND TOR TORDER TBY TASC TDESC
%token TTRUE TFALSE TUPDATE TSET TDELETE TINSERT TINTO TVALUES TCREATE TTABLE TNOT TNULL
%token TTYPE_STRING TTYPE_INT64 TTYPE_FLOAT64 TTYPE_OBJECT_ID TTYPE_BOOL TCAST TINTEGER TDOUBLE
%token TTYPE_FLOAT32 TTYPE_BYTES TTYPE_DATE TTYPE_DATETIME TTYPE_UUID TTYPE_ARRAY
%token TPRIMARY TKEY TUNIQUE TINDEX TALTER TWADD TDROP TCOLUMN TESCAPED_IDENTIFIER TLIMIT TOFFSET TAS TGROUP TSHOW TCONSTRAINT TCHECK
%token TCOLUMNS TTABLES TDESCRIBE TDATABASES TDATABASE TAT LBRACE RBRACE TINDEXES TLIKE TILIKE TDEFAULT TIF TEXISTS TON TIN TIS
%token TREGEXMATCH TREGEXIMATCH TREGEXNOTMATCH TREGEXNOTIMATCH
%token TBEGIN TSTART TTRANSACTION TROLLBACK TCOMMIT TJOIN TINNER TDOT THAVING TDISTINCT TBETWEEN TEXPLAIN
%token TRENAME TTO TANALYZE TBRANCH TBRANCHES TANCESTORS TEVICT TFORCE TRELINK TORPHAN
%token TTRUNCATE
%token TCASE TWHEN TTHEN TELSE TEND
%token TINCLUDE
%token TASOFSYSTEMTIME
%token TCOMMENT
%token TUSER TIDENTIFIED TWITH TGRANT TGRANTS TREVOKE TPRIVILEGES TFOR
%token TRESET
/* VIEW/VIEWS/MATERIALIZED/REFRESH are the only new reserved words for views. Every other word the
   view statements need — REPLACE, CASCADE, RESTRICT, CASCADED, LOCAL, OPTION, OWNER, CONCURRENTLY —
   is matched as a plain identifier and validated in the parse action, so each stays usable as a
   table or column name. "owner" in particular is already a column name in the test corpus. */
%token TVIEW TVIEWS TMATERIALIZED TREFRESH
/* One unquoted "table@index" pair, produced by a single scanner rule so the '@' never reaches the
   parser as a placeholder. Accepted only in the SHOW ... FROM INDEX productions and split there. */
%token TQUALIFIED_INDEX

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
        | truncate_table_stmt { $$.n = $1.n; }
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
        | set_cluster_setting_stmt { $$.n = $1.n; }
        | reset_cluster_setting_stmt { $$.n = $1.n; }
        | analyze_stmt { $$.n = $1.n; }
        | evict_cache_stmt { $$.n = $1.n; }
        | comment_stmt { $$.n = $1.n; }
        | create_user_stmt { $$.n = $1.n; }
        | alter_user_stmt { $$.n = $1.n; }
        | drop_user_stmt { $$.n = $1.n; }
        | grant_stmt { $$.n = $1.n; }
        | revoke_stmt { $$.n = $1.n; }
        | create_view_stmt { $$.n = $1.n; }
        | drop_view_stmt { $$.n = $1.n; }
        | alter_view_stmt { $$.n = $1.n; }
        | create_matview_stmt { $$.n = $1.n; }
        | refresh_matview_stmt { $$.n = $1.n; }
        | drop_matview_stmt { $$.n = $1.n; }
        | alter_matview_stmt { $$.n = $1.n; }
        ;

opt_distinct : TDISTINCT { $$.s = "1"; }
             | { $$.s = null; }
             ;

select_stmt : TSELECT opt_distinct select_field_list TFROM select_table opt_as_of opt_where opt_group opt_having opt_order opt_limit opt_offset
            { $$.n = new(NodeType.Select, $3.n, $5.n, $7.n, $10.n, $11.n, $12.n, $8.n, $2.s, $9.n, $6.n); }
            | TSELECT opt_distinct select_field_list opt_limit opt_offset
            { $$.n = new(NodeType.Select, $3.n, null, null, null, $4.n, $5.n, null, $2.s, null); }
            ;

/* AS OF SYSTEM TIME '<expr>' — time-travel read. Placed immediately after the FROM clause and before
   WHERE, matching the standard SQL time-travel placement. The scanner recognises the whole
   "AS OF SYSTEM TIME" phrase as a single TASOFSYSTEMTIME token, so it never collides with the
   table-alias rule (opt_table_alias : TAS any_identifier). The value is a string ('-10s' relative
   offset, or an absolute timestamp), a bare integer (Unix epoch milliseconds), or a @placeholder;
   the executor resolves it to an HLC snapshot timestamp and pins the read to it. */
opt_as_of : TASOFSYSTEMTIME as_of_value { $$.n = $2.n; }
          | { $$.n = null; }
          ;

as_of_value : string { $$.n = $1.n; $$.s = $1.s; }
            | int { $$.n = $1.n; $$.s = $1.s; }
            | placeholder { $$.n = $1.n; $$.s = $1.s; }
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
            /* INSERT ... SELECT: the source query hangs off extendedOne, the same slot the VALUES
               batch list uses, so both INSERT forms stay structurally parallel. */
            | TINSERT TINTO any_identifier LPAREN insert_field_list RPAREN select_stmt { $$.n = new(NodeType.InsertSelect, $3.n, $5.n, $7.n, null, null, null, null, null); }
            | TINSERT TINTO any_identifier select_stmt { $$.n = new(NodeType.InsertSelect, $3.n, null, $4.n, null, null, null, null, null); }
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
    | TSET TTRANSACTION TIDENTIFIER TIDENTIFIER
      {
          // SET TRANSACTION LOCKING  { PESSIMISTIC | OPTIMISTIC }
          // SET TRANSACTION PRIORITY { BACKGROUND | LOW | NORMAL | HIGH | CRITICAL }
          //
          // Both settings share ONE 4-token production and dispatch on $3. A second production with
          // the same token shape would be a reduce/reduce conflict, so a new 4-token SET TRANSACTION
          // setting must be added here rather than alongside.
          //
          // This 4-token form must be listed last so the parser prefers to shift when a 5th
          // TIDENTIFIER follows (gppg resolves shift/reduce by preferring shift, so the 5-token
          // isolation productions fire when there are more identifiers ahead).
          if (string.Equals($3.s, "locking", StringComparison.OrdinalIgnoreCase))
          {
              string lockingMode = $4.s.ToUpperInvariant() switch {
                  "PESSIMISTIC" => "Pessimistic",
                  "OPTIMISTIC"  => "Optimistic",
                  _ => throw new CamusDBException(
                          CamusDBErrorCodes.InvalidInput,
                          "Unknown locking mode '" + $4.s + "'. Expected: PESSIMISTIC or OPTIMISTIC")
              };
              $$.n = new(NodeType.SetTransactionLocking,
                         null, null, null, null, null, null, null, lockingMode);
          }
          else if (string.Equals($3.s, "priority", StringComparison.OrdinalIgnoreCase))
          {
              string priority = $4.s.ToUpperInvariant() switch {
                  "BACKGROUND" => "Background",
                  "LOW"        => "Low",
                  "NORMAL"     => "Normal",
                  "HIGH"       => "High",
                  "CRITICAL"   => "Critical",
                  _ => throw new CamusDBException(
                          CamusDBErrorCodes.InvalidInput,
                          "Unknown transaction priority '" + $4.s +
                          "'. Expected: BACKGROUND, LOW, NORMAL, HIGH or CRITICAL")
              };
              $$.n = new(NodeType.SetTransactionPriority,
                         null, null, null, null, null, null, null, priority);
          }
          else
              throw new CamusDBException(
                  CamusDBErrorCodes.InvalidInput,
                  "Expected: SET TRANSACTION LOCKING { PESSIMISTIC | OPTIMISTIC } or " +
                  "SET TRANSACTION PRIORITY { BACKGROUND | LOW | NORMAL | HIGH | CRITICAL }");
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

/* SET CLUSTER SETTING <name> = <value>
 * RESET CLUSTER SETTING <name>
 *
 * CLUSTER and SETTING are matched as plain identifiers, not keywords, so both stay usable as
 * column and table names; the words are validated in the action like SHOW ENGINE STATS. No
 * conflict with SET TRANSACTION (disambiguated by the reserved TTRANSACTION token) or with
 * ALTER TABLE ... RESET (TRESET there never starts a statement).
 *
 * Node layout: leftAst = setting-name identifier (dotted names like kahuna.x parse as one
 * qualified identifier, so they reach the executor and get the accurate restart-class rejection
 * instead of a syntax error); rightAst = the value literal (String/Integer/Float/Bool, or a bare
 * identifier so enum spellings like read_committed need no quotes).
 */
set_cluster_setting_stmt
    : TSET TIDENTIFIER TIDENTIFIER any_identifier TEQUALS cluster_setting_value
      {
          if (!string.Equals($2.s, "cluster", System.StringComparison.OrdinalIgnoreCase) ||
              !string.Equals($3.s, "setting", System.StringComparison.OrdinalIgnoreCase))
              throw new CamusDB.Core.CamusDBException(
                  CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                  "Expected: SET CLUSTER SETTING <name> = <value>");
          $$.n = new(NodeType.SetClusterSetting, $4.n, $6.n, null, null, null, null, null, null);
      }
    ;

reset_cluster_setting_stmt
    : TRESET TIDENTIFIER TIDENTIFIER any_identifier
      {
          if (!string.Equals($2.s, "cluster", System.StringComparison.OrdinalIgnoreCase) ||
              !string.Equals($3.s, "setting", System.StringComparison.OrdinalIgnoreCase))
              throw new CamusDB.Core.CamusDBException(
                  CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                  "Expected: RESET CLUSTER SETTING <name>");
          $$.n = new(NodeType.ResetClusterSetting, $4.n, null, null, null, null, null, null, null);
      }
    ;

cluster_setting_value : string { $$.n = $1.n; }
                      | int { $$.n = $1.n; }
                      | float { $$.n = $1.n; }
                      | bool { $$.n = $1.n; }
                      | identifier { $$.n = $1.n; }
                      | TMINUS TDIGIT { $$.n = new(NodeType.Integer, null, null, null, null, null, null, null, string.Concat("-", $2.s)); }
                      | TMINUS TFLOAT { $$.n = new(NodeType.Float, null, null, null, null, null, null, null, string.Concat("-", $2.s)); }
                      ;

create_table_stmt : TCREATE TTABLE any_identifier TRELINK TTO string { $$.n = new(NodeType.CreateTableRelink, $3.n, $6.n, null, null, null, null, null, null); }
                  | TCREATE TTABLE any_identifier LPAREN create_table_item_list RPAREN opt_table_comment opt_table_settings { $$.n = new(NodeType.CreateTable, $3.n, $5.n, null, $7.n, $8.n, null, null, null); }
                  | TCREATE TTABLE TIF TNOT TEXISTS any_identifier LPAREN create_table_item_list RPAREN opt_table_comment opt_table_settings { $$.n = new(NodeType.CreateTableIfNotExists, $6.n, $8.n, null, $10.n, $11.n, null, null, null); }
                  | TCREATE TTABLE any_identifier LPAREN create_table_item_list RPAREN create_table_constraint_list opt_table_comment opt_table_settings { $$.n = new(NodeType.CreateTable, $3.n, $5.n, $7.n, $8.n, $9.n, null, null, null); }
                  | TCREATE TTABLE TIF TNOT TEXISTS any_identifier LPAREN create_table_item_list RPAREN create_table_constraint_list opt_table_comment opt_table_settings { $$.n = new(NodeType.CreateTableIfNotExists, $6.n, $8.n, $10.n, $11.n, $12.n, null, null, null); }
                  /* CREATE TABLE ... AS SELECT: no column list — names and types are derived from
                     the source query's output columns. */
                  | TCREATE TTABLE any_identifier TAS select_stmt opt_with_data { $$.n = new(NodeType.CreateTableAsSelect, $3.n, $5.n, null, null, null, null, null, $6.s); }
                  | TCREATE TTABLE TIF TNOT TEXISTS any_identifier TAS select_stmt opt_with_data { $$.n = new(NodeType.CreateTableAsSelectIfNotExists, $6.n, $8.n, null, null, null, null, null, $9.s); }
                  ;

/* WITH [NO] DATA. NO and DATA are matched as plain identifiers and validated here rather than
   promoted to reserved words, so an existing table or column named "data" keeps working. */
opt_with_data : TWITH TIDENTIFIER
                {
                  if (!string.Equals($2.s, "data", StringComparison.OrdinalIgnoreCase))
                      throw new CamusDBException(
                          CamusDBErrorCodes.InvalidInput,
                          "Expected WITH DATA or WITH NO DATA, got 'WITH " + $2.s + "'");
                  $$.s = "data";
                }
              | TWITH TIDENTIFIER TIDENTIFIER
                {
                  if (!string.Equals($2.s, "no", StringComparison.OrdinalIgnoreCase) ||
                      !string.Equals($3.s, "data", StringComparison.OrdinalIgnoreCase))
                      throw new CamusDBException(
                          CamusDBErrorCodes.InvalidInput,
                          "Expected WITH DATA or WITH NO DATA, got 'WITH " + $2.s + " " + $3.s + "'");
                  $$.s = "no data";
                }
              | { $$.s = null; }
              ;

/* TRUNCATE [TABLE] name — empties a base table by replacing its physical contents generation.
   The TABLE keyword is optional, matching PostgreSQL and MySQL. Exactly one target: several
   tables in one statement are out of scope, and widening any_identifier to a list later is not a
   breaking grammar change. */
truncate_table_stmt : TTRUNCATE TTABLE any_identifier { $$.n = new(NodeType.TruncateTable, $3.n, null, null, null, null, null, null, null); }
                    | TTRUNCATE any_identifier { $$.n = new(NodeType.TruncateTable, $2.n, null, null, null, null, null, null, null); }
                    ;

drop_table_stmt : TDROP TTABLE any_identifier { $$.n = new(NodeType.DropTable, $3.n, null, null, null, null, null, null, null); }
                | TDROP TTABLE any_identifier TFORCE { $$.n = new(NodeType.DropTable, $3.n, null, null, null, null, null, null, "force"); }
                | TDROP TTABLE TIF TEXISTS any_identifier { $$.n = new(NodeType.DropTableIfExists, $5.n, null, null, null, null, null, null, null); }
                | TDROP TTABLE TIF TEXISTS any_identifier TFORCE { $$.n = new(NodeType.DropTableIfExists, $5.n, null, null, null, null, null, null, "force"); }
                ;

/* ---------------------------------------------------------------------------------------------
   Views and materialized views.

   A non-materialized view carries: leftAst = name, rightAst = the body SELECT,
   extendedOne = the optional column-alias list, yytext = the WITH CHECK OPTION kind
   ("local"/"cascaded") or null.

   A materialized view carries: leftAst = name, rightAst = the body SELECT,
   extendedOne = the optional column-alias list, yytext = "data" or "nodata".
   --------------------------------------------------------------------------------------------- */

create_view_stmt : TCREATE TVIEW any_identifier opt_view_column_list TAS select_stmt opt_check_option
                   { $$.n = new(NodeType.CreateView, $3.n, $6.n, $4.n, null, null, null, null, $7.s); }
                 | TCREATE TOR TIDENTIFIER TVIEW any_identifier opt_view_column_list TAS select_stmt opt_check_option
                   {
                     if (!string.Equals($3.s, "replace", System.StringComparison.OrdinalIgnoreCase))
                         throw new CamusDB.Core.CamusDBException(
                             CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                             "Expected: CREATE OR REPLACE VIEW, got 'CREATE OR " + $3.s + "'");
                     $$.n = new(NodeType.CreateOrReplaceView, $5.n, $8.n, $6.n, null, null, null, null, $9.s);
                   }
                 ;

opt_view_column_list : LPAREN view_column_list RPAREN { $$.n = $2.n; }
                     | { $$.n = null; }
                     ;

view_column_list : view_column_list TCOMMA any_identifier { $$.n = new(NodeType.IdentifierList, $1.n, $3.n, null, null, null, null, null, null); }
                 | any_identifier { $$.n = $1.n; $$.s = $1.s; }
                 ;

/* WITH [LOCAL | CASCADED] CHECK OPTION. CASCADED is the default when neither word is given,
   matching PostgreSQL. OPTION/LOCAL/CASCADED are plain identifiers validated here. */
opt_check_option : TWITH TCHECK TIDENTIFIER
                   {
                     if (!string.Equals($3.s, "option", System.StringComparison.OrdinalIgnoreCase))
                         throw new CamusDB.Core.CamusDBException(
                             CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                             "Expected: WITH [LOCAL | CASCADED] CHECK OPTION, got 'WITH CHECK " + $3.s + "'");
                     $$.s = "cascaded";
                   }
                 | TWITH TIDENTIFIER TCHECK TIDENTIFIER
                   {
                     if (!string.Equals($4.s, "option", System.StringComparison.OrdinalIgnoreCase))
                         throw new CamusDB.Core.CamusDBException(
                             CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                             "Expected: WITH [LOCAL | CASCADED] CHECK OPTION, got 'WITH " + $2.s + " CHECK " + $4.s + "'");
                     if (string.Equals($2.s, "local", System.StringComparison.OrdinalIgnoreCase))
                         $$.s = "local";
                     else if (string.Equals($2.s, "cascaded", System.StringComparison.OrdinalIgnoreCase))
                         $$.s = "cascaded";
                     else
                         throw new CamusDB.Core.CamusDBException(
                             CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                             "Expected: WITH [LOCAL | CASCADED] CHECK OPTION, got 'WITH " + $2.s + " CHECK OPTION'");
                   }
                 | { $$.s = null; }
                 ;

/* CASCADE / RESTRICT. RESTRICT is the default: a drop that would orphan a dependent view is
   refused unless the user asks for the cascade explicitly. */
opt_drop_behavior : TIDENTIFIER
                    {
                      if (string.Equals($1.s, "cascade", System.StringComparison.OrdinalIgnoreCase))
                          $$.s = "cascade";
                      else if (string.Equals($1.s, "restrict", System.StringComparison.OrdinalIgnoreCase))
                          $$.s = "restrict";
                      else
                          throw new CamusDB.Core.CamusDBException(
                              CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                              "Expected CASCADE or RESTRICT, got '" + $1.s + "'");
                    }
                  | { $$.s = null; }
                  ;

drop_view_stmt : TDROP TVIEW view_name_list opt_drop_behavior { $$.n = new(NodeType.DropView, $3.n, null, null, null, null, null, null, $4.s); }
               | TDROP TVIEW TIF TEXISTS view_name_list opt_drop_behavior { $$.n = new(NodeType.DropViewIfExists, $5.n, null, null, null, null, null, null, $6.s); }
               ;

view_name_list : view_name_list TCOMMA any_identifier { $$.n = new(NodeType.IdentifierList, $1.n, $3.n, null, null, null, null, null, null); }
               | any_identifier { $$.n = $1.n; $$.s = $1.s; }
               ;

alter_view_stmt : TALTER TVIEW any_identifier TRENAME TTO any_identifier { $$.n = new(NodeType.AlterViewRenameTo, $3.n, $6.n, null, null, null, null, null, null); }
                | TALTER TVIEW any_identifier TIDENTIFIER TTO any_identifier
                  {
                    if (!string.Equals($4.s, "owner", System.StringComparison.OrdinalIgnoreCase))
                        throw new CamusDB.Core.CamusDBException(
                            CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                            "Expected: ALTER VIEW <name> OWNER TO <user>, got '" + $4.s + " TO'");
                    $$.n = new(NodeType.AlterViewOwnerTo, $3.n, $6.n, null, null, null, null, null, null);
                  }
                ;

create_matview_stmt : TCREATE TMATERIALIZED TVIEW any_identifier opt_view_column_list TAS select_stmt opt_with_data
                      { $$.n = new(NodeType.CreateMaterializedView, $4.n, $7.n, $5.n, null, null, null, null, $8.s); }
                    | TCREATE TMATERIALIZED TVIEW TIF TNOT TEXISTS any_identifier opt_view_column_list TAS select_stmt opt_with_data
                      { $$.n = new(NodeType.CreateMaterializedViewIfNotExists, $7.n, $10.n, $8.n, null, null, null, null, $11.s); }
                    ;

/* REFRESH MATERIALIZED VIEW [CONCURRENTLY] name [WITH [NO] DATA]. CONCURRENTLY parses but is
   refused at execution rather than silently treated as a synonym for the plain form. */
refresh_matview_stmt : TREFRESH TMATERIALIZED TVIEW any_identifier opt_with_data
                       { $$.n = new(NodeType.RefreshMaterializedView, $4.n, null, null, null, null, null, null, $5.s); }
                     | TREFRESH TMATERIALIZED TVIEW TIDENTIFIER any_identifier opt_with_data
                       {
                         if (!string.Equals($4.s, "concurrently", System.StringComparison.OrdinalIgnoreCase))
                             throw new CamusDB.Core.CamusDBException(
                                 CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                                 "Expected: REFRESH MATERIALIZED VIEW [CONCURRENTLY] <name>, got '" + $4.s + "'");
                         $$.n = new(NodeType.RefreshMaterializedView, $5.n, null, null, null, null, null, null, $6.s == null ? "concurrently" : "concurrently," + $6.s);
                       }
                     ;

drop_matview_stmt : TDROP TMATERIALIZED TVIEW view_name_list opt_drop_behavior { $$.n = new(NodeType.DropMaterializedView, $4.n, null, null, null, null, null, null, $5.s); }
                  | TDROP TMATERIALIZED TVIEW TIF TEXISTS view_name_list opt_drop_behavior { $$.n = new(NodeType.DropMaterializedViewIfExists, $6.n, null, null, null, null, null, null, $7.s); }
                  ;

alter_matview_stmt : TALTER TMATERIALIZED TVIEW any_identifier TRENAME TTO any_identifier { $$.n = new(NodeType.AlterMaterializedViewRenameTo, $4.n, $7.n, null, null, null, null, null, null); }
                   ;

create_database_stmt : TCREATE TDATABASE any_identifier TRELINK TTO string { $$.n = new(NodeType.CreateDatabaseRelink, $3.n, $6.n, null, null, null, null, null, null); }
                     | TCREATE TDATABASE any_identifier { $$.n = new(NodeType.CreateDatabase, $3.n, null, null, null, null, null, null, null); }
                     | TCREATE TDATABASE TIF TNOT TEXISTS any_identifier { $$.n = new(NodeType.CreateDatabaseIfNotExists, $6.n, null, null, null, null, null, null, null); }
                     | TCREATE TDATABASE any_identifier TBRANCH TFROM any_identifier { $$.n = new(NodeType.CreateDatabaseBranch, $3.n, $6.n, null, null, null, null, null, null); }
                     | TCREATE TDATABASE TIF TNOT TEXISTS any_identifier TBRANCH TFROM any_identifier { $$.n = new(NodeType.CreateDatabaseBranchIfNotExists, $6.n, $9.n, null, null, null, null, null, null); }
                     ;

drop_database_stmt : TDROP TDATABASE any_identifier { $$.n = new(NodeType.DropDatabase, $3.n, null, null, null, null, null, null, null); }
                   | TDROP TDATABASE any_identifier TFORCE { $$.n = new(NodeType.DropDatabase, $3.n, null, null, null, null, null, null, "force"); }
                   | TDROP TDATABASE TIF TEXISTS any_identifier { $$.n = new(NodeType.DropDatabaseIfExists, $5.n, null, null, null, null, null, null, null); }
                   | TDROP TDATABASE TIF TEXISTS any_identifier TFORCE { $$.n = new(NodeType.DropDatabaseIfExists, $5.n, null, null, null, null, null, null, "force"); }
				;

/* Both spellings produce the same node. ALTER TABLE already has a RENAME TO form, so requiring
   RENAME DATABASE for the database case was an inconsistency users hit. */
rename_database_stmt : TRENAME TDATABASE any_identifier TTO any_identifier { $$.n = new(NodeType.RenameDatabase, $3.n, $5.n, null, null, null, null, null, null); }
                     | TALTER TDATABASE any_identifier TRENAME TTO any_identifier { $$.n = new(NodeType.RenameDatabase, $3.n, $6.n, null, null, null, null, null, null); }
                     ;

comment_stmt : TCOMMENT TON TTABLE any_identifier TIS comment_value { $$.n = new(NodeType.CommentOnTable, $4.n, $6.n, null, null, null, null, null, null); }
             | TCOMMENT TON TCOLUMN any_identifier TIS comment_value { $$.n = new(NodeType.CommentOnColumn, $4.n, $6.n, null, null, null, null, null, null); }
             | TCOMMENT TON TINDEX any_identifier TIS comment_value { $$.n = new(NodeType.CommentOnIndex, $4.n, $6.n, null, null, null, null, null, null); }
             | TCOMMENT TON TDATABASE any_identifier TIS comment_value { $$.n = new(NodeType.CommentOnDatabase, $4.n, $6.n, null, null, null, null, null, null); }
             ;

/* A null node distinguishes "IS NULL" (remove the comment) from "IS ''" (store an empty string). */
comment_value : string { $$.n = $1.n; $$.s = $1.s; }
              | TNULL { $$.n = null; }
              ;

/* User / privilege DDL. All server-level (no context database). The password value reuses the
   existing `string`/`placeholder` nonterminals so a network client can bind it as a parameter and
   keep the cleartext out of the SQL text. rightAst = secret (null = no password), extendedOne =
   plugin identifier (null = defaulted). */
create_user_stmt : TCREATE TUSER any_identifier { $$.n = new(NodeType.CreateUser, $3.n, null, null, null, null, null, null, null); }
                 | TCREATE TUSER any_identifier TIDENTIFIED TWITH any_identifier TBY auth_secret { $$.n = new(NodeType.CreateUser, $3.n, $8.n, $6.n, null, null, null, null, null); }
                 | TCREATE TUSER any_identifier TIDENTIFIED TBY auth_secret { $$.n = new(NodeType.CreateUser, $3.n, $6.n, null, null, null, null, null, null); }
                 | TCREATE TUSER TIF TNOT TEXISTS any_identifier { $$.n = new(NodeType.CreateUserIfNotExists, $6.n, null, null, null, null, null, null, null); }
                 | TCREATE TUSER TIF TNOT TEXISTS any_identifier TIDENTIFIED TWITH any_identifier TBY auth_secret { $$.n = new(NodeType.CreateUserIfNotExists, $6.n, $11.n, $9.n, null, null, null, null, null); }
                 | TCREATE TUSER TIF TNOT TEXISTS any_identifier TIDENTIFIED TBY auth_secret { $$.n = new(NodeType.CreateUserIfNotExists, $6.n, $9.n, null, null, null, null, null, null); }
                 ;

alter_user_stmt : TALTER TUSER any_identifier TIDENTIFIED TWITH any_identifier TBY auth_secret { $$.n = new(NodeType.AlterUser, $3.n, $8.n, $6.n, null, null, null, null, null); }
                | TALTER TUSER any_identifier TIDENTIFIED TBY auth_secret { $$.n = new(NodeType.AlterUser, $3.n, $6.n, null, null, null, null, null, null); }
                ;

drop_user_stmt : TDROP TUSER any_identifier { $$.n = new(NodeType.DropUser, $3.n, null, null, null, null, null, null, null); }
               | TDROP TUSER TIF TEXISTS any_identifier { $$.n = new(NodeType.DropUserIfExists, $5.n, null, null, null, null, null, null, null); }
               ;

/* Scope kind carried in yytext; extendedOne holds the object identifier node (db name, or dotted
   db.table for the table form), null for the global *.* form. */
grant_stmt : TGRANT privilege_list TON TMULT TDOT TMULT TTO any_identifier { $$.n = new(NodeType.Grant, $2.n, $8.n, null, null, null, null, null, "global"); }
           | TGRANT privilege_list TON any_identifier TDOT TMULT TTO any_identifier { $$.n = new(NodeType.Grant, $2.n, $8.n, $4.n, null, null, null, null, "database"); }
           | TGRANT privilege_list TON any_identifier TTO any_identifier { $$.n = new(NodeType.Grant, $2.n, $6.n, $4.n, null, null, null, null, "table"); }
           ;

revoke_stmt : TREVOKE privilege_list TON TMULT TDOT TMULT TFROM any_identifier { $$.n = new(NodeType.Revoke, $2.n, $8.n, null, null, null, null, null, "global"); }
            | TREVOKE privilege_list TON any_identifier TDOT TMULT TFROM any_identifier { $$.n = new(NodeType.Revoke, $2.n, $8.n, $4.n, null, null, null, null, "database"); }
            | TREVOKE privilege_list TON any_identifier TFROM any_identifier { $$.n = new(NodeType.Revoke, $2.n, $6.n, $4.n, null, null, null, null, "table"); }
            ;

privilege_list : privilege_list TCOMMA privilege { $$.n = new(NodeType.GrantPrivilegeList, $1.n, $3.n, null, null, null, null, null, null); }
               | privilege { $$.n = $1.n; }
               ;

privilege : TSELECT { $$.n = new(NodeType.GrantPrivilege, null, null, null, null, null, null, null, "select"); }
          | TINSERT { $$.n = new(NodeType.GrantPrivilege, null, null, null, null, null, null, null, "insert"); }
          | TUPDATE { $$.n = new(NodeType.GrantPrivilege, null, null, null, null, null, null, null, "update"); }
          | TDELETE { $$.n = new(NodeType.GrantPrivilege, null, null, null, null, null, null, null, "delete"); }
          | TCREATE TTABLE { $$.n = new(NodeType.GrantPrivilege, null, null, null, null, null, null, null, "create table"); }
          | TDROP { $$.n = new(NodeType.GrantPrivilege, null, null, null, null, null, null, null, "drop"); }
          | TALTER { $$.n = new(NodeType.GrantPrivilege, null, null, null, null, null, null, null, "alter"); }
          | TINDEX { $$.n = new(NodeType.GrantPrivilege, null, null, null, null, null, null, null, "index"); }
          | TCREATE { $$.n = new(NodeType.GrantPrivilege, null, null, null, null, null, null, null, "create"); }
          /* ALL / ALL PRIVILEGES: 'all' is deliberately NOT a reserved keyword (it is a legal column
             name and appears in EVICT CACHE ALL), so it arrives as an identifier and the creator
             validates its text. */
          | TIDENTIFIER TPRIVILEGES { $$.n = new(NodeType.GrantPrivilege, null, null, null, null, null, null, null, $1.s); }
          | TIDENTIFIER { $$.n = new(NodeType.GrantPrivilege, null, null, null, null, null, null, null, $1.s); }
          ;

auth_secret : string { $$.n = $1.n; }
            | placeholder { $$.n = $1.n; }
            ;

alter_table_stmt : TALTER TTABLE any_identifier TWADD any_identifier field_type { $$.n = new(NodeType.AlterTableAddColumn, $3.n, $5.n, $6.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD any_identifier field_type create_table_field_constraint_list { $$.n = new(NodeType.AlterTableAddColumn, $3.n, $5.n, $6.n, $7.n, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TCOLUMN any_identifier field_type { $$.n = new(NodeType.AlterTableAddColumn, $3.n, $6.n, $7.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TCOLUMN any_identifier field_type create_table_field_constraint_list { $$.n = new(NodeType.AlterTableAddColumn, $3.n, $6.n, $7.n, $8.n, null, null, null, null); }
				 | TALTER TTABLE any_identifier TDROP any_identifier { $$.n = new(NodeType.AlterTableDropColumn, $3.n, $5.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TDROP TCOLUMN any_identifier { $$.n = new(NodeType.AlterTableDropColumn, $3.n, $6.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TINDEX any_identifier LPAREN identifier_index_list RPAREN index_include_clause { $$.n = new(NodeType.AlterTableAddIndex, $3.n, $6.n, $8.n, $10.n, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TINDEX any_identifier TON LPAREN identifier_index_list RPAREN index_include_clause { $$.n = new(NodeType.AlterTableAddIndex, $3.n, $6.n, $9.n, $11.n, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TUNIQUE any_identifier LPAREN identifier_index_list RPAREN index_include_clause { $$.n = new(NodeType.AlterTableAddUniqueIndex, $3.n, $6.n, $8.n, $10.n, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TUNIQUE any_identifier TON LPAREN identifier_index_list RPAREN index_include_clause { $$.n = new(NodeType.AlterTableAddUniqueIndex, $3.n, $6.n, $9.n, $11.n, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TUNIQUE TINDEX any_identifier LPAREN identifier_index_list RPAREN index_include_clause { $$.n = new(NodeType.AlterTableAddUniqueIndex, $3.n, $7.n, $9.n, $11.n, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TUNIQUE TINDEX any_identifier TON LPAREN identifier_index_list RPAREN index_include_clause { $$.n = new(NodeType.AlterTableAddUniqueIndex, $3.n, $7.n, $10.n, $12.n, null, null, null, null); }
                 | TALTER TTABLE any_identifier TDROP TINDEX any_identifier { $$.n = new(NodeType.AlterTableDropIndex, $3.n, $6.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TPRIMARY TKEY LPAREN identifier_index_list RPAREN { $$.n = new(NodeType.AlterTableAddPrimaryKey, $3.n, $8.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TDROP TPRIMARY TKEY { $$.n = new(NodeType.AlterTableDropPrimaryKey, $3.n, null, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TRENAME TTO any_identifier { $$.n = new(NodeType.AlterTableRenameTo, $3.n, $6.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TRENAME TCOLUMN any_identifier TTO any_identifier { $$.n = new(NodeType.AlterTableRenameColumn, $3.n, $6.n, $8.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TRENAME TINDEX any_identifier TTO any_identifier { $$.n = new(NodeType.AlterTableRenameIndex, $3.n, $6.n, $8.n, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TWADD TCONSTRAINT any_identifier TCHECK LPAREN condition RPAREN { $$.n = new(NodeType.AlterTableAddConstraintCheck, $3.n, $9.n, null, null, null, null, null, $6.s); }
                 | TALTER TTABLE any_identifier TDROP TCONSTRAINT any_identifier { $$.n = new(NodeType.AlterTableDropConstraint, $3.n, null, null, null, null, null, null, $6.s); }
                 | TALTER TTABLE any_identifier TALTER any_identifier TSET TNOT TNULL { $$.n = new(NodeType.AlterTableSetNotNull, $3.n, $5.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TALTER TCOLUMN any_identifier TSET TNOT TNULL { $$.n = new(NodeType.AlterTableSetNotNull, $3.n, $6.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TALTER any_identifier TDROP TNOT TNULL { $$.n = new(NodeType.AlterTableDropNotNull, $3.n, $5.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TALTER TCOLUMN any_identifier TDROP TNOT TNULL { $$.n = new(NodeType.AlterTableDropNotNull, $3.n, $6.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TSET LPAREN table_setting_list RPAREN { $$.n = new(NodeType.AlterTableSetSetting, $3.n, $6.n, null, null, null, null, null, null); }
                 | TALTER TTABLE any_identifier TRESET LPAREN table_setting_key_list RPAREN { $$.n = new(NodeType.AlterTableResetSetting, $3.n, $6.n, null, null, null, null, null, null); }
				 ;

table_setting_list : table_setting_list TCOMMA table_setting { $$.n = new(NodeType.UpdateList, $1.n, $3.n, null, null, null, null, null, null); }
                   | table_setting { $$.n = $1.n; }
                   ;

table_setting : any_identifier TEQUALS bool { $$.n = new(NodeType.UpdateItem, $1.n, $3.n, null, null, null, null, null, null); }
              | any_identifier TEQUALS string { $$.n = new(NodeType.UpdateItem, $1.n, $3.n, null, null, null, null, null, null); }
              | any_identifier TEQUALS int { $$.n = new(NodeType.UpdateItem, $1.n, $3.n, null, null, null, null, null, null); }
              ;

table_setting_key_list : table_setting_key_list TCOMMA any_identifier { $$.n = new(NodeType.IdentifierList, $1.n, $3.n, null, null, null, null, null, null); }
                       | any_identifier { $$.n = $1.n; $$.s = $1.s; }
                       ;

create_index_stmt : TCREATE TINDEX any_identifier TON any_identifier LPAREN identifier_index_list RPAREN index_include_clause { $$.n = new(NodeType.AlterTableAddIndex, $5.n, $3.n, $7.n, $9.n, null, null, null, null); }
                  | TCREATE TINDEX TIF TNOT TEXISTS any_identifier TON any_identifier LPAREN identifier_index_list RPAREN index_include_clause { $$.n = new(NodeType.AlterTableAddIndexIfNotExists, $8.n, $6.n, $10.n, $12.n, null, null, null, null); }
                  | TCREATE TUNIQUE TINDEX any_identifier TON any_identifier LPAREN identifier_index_list RPAREN index_include_clause { $$.n = new(NodeType.AlterTableAddUniqueIndex, $6.n, $4.n, $8.n, $10.n, null, null, null, null); }
                  | TCREATE TUNIQUE TINDEX TIF TNOT TEXISTS any_identifier TON any_identifier LPAREN identifier_index_list RPAREN index_include_clause { $$.n = new(NodeType.AlterTableAddUniqueIndexIfNotExists, $9.n, $7.n, $11.n, $13.n, null, null, null, null); }
                  ;

index_include_clause : { $$.n = null; }
                     | TINCLUDE LPAREN identifier_index_list RPAREN { $$.n = $3.n; }
                     ;

/* Optional trailing COMMENT '...' on an inline KEY / UNIQUE KEY definition, so the DDL that
   SHOW CREATE TABLE emits parses back to the same index comment. Null when absent. */
opt_inline_comment : { $$.n = null; }
                   | TCOMMENT string { $$.n = $2.n; }
                   ;

/* Optional trailing COMMENT '...' after the closing paren of CREATE TABLE. Null when absent. */
opt_table_comment : { $$.n = null; }
                  | TCOMMENT string { $$.n = $2.n; }
                  ;

opt_table_settings : { $$.n = null; }
                   | TWITH LPAREN table_setting_list RPAREN { $$.n = $3.n; }
                   ;

show_stmt : TSHOW TCOLUMNS TFROM any_identifier { $$.n = new(NodeType.ShowColumns, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TTABLES { $$.n = NodeAst.ShowTables; }
          | TSHOW TTABLES TLIKE string { $$.n = new(NodeType.ShowTables, $4.n, null, null, null, null, null, null, null); }
          | TDESCRIBE any_identifier { $$.n = new(NodeType.ShowColumns, $2.n, null, null, null, null, null, null, null); }
          | TDESC any_identifier { $$.n = new(NodeType.ShowColumns, $2.n, null, null, null, null, null, null, null); }
          | TSHOW TCREATE TTABLE any_identifier { $$.n = new(NodeType.ShowCreateTable, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TVIEWS { $$.n = new(NodeType.ShowViews, null, null, null, null, null, null, null, null); }
          | TSHOW TVIEWS TLIKE string { $$.n = new(NodeType.ShowViews, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TMATERIALIZED TVIEWS { $$.n = new(NodeType.ShowMaterializedViews, null, null, null, null, null, null, null, null); }
          | TSHOW TMATERIALIZED TVIEWS TLIKE string { $$.n = new(NodeType.ShowMaterializedViews, $5.n, null, null, null, null, null, null, null); }
          | TSHOW TCREATE TVIEW any_identifier { $$.n = new(NodeType.ShowCreateView, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TCREATE TMATERIALIZED TVIEW any_identifier { $$.n = new(NodeType.ShowCreateMaterializedView, $5.n, null, null, null, null, null, null, null); }
          | TSHOW TDATABASE { $$.n = NodeAst.ShowDatabase; }
          | TSHOW TDATABASES { $$.n = NodeAst.ShowDatabases; }
          | TSHOW TDATABASES TLIKE string { $$.n = new(NodeType.ShowDatabases, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TINDEXES TFROM any_identifier { $$.n = new(NodeType.ShowIndexes, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TINDEX TFROM any_identifier { $$.n = new(NodeType.ShowIndexes, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TBRANCHES TFROM any_identifier { $$.n = new(NodeType.ShowBranches, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TANCESTORS TFROM any_identifier { $$.n = new(NodeType.ShowAncestors, $4.n, null, null, null, null, null, null, null); }
          | TSHOW TORPHAN TTABLES { $$.n = new(NodeType.ShowOrphanTables, null, null, null, null, null, null, null, null); }
          | TSHOW TORPHAN TDATABASES { $$.n = new(NodeType.ShowOrphanDatabases, null, null, null, null, null, null, null, null); }
          | TSHOW TGRANTS { $$.n = new(NodeType.ShowGrants, null, null, null, null, null, null, null, null); }
          | TSHOW TGRANTS TFOR any_identifier { $$.n = new(NodeType.ShowGrants, $4.n, null, null, null, null, null, null, null); }
          /* STATISTICS is likewise matched as a plain identifier, so the word stays usable as a
             column and table name. The TFOR in third position keeps this production distinct from
             the two-identifier ENGINE STATS / CLUSTER SETTINGS shape below, so neither conflicts. */
          | TSHOW TIDENTIFIER TFOR any_identifier
          {
            if (!string.Equals($2.s, "statistics", System.StringComparison.OrdinalIgnoreCase))
                throw new CamusDB.Core.CamusDBException(
                    CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                    "Expected: SHOW STATISTICS FOR [TABLE] <table>");
            $$.n = new(NodeType.ShowStatistics, $4.n, null, null, null, null, null, null, null);
          }
          | TSHOW TIDENTIFIER TFOR TTABLE any_identifier
          {
            if (!string.Equals($2.s, "statistics", System.StringComparison.OrdinalIgnoreCase))
                throw new CamusDB.Core.CamusDBException(
                    CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                    "Expected: SHOW STATISTICS FOR [TABLE] <table>");
            $$.n = new(NodeType.ShowStatistics, $5.n, null, null, null, null, null, null, null);
          }
          /* ENGINE STATS and CLUSTER SETTINGS are matched as plain identifiers, not keywords, so
             all four words remain usable as column and table names. The two statements share ONE
             two-identifier production dispatched on the words — a second production with the same
             token shape would be a reduce/reduce conflict, the same constraint SET TRANSACTION
             LOCKING/PRIORITY documents. */
          | TSHOW TIDENTIFIER TIDENTIFIER
          {
            if (string.Equals($2.s, "engine", System.StringComparison.OrdinalIgnoreCase) &&
                string.Equals($3.s, "stats", System.StringComparison.OrdinalIgnoreCase))
                $$.n = new(NodeType.ShowEngineStats, null, null, null, null, null, null, null, null);
            else if (string.Equals($2.s, "cluster", System.StringComparison.OrdinalIgnoreCase) &&
                string.Equals($3.s, "settings", System.StringComparison.OrdinalIgnoreCase))
                $$.n = new(NodeType.ShowClusterSettings, null, null, null, null, null, null, null, null);
            else
                throw new CamusDB.Core.CamusDBException(
                    CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                    "Expected: SHOW ENGINE STATS [LIKE '<pattern>'] or SHOW CLUSTER SETTINGS [LIKE '<pattern>']");
          }
          | TSHOW TIDENTIFIER TIDENTIFIER TLIKE string
          {
            if (string.Equals($2.s, "engine", System.StringComparison.OrdinalIgnoreCase) &&
                string.Equals($3.s, "stats", System.StringComparison.OrdinalIgnoreCase))
                $$.n = new(NodeType.ShowEngineStats, $5.n, null, null, null, null, null, null, null);
            else if (string.Equals($2.s, "cluster", System.StringComparison.OrdinalIgnoreCase) &&
                string.Equals($3.s, "settings", System.StringComparison.OrdinalIgnoreCase))
                $$.n = new(NodeType.ShowClusterSettings, $5.n, null, null, null, null, null, null, null);
            else
                throw new CamusDB.Core.CamusDBException(
                    CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                    "Expected: SHOW ENGINE STATS [LIKE '<pattern>'] or SHOW CLUSTER SETTINGS [LIKE '<pattern>']");
          }
          /* VARIABLES is likewise a plain identifier rather than a keyword, so it stays usable as a
             column and table name. */
          | TSHOW TIDENTIFIER
          {
            if (!string.Equals($2.s, "variables", System.StringComparison.OrdinalIgnoreCase))
                throw new CamusDB.Core.CamusDBException(
                    CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                    "Expected: SHOW VARIABLES [LIKE '<pattern>']");
            $$.n = new(NodeType.ShowVariables, null, null, null, null, null, null, null, null);
          }
          | TSHOW TIDENTIFIER TLIKE string
          {
            if (!string.Equals($2.s, "variables", System.StringComparison.OrdinalIgnoreCase))
                throw new CamusDB.Core.CamusDBException(
                    CamusDB.Core.CamusDBErrorCodes.InvalidInput,
                    "Expected: SHOW VARIABLES [LIKE '<pattern>']");
            $$.n = new(NodeType.ShowVariables, $4.n, null, null, null, null, null, null, null);
          }
          /* RANGES, RANGE and ROW are matched as plain identifiers and validated in the action, so
             all three stay usable as table and column names — "range" and "rows" in particular are
             common enough that reserving them would be a real regression. The plural/singular word
             and the presence of FOR ROW are paired here rather than accepted in any combination:
             the four productions below are the only accepted shapes.

             The FROM continuation keeps these distinct from SHOW VARIABLES (TSHOW TIDENTIFIER) and
             from ENGINE STATS / CLUSTER SETTINGS (TSHOW TIDENTIFIER TIDENTIFIER) on one token of
             lookahead. The two FROM TABLE productions differ only by the trailing FOR ROW clause,
             which is a shift-versus-reduce decision one lookahead token settles — unlike two
             productions of identical token shape, which would be a reduce/reduce conflict. */
          | TSHOW TIDENTIFIER TFROM TTABLE any_identifier
          {
            RequireShowRangesWord($2.s, plural: true);
            $$.n = new(NodeType.ShowRanges, $5.n, null, null, null, null, null, null, null);
          }
          | TSHOW TIDENTIFIER TFROM TINDEX TQUALIFIED_INDEX
          {
            RequireShowRangesWord($2.s, plural: true);
            $$.n = QualifiedIndexRanges($5.s, null);
          }
          | TSHOW TIDENTIFIER TFROM TTABLE any_identifier TFOR TIDENTIFIER LPAREN in_value_list RPAREN
          {
            RequireShowRangesWord($2.s, plural: false);
            RequireRowWord($7.s);
            $$.n = new(NodeType.ShowRanges, $5.n, null, $9.n, null, null, null, null, null);
          }
          | TSHOW TIDENTIFIER TFROM TINDEX TQUALIFIED_INDEX TFOR TIDENTIFIER LPAREN in_value_list RPAREN
          {
            RequireShowRangesWord($2.s, plural: false);
            RequireRowWord($7.s);
            $$.n = QualifiedIndexRanges($5.s, $9.n);
          }
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
                 if ($3.n.yytext!.Equals("cache", System.StringComparison.OrdinalIgnoreCase))
                     $$.n = new(NodeType.CacheHint, null, null, null, null, null, null, null, $5.n.yytext);
                 else
                     $$.n = new(NodeType.IdentifierWithOpts, null, $3.n, $5.n, null, null, null, null, null);
               }
               | TAT LBRACE identifier TEQUALS identifier TCOMMA cache_hint_options RBRACE
               {
                 // ttl/strict options are only meaningful for the cache hint, never for FORCE_INDEX.
                 if (!$3.n.yytext!.Equals("cache", System.StringComparison.OrdinalIgnoreCase))
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
 *   yytext      = cache name, verbatim (case-folded to lower-case later, where the hint is consumed in SelectQueryCreator)
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
                               | TKEY any_identifier LPAREN identifier_index_list RPAREN index_include_clause opt_inline_comment { $$.n = new(NodeType.CreateTableConstraintMultiIndex, $2.n, $4.n, $6.n, $7.n, null, null, null, null); }
                               | TUNIQUE TKEY any_identifier LPAREN identifier_index_list RPAREN index_include_clause opt_inline_comment { $$.n = new(NodeType.CreateTableConstraintUniqueIndex, $3.n, $5.n, $7.n, $8.n, null, null, null, null); }
                               | TCONSTRAINT any_identifier TCHECK LPAREN condition RPAREN { $$.n = new(NodeType.CreateTableConstraintCheck, $5.n, null, null, null, null, null, null, $2.s); }
                               | TCHECK LPAREN condition RPAREN { $$.n = new(NodeType.CreateTableConstraintCheck, $3.n, null, null, null, null, null, null, null); }
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
                        | TCONSTRAINT any_identifier TNOT TNULL { $$.n = new(NodeType.ConstraintNotNullNamed, null, null, null, null, null, null, null, $2.s); }
						| TPRIMARY TKEY { $$.n = NodeAst.ConstraintPrimaryKey; }
                        | TUNIQUE { $$.n = NodeAst.ConstraintUnique; }
                        | TDEFAULT LPAREN default_expr RPAREN { $$.n = new(NodeType.ConstraintDefault, $3.n, null, null, null, null, null, null, null); }
                        | TCHECK LPAREN condition RPAREN { $$.n = new(NodeType.ConstraintCheck, $3.n, null, null, null, null, null, null, null); }
                        | TCOMMENT string { $$.n = new(NodeType.ConstraintComment, $2.n, null, null, null, null, null, null, null); }
                        ;

default_expr : int { $$.n = $1.n; $$.s = $1.s; }
             | float { $$.n = $1.n; $$.s = $1.s; }
             | bytes { $$.n = $1.n; $$.s = $1.s; }
             | string { $$.n = $1.n; $$.s = $1.s; }
             | bool { $$.n = $1.n; $$.s = $1.s; }
             | null { $$.n = $1.n; $$.s = $1.s; }
             | fcall_expr { $$.n = $1.n; $$.s = $1.s; }
			 ;

field_type : TTYPE_OBJECT_ID { $$.n = NodeAst.TypeObjectId; }
           | TTYPE_STRING { $$.n = NodeAst.TypeString; }
           | TTYPE_STRING LPAREN TDIGIT RPAREN { $$.n = new(NodeType.TypeStringSized, null, null, null, null, null, null, null, $3.s); }
           | TTYPE_INT64 { $$.n = NodeAst.TypeInteger64; }
           | TTYPE_FLOAT64 { $$.n = NodeAst.TypeFloat64; }
           | TTYPE_BOOL { $$.n = NodeAst.TypeBool; }
           | TTYPE_FLOAT32 { $$.n = NodeAst.TypeFloat32; }
           | TTYPE_BYTES { $$.n = NodeAst.TypeBytes; }
           | TTYPE_BYTES LPAREN TDIGIT RPAREN { $$.n = new(NodeType.TypeBytesSized, null, null, null, null, null, null, null, $3.s); }
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
           | regex_match_expr { $$.n = $1.n; }
           | regex_imatch_expr { $$.n = $1.n; }
           | regex_not_match_expr { $$.n = $1.n; }
           | regex_not_imatch_expr { $$.n = $1.n; }
           | simple_expr { $$.n = $1.n; }
           | group_paren_expr { $$.n = $1.n; }
           | fcall_expr { $$.n = $1.n; }
           | cast_expr { $$.n = $1.n; }
           | case_expr { $$.n = $1.n; }
           | projection_all { $$.n = $1.n; }
           | use_default_expr { $$.n = $1.n; }
           | is_null_expr { $$.n = $1.n; }
           | is_not_null_expr { $$.n = $1.n; }
           | is_true_expr { $$.n = $1.n; }
           | is_not_true_expr { $$.n = $1.n; }
           | is_false_expr { $$.n = $1.n; }
           | is_not_false_expr { $$.n = $1.n; }
           | in_subquery_expr { $$.n = $1.n; }
           | not_in_subquery_expr { $$.n = $1.n; }
           | exists_subquery_expr { $$.n = $1.n; }
           | scalar_subquery_expr { $$.n = $1.n; }
           ;

/* The bounds are `between_bound`, not the general `condition`, and that restriction is what keeps the
   grammar conflict-free. If a bound could be any condition it could itself be an `x AND y`, so after
   `x BETWEEN a AND b` with another AND/OR ahead the parser could not decide whether to finish the
   BETWEEN or fold `b AND c` into the upper bound — an ambiguity precedence cannot resolve, because it
   is a reduce/reduce tie between two complete rules rather than a shift-versus-reduce choice.
   Excluding the boolean operators from a bound removes the choice entirely: `x BETWEEN a AND b AND c`
   can only be `(x BETWEEN a AND b) AND c`, which is also SQL's rule that BETWEEN binds tighter than
   AND. A bound is therefore an arithmetic-level expression — literal, identifier, function call, CAST,
   CASE, scalar subquery, parenthesised expression, or those combined with + - * / — matching the
   standard's "row value predicand". Anything boolean still works inside parentheses: the LPAREN
   delimits it, so `x BETWEEN (a AND b) AND c` parses. */
between_expr : condition TBETWEEN between_bound TAND between_bound { $$.n = new(NodeType.ExprBetween, $1.n, null, $3.n, $5.n, null, null, null, null); }
             ;

/* Arithmetic-level operand used only for BETWEEN bounds; see between_expr for why it exists. The
   arithmetic operators are duplicated here rather than shared with add_expr/sub_expr/mult_expr/div_expr
   because those take `condition` operands, which would re-admit the boolean forms this level exists to
   exclude. Operator precedence and associativity come from the same %left declarations, so a bound
   groups exactly as the equivalent expression would elsewhere. */
between_bound : simple_expr { $$.n = $1.n; }
              | group_paren_expr { $$.n = $1.n; }
              | fcall_expr { $$.n = $1.n; }
              | cast_expr { $$.n = $1.n; }
              | case_expr { $$.n = $1.n; }
              | scalar_subquery_expr { $$.n = $1.n; }
              | between_bound TADD between_bound { $$.n = new(NodeType.ExprAdd, $1.n, $3.n, null, null, null, null, null, null); }
              | between_bound TMINUS between_bound { $$.n = new(NodeType.ExprSub, $1.n, $3.n, null, null, null, null, null, null); }
              | between_bound TMULT between_bound { $$.n = new(NodeType.ExprMult, $1.n, $3.n, null, null, null, null, null, null); }
              | between_bound TDIV between_bound { $$.n = new(NodeType.ExprDiv, $1.n, $3.n, null, null, null, null, null, null); }
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

regex_match_expr      : condition TREGEXMATCH    condition { $$.n = new(NodeType.ExprRegexMatch,      $1.n, $3.n, null, null, null, null, null, null); } ;
regex_imatch_expr     : condition TREGEXIMATCH   condition { $$.n = new(NodeType.ExprRegexMatchCi,    $1.n, $3.n, null, null, null, null, null, null); } ;
regex_not_match_expr  : condition TREGEXNOTMATCH  condition { $$.n = new(NodeType.ExprRegexNotMatch,   $1.n, $3.n, null, null, null, null, null, null); } ;
regex_not_imatch_expr : condition TREGEXNOTIMATCH condition { $$.n = new(NodeType.ExprRegexNotMatchCi, $1.n, $3.n, null, null, null, null, null, null); } ;

is_null_expr : condition TIS TNULL { $$.n = new(NodeType.ExprIsNull, $1.n, NodeAst.Null, null, null, null, null, null, null); }
             ;

is_not_null_expr : condition TIS TNOT TNULL { $$.n = new(NodeType.ExprIsNotNull, $1.n, $3.n, null, null, null, null, null, null); }
                 ;

/* IS TRUE/FALSE are truth tests, not comparisons: a NULL operand yields FALSE, never unknown.
   The negated forms therefore match NULL as well, which is why they are distinct node types
   rather than sugar for `= TRUE` / `= FALSE`. */
is_true_expr : condition TIS TTRUE { $$.n = new(NodeType.ExprIsTrue, $1.n, null, null, null, null, null, null, null); }
             ;

is_not_true_expr : condition TIS TNOT TTRUE { $$.n = new(NodeType.ExprIsNotTrue, $1.n, null, null, null, null, null, null, null); }
                 ;

is_false_expr : condition TIS TFALSE { $$.n = new(NodeType.ExprIsFalse, $1.n, null, null, null, null, null, null, null); }
              ;

is_not_false_expr : condition TIS TNOT TFALSE { $$.n = new(NodeType.ExprIsNotFalse, $1.n, null, null, null, null, null, null, null); }
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

/* CASE is self-delimiting (TCASE … TEND), so it needs no precedence declaration and slots into the
 * expr alternatives beside cast_expr / fcall_expr. Two forms share the WHEN list and ELSE:
 *   searched — TCASE <when-list> [ELSE r] TEND      (leftAst = null)
 *   simple   — TCASE <op> <when-list> [ELSE r] TEND (leftAst = op, each WHEN compares op = value)
 * The parser tells them apart because a condition can never begin with TWHEN. */
case_expr : TCASE case_when_list case_else_opt TEND
              { $$.n = new(NodeType.ExprCase, null, $2.n, $3.n, null, null, null, null, null); }
          | TCASE condition case_when_list case_else_opt TEND
              { $$.n = new(NodeType.ExprCase, $2.n, $3.n, $4.n, null, null, null, null, null); }
          ;

case_when_list : case_when_list case_when_clause
                   { $$.n = new(NodeType.ExprCaseWhenList, $1.n, $2.n, null, null, null, null, null, null); }
               | case_when_clause { $$.n = $1.n; $$.s = $1.s; }
               ;

case_when_clause : TWHEN condition TTHEN condition
                     { $$.n = new(NodeType.ExprCaseWhen, $2.n, $4.n, null, null, null, null, null, null); }
                 ;

case_else_opt : TELSE condition { $$.n = $2.n; }
              | { $$.n = null; }
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
            | bytes { $$.n = $1.n; $$.s = $1.s; }
            | array_literal { $$.n = $1.n; $$.s = $1.s; }
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
           
identifier  : TIDENTIFIER { $$.n = new(NodeType.Identifier, null, null, null, null, null, null, null, $$.s); }
            ;

escaped_identifier  : TESCAPED_IDENTIFIER { $$.n = new(NodeType.Identifier, null, null, null, null, null, null, null, $$.s.Trim('`')); }
                    ;

int     : TDIGIT { $$.n = new(NodeType.Integer, null, null, null, null, null, null, null, $$.s); }
        ;

float    : TFLOAT { $$.n = new(NodeType.Float, null, null, null, null, null, null, null, $$.s); }
         ;

string  : TSTRING { $$.n = new(NodeType.String, null, null, null, null, null, null, null, $$.s); }
        ;

bytes   : TBYTESLIT { $$.n = new(NodeType.BytesLiteral, null, null, null, null, null, null, null, $$.s); }
        ;

array_literal : TTYPE_ARRAY LBRACKET RBRACKET { $$.n = new(NodeType.ArrayLiteral, null, null, null, null, null, null, null, null); }
              | TTYPE_ARRAY LBRACKET array_element_list RBRACKET { $$.n = new(NodeType.ArrayLiteral, $3.n, null, null, null, null, null, null, null); }
              ;

array_element_list : array_element_list TCOMMA simple_expr { $$.n = new(NodeType.ExprList, $1.n, $3.n, null, null, null, null, null, null); }
                   | simple_expr { $$.n = $1.n; $$.s = $1.s; }
                   ;

bool    : TTRUE { $$.n = NodeAst.True; }
        | TFALSE { $$.n = NodeAst.False; }
        ;

null    : TNULL { $$.n = NodeAst.Null; }
        ;

placeholder : TPLACEHOLDER { $$.n = new(NodeType.Placeholder, null, null, null, null, null, null, null, $$.s); }
            ;

%%
