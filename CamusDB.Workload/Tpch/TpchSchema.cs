/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Tpch;

/// <summary>
/// The eight TPC-H tables, their indexes and the one-row table that marks a finished load.
///
/// <para>Keys are INT64 everywhere so every join compares one type (a join key compared across two
/// types runs as a nested loop). Money and quantity columns are bare NUMERIC: CamusDB refuses
/// <c>DECIMAL(15,2)</c>. There are no foreign keys, because they are enforced and would be paid for on
/// every loaded row.</para>
/// </summary>
public static class TpchSchema
{
    public const string MetaTable = "tpch_meta";

    /// <summary>Table names in load order (a table never refers to one after it).</summary>
    public static readonly IReadOnlyList<string> Tables =
        ["region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"];

    public const string RegionColumns = "r_regionkey, r_name, r_comment";
    public const string NationColumns = "n_nationkey, n_name, n_regionkey, n_comment";
    public const string SupplierColumns = "s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment";
    public const string PartColumns = "p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment";
    public const string PartSuppColumns = "ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment";
    public const string CustomerColumns = "c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment";
    public const string OrdersColumns =
        "o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment";
    public const string LineItemColumns =
        "l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, " +
        "l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment";

    /// <summary>Every DDL statement with a short description, in the order they must run.</summary>
    public static readonly IReadOnlyList<(string Sql, string What)> Ddl =
    [
        ("CREATE TABLE region (r_regionkey INT64 PRIMARY KEY, r_name STRING NOT NULL, r_comment STRING NOT NULL)", "table region"),
        ("CREATE TABLE nation (n_nationkey INT64 PRIMARY KEY, n_name STRING NOT NULL, n_regionkey INT64 NOT NULL, " +
         "n_comment STRING NOT NULL)", "table nation"),
        ("CREATE TABLE supplier (s_suppkey INT64 PRIMARY KEY, s_name STRING NOT NULL, s_address STRING NOT NULL, " +
         "s_nationkey INT64 NOT NULL, s_phone STRING NOT NULL, s_acctbal NUMERIC NOT NULL, s_comment STRING NOT NULL)", "table supplier"),
        ("CREATE TABLE part (p_partkey INT64 PRIMARY KEY, p_name STRING NOT NULL, p_mfgr STRING NOT NULL, p_brand STRING NOT NULL, " +
         "p_type STRING NOT NULL, p_size INT64 NOT NULL, p_container STRING NOT NULL, p_retailprice NUMERIC NOT NULL, " +
         "p_comment STRING NOT NULL)", "table part"),
        ("CREATE TABLE partsupp (ps_partkey INT64 NOT NULL, ps_suppkey INT64 NOT NULL, ps_availqty INT64 NOT NULL, " +
         "ps_supplycost NUMERIC NOT NULL, ps_comment STRING NOT NULL, PRIMARY KEY (ps_partkey, ps_suppkey))", "table partsupp"),
        ("CREATE TABLE customer (c_custkey INT64 PRIMARY KEY, c_name STRING NOT NULL, c_address STRING NOT NULL, " +
         "c_nationkey INT64 NOT NULL, c_phone STRING NOT NULL, c_acctbal NUMERIC NOT NULL, c_mktsegment STRING NOT NULL, " +
         "c_comment STRING NOT NULL)", "table customer"),
        ("CREATE TABLE orders (o_orderkey INT64 PRIMARY KEY, o_custkey INT64 NOT NULL, o_orderstatus STRING NOT NULL, " +
         "o_totalprice NUMERIC NOT NULL, o_orderdate DATE NOT NULL, o_orderpriority STRING NOT NULL, o_clerk STRING NOT NULL, " +
         "o_shippriority INT64 NOT NULL, o_comment STRING NOT NULL)", "table orders"),
        ("CREATE TABLE lineitem (l_orderkey INT64 NOT NULL, l_partkey INT64 NOT NULL, l_suppkey INT64 NOT NULL, " +
         "l_linenumber INT64 NOT NULL, l_quantity NUMERIC NOT NULL, l_extendedprice NUMERIC NOT NULL, l_discount NUMERIC NOT NULL, " +
         "l_tax NUMERIC NOT NULL, l_returnflag STRING NOT NULL, l_linestatus STRING NOT NULL, l_shipdate DATE NOT NULL, " +
         "l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct STRING NOT NULL, l_shipmode STRING NOT NULL, " +
         "l_comment STRING NOT NULL, PRIMARY KEY (l_orderkey, l_linenumber))", "table lineitem"),
        ($"CREATE TABLE {MetaTable} (id INT64 PRIMARY KEY, seed STRING NOT NULL, scale STRING NOT NULL, facts STRING NOT NULL)",
         $"table {MetaTable}"),
        ("CREATE INDEX partsupp_supp ON partsupp (ps_suppkey)", "index partsupp_supp"),
        ("CREATE INDEX orders_cust ON orders (o_custkey)", "index orders_cust"),
        ("CREATE INDEX lineitem_part_supp ON lineitem (l_partkey, l_suppkey)", "index lineitem_part_supp"),
    ];
}
