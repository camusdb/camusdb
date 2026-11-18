/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Workload.Tpcc;

/// <summary>
/// The nine TPC-C tables as CamusDB DDL.
///
/// <para>Keys and counters are INT64 and money columns are bare NUMERIC (CamusDB refuses
/// <c>DECIMAL(12,2)</c>). Date columns hold milliseconds since the Unix epoch as INT64, so a value the
/// client wrote is the value it reads back on every wire protocol. <c>history</c> has no primary key in
/// the specification; here it has <c>h_id</c>, a string the client makes unique. There are no foreign
/// keys, because they are enforced and would be paid for on every loaded row.</para>
/// </summary>
public static class TpccSchema
{
    public const string MetaTable = "tpcc_meta";

    /// <summary>Table names in load order.</summary>
    public static readonly IReadOnlyList<string> Tables =
        ["item", "warehouse", "district", "stock", "customer", "history", "orders", "order_line", "new_order"];

    public const string ItemColumns = "i_id, i_im_id, i_name, i_price, i_data";
    public const string WarehouseColumns = "w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd";
    public const string DistrictColumns =
        "d_w_id, d_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id";
    public const string StockColumns =
        "s_w_id, s_i_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, " +
        "s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data";
    public const string CustomerColumns =
        "c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, " +
        "c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data";
    public const string HistoryColumns = "h_id, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data";
    public const string OrdersColumns = "o_w_id, o_d_id, o_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local";
    public const string OrderLineColumns =
        "ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info";
    public const string NewOrderColumns = "no_w_id, no_d_id, no_o_id";

    /// <summary>Every DDL statement with a short description, in the order they must run.</summary>
    public static readonly IReadOnlyList<(string Sql, string What)> Ddl =
    [
        ("CREATE TABLE item (i_id INT64 PRIMARY KEY, i_im_id INT64 NOT NULL, i_name STRING NOT NULL, i_price NUMERIC NOT NULL, " +
         "i_data STRING NOT NULL)", "table item"),
        ("CREATE TABLE warehouse (w_id INT64 PRIMARY KEY, w_name STRING NOT NULL, w_street_1 STRING NOT NULL, " +
         "w_street_2 STRING NOT NULL, w_city STRING NOT NULL, w_state STRING NOT NULL, w_zip STRING NOT NULL, " +
         "w_tax NUMERIC NOT NULL, w_ytd NUMERIC NOT NULL)", "table warehouse"),
        ("CREATE TABLE district (d_w_id INT64 NOT NULL, d_id INT64 NOT NULL, d_name STRING NOT NULL, d_street_1 STRING NOT NULL, " +
         "d_street_2 STRING NOT NULL, d_city STRING NOT NULL, d_state STRING NOT NULL, d_zip STRING NOT NULL, " +
         "d_tax NUMERIC NOT NULL, d_ytd NUMERIC NOT NULL, d_next_o_id INT64 NOT NULL, PRIMARY KEY (d_w_id, d_id))", "table district"),
        ("CREATE TABLE stock (s_w_id INT64 NOT NULL, s_i_id INT64 NOT NULL, s_quantity INT64 NOT NULL, s_dist_01 STRING NOT NULL, " +
         "s_dist_02 STRING NOT NULL, s_dist_03 STRING NOT NULL, s_dist_04 STRING NOT NULL, s_dist_05 STRING NOT NULL, " +
         "s_dist_06 STRING NOT NULL, s_dist_07 STRING NOT NULL, s_dist_08 STRING NOT NULL, s_dist_09 STRING NOT NULL, " +
         "s_dist_10 STRING NOT NULL, s_ytd INT64 NOT NULL, s_order_cnt INT64 NOT NULL, s_remote_cnt INT64 NOT NULL, " +
         "s_data STRING NOT NULL, PRIMARY KEY (s_w_id, s_i_id))", "table stock"),
        ("CREATE TABLE customer (c_w_id INT64 NOT NULL, c_d_id INT64 NOT NULL, c_id INT64 NOT NULL, c_first STRING NOT NULL, " +
         "c_middle STRING NOT NULL, c_last STRING NOT NULL, c_street_1 STRING NOT NULL, c_street_2 STRING NOT NULL, " +
         "c_city STRING NOT NULL, c_state STRING NOT NULL, c_zip STRING NOT NULL, c_phone STRING NOT NULL, c_since INT64 NOT NULL, " +
         "c_credit STRING NOT NULL, c_credit_lim NUMERIC NOT NULL, c_discount NUMERIC NOT NULL, c_balance NUMERIC NOT NULL, " +
         "c_ytd_payment NUMERIC NOT NULL, c_payment_cnt INT64 NOT NULL, c_delivery_cnt INT64 NOT NULL, c_data STRING NOT NULL, " +
         "PRIMARY KEY (c_w_id, c_d_id, c_id))", "table customer"),
        ("CREATE TABLE history (h_id STRING PRIMARY KEY, h_c_id INT64 NOT NULL, h_c_d_id INT64 NOT NULL, h_c_w_id INT64 NOT NULL, " +
         "h_d_id INT64 NOT NULL, h_w_id INT64 NOT NULL, h_date INT64 NOT NULL, h_amount NUMERIC NOT NULL, h_data STRING NOT NULL)",
         "table history"),
        ("CREATE TABLE orders (o_w_id INT64 NOT NULL, o_d_id INT64 NOT NULL, o_id INT64 NOT NULL, o_c_id INT64 NOT NULL, " +
         "o_entry_d INT64 NOT NULL, o_carrier_id INT64, o_ol_cnt INT64 NOT NULL, o_all_local INT64 NOT NULL, " +
         "PRIMARY KEY (o_w_id, o_d_id, o_id))", "table orders"),
        ("CREATE TABLE order_line (ol_w_id INT64 NOT NULL, ol_d_id INT64 NOT NULL, ol_o_id INT64 NOT NULL, ol_number INT64 NOT NULL, " +
         "ol_i_id INT64 NOT NULL, ol_supply_w_id INT64 NOT NULL, ol_delivery_d INT64, ol_quantity INT64 NOT NULL, " +
         "ol_amount NUMERIC NOT NULL, ol_dist_info STRING NOT NULL, PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))",
         "table order_line"),
        ("CREATE TABLE new_order (no_w_id INT64 NOT NULL, no_d_id INT64 NOT NULL, no_o_id INT64 NOT NULL, " +
         "PRIMARY KEY (no_w_id, no_d_id, no_o_id))", "table new_order"),
        ($"CREATE TABLE {MetaTable} (id INT64 PRIMARY KEY, seed STRING NOT NULL, warehouses INT64 NOT NULL, scale STRING NOT NULL)",
         $"table {MetaTable}"),
        ("CREATE INDEX customer_name ON customer (c_w_id, c_d_id, c_last, c_first)", "index customer_name"),
        ("CREATE INDEX orders_customer ON orders (o_w_id, o_d_id, o_c_id, o_id)", "index orders_customer"),
    ];
}
