/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor.Models;

using CamusDB.PlanQuality.Tests.Engine;

namespace CamusDB.PlanQuality.Tests.Data;

/// <summary>
/// The seeded retail data set of the small tier: 19 tables in a snowflake around <c>orders</c> and
/// <c>order_items</c>, big enough for chain, star, cycle and clique joins of up to 17 tables.
///
/// <para><b>Built to expose estimate errors, not to look real.</b></para>
/// <list type="bullet">
///   <item>Foreign keys are drawn with a Zipf distribution, so a few customers, products and
///   cities are very popular.</item>
///   <item>Columns are correlated: most orders go to a store in the customer's own city, and a
///   product's price depends on its category. An estimator that multiplies independent
///   selectivities is wrong on these, and the q-error shows it.</item>
///   <item>Join keys and filter columns have NULLs (<c>orders.customer_id</c>,
///   <c>orders.promotion_id</c>, <c>customers.tier</c>, <c>customers.referred_by</c>,
///   <c>employees.manager_id</c>, <c>categories.parent_id</c>, <c>reviews.body</c>), and indexed
///   columns have many duplicate values.</item>
///   <item>Some foreign keys have an index and some do not, so the planner has index, hash and
///   nested-loop joins to choose from.</item>
/// </list>
///
/// <para><b>Same seed, same rows, same order.</b> The tables are created and loaded in one fixed
/// order, because table ids come from a per-store sequence and row ids keep insertion order. Every
/// value comes from <see cref="DeterministicRandom"/>. The primary keys are explicit integers, so a
/// query never depends on a generated row id.</para>
/// </summary>
internal static class RetailDataSet
{
    /// <summary>The seed of the small tier. Changing it changes every row, so it also needs a baseline update.</summary>
    public const ulong DefaultSeed = 20261013;

    private const int BatchSize = 1000;

    private static readonly string[] Ddl =
    [
        "CREATE TABLE regions (id int64 PRIMARY KEY, name string NOT NULL)",
        "CREATE TABLE countries (id int64 PRIMARY KEY, region_id int64 NOT NULL, name string NOT NULL, code string NOT NULL)",
        "CREATE TABLE cities (id int64 PRIMARY KEY, country_id int64 NOT NULL, name string NOT NULL, population int64 NOT NULL)",
        "CREATE TABLE segments (id int64 PRIMARY KEY, name string NOT NULL)",
        "CREATE TABLE customers (id int64 PRIMARY KEY, city_id int64 NOT NULL, segment_id int64 NOT NULL, name string NOT NULL, tier string NULL, credit int64 NOT NULL, signup_year int64 NOT NULL, referred_by int64 NULL)",
        "CREATE TABLE categories (id int64 PRIMARY KEY, parent_id int64 NULL, name string NOT NULL)",
        "CREATE TABLE brands (id int64 PRIMARY KEY, country_id int64 NOT NULL, name string NOT NULL)",
        "CREATE TABLE suppliers (id int64 PRIMARY KEY, city_id int64 NOT NULL, name string NOT NULL, rating int64 NOT NULL)",
        "CREATE TABLE products (id int64 PRIMARY KEY, category_id int64 NOT NULL, brand_id int64 NOT NULL, supplier_id int64 NOT NULL, name string NOT NULL, price int64 NOT NULL, discontinued bool NOT NULL)",
        "CREATE TABLE stores (id int64 PRIMARY KEY, city_id int64 NOT NULL, name string NOT NULL, opened_year int64 NOT NULL)",
        "CREATE TABLE employees (id int64 PRIMARY KEY, store_id int64 NOT NULL, manager_id int64 NULL, name string NOT NULL, salary int64 NOT NULL)",
        "CREATE TABLE promotions (id int64 PRIMARY KEY, name string NOT NULL, discount_pct int64 NOT NULL)",
        "CREATE TABLE days (id int64 PRIMARY KEY, cal_year int64 NOT NULL, cal_month int64 NOT NULL, weekday int64 NOT NULL, is_holiday bool NOT NULL)",
        "CREATE TABLE carriers (id int64 PRIMARY KEY, country_id int64 NOT NULL, name string NOT NULL)",
        "CREATE TABLE orders (id int64 PRIMARY KEY, customer_id int64 NULL, store_id int64 NOT NULL, employee_id int64 NOT NULL, day_id int64 NOT NULL, promotion_id int64 NULL, status string NOT NULL, total int64 NOT NULL)",
        "CREATE TABLE order_items (id int64 PRIMARY KEY, order_id int64 NOT NULL, product_id int64 NOT NULL, quantity int64 NOT NULL, price int64 NOT NULL)",
        "CREATE TABLE shipments (id int64 PRIMARY KEY, order_id int64 NOT NULL, carrier_id int64 NOT NULL, day_id int64 NOT NULL, status string NOT NULL)",
        "CREATE TABLE payments (id int64 PRIMARY KEY, order_id int64 NOT NULL, day_id int64 NOT NULL, method string NOT NULL, amount int64 NOT NULL)",
        "CREATE TABLE reviews (id int64 PRIMARY KEY, product_id int64 NOT NULL, customer_id int64 NOT NULL, day_id int64 NOT NULL, stars int64 NOT NULL, body string NULL)",
    ];

    // Indexed and unindexed foreign keys are mixed on purpose: brands.country_id, suppliers.city_id,
    // products.supplier_id, shipments.carrier_id, payments.day_id, reviews.customer_id and
    // reviews.day_id have no index, so a join on them is a hash or nested-loop join.
    private static readonly string[] Indexes =
    [
        "CREATE INDEX countries_region_idx ON countries (region_id)",
        "CREATE INDEX cities_country_idx ON cities (country_id)",
        "CREATE INDEX customers_city_idx ON customers (city_id)",
        "CREATE INDEX customers_segment_idx ON customers (segment_id)",
        "CREATE INDEX customers_tier_idx ON customers (tier)",
        "CREATE INDEX customers_signup_desc_idx ON customers (signup_year DESC)",
        "CREATE INDEX categories_parent_idx ON categories (parent_id)",
        "CREATE INDEX products_category_idx ON products (category_id)",
        "CREATE INDEX products_brand_idx ON products (brand_id)",
        "CREATE INDEX products_price_idx ON products (price)",
        "CREATE INDEX stores_city_idx ON stores (city_id)",
        "CREATE INDEX employees_store_idx ON employees (store_id)",
        "CREATE INDEX orders_customer_idx ON orders (customer_id)",
        "CREATE INDEX orders_store_idx ON orders (store_id)",
        "CREATE INDEX orders_day_idx ON orders (day_id)",
        "CREATE INDEX orders_status_total_idx ON orders (status, total)",
        "CREATE INDEX order_items_order_idx ON order_items (order_id)",
        "CREATE INDEX order_items_product_idx ON order_items (product_id)",
        "CREATE INDEX shipments_order_idx ON shipments (order_id)",
        "CREATE INDEX payments_order_idx ON payments (order_id)",
        "CREATE INDEX reviews_product_idx ON reviews (product_id)",
    ];

    private static readonly string[] Views =
    [
        "CREATE VIEW big_orders AS SELECT id, customer_id, store_id, day_id, total FROM orders WHERE total >= 2000",
        "CREATE VIEW active_products AS SELECT id, category_id, brand_id, price FROM products WHERE discontinued = false",
    ];

    /// <summary>The tables in creation and load order.</summary>
    public static readonly string[] Tables =
    [
        "regions", "countries", "cities", "segments", "customers", "categories", "brands", "suppliers",
        "products", "stores", "employees", "promotions", "days", "carriers", "orders", "order_items",
        "shipments", "payments", "reviews",
    ];

    /// <summary>
    /// Creates the schema, loads every table and runs <c>ANALYZE</c> on each one.
    /// <paramref name="scale"/> multiplies the fact tables and the large dimensions (customers,
    /// products, employees); the small dimensions keep their size, as in a real snowflake.
    /// </summary>
    public static async Task LoadAsync(PlanQualityEngine engine, ulong seed, int scale)
    {
        if (scale < 1)
            throw new ArgumentOutOfRangeException(nameof(scale), "the scale factor is a positive integer");

        foreach (string ddl in Ddl)
            await engine.ExecuteDdlAsync(ddl).ConfigureAwait(false);

        foreach (string index in Indexes)
            await engine.ExecuteDdlAsync(index).ConfigureAwait(false);

        foreach (string view in Views)
            await engine.ExecuteDdlAsync(view).ConfigureAwait(false);

        Dictionary<string, List<Dictionary<string, ColumnValue>>> rows = Generate(seed, scale);

        foreach (string table in Tables)
        {
            List<Dictionary<string, ColumnValue>> tableRows = rows[table];
            for (int start = 0; start < tableRows.Count; start += BatchSize)
                await engine.InsertAsync(table, tableRows.GetRange(start, Math.Min(BatchSize, tableRows.Count - start)))
                    .ConfigureAwait(false);
        }

        foreach (string table in Tables)
            await engine.AnalyzeAsync(table).ConfigureAwait(false);
    }

    /// <summary>
    /// Every row of every table, from <paramref name="seed"/>. Each table draws from its own
    /// generator, seeded from the data-set seed and the table's position, so a change to one table's
    /// generator does not shift the values of the tables after it.
    /// </summary>
    public static Dictionary<string, List<Dictionary<string, ColumnValue>>> Generate(ulong seed, int scale)
    {
        Dictionary<string, List<Dictionary<string, ColumnValue>>> tables = new(StringComparer.Ordinal);
        DeterministicRandom For(string table) => new(seed * 31 + (ulong)Array.IndexOf(Tables, table) * 0x9E3779B97F4A7C15UL);

        // ── small dimensions ─────────────────────────────────────────────────

        string[] regionNames = ["north", "south", "east", "west", "central", "islands"];
        tables["regions"] = regionNames.Select((name, i) => Row(("id", I(i + 1)), ("name", S(name)))).ToList();

        const int countryCount = 30;
        DeterministicRandom r = For("countries");
        tables["countries"] = Enumerable.Range(1, countryCount).Select(id => Row(
            ("id", I(id)),
            ("region_id", I(1 + (id - 1) % regionNames.Length)),
            ("name", S("country " + id)),
            ("code", S(((char)('A' + id % 26)).ToString() + (char)('A' + r.Next(0, 26)))))).ToList();

        const int cityCount = 300;
        r = For("cities");
        ZipfSampler cityCountry = new(countryCount, 1.0);
        // A cube of a uniform value gives a long tail of small cities with integer arithmetic only,
        // so the values cannot differ between platforms.
        tables["cities"] = Enumerable.Range(1, cityCount).Select(id =>
        {
            long root = r.Next(1, 100);
            return Row(
                ("id", I(id)),
                ("country_id", I(cityCountry.Next(r))),
                ("name", S("city " + id)),
                ("population", I(10 * root * root * root)));
        }).ToList();

        string[] segmentNames = ["consumer", "corporate", "home office", "small business", "government"];
        double[] segmentWeights = [60, 20, 10, 8, 2];
        tables["segments"] = segmentNames.Select((name, i) => Row(("id", I(i + 1)), ("name", S(name)))).ToList();

        // Customers live mostly in the popular cities, and so do the stores: that is the correlation
        // that the customers - cities - stores - orders cycle depends on.
        int customerCount = 2000 * scale;
        r = For("customers");
        ZipfSampler popularCity = new(cityCount, 1.1);
        string?[] tiers = [null, "gold", "silver", "bronze"];
        double[] tierWeights = [20, 10, 30, 40];
        List<Dictionary<string, ColumnValue>> customers = new(customerCount);
        int[] customerCity = new int[customerCount + 1];
        for (int id = 1; id <= customerCount; id++)
        {
            int city = popularCity.Next(r);
            customerCity[id] = city;
            customers.Add(Row(
                ("id", I(id)),
                ("city_id", I(city)),
                ("segment_id", I(1 + r.PickWeighted(segmentWeights))),
                ("name", S("customer " + id)),
                ("tier", Nullable(tiers[r.PickWeighted(tierWeights)])),
                ("credit", I(50L * r.Next(-10, 101))),
                ("signup_year", I(2025 - (long)Math.Floor(Math.Sqrt(r.NextDouble()) * 11))),
                ("referred_by", id > 1 && r.Chance(0.3) ? I(r.Next(1, id)) : ColumnValue.Null)));
        }
        tables["customers"] = customers;

        const int topCategories = 8;
        const int categoryCount = 40;
        tables["categories"] = Enumerable.Range(1, categoryCount).Select(id => Row(
            ("id", I(id)),
            ("parent_id", id <= topCategories ? ColumnValue.Null : I(1 + (id - 1) % topCategories)),
            ("name", S("category " + id)))).ToList();

        const int brandCount = 120;
        r = For("brands");
        ZipfSampler brandCountry = new(countryCount, 1.0);
        tables["brands"] = Enumerable.Range(1, brandCount).Select(id => Row(
            ("id", I(id)),
            ("country_id", I(brandCountry.Next(r))),
            ("name", S("brand " + id)))).ToList();

        const int supplierCount = 200;
        r = For("suppliers");
        tables["suppliers"] = Enumerable.Range(1, supplierCount).Select(id => Row(
            ("id", I(id)),
            ("city_id", I(r.Next(1, cityCount + 1))),
            ("name", S("supplier " + id)),
            ("rating", I(r.Next(1, 6))))).ToList();

        // A product's price depends on its category, so a filter on both is not two independent
        // selectivities.
        int productCount = 1500 * scale;
        r = For("products");
        ZipfSampler productCategory = new(categoryCount - topCategories, 0.8);
        ZipfSampler productBrand = new(brandCount, 1.0);
        long[] productPrice = new long[productCount + 1];
        List<Dictionary<string, ColumnValue>> products = new(productCount);
        for (int id = 1; id <= productCount; id++)
        {
            int category = topCategories + productCategory.Next(r);
            long basePrice = 100L * (1 + category % 10);
            long price = basePrice + 10L * r.Next(0, (int)(basePrice / 10) + 1);
            productPrice[id] = price;
            products.Add(Row(
                ("id", I(id)),
                ("category_id", I(category)),
                ("brand_id", I(productBrand.Next(r))),
                ("supplier_id", I(r.Next(1, supplierCount + 1))),
                ("name", S("product " + id)),
                ("price", I(price)),
                ("discontinued", B(r.Chance(0.1)))));
        }
        tables["products"] = products;

        const int storeCount = 150;
        r = For("stores");
        int[] storeCity = new int[storeCount + 1];
        Dictionary<int, List<int>> storesByCity = new();
        List<Dictionary<string, ColumnValue>> stores = new(storeCount);
        for (int id = 1; id <= storeCount; id++)
        {
            int city = popularCity.Next(r);
            storeCity[id] = city;
            if (!storesByCity.TryGetValue(city, out List<int>? list))
                storesByCity[city] = list = new();
            list.Add(id);
            stores.Add(Row(
                ("id", I(id)),
                ("city_id", I(city)),
                ("name", S("store " + id)),
                ("opened_year", I(r.Next(1995, 2025)))));
        }
        tables["stores"] = stores;

        // Six employees for each store at scale 1. The first employee of a store is its manager.
        int employeesPerStore = 6 * scale;
        r = For("employees");
        List<Dictionary<string, ColumnValue>> employees = new(storeCount * employeesPerStore);
        for (int store = 1; store <= storeCount; store++)
        {
            int managerId = (store - 1) * employeesPerStore + 1;
            for (int j = 0; j < employeesPerStore; j++)
            {
                int id = managerId + j;
                employees.Add(Row(
                    ("id", I(id)),
                    ("store_id", I(store)),
                    ("manager_id", j == 0 ? ColumnValue.Null : I(managerId)),
                    ("name", S("employee " + id)),
                    ("salary", I(1000L * r.Next(20, 121)))));
            }
        }
        tables["employees"] = employees;

        const int promotionCount = 50;
        r = For("promotions");
        tables["promotions"] = Enumerable.Range(1, promotionCount).Select(id => Row(
            ("id", I(id)),
            ("name", S("promotion " + id)),
            ("discount_pct", I(5 * r.Next(1, 11))))).ToList();

        // Two calendar years, one row per day.
        const int dayCount = 731;
        r = For("days");
        DateTime firstDay = new(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        tables["days"] = Enumerable.Range(1, dayCount).Select(id =>
        {
            DateTime day = firstDay.AddDays(id - 1);
            return Row(
                ("id", I(id)),
                ("cal_year", I(day.Year)),
                ("cal_month", I(day.Month)),
                ("weekday", I((int)day.DayOfWeek)),
                ("is_holiday", B(r.Chance(0.03))));
        }).ToList();

        const int carrierCount = 12;
        r = For("carriers");
        tables["carriers"] = Enumerable.Range(1, carrierCount).Select(id => Row(
            ("id", I(id)),
            ("country_id", I(r.Next(1, countryCount + 1))),
            ("name", S("carrier " + id)))).ToList();

        // ── facts ────────────────────────────────────────────────────────────

        int orderCount = 6000 * scale;
        r = For("orders");
        ZipfSampler orderCustomer = new(customerCount, 1.0);
        ZipfSampler orderStore = new(storeCount, 0.9);
        ZipfSampler orderPromotion = new(promotionCount, 1.2);
        string[] statuses = ["delivered", "shipped", "pending", "cancelled"];
        double[] statusWeights = [70, 15, 10, 5];
        string[] orderStatus = new string[orderCount + 1];
        int[] orderDay = new int[orderCount + 1];
        List<Dictionary<string, ColumnValue>> orders = new(orderCount);
        for (int id = 1; id <= orderCount; id++)
        {
            bool guest = r.Chance(0.03);
            int customer = guest ? 0 : orderCustomer.Next(r);

            // Eight orders in ten go to a store in the customer's own city when there is one.
            int store = customer > 0 && storesByCity.TryGetValue(customerCity[customer], out List<int>? local) && r.Chance(0.8)
                ? r.Pick(local)
                : orderStore.Next(r);

            int day = r.Next(1, dayCount + 1);
            string status = statuses[r.PickWeighted(statusWeights)];
            orderStatus[id] = status;
            orderDay[id] = day;

            orders.Add(Row(
                ("id", I(id)),
                ("customer_id", guest ? ColumnValue.Null : I(customer)),
                ("store_id", I(store)),
                ("employee_id", I((store - 1) * employeesPerStore + 1 + r.Next(0, employeesPerStore))),
                ("day_id", I(day)),
                ("promotion_id", r.Chance(0.7) ? ColumnValue.Null : I(orderPromotion.Next(r))),
                ("status", S(status)),
                ("total", I(10L * r.Next(1, 501)))));
        }
        tables["orders"] = orders;

        r = For("order_items");
        ZipfSampler itemProduct = new(productCount, 1.1);
        List<Dictionary<string, ColumnValue>> items = new(orderCount * 3);
        int itemId = 0;
        for (int order = 1; order <= orderCount; order++)
        {
            int count = 1 + r.PickWeighted([30, 30, 20, 12, 8]);
            for (int j = 0; j < count; j++)
            {
                int product = itemProduct.Next(r);
                items.Add(Row(
                    ("id", I(++itemId)),
                    ("order_id", I(order)),
                    ("product_id", I(product)),
                    ("quantity", I(r.Next(1, 11))),
                    ("price", I(productPrice[product]))));
            }
        }
        tables["order_items"] = items;

        // At most one shipment for each delivered or shipped order.
        r = For("shipments");
        ZipfSampler shipmentCarrier = new(carrierCount, 1.0);
        List<Dictionary<string, ColumnValue>> shipments = new(orderCount);
        int shipmentId = 0;
        for (int order = 1; order <= orderCount; order++)
        {
            if (orderStatus[order] is not ("delivered" or "shipped"))
                continue;

            shipments.Add(Row(
                ("id", I(++shipmentId)),
                ("order_id", I(order)),
                ("carrier_id", I(shipmentCarrier.Next(r))),
                ("day_id", I(Math.Min(dayCount, orderDay[order] + r.Next(1, 6)))),
                ("status", S(orderStatus[order] == "delivered" ? "delivered" : "in transit"))));
        }
        tables["shipments"] = shipments;

        // One payment for each order that is not cancelled; one in ten is split in two.
        r = For("payments");
        string[] methods = ["card", "cash", "transfer", "voucher"];
        double[] methodWeights = [60, 20, 15, 5];
        List<Dictionary<string, ColumnValue>> payments = new(orderCount);
        int paymentId = 0;
        for (int order = 1; order <= orderCount; order++)
        {
            if (orderStatus[order] == "cancelled")
                continue;

            int parts = r.Chance(0.1) ? 2 : 1;
            for (int j = 0; j < parts; j++)
            {
                payments.Add(Row(
                    ("id", I(++paymentId)),
                    ("order_id", I(order)),
                    ("day_id", I(orderDay[order])),
                    ("method", S(methods[r.PickWeighted(methodWeights)])),
                    ("amount", I(10L * r.Next(1, 251)))));
            }
        }
        tables["payments"] = payments;

        int reviewCount = 4000 * scale;
        r = For("reviews");
        ZipfSampler reviewProduct = new(productCount, 1.1);
        ZipfSampler reviewCustomer = new(customerCount, 1.0);
        tables["reviews"] = Enumerable.Range(1, reviewCount).Select(id =>
        {
            int stars = 1 + r.PickWeighted([5, 7, 13, 35, 40]);
            return Row(
                ("id", I(id)),
                ("product_id", I(reviewProduct.Next(r))),
                ("customer_id", I(reviewCustomer.Next(r))),
                ("day_id", I(r.Next(1, dayCount + 1))),
                ("stars", I(stars)),
                ("body", r.Chance(0.5) ? ColumnValue.Null : S(stars >= 4 ? "good product" : "poor product")));
        }).ToList();

        return tables;
    }

    private static ColumnValue I(long value) => new(ColumnType.Integer64, value);

    private static ColumnValue S(string value) => new(ColumnType.String, value);

    private static ColumnValue B(bool value) => new(ColumnType.Bool, value);

    private static ColumnValue Nullable(string? value) => value is null ? ColumnValue.Null : S(value);

    /// <summary>One row. A NULL column is left out of the row, which is how an insert stores NULL.</summary>
    private static Dictionary<string, ColumnValue> Row(params (string Column, ColumnValue Value)[] cells)
    {
        Dictionary<string, ColumnValue> row = new(cells.Length, StringComparer.Ordinal);
        foreach ((string column, ColumnValue value) in cells)
        {
            if (value.Type != ColumnType.Null)
                row[column] = value;
        }
        return row;
    }
}
