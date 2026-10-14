-- Star and snowflake joins: one fact table with filters on several dimensions. A good plan joins
-- the most selective dimension first.

-- query: js-orders-4
-- tags: join, join:star, tables:4
SELECT st.name, COUNT(*) AS n FROM orders o JOIN stores st ON st.id = o.store_id JOIN days d ON d.id = o.day_id JOIN employees e ON e.id = o.employee_id WHERE d.cal_month = 12 AND d.cal_year = 2025 AND st.id = 10 GROUP BY st.name;

-- query: js-orders-6
-- tags: join, join:star, tables:6
SELECT seg.name, COUNT(*) AS n, SUM(o.total) AS s FROM orders o JOIN customers c ON c.id = o.customer_id JOIN segments seg ON seg.id = c.segment_id JOIN stores st ON st.id = o.store_id JOIN days d ON d.id = o.day_id JOIN employees e ON e.id = o.employee_id WHERE d.is_holiday = true AND e.salary > 100000 GROUP BY seg.name ORDER BY seg.name;

-- query: js-items-5
-- tags: join, join:snowflake, tables:5
SELECT b.name, SUM(i.quantity) AS q FROM order_items i JOIN products p ON p.id = i.product_id JOIN brands b ON b.id = p.brand_id JOIN suppliers su ON su.id = p.supplier_id JOIN categories cat ON cat.id = p.category_id WHERE su.rating = 5 AND cat.parent_id = 2 GROUP BY b.name ORDER BY q DESC, b.name LIMIT 10;

-- query: js-promotions
-- tags: join, join:star, tables:3, nullable-key
SELECT pr.name, COUNT(*) AS n FROM orders o JOIN promotions pr ON pr.id = o.promotion_id JOIN stores st ON st.id = o.store_id WHERE pr.discount_pct >= 40 GROUP BY pr.name ORDER BY pr.name;

-- query: js-comma
-- tags: join, join:star, tables:5, comma-join
SELECT COUNT(*) AS n FROM days d, stores st, orders o, customers c, employees e WHERE o.day_id = d.id AND o.store_id = st.id AND o.customer_id = c.id AND o.employee_id = e.id AND d.weekday = 0 AND c.tier = 'gold' AND st.opened_year < 2000;

-- query: js-unindexed-dimensions
-- tags: join, join:star, tables:4, hash-join
SELECT ca.name, COUNT(*) AS n FROM shipments sh JOIN carriers ca ON ca.id = sh.carrier_id JOIN orders o ON o.id = sh.order_id JOIN days d ON d.id = sh.day_id WHERE d.cal_month = 6 GROUP BY ca.name ORDER BY ca.name;

-- query: js-reviews-segments
-- tags: join, join:star, tables:3, hash-join
SELECT seg.name, AVG(r.stars) AS avg_stars, COUNT(*) AS n FROM reviews r JOIN customers c ON c.id = r.customer_id JOIN segments seg ON seg.id = c.segment_id GROUP BY seg.name ORDER BY seg.name;
