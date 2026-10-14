-- Outer and cross joins. An outer join limits the orders that the planner may use, and its NULL
-- padding must not change the counts.

-- query: jo-left-anti
-- tags: join, outer-join, left-join, tables:2, anti-join
SELECT COUNT(*) AS n FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE o.id IS NULL;

-- query: jo-left-count
-- tags: join, outer-join, left-join, tables:2, group-by
SELECT c.id, COUNT(o.id) AS n FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE c.id <= 20 GROUP BY c.id ORDER BY c.id;

-- query: jo-left-chain-3
-- tags: join, outer-join, left-join, tables:3
SELECT COUNT(*) AS n, COUNT(sh.id) AS shipped FROM orders o LEFT JOIN shipments sh ON sh.order_id = o.id LEFT JOIN carriers ca ON ca.id = sh.carrier_id WHERE o.store_id = 20;

-- query: jo-left-after-inner
-- tags: join, outer-join, left-join, tables:3
SELECT st.name, COUNT(pr.id) AS promoted FROM orders o JOIN stores st ON st.id = o.store_id LEFT JOIN promotions pr ON pr.id = o.promotion_id WHERE st.id <= 4 GROUP BY st.name ORDER BY st.name;

-- query: jo-left-on-filter
-- tags: join, outer-join, left-join, tables:2
SELECT COUNT(*) AS n, COUNT(r.id) AS reviewed FROM products p LEFT JOIN reviews r ON r.product_id = p.id AND r.stars = 1 WHERE p.category_id = 12;

-- query: jo-right
-- tags: join, outer-join, right-join, tables:2, nullable-key
SELECT COUNT(*) AS n FROM orders o RIGHT JOIN promotions pr ON pr.id = o.promotion_id WHERE pr.discount_pct = 50;

-- query: jo-cross
-- tags: join, cross-join, tables:2
SELECT COUNT(*) AS n FROM regions r CROSS JOIN segments s;
