-- Wide snowflake joins from 10 to 17 tables. The cost-based join-order search stops at 12 tables,
-- so the 13- to 17-table queries take the heuristic path; join-steps-costed shows the search work.

-- query: jw-10
-- tags: join, join:snowflake, tables:10
SELECT r.name, COUNT(*) AS n
FROM order_items i
JOIN orders o ON o.id = i.order_id
JOIN customers c ON c.id = o.customer_id
JOIN cities ci ON ci.id = c.city_id
JOIN countries co ON co.id = ci.country_id
JOIN regions r ON r.id = co.region_id
JOIN products p ON p.id = i.product_id
JOIN categories cat ON cat.id = p.category_id
JOIN brands b ON b.id = p.brand_id
JOIN days d ON d.id = o.day_id
WHERE d.cal_year = 2025 AND d.cal_month = 3 AND cat.parent_id = 1
GROUP BY r.name ORDER BY r.name;

-- query: jw-12
-- tags: join, join:snowflake, tables:12
SELECT r.name, COUNT(*) AS n
FROM order_items i
JOIN orders o ON o.id = i.order_id
JOIN customers c ON c.id = o.customer_id
JOIN cities ci ON ci.id = c.city_id
JOIN countries co ON co.id = ci.country_id
JOIN regions r ON r.id = co.region_id
JOIN products p ON p.id = i.product_id
JOIN categories cat ON cat.id = p.category_id
JOIN brands b ON b.id = p.brand_id
JOIN days d ON d.id = o.day_id
JOIN stores st ON st.id = o.store_id
JOIN employees e ON e.id = o.employee_id
WHERE d.cal_year = 2024 AND e.salary >= 110000
GROUP BY r.name ORDER BY r.name;

-- query: jw-12-selective-far-end
-- tags: join, join:snowflake, tables:12, selective
SELECT COUNT(*) AS n
FROM regions r
JOIN countries co ON co.region_id = r.id
JOIN cities ci ON ci.country_id = co.id
JOIN customers c ON c.city_id = ci.id
JOIN orders o ON o.customer_id = c.id
JOIN days d ON d.id = o.day_id
JOIN stores st ON st.id = o.store_id
JOIN employees e ON e.id = o.employee_id
JOIN order_items i ON i.order_id = o.id
JOIN products p ON p.id = i.product_id
JOIN categories cat ON cat.id = p.category_id
JOIN brands b ON b.id = p.brand_id
WHERE p.id = 7;

-- query: jw-13
-- tags: join, join:snowflake, tables:13, over-dp-limit
SELECT seg.name, COUNT(*) AS n
FROM order_items i
JOIN orders o ON o.id = i.order_id
JOIN customers c ON c.id = o.customer_id
JOIN segments seg ON seg.id = c.segment_id
JOIN cities ci ON ci.id = c.city_id
JOIN countries co ON co.id = ci.country_id
JOIN regions r ON r.id = co.region_id
JOIN products p ON p.id = i.product_id
JOIN categories cat ON cat.id = p.category_id
JOIN brands b ON b.id = p.brand_id
JOIN days d ON d.id = o.day_id
JOIN stores st ON st.id = o.store_id
JOIN employees e ON e.id = o.employee_id
WHERE r.name = 'west' AND d.cal_month = 7
GROUP BY seg.name ORDER BY seg.name;

-- query: jw-13-comma
-- tags: join, join:snowflake, tables:13, over-dp-limit, comma-join
SELECT COUNT(*) AS n
FROM days d, regions r, order_items i, brands b, orders o, customers c, cities ci, countries co,
     products p, categories cat, segments seg, stores st, employees e
WHERE o.id = i.order_id AND c.id = o.customer_id AND seg.id = c.segment_id AND ci.id = c.city_id
  AND co.id = ci.country_id AND r.id = co.region_id AND p.id = i.product_id AND cat.id = p.category_id
  AND b.id = p.brand_id AND d.id = o.day_id AND st.id = o.store_id AND e.id = o.employee_id
  AND r.name = 'west' AND d.cal_month = 7;

-- query: jw-15
-- tags: join, join:snowflake, tables:15, over-dp-limit
SELECT su.rating, COUNT(*) AS n
FROM order_items i
JOIN orders o ON o.id = i.order_id
JOIN customers c ON c.id = o.customer_id
JOIN segments seg ON seg.id = c.segment_id
JOIN cities ci ON ci.id = c.city_id
JOIN countries co ON co.id = ci.country_id
JOIN regions r ON r.id = co.region_id
JOIN products p ON p.id = i.product_id
JOIN categories cat ON cat.id = p.category_id
JOIN brands b ON b.id = p.brand_id
JOIN suppliers su ON su.id = p.supplier_id
JOIN days d ON d.id = o.day_id
JOIN stores st ON st.id = o.store_id
JOIN employees e ON e.id = o.employee_id
JOIN promotions pr ON pr.id = o.promotion_id
WHERE pr.discount_pct >= 45 AND seg.name = 'corporate'
GROUP BY su.rating ORDER BY su.rating;

-- query: jw-17
-- tags: join, join:snowflake, tables:17, over-dp-limit
SELECT ca.name, COUNT(*) AS n
FROM order_items i
JOIN orders o ON o.id = i.order_id
JOIN customers c ON c.id = o.customer_id
JOIN segments seg ON seg.id = c.segment_id
JOIN cities ci ON ci.id = c.city_id
JOIN countries co ON co.id = ci.country_id
JOIN regions r ON r.id = co.region_id
JOIN products p ON p.id = i.product_id
JOIN categories cat ON cat.id = p.category_id
JOIN brands b ON b.id = p.brand_id
JOIN suppliers su ON su.id = p.supplier_id
JOIN days d ON d.id = o.day_id
JOIN stores st ON st.id = o.store_id
JOIN employees e ON e.id = o.employee_id
JOIN promotions pr ON pr.id = o.promotion_id
JOIN shipments sh ON sh.order_id = o.id
JOIN carriers ca ON ca.id = sh.carrier_id
WHERE d.cal_year = 2025 AND cat.parent_id = 3
GROUP BY ca.name ORDER BY ca.name;
