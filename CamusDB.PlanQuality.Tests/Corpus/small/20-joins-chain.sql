-- Chain joins: each table joins the next one. A selective filter at one end lets a good plan
-- start there. The comma forms take the path through CommaJoinNormalizer.

-- query: jc-2-point
-- tags: join, join:chain, tables:2, index-join
SELECT c.name, COUNT(*) AS n FROM customers c JOIN orders o ON o.customer_id = c.id WHERE c.id = 7 GROUP BY c.name;

-- query: jc-2-in-list
-- tags: join, join:chain, tables:2, in-list
SELECT COUNT(*) AS n FROM orders o JOIN customers c ON c.id = o.customer_id WHERE c.id IN (1, 2, 3, 4, 5);

-- query: jc-2-range
-- tags: join, join:chain, tables:2, range
SELECT COUNT(*) AS n FROM orders o JOIN days d ON d.id = o.day_id WHERE d.id BETWEEN 200 AND 230;

-- query: jc-2-group-by
-- tags: join, join:chain, tables:2, group-by
SELECT p.category_id, SUM(i.quantity) AS q FROM order_items i JOIN products p ON p.id = i.product_id GROUP BY p.category_id ORDER BY p.category_id;

-- query: jc-2-order-limit
-- tags: join, join:chain, tables:2, order-limit
SELECT o.id, c.name FROM orders o JOIN customers c ON c.id = o.customer_id ORDER BY o.total DESC, o.id LIMIT 10;

-- query: jc-2-distinct
-- tags: join, join:chain, tables:2, distinct
SELECT DISTINCT ci.country_id FROM customers c JOIN cities ci ON ci.id = c.city_id WHERE c.credit > 4500 ORDER BY ci.country_id;

-- query: jc-2-big-both-sides
-- tags: join, join:chain, tables:2
SELECT COUNT(*) AS n, SUM(i.quantity) AS q FROM orders o JOIN order_items i ON i.order_id = o.id WHERE o.status = 'pending';

-- query: jc-3-geo
-- tags: join, join:chain, tables:3
SELECT co.name, COUNT(*) AS n FROM regions r JOIN countries co ON co.region_id = r.id JOIN cities ci ON ci.country_id = co.id WHERE r.name = 'north' GROUP BY co.name ORDER BY co.name;

-- query: jc-3-like
-- tags: join, join:chain, tables:3, like
SELECT COUNT(*) AS n FROM customers c JOIN cities ci ON ci.id = c.city_id JOIN countries co ON co.id = ci.country_id WHERE co.name LIKE 'country 1%';

-- query: jc-3-reviews
-- tags: join, join:chain, tables:3
SELECT b.name, COUNT(*) AS n FROM reviews r JOIN products p ON p.id = r.product_id JOIN brands b ON b.id = p.brand_id WHERE r.stars = 5 AND b.country_id = 1 GROUP BY b.name ORDER BY b.name;

-- query: jc-4-geo-customers
-- tags: join, join:chain, tables:4
SELECT r.name, COUNT(*) AS n FROM regions r JOIN countries co ON co.region_id = r.id JOIN cities ci ON ci.country_id = co.id JOIN customers c ON c.city_id = ci.id GROUP BY r.name ORDER BY r.name;

-- query: jc-4-comma
-- tags: join, join:chain, tables:4, comma-join
SELECT r.name, COUNT(*) AS n FROM regions r, countries co, cities ci, customers c WHERE co.region_id = r.id AND ci.country_id = co.id AND c.city_id = ci.id GROUP BY r.name ORDER BY r.name;

-- query: jc-5-geo-orders
-- tags: join, join:chain, tables:5
SELECT r.name, COUNT(*) AS n, SUM(o.total) AS s FROM regions r JOIN countries co ON co.region_id = r.id JOIN cities ci ON ci.country_id = co.id JOIN customers c ON c.city_id = ci.id JOIN orders o ON o.customer_id = c.id WHERE co.code LIKE 'B%' GROUP BY r.name ORDER BY r.name;

-- query: jc-5-comma-scrambled
-- tags: join, join:chain, tables:5, comma-join
SELECT COUNT(*) AS n FROM regions r, customers c, countries co, orders o, cities ci WHERE co.region_id = r.id AND ci.country_id = co.id AND c.city_id = ci.id AND o.customer_id = c.id AND r.name = 'east';

-- query: jc-6-categories
-- tags: join, join:chain, tables:6
SELECT cat.name, SUM(i.quantity) AS q FROM customers c JOIN orders o ON o.customer_id = c.id JOIN order_items i ON i.order_id = o.id JOIN products p ON p.id = i.product_id JOIN categories cat ON cat.id = p.category_id JOIN categories parent ON parent.id = cat.parent_id WHERE c.id = 1 GROUP BY cat.name ORDER BY cat.name;

-- query: jc-7-selective-far-end
-- tags: join, join:chain, tables:7, selective
SELECT COUNT(*) AS n FROM regions r JOIN countries co ON co.region_id = r.id JOIN cities ci ON ci.country_id = co.id JOIN customers c ON c.city_id = ci.id JOIN orders o ON o.customer_id = c.id JOIN order_items i ON i.order_id = o.id JOIN products p ON p.id = i.product_id WHERE p.id = 42;

-- query: jc-7-comma-reversed
-- tags: join, join:chain, tables:7, comma-join, selective
SELECT COUNT(*) AS n FROM products p, order_items i, orders o, customers c, cities ci, countries co, regions r WHERE p.id = 42 AND i.product_id = p.id AND o.id = i.order_id AND c.id = o.customer_id AND ci.id = c.city_id AND co.id = ci.country_id AND r.id = co.region_id;

-- query: jc-8-revenue
-- tags: join, join:chain, tables:8
SELECT r.name, SUM(i.quantity * i.price) AS revenue FROM regions r JOIN countries co ON co.region_id = r.id JOIN cities ci ON ci.country_id = co.id JOIN customers c ON c.city_id = ci.id JOIN orders o ON o.customer_id = c.id JOIN order_items i ON i.order_id = o.id JOIN products p ON p.id = i.product_id JOIN brands b ON b.id = p.brand_id WHERE b.id = 3 GROUP BY r.name ORDER BY r.name;
