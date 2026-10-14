-- Subqueries, derived tables and views. Note: the reads of a subquery that the bind stage folds
-- into a literal are not on any plan node, so they are not in the work of the outer query.

-- query: sq-exists-correlated
-- tags: subquery, exists, correlated, semi-join
-- reviews.customer_id has no index, so the subquery scans reviews once for each outer row. Its reads
-- are on no plan node; only the statement rows read sees them. The outer side is kept at 100 rows,
-- because at 2,000 rows this one query took most of the tier's time.
SELECT COUNT(*) AS n FROM customers c WHERE c.id <= 100 AND EXISTS (SELECT 1 FROM reviews r WHERE r.customer_id = c.id AND r.stars = 1);

-- query: sq-not-exists
-- tags: subquery, not-exists, correlated, anti-join
SELECT COUNT(*) AS n FROM products p WHERE NOT EXISTS (SELECT 1 FROM order_items i WHERE i.product_id = p.id);

-- query: sq-exists-after-join
-- tags: subquery, exists, correlated, join, tables:2
SELECT COUNT(*) AS n FROM orders o JOIN stores st ON st.id = o.store_id WHERE st.opened_year > 2020 AND EXISTS (SELECT 1 FROM shipments sh WHERE sh.order_id = o.id AND sh.status = 'in transit');

-- query: sq-in-subquery
-- tags: subquery, in-subquery, semi-join
SELECT COUNT(*) AS n FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE tier = 'gold');

-- query: sq-in-subquery-nested-in-list
-- tags: subquery, in-subquery, in-list
SELECT COUNT(*) AS n FROM order_items WHERE product_id IN (SELECT id FROM products WHERE brand_id IN (1, 2, 3));

-- query: sq-not-in-with-null
-- tags: subquery, not-in, null
SELECT COUNT(*) AS n FROM customers WHERE id NOT IN (SELECT customer_id FROM orders WHERE store_id = 1);

-- query: sq-not-in
-- tags: subquery, not-in
SELECT COUNT(*) AS n FROM stores WHERE id NOT IN (SELECT store_id FROM orders WHERE total > 4990);

-- query: sq-scalar
-- tags: subquery, scalar
SELECT COUNT(*) AS n FROM orders WHERE total > (SELECT AVG(total) FROM orders WHERE store_id = 2);

-- query: sq-quantified-all
-- tags: subquery, quantified
SELECT COUNT(*) AS n FROM products WHERE price > ALL (SELECT price FROM products WHERE category_id = 9);

-- query: sq-derived-aggregate
-- tags: subquery, derived-table, join, tables:2
SELECT st.name, x.n FROM (SELECT store_id, COUNT(*) AS n FROM orders GROUP BY store_id) x JOIN stores st ON st.id = x.store_id WHERE x.n > 100 ORDER BY x.n DESC, st.name LIMIT 10;

-- query: sq-derived-filter
-- tags: subquery, derived-table
SELECT COUNT(*) AS n FROM (SELECT id, total FROM orders WHERE status = 'shipped') s WHERE s.total < 100;

-- query: sq-view-join
-- tags: subquery, view, join, tables:2
SELECT c.tier, COUNT(*) AS n FROM big_orders bo JOIN customers c ON c.id = bo.customer_id GROUP BY c.tier ORDER BY c.tier;

-- query: sq-view-join-3
-- tags: subquery, view, join, tables:3
SELECT b.name, COUNT(*) AS n FROM active_products ap JOIN brands b ON b.id = ap.brand_id JOIN order_items i ON i.product_id = ap.id WHERE b.id <= 3 GROUP BY b.name ORDER BY b.name;
