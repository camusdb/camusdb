-- Cycles, cliques, self-joins and a non-equi join. A cycle has one more predicate than a tree
-- needs, and in this data set that predicate is correlated with the others (most orders go to a
-- store in the customer's own city), so an estimator that assumes independence is wrong here.

-- query: jy-4-cycle-local-store
-- tags: join, join:cycle, tables:4, correlation
SELECT COUNT(*) AS n FROM customers c JOIN orders o ON o.customer_id = c.id JOIN stores st ON st.id = o.store_id JOIN cities ci ON ci.id = c.city_id AND ci.id = st.city_id;

-- query: jy-4-cycle-filtered
-- tags: join, join:cycle, tables:4, correlation
SELECT ci.name, COUNT(*) AS n FROM cities ci JOIN customers c ON c.city_id = ci.id JOIN orders o ON o.customer_id = c.id JOIN stores st ON st.id = o.store_id AND st.city_id = ci.id WHERE ci.id <= 3 GROUP BY ci.name ORDER BY ci.name;

-- query: jy-5-cycle-supplier-city
-- tags: join, join:cycle, tables:5
SELECT COUNT(*) AS n FROM order_items i JOIN products p ON p.id = i.product_id JOIN suppliers su ON su.id = p.supplier_id JOIN orders o ON o.id = i.order_id JOIN stores st ON st.id = o.store_id AND st.city_id = su.city_id;

-- query: jy-4-cycle-comma
-- tags: join, join:cycle, tables:4, comma-join, correlation
SELECT COUNT(*) AS n FROM cities ci, stores st, orders o, customers c WHERE st.city_id = ci.id AND o.store_id = st.id AND o.customer_id = c.id AND c.city_id = ci.id AND ci.population > 5000000;

-- query: jq-3-clique
-- tags: join, join:clique, tables:3
SELECT COUNT(*) AS n FROM orders o JOIN shipments sh ON sh.order_id = o.id JOIN payments pa ON pa.order_id = o.id AND pa.order_id = sh.order_id WHERE o.store_id = 12;

-- query: jq-4-clique
-- tags: join, join:clique, tables:4
SELECT COUNT(*) AS n, SUM(i.quantity) AS q FROM orders o JOIN shipments sh ON sh.order_id = o.id JOIN payments pa ON pa.order_id = o.id AND pa.order_id = sh.order_id JOIN order_items i ON i.order_id = o.id AND i.order_id = sh.order_id AND i.order_id = pa.order_id WHERE o.day_id BETWEEN 100 AND 130;

-- query: jq-4-clique-comma
-- tags: join, join:clique, tables:4, comma-join
SELECT COUNT(*) AS n FROM payments pa, order_items i, shipments sh, orders o WHERE pa.order_id = i.order_id AND pa.order_id = sh.order_id AND pa.order_id = o.id AND i.order_id = sh.order_id AND i.order_id = o.id AND sh.order_id = o.id AND sh.carrier_id = 1 AND pa.method = 'voucher';

-- query: jq-self-join-manager
-- tags: join, self-join, tables:3, nullable-key
SELECT st.name, COUNT(*) AS n FROM employees e JOIN employees m ON m.id = e.manager_id JOIN stores st ON st.id = m.store_id WHERE st.id <= 5 GROUP BY st.name ORDER BY st.name;

-- query: jq-self-join-referral
-- tags: join, self-join, tables:2, nullable-key
SELECT COUNT(*) AS n FROM customers c JOIN customers ref ON ref.id = c.referred_by WHERE ref.tier = 'gold';

-- query: jq-non-equi
-- tags: join, non-equi, tables:2, nested-loop
SELECT COUNT(*) AS n FROM promotions p1 JOIN promotions p2 ON p1.discount_pct < p2.discount_pct;
