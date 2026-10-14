-- Single-table shapes: access paths, filters, ordering, grouping and distinct.
-- Every ORDER BY with a LIMIT ends with a unique column, so the kept rows have one right answer.

-- query: st-pk-point
-- tags: single-table, point-lookup, primary-key
SELECT id, name, credit FROM customers WHERE id = 1234;

-- query: st-pk-in-list
-- tags: single-table, in-list, primary-key
SELECT id, name FROM customers WHERE id IN (3, 17, 250, 999, 1500, 1999);

-- query: st-pk-range
-- tags: single-table, range, primary-key
SELECT COUNT(*) AS n, SUM(total) AS s FROM orders WHERE id >= 1000 AND id < 1200;

-- query: st-pk-range-order-limit
-- tags: single-table, range, order-limit, primary-key
SELECT id FROM order_items WHERE id > 10000 ORDER BY id LIMIT 5;

-- query: st-secondary-point-popular
-- tags: single-table, point-lookup, secondary-index, skew
SELECT COUNT(*) AS n FROM customers WHERE city_id = 1;

-- query: st-secondary-point-rare
-- tags: single-table, point-lookup, secondary-index, skew
SELECT id, name FROM customers WHERE city_id = 290 ORDER BY id;

-- query: st-secondary-range-narrow
-- tags: single-table, range, secondary-index
SELECT id, price FROM products WHERE price BETWEEN 950 AND 1000 ORDER BY price, id;

-- query: st-secondary-range-wide
-- tags: single-table, range, secondary-index
SELECT COUNT(*) AS n FROM products WHERE price > 300;

-- query: st-two-ranges
-- tags: single-table, range, secondary-index
SELECT COUNT(*) AS n FROM orders WHERE total BETWEEN 100 AND 200 AND day_id BETWEEN 1 AND 100;

-- query: st-in-list-secondary
-- tags: single-table, in-list, secondary-index
SELECT COUNT(*) AS n FROM orders WHERE store_id IN (1, 2, 3, 77, 149);

-- query: st-or-same-column
-- tags: single-table, or, secondary-index
SELECT COUNT(*) AS n FROM customers WHERE segment_id = 4 OR segment_id = 5;

-- query: st-or-two-columns
-- tags: single-table, or
SELECT COUNT(*) AS n FROM customers WHERE tier = 'gold' OR credit < 0;

-- query: st-like-prefix
-- tags: single-table, like
SELECT COUNT(*) AS n FROM products WHERE name LIKE 'product 12%';

-- query: st-like-infix
-- tags: single-table, like, full-scan
SELECT COUNT(*) AS n FROM customers WHERE name LIKE '%99%';

-- query: st-is-null
-- tags: single-table, null, secondary-index
SELECT COUNT(*) AS n FROM customers WHERE tier IS NULL;

-- query: st-is-not-null-group
-- tags: single-table, null, group-by
SELECT tier, COUNT(*) AS n FROM customers WHERE tier IS NOT NULL GROUP BY tier ORDER BY tier;

-- query: st-correlated-columns
-- tags: single-table, correlation
SELECT COUNT(*) AS n FROM products WHERE category_id = 9 AND price >= 1000;

-- query: st-composite-prefix
-- tags: single-table, composite-index
SELECT COUNT(*) AS n FROM orders WHERE status = 'pending';

-- query: st-composite-full
-- tags: single-table, composite-index, range
SELECT id, total FROM orders WHERE status = 'cancelled' AND total >= 4500 ORDER BY total, id;

-- query: st-order-limit-indexed
-- tags: single-table, order-limit, secondary-index
SELECT id, price FROM products ORDER BY price, id LIMIT 10;

-- query: st-order-limit-desc
-- tags: single-table, order-limit
SELECT id, price FROM products ORDER BY price DESC, id LIMIT 10;

-- query: st-order-limit-pk
-- tags: single-table, order-limit, primary-key
SELECT id, total FROM orders ORDER BY id LIMIT 20;

-- query: st-order-limit-filtered
-- tags: single-table, order-limit, secondary-index
SELECT id, total FROM orders WHERE store_id = 5 ORDER BY total DESC, id LIMIT 5;

-- query: st-order-offset
-- tags: single-table, order-limit
SELECT id FROM customers ORDER BY credit, id LIMIT 10 OFFSET 100;

-- query: st-desc-index-range
-- tags: single-table, descending-index, range
SELECT COUNT(*) AS n FROM customers WHERE signup_year >= 2024;

-- query: st-desc-index-order
-- tags: single-table, descending-index, order-limit
SELECT id, signup_year FROM customers ORDER BY signup_year DESC, id LIMIT 15;

-- query: st-group-by-indexed
-- tags: single-table, group-by, secondary-index
SELECT store_id, COUNT(*) AS n, SUM(total) AS s FROM orders GROUP BY store_id ORDER BY n DESC, store_id LIMIT 10;

-- query: st-group-by-unindexed
-- tags: single-table, group-by
SELECT method, COUNT(*) AS n, SUM(amount) AS s FROM payments GROUP BY method ORDER BY method;

-- query: st-group-by-two-columns
-- tags: single-table, group-by
SELECT status, store_id, COUNT(*) AS n FROM orders WHERE store_id <= 3 GROUP BY status, store_id ORDER BY status, store_id;

-- query: st-group-by-having
-- tags: single-table, group-by, having
SELECT product_id, COUNT(*) AS n FROM order_items GROUP BY product_id HAVING COUNT(*) > 40 ORDER BY product_id;

-- query: st-distinct-indexed
-- tags: single-table, distinct, secondary-index
SELECT DISTINCT city_id FROM stores ORDER BY city_id;

-- query: st-distinct-unindexed
-- tags: single-table, distinct
SELECT DISTINCT stars FROM reviews ORDER BY stars;

-- query: st-aggregate-all
-- tags: single-table, aggregate, full-scan
SELECT COUNT(*) AS n, MIN(total) AS lo, MAX(total) AS hi, AVG(total) AS mean FROM orders;

-- query: st-bool-filter
-- tags: single-table, bool
SELECT COUNT(*) AS n FROM products WHERE discontinued = true;

-- query: st-not-equal
-- tags: single-table, negation
SELECT COUNT(*) AS n FROM orders WHERE status <> 'delivered';

-- query: st-empty-result
-- tags: single-table, empty
SELECT id FROM orders WHERE total > 999999;

-- query: st-view
-- tags: single-table, view
SELECT COUNT(*) AS n, SUM(total) AS s FROM big_orders WHERE store_id = 3;
