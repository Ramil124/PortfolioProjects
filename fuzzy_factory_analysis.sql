USE mavenfuzzyfactory;

-- Displaying Volume of growth.
-- Pulling overall sessions and order volume. trended by quarte for the life of the business.

SELECT YEAR(website_sessions.created_at) as yr,
QUARTER(website_sessions.created_at) as qrt,
COUNT(DISTINCT website_sessions.website_session_id) as sessions,
COUNT(DISTINCT orders.order_id) as orders
FROM website_sessions LEFT JOIN orders
ON website_sessions.website_session_id = orders.website_session_id
WHERE website_sessions.created_at <'2015-03-20'
GROUP BY 1,2
ORDER BY 1,2;



/* Showcasing efficiency improvements
Quarterly figures for sessio-to-order conversion rate, revenue per order
and revenue per session*/

SELECT YEAR(website_sessions.created_at) as yr,
QUARTER(website_sessions.created_at) as qrt,
COUNT(DISTINCT website_sessions.website_session_id) as sessions,
COUNT(DISTINCT orders.order_id)/COUNT(DISTINCT website_sessions.website_session_id) as conv_rt,
SUM(price_usd)/ COUNT(DISTINCT orders.order_id) as revenue_per_order,
SUM(price_usd)/COUNT(DISTINCT website_sessions.website_session_id) as revenue_per_session
FROM website_sessions LEFT JOIN orders
ON website_sessions.website_session_id = orders.website_session_id
WHERE website_sessions.created_at <'2015-03-20'
GROUP BY 1,2
ORDER BY 1,2;


/*The growth of specific channels
Quarterly view of orders from Gsearch nonbrand, Bsearch ninbrand, brand search overall, 
organic search and direct type-in */


SELECT YEAR(website_sessions.created_at) as yr,
QUARTER(website_sessions.created_at) as qrt,
COUNT(DISTINCT CASE WHEN utm_campaign = 'nonbrand' AND utm_source = 'gsearch' THEN orders.order_id ELSE NULL END) as gsearch_nonbrand_orders,
COUNT(DISTINCT CASE	WHEN utm_campaign = 'nonbrand' AND utm_source = 'bsearch' THEN orders.order_id ELSE NULL END) as bsearch_nonbrand_orders,
COUNT(DISTINCT CASE	WHEN utm_campaign = 'brand' THEN orders.order_id ELSE NULL END) as brand_orders,
COUNT(DISTINCT CASE WHEN utm_source IS NULL AND http_referer IS NOT NULL THEN orders.order_id ELSE NULL END) as organic_search_orders,
COUNT(DISTINCT CASE WHEN utm_source IS NULL AND http_referer IS NULL THEN orders.order_id ELSE NULL END) as direct_type_in_orders
FROM website_sessions LEFT JOIN orders
ON website_sessions.website_session_id = orders.website_session_id
WHERE website_sessions.created_at <'2015-03-20'
GROUP BY 1,2
ORDER BY 1,2;



/*Overall session-to-order conversion rate trends for the same channels, by quarter.
*/

SELECT YEAR(website_sessions.created_at) as yr,
QUARTER(website_sessions.created_at) as qrt,
COUNT(DISTINCT website_sessions.website_session_id) as sessions,
COUNT(DISTINCT CASE WHEN utm_campaign = 'nonbrand' AND utm_source = 'gsearch' THEN orders.order_id ELSE NULL END) as gsearch_nonbrand_sessions,
COUNT(DISTINCT CASE	WHEN utm_campaign = 'nonbrand' AND utm_source = 'bsearch' THEN orders.order_id ELSE NULL END) as bsearch_nonbrand_sessions,
COUNT(DISTINCT CASE	WHEN utm_campaign = 'brand' THEN orders.order_id ELSE NULL END) as brand_to_sessions,
COUNT(DISTINCT CASE WHEN utm_source IS NULL AND http_referer IS NOT NULL THEN orders.order_id ELSE NULL END) as organic_search_sessions,
COUNT(DISTINCT CASE WHEN utm_source IS NULL AND http_referer IS NULL THEN orders.order_id ELSE NULL END) as direct_type_in_sessions
FROM website_sessions LEFT JOIN orders
ON website_sessions.website_session_id = orders.website_session_id
WHERE website_sessions.created_at <'2015-03-20'
GROUP BY 1,2;



SELECT YEAR(website_sessions.created_at) as yr,
QUARTER(website_sessions.created_at) as qrt,
COUNT(DISTINCT website_sessions.website_session_id) as sessions,
COUNT(DISTINCT CASE WHEN utm_campaign = 'nonbrand' AND utm_source = 'gsearch' THEN orders.order_id ELSE NULL END)/
COUNT(DISTINCT website_sessions.website_session_id) as gsearch_nonbrand_conv_rt,
COUNT(DISTINCT CASE	WHEN utm_campaign = 'nonbrand' AND utm_source = 'bsearch' THEN orders.order_id ELSE NULL END)/
COUNT(DISTINCT website_sessions.website_session_id) as bsearch_nonbrand_conv_rt,
COUNT(DISTINCT CASE	WHEN utm_campaign = 'brand' THEN orders.order_id ELSE NULL END)/
COUNT(DISTINCT website_sessions.website_session_id) as brand_to_sessions_conv_rt,
COUNT(DISTINCT CASE WHEN utm_source IS NULL AND http_referer IS NOT NULL THEN orders.order_id ELSE NULL END)/
COUNT(DISTINCT website_sessions.website_session_id) as organic_search_conv_rt,
COUNT(DISTINCT CASE WHEN utm_source IS NULL AND http_referer IS NULL THEN orders.order_id ELSE NULL END)/
COUNT(DISTINCT website_sessions.website_session_id) as direct_type_in_conv_rt
FROM website_sessions LEFT JOIN orders
ON website_sessions.website_session_id = orders.website_session_id
WHERE website_sessions.created_at <'2015-03-20'
GROUP BY 1,2;




/*Monthly trending for revenue and margin by product, along with total sales and revenue*/

SELECT YEAR(created_at) as yr,
MONTH(created_at) as mo,
COUNT(DISTINCT order_id) as total_sales,
SUM(price_usd) as total_revenue,
SUM(CASE WHEN product_id = 1 THEN price_usd ELSE NULL END) as p1_revenue,
SUM(CASE WHEN product_id = 1 THEN price_usd ELSE NULL END) - SUM(CASE WHEN product_id = 1 THEN cogs_usd ELSE NULL END) as p1_margin,
SUM(CASE WHEN product_id = 2 THEN price_usd ELSE NULL END) as p2_revenue,
SUM(CASE WHEN product_id = 2 THEN price_usd ELSE NULL END) - SUM(CASE WHEN product_id = 2 THEN cogs_usd ELSE NULL END) as p2_margin,
SUM(CASE WHEN product_id = 3 THEN price_usd ELSE NULL END) as p3_revenue,
SUM(CASE WHEN product_id = 3 THEN price_usd ELSE NULL END) - SUM(CASE WHEN product_id = 3 THEN cogs_usd ELSE NULL END) as p3_margin,
SUM(CASE WHEN product_id = 4 THEN price_usd ELSE NULL END) as p4_revenue,
SUM(CASE WHEN product_id = 4 THEN price_usd ELSE NULL END) - SUM(CASE WHEN product_id = 4 THEN cogs_usd ELSE NULL END) as p4_margin
FROM order_items
WHERE created_at <'2015-03-20'
GROUP BY 1,2;


/*Monthly session to the /products page, and displaying how the percentage of those sessions clicking through 
another page has changed over time, along with a view of how conversion from /products to placing an order has improved*/

DROP TEMPORARY TABLE product_sessions_w_next_page;
CREATE TEMPORARY TABLE product_sessions_w_next_page
SELECT 
product_sessions.website_session_id,
MIN(website_pageviews.website_pageview_id) as min_pgv,
website_pageviews.pageview_url as next_product_page,
product_sessions.created_at
FROM (
SELECT website_session_id, website_pageview_id, created_at
FROM website_pageviews
WHERE created_at <'2015-03-20'
AND pageview_url = '/products'
) as product_sessions LEFT JOIN website_pageviews
ON product_sessions.website_session_id = website_pageviews.website_session_id
AND website_pageviews.website_pageview_id > product_sessions.website_pageview_id
GROUP BY product_sessions.website_session_id;


SELECT YEAR(product_sessions_w_next_page.created_at) as yr,
 MONTH(product_sessions_w_next_page.created_at) as mo,
COUNT(DISTINCT product_sessions_w_next_page.website_session_id) as product_sessions,
-- COUNT(CASE WHEN next_product_page IS NOT NULL THEN product_sessions_w_next_page.website_session_id ELSE NULL END) as next_page,
COUNT(CASE WHEN next_product_page IS NOT NULL THEN product_sessions_w_next_page.website_session_id ELSE NULL END)/
COUNT(DISTINCT product_sessions_w_next_page.website_session_id) as pct_to_next_page,
-- COUNT(DISTINCT orders.order_id) as placed_order,
COUNT(DISTINCT orders.order_id)/COUNT(DISTINCT product_sessions_w_next_page.website_session_id) as products_to_order_conv_rt
FROM product_sessions_w_next_page LEFT JOIN orders
ON  product_sessions_w_next_page.website_session_id = orders.website_session_id
GROUP BY YEAR(product_sessions_w_next_page.created_at),
		MONTH(product_sessions_w_next_page.created_at);


/*As the 4th product has been launched on 2014-12-05 which previously was only a cross-sell item 
Displaying the sales data since that date to show how well each product cross-sells from one another*/

DROP TEMPORARY TABLE primary_products;
CREATE TEMPORARY TABLE primary_products
SELECT order_id,
primary_product_id,
created_at as ordered_at
FROM orders
WHERE created_at >'2014-12-05'; -- when the 4th product was added


-- we are only selecting product ids that cross selled
-- then adding it as a subquery for the final output analysis

SELECT primary_products.*,
order_items.product_id as cross_sell_products
FROM primary_products LEFT JOIN order_items
ON primary_products.order_id = order_items.order_id
AND order_items.is_primary_item = 0; -- only bringing cross_sell products


-- finally as per request we are looking at the total num of orders
-- and how well each of the products cross sold with each other

SELECT primary_product_id,
COUNT(DISTINCT order_id) as orders,
COUNT(DISTINCT CASE WHEN cross_sell_products = 1 THEN order_id ELSE NULL END) as xsold_p1,
COUNT(DISTINCT CASE WHEN cross_sell_products = 2 THEN order_id ELSE NULL END) as xsold_p2,
COUNT(DISTINCT CASE WHEN cross_sell_products = 3 THEN order_id ELSE NULL END) as xsold_p3,
COUNT(DISTINCT CASE WHEN cross_sell_products = 4 THEN order_id ELSE NULL END) as xsold_p4,
COUNT(DISTINCT CASE WHEN cross_sell_products = 1 THEN order_id ELSE NULL END)/
COUNT(DISTINCT order_id) as xsold_p1_rt ,
COUNT(DISTINCT CASE WHEN cross_sell_products = 2 THEN order_id ELSE NULL END)/
COUNT(DISTINCT order_id) as xsold_p2_rt,
COUNT(DISTINCT CASE WHEN cross_sell_products = 3 THEN order_id ELSE NULL END)/
COUNT(DISTINCT order_id) as xsold_p3_rt,
COUNT(DISTINCT CASE WHEN cross_sell_products = 4 THEN order_id ELSE NULL END)/
COUNT(DISTINCT order_id) as xsold_p4_rt
FROM (
SELECT primary_products.*,
order_items.product_id as cross_sell_products
FROM primary_products LEFT JOIN order_items
ON primary_products.order_id = order_items.order_id
AND order_items.is_primary_item = 0
) as primary_w_cross_sells
GROUP BY 1;


