-- to test if the values are within an acceptable date range
SELECT *
FROM {{
    ref('fct_orders')
}}
WHERE date(order_date) > CURRENT_DATE()
    or date(order_date) < date('1990-01-01')