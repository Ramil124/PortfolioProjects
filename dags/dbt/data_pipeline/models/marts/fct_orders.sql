select orders.*,
    order_items_summary.gross_items_sales_amount,
    order_items_summary.item_discount_amounts
from 
{{
    ref('stg_tpch_orders')
}} as orders
join 
{{
    ref('int_order_items_summary')
}} as order_items_summary
on orders.order_key = order_items_summary.order_key
order by orders.order_date