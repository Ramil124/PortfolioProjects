select 
order_key,
sum(extended_price) as gross_items_sales_amount,
sum(item_discount_amount) as item_discount_amounts
FROM 
{{
    ref('int_order_items')
}}
group by order_key