-- to test if all item_discount_amounts are above zero
select *
FROm 
{{
    ref('fct_orders')
}}
where item_discount_amounts > 0