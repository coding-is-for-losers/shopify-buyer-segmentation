SELECT
store,
cast(created_at as date) order_date,
order_number,
customer_id,
sum(quantity) quantity,
sum(revenue) revenue,
max(total_order_shipping_price) shipping_price
FROM
{{ ref('shopify_orders_proc') }}
GROUP BY store, order_date, order_number, customer_id
