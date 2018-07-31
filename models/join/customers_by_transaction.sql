SELECT
store,
customer_id,
order_number,
order_date,
recent_order_date,
first_order_date,
case when first_order_number = order_number then 'New'
	when date_diff(order_date, recent_order_date, DAY) <= 365 then 'Repeat'
	when date_diff(order_date, recent_order_date, DAY) > 365 then 'Reactivated'
 else '' end as order_type,
quantity,
revenue,
1 as orders,
first_order_revenue,
lifetime_revenue
FROM

(

	SELECT
	store,
	customer_id,
	order_number,
	order_date,
	quantity,
	revenue,
	lag(order_date) over w1 recent_order_date,
	first_value(order_date) over w1 first_order_date,
	first_value(order_number) over w1 first_order_number,
	first_value(revenue) over w1 first_order_revenue,
	sum(revenue) over w2 lifetime_revenue
	FROM {{ ref('transaction_by_order_number') }}
	WINDOW w1 as (PARTITION BY store, customer_id ORDER BY order_date asc),
	w2 as (PARTITION BY store, customer_id)
)
