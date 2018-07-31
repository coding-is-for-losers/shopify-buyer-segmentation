with ga_transaction as (

	SELECT
	date, 
	store,
	transactionid,
	channel,
	platform,
	url,
	campaign
	FROM {{ ref('ga_transactions')}}
),

customers_by_transaction as (
	
	SELECT
	store,
	customer_id,
	order_number,
	order_date,
	first_order_date,
	recent_order_date,
	order_type,
	quantity,
	revenue,
	orders,
	first_order_revenue,
	lifetime_revenue
	FROM {{ ref('customers_by_transaction')}}
)

SELECT
store,
customer_id,
order_number,
transactionid,
order_date,
first_order_date,
format_date("%Y-%m", first_order_date) AS first_order_month,
order_type,
first_order_revenue,
lifetime_revenue,
first_value(channel) over w1 as first_order_channel,
first_value(platform) over w1 as first_order_platform,
channel,
platform,
url,
campaign,
quantity,
revenue,
orders
FROM (

	SELECT
	a.store,
	a.customer_id,
	a.order_number,
	b.transactionid,
	a.order_date, 
	a.first_order_date, 
	a.order_type,
	a.first_order_revenue,
	a.lifetime_revenue,
	b.channel,
	b.platform,
	b.url,
	b.campaign,
	a.quantity,
	a.revenue,
	a.orders
	FROM customers_by_transaction a
	LEFT JOIN ga_transaction b
	ON (
	    a.store = b.store AND
	    a.order_number = b.transactionid
	)	
)
WINDOW w1 as (PARTITION BY store, customer_id ORDER BY order_date asc)