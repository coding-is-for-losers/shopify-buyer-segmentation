SELECT
store,
period,
customer_id,
date,
first_order_unix_date,
first_order_channel,
first_order_platform,
recency_days,
frequency,
quantity,
revenue, 
revenue_90pct,
revenue_10pct,
case when first_order_unix_date >= window_start_unix_date then 'New'
	else 'Existing' end as newness_segment,
case when revenue >= revenue_90pct then 'Top 10%'
	when revenue <= revenue_10pct then 'Bottom 10%'
	else 'Middle 80%' end as revenue_segment,
case when frequency = 1 then '1'
	when frequency = 2 then '2'
	when frequency > 2 then '3+'
	else null end as frequency_segment
FROM {{ ref('customers_proc') }}
