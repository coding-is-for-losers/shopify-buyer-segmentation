SELECT 
store,
period,
customer_id,
date,
window_end_unix_date,
window_start_unix_date,
first_order_unix_date,
first_order_channel,
first_order_platform,
recency_days,
frequency,
quantity,
revenue, 
revenue_90pct,
revenue_10pct
FROM 
{{ ref('customers_proc_qoq')}}

UNION ALL

SELECT 
store,
period,
customer_id,
date,
window_end_unix_date,
window_start_unix_date,
first_order_unix_date,
first_order_channel,
first_order_platform,
recency_days,
frequency,
quantity,
revenue, 
revenue_90pct,
revenue_10pct
FROM 
{{ ref('customers_proc_yoy')}}
