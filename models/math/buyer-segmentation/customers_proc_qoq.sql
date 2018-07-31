WITH customers AS (

  SELECT 
  store,
  customer_id,
  order_date,
  unix_date(order_date) unix_order_date, 
  first_order_date,
  unix_date(first_order_date) first_order_unix_date,
  first_order_revenue,
  first_order_channel,
  first_order_platform,
  channel,
  platform,
  url,
  campaign,
  quantity,
  revenue,
  orders
  FROM {{ref('agg_transactions')}}
),

daterange AS (
  SELECT * FROM {{ref('monthend_dates')}}    
)

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
PERCENTILE_CONT(revenue, 0.90) OVER w1 AS revenue_90pct,
PERCENTILE_CONT(revenue, 0.10) OVER w1 AS revenue_10pct
FROM (
   
  SELECT 
  store,
  period,
  customer_id,
  date,
  window_end_unix_date,
  window_start_unix_date,
  first_order_unix_date,
  window_end_unix_date - unix_date(recent_order) recency_days,
  first_order_channel,
  first_order_platform,
  quantity,
  revenue, 
  frequency
  FROM 
  (  

    SELECT 
    store,
    'Rolling Quarter' as period,
    customer_id,
    date_in_range date,
    unix_date_in_range, 
    unix_date_in_range window_end_unix_date, 
    unix_date_in_range - 90 window_start_unix_date, 
    first_order_unix_date,
    first_order_channel,
    first_order_platform,
    max(order_date) recent_order,
    sum(quantity) as quantity,
    sum(revenue) as revenue,
    sum(orders) as frequency
    FROM daterange
    JOIN customers
    ON customers.unix_order_date > ( daterange.unix_date_in_range - 90 )
    AND customers.unix_order_date <= daterange.unix_date_in_range
    GROUP BY store, customer_id, date, unix_date_in_range, window_end_unix_date, 
    window_start_unix_date, first_order_unix_date, first_order_channel, first_order_platform

    UNION ALL

    SELECT 
    store,
    'Rolling Previous Quarter' as period,
    customer_id,
    date_in_range date,
    unix_date_in_range, 
    unix_date_in_range - 90 window_end_unix_date, 
    unix_date_in_range - 180 window_start_unix_date, 
    first_order_unix_date,
    first_order_channel,
    first_order_platform,
    max(order_date) recent_order,
    sum(quantity) as quantity,
    sum(revenue) as revenue,
    sum(orders) as frequency
    FROM daterange
    JOIN customers
    ON customers.unix_order_date > ( daterange.unix_date_in_range - 180 )
    AND customers.unix_order_date <= ( daterange.unix_date_in_range - 90 )
    GROUP BY store, customer_id, date, unix_date_in_range, window_end_unix_date, 
    window_start_unix_date, first_order_unix_date, first_order_channel, first_order_platform

  )
)
WINDOW w1 as (PARTITION BY store, period, date)
