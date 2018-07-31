WITH ty as (

	SELECT
	store,
	'Rolling YoY' as period,
	date,
	customer_id,
	first_order_channel,
	first_order_platform,
	revenue_segment as revenue_segment,
	frequency_segment as frequency_segment,
	newness_segment as newness_segment,
	recency_days as recency,
	frequency as frequency,
	revenue as revenue,
	0 as recency_prev,
	0 as frequency_prev,
	0 as revenue_prev,
	0 as retention_eligible,
	case when newness_segment = 'Existing' then 1 else 0 end as retention_success	
	FROM {{ ref('segment_proc_buyers') }}
	WHERE period = 'Rolling Year'

),

this_month_1yr as (

	SELECT
	store,
	'Rolling YoY' as period,
	date,
	customer_id,
	first_order_channel,
	first_order_platform,	
	revenue_segment,
	frequency_segment,
	newness_segment,
	0 as recency,
	0 as frequency,
	0 as revenue,
	recency_days as recency_prev,
	frequency as frequency_prev,
	revenue as revenue_prev,
	1 as retention_eligible,
	0 as retention_success	
	FROM {{ ref('segment_proc_buyers') }}
	WHERE period = 'Rolling Previous Year'

),

this_quarter as (

	SELECT
	store,
	'Rolling QoQ' as period,	
	date,
	customer_id,
	first_order_channel,
	first_order_platform,	
	revenue_segment,
	frequency_segment,
	newness_segment,
	recency_days as recency,
	frequency as frequency,
	revenue as revenue,
	0 as recency_prev,
	0 as frequency_prev,
	0 as revenue_prev,
	0 as retention_eligible,
	case when newness_segment = 'Existing' then 1 else 0 end as retention_success	
	FROM {{ ref('segment_proc_buyers') }}
	WHERE period = 'Rolling Quarter'

),

last_quarter as (

	SELECT
	store,
	'Rolling QoQ' as period,
	date,
	customer_id,
	first_order_channel,
	first_order_platform,	
	revenue_segment as revenue_segment,
	frequency_segment as frequency_segment,
	newness_segment as newness_segment,
	0 as recency,
	0 as frequency,
	0 as revenue,
	recency_days as recency_prev,
	frequency as frequency_prev,
	revenue as revenue_prev,
	1 as retention_eligible,
	0 as retention_success
	FROM {{ ref('segment_proc_buyers') }}
	WHERE period = 'Rolling Previous Quarter'

)

SELECT
store,
period,
date, 
customer_id,
case when sum(revenue) > 0 then 1 else 0 end as buyers,
first_order_channel,
first_order_platform,	
revenue_segment,
frequency_segment,
newness_segment,	
ifnull(sum(recency), 0) recency,
ifnull(sum(frequency), 0) frequency,
ifnull(sum(revenue), 0) revenue,
case when sum(frequency) > 0 then sum(revenue)/sum(frequency) else 0 end as aov,
ifnull(sum(recency_prev), 0) recency_prev,
ifnull(sum(frequency_prev), 0) frequency_prev,
ifnull(sum(revenue_prev), 0) revenue_prev,
case when sum(frequency_prev) > 0 then sum(revenue_prev)/sum(frequency_prev) else 0 end as aov_prev,
sum(retention_eligible) retention_eligible,
sum(retention_success) retention_success
FROM
(
	SELECT * FROM ty
	UNION ALL
	SELECT * FROM this_month_1yr
	UNION ALL
	SELECT * FROM this_quarter
	UNION ALL
	SELECT * FROM last_quarter

)
GROUP BY store, period, date, customer_id, first_order_channel, first_order_platform,
revenue_segment, frequency_segment, newness_segment	
