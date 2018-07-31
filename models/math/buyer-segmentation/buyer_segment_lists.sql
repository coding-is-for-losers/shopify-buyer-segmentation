with buyer_lists as (

	SELECT
	a.store,
	period,
	date,
	customer_id,
	b.first_name,
	b.last_name,
	b.email,
	recency,
	frequency,
	revenue,
	aov,
	revenue_segment,
	frequency_segment
	FROM {{ ref('segment_stats_buyers_agg')}} a
	LEFT JOIN {{ ref('agg_customers') }} b
	ON (
		a.store = b.store AND
		a.customer_id = b.id
	)
	where revenue > 0
)

SELECT
store,
period,
date,
'Revenue' as segment_type,
revenue_segment as segment,
customer_id,
first_name,
last_name,
email,
recency,
frequency,
revenue,
aov
FROM buyer_lists

UNION ALL

SELECT
store,
period,
date,
'Frequency' as segment_type,
frequency_segment as segment,
customer_id,
first_name,
last_name,
email,
recency,
frequency,
revenue,
aov
FROM buyer_lists





