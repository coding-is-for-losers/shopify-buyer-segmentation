SELECT
store,
period,
date,
view,
view_segment,
segment_type,
segment,
buyers,
total_view_buyers,
case when ( total_segment_buyers / total_buyers ) > 0 
	then ( buyers / total_view_buyers ) / ( total_segment_buyers / total_buyers ) 
	else null end as segment_buyer_index,
recency,
orders,
case when buyers > 0 then orders / buyers else null end as frequency,
revenue,
case when total_revenue > 0 then revenue / total_revenue else null end as pct_of_revenue,
revenue_prev,
total_view_revenue,
case when ( total_segment_revenue / total_revenue ) > 0 
	then ( revenue / total_view_revenue ) / ( total_segment_revenue / total_revenue ) 
	else null end as segment_revenue_index,
aov,
recency_growth,
frequency_growth,
revenue_growth,
aov_growth,
retention_rate
FROM (

	SELECT
	store,
	period,
	date,
	view,
	view_segment,
	segment_type,
	segment,
	buyers,
	sum(buyers) over w1 as total_view_buyers,
	sum(buyers) over w2 as total_segment_buyers,
	sum(buyers) over w3 as total_buyers,
	recency,
	frequency orders,
	revenue,
	sum(revenue) over w1 as total_view_revenue,
	sum(revenue) over w2 as total_segment_revenue,
	sum(revenue) over w3 as total_revenue,
	aov,
	case when recency_prev > 0 then ( recency_prev - recency ) / recency_prev else null end as recency_growth,
	case when frequency_prev > 0 then ( frequency - frequency_prev ) / frequency_prev else null end as frequency_growth,
	revenue_prev,
	case when revenue_prev > 0 then ( revenue - revenue_prev ) / revenue_prev else null end as revenue_growth,
	case when aov_prev > 0 then ( aov - aov_prev ) / aov_prev else null end as aov_growth,
	retention_rate
	FROM {{ ref('segment_stats_buyers_view')}}
	WINDOW w1 as (PARTITION BY store, period, date, segment_type, view, view_segment),
	w2 as (PARTITION BY store, period, date, segment_type, view, segment),
	w3 as (PARTITION BY store, period, date, segment_type, view)
)