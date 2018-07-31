SELECT
store,
period,
date,
'Overall' as view,
'Overall' as view_segment,
'Revenue' as segment_type,
revenue_segment as segment,
sum(buyers) buyers,
case when sum(buyers) > 0 then sum(recency)/sum(buyers) else null end as  recency,
sum(frequency) frequency,
sum(revenue) revenue,
case when sum(frequency) > 0 then sum(revenue)/sum(frequency) else null end as aov,
case when sum(buyers) > 0 then sum(recency_prev)/sum(buyers) else null end as  recency_prev,
sum(frequency_prev) frequency_prev,
sum(revenue_prev) revenue_prev,
case when sum(frequency_prev) > 0 then sum(revenue_prev)/sum(frequency_prev) else null end as aov_prev,
case when sum(retention_eligible) > 0 then sum(retention_success)/sum(retention_eligible) else null end as retention_rate
FROM {{ ref('segment_stats_buyers_agg')}}
GROUP BY store, period, date, segment, view, view_segment, segment_type

UNION ALL

SELECT
store,
period,
date,
'First Order Channel' as view,
first_order_channel as view_segment,
'Revenue' as segment_type,
revenue_segment as segment,
sum(buyers) buyers,
case when sum(buyers) > 0 then sum(recency)/sum(buyers) else null end as  recency,
sum(frequency) frequency,
sum(revenue) revenue,
case when sum(frequency) > 0 then sum(revenue)/sum(frequency) else null end as aov,
case when sum(buyers) > 0 then sum(recency_prev)/sum(buyers) else null end as  recency_prev,
sum(frequency_prev) frequency_prev,
sum(revenue_prev) revenue_prev,
case when sum(frequency_prev) > 0 then sum(revenue_prev)/sum(frequency_prev) else null end as aov_prev,
case when sum(retention_eligible) > 0 then sum(retention_success)/sum(retention_eligible) else null end as retention_rate
FROM {{ ref('segment_stats_buyers_agg')}}
GROUP BY store, period, date, segment, view, view_segment, segment_type

UNION ALL

SELECT
store,
period,
date,
'First Order Platform' as view,
first_order_platform as view_segment,
'Revenue' as segment_type,
revenue_segment as segment,
sum(buyers) buyers,
case when sum(buyers) > 0 then sum(recency)/sum(buyers) else null end as  recency,
sum(frequency) frequency,
sum(revenue) revenue,
case when sum(frequency) > 0 then sum(revenue)/sum(frequency) else null end as aov,
case when sum(buyers) > 0 then sum(recency_prev)/sum(buyers) else null end as  recency_prev,
sum(frequency_prev) frequency_prev,
sum(revenue_prev) revenue_prev,
case when sum(frequency_prev) > 0 then sum(revenue_prev)/sum(frequency_prev) else null end as aov_prev,
case when sum(retention_eligible) > 0 then sum(retention_success)/sum(retention_eligible) else null end as retention_rate
FROM {{ ref('segment_stats_buyers_agg')}}
GROUP BY store, period, date, segment, view, view_segment, segment_type

UNION ALL

SELECT
store,
period,
date,
'Overall' as view,
'Overall' as view_segment,
'Frequency' as segment_type,
frequency_segment as segment,
sum(buyers) buyers,
case when sum(buyers) > 0 then sum(recency)/sum(buyers) else null end as  recency,
sum(frequency) frequency,
sum(revenue) revenue,
case when sum(frequency) > 0 then sum(revenue)/sum(frequency) else null end as aov,
case when sum(buyers) > 0 then sum(recency_prev)/sum(buyers) else null end as  recency_prev,
sum(frequency_prev) frequency_prev,
sum(revenue_prev) revenue_prev,
case when sum(frequency_prev) > 0 then sum(revenue_prev)/sum(frequency_prev) else null end as aov_prev,
case when sum(retention_eligible) > 0 then sum(retention_success)/sum(retention_eligible) else null end as retention_rate
FROM {{ ref('segment_stats_buyers_agg')}}
GROUP BY store, period, date, segment, view, view_segment, segment_type

UNION ALL

SELECT
store,
period,
date,
'First Order Channel' as view,
first_order_channel as view_segment,
'Frequency' as segment_type,
frequency_segment as segment,
sum(buyers) buyers,
case when sum(buyers) > 0 then sum(recency)/sum(buyers) else null end as  recency,
sum(frequency) frequency,
sum(revenue) revenue,
case when sum(frequency) > 0 then sum(revenue)/sum(frequency) else null end as aov,
case when sum(buyers) > 0 then sum(recency_prev)/sum(buyers) else null end as  recency_prev,
sum(frequency_prev) frequency_prev,
sum(revenue_prev) revenue_prev,
case when sum(frequency_prev) > 0 then sum(revenue_prev)/sum(frequency_prev) else null end as aov_prev,
case when sum(retention_eligible) > 0 then sum(retention_success)/sum(retention_eligible) else null end as retention_rate
FROM {{ ref('segment_stats_buyers_agg')}}
GROUP BY store, period, date, segment, view, view_segment, segment_type

UNION ALL

SELECT
store,
period,
date,
'First Order Platform' as view,
first_order_platform as view_segment,
'Frequency' as segment_type,
frequency_segment as segment,
sum(buyers) buyers,
case when sum(buyers) > 0 then sum(recency)/sum(buyers) else null end as  recency,
sum(frequency) frequency,
sum(revenue) revenue,
case when sum(frequency) > 0 then sum(revenue)/sum(frequency) else null end as aov,
case when sum(buyers) > 0 then sum(recency_prev)/sum(buyers) else null end as  recency_prev,
sum(frequency_prev) frequency_prev,
sum(revenue_prev) revenue_prev,
case when sum(frequency_prev) > 0 then sum(revenue_prev)/sum(frequency_prev) else null end as aov_prev,
case when sum(retention_eligible) > 0 then sum(retention_success)/sum(retention_eligible) else null end as retention_rate
FROM {{ ref('segment_stats_buyers_agg')}}
GROUP BY store, period, date, segment, view, view_segment, segment_type

UNION ALL

SELECT
store,
period,
date,
'Overall' as view,
'Overall' as view_segment,
'Total' as segment_type,
'Total' as segment,
sum(buyers) buyers,
case when sum(buyers) > 0 then sum(recency)/sum(buyers) else null end as  recency,
sum(frequency) frequency,
sum(revenue) revenue,
case when sum(frequency) > 0 then sum(revenue)/sum(frequency) else null end as aov,
case when sum(buyers) > 0 then sum(recency_prev)/sum(buyers) else null end as  recency_prev,
sum(frequency_prev) frequency_prev,
sum(revenue_prev) revenue_prev,
case when sum(frequency_prev) > 0 then sum(revenue_prev)/sum(frequency_prev) else null end as aov_prev,
case when sum(retention_eligible) > 0 then sum(retention_success)/sum(retention_eligible) else null end as retention_rate
FROM {{ ref('segment_stats_buyers_agg')}}
GROUP BY store, period, date, segment, view, view_segment, segment_type

UNION ALL

SELECT
store,
period,
date,
'First Order Channel' as view,
first_order_channel as view_segment,
'Total' as segment_type,
'Total' as segment,
sum(buyers) buyers,
case when sum(buyers) > 0 then sum(recency)/sum(buyers) else null end as  recency,
sum(frequency) frequency,
sum(revenue) revenue,
case when sum(frequency) > 0 then sum(revenue)/sum(frequency) else null end as aov,
case when sum(buyers) > 0 then sum(recency_prev)/sum(buyers) else null end as  recency_prev,
sum(frequency_prev) frequency_prev,
sum(revenue_prev) revenue_prev,
case when sum(frequency_prev) > 0 then sum(revenue_prev)/sum(frequency_prev) else null end as aov_prev,
case when sum(retention_eligible) > 0 then sum(retention_success)/sum(retention_eligible) else null end as retention_rate
FROM {{ ref('segment_stats_buyers_agg')}}
GROUP BY store, period, date, segment, view, view_segment, segment_type

UNION ALL

SELECT
store,
period,
date,
'First Order Platform' as view,
first_order_platform as view_segment,
'Total' as segment_type,
'Total' as segment,
sum(buyers) buyers,
case when sum(buyers) > 0 then sum(recency)/sum(buyers) else null end as  recency,
sum(frequency) frequency,
sum(revenue) revenue,
case when sum(frequency) > 0 then sum(revenue)/sum(frequency) else null end as aov,
case when sum(buyers) > 0 then sum(recency_prev)/sum(buyers) else null end as  recency_prev,
sum(frequency_prev) frequency_prev,
sum(revenue_prev) revenue_prev,
case when sum(frequency_prev) > 0 then sum(revenue_prev)/sum(frequency_prev) else null end as aov_prev,
case when sum(retention_eligible) > 0 then sum(retention_success)/sum(retention_eligible) else null end as retention_rate
FROM {{ ref('segment_stats_buyers_agg')}}
GROUP BY store, period, date, segment, view, view_segment, segment_type