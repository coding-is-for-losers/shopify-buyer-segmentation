SELECT
date_in_range,
date_in_range_bom,
date_in_range_bom_mom,
unix_date_in_range,
unix_date_in_range_bom,
unix_date(date_in_range_bom_mom) unix_date_in_range_bom_mom,
yyyymm,
date_in_range_yoy,
unix_date(date_in_range_yoy) unix_date_in_range_yoy

FROM
(
	SELECT 
	date_in_range,
	date_in_range_bom,
	date_sub(date_in_range_bom, INTERVAL 1 MONTH) date_in_range_bom_mom,
	date_sub(date_in_range, INTERVAL 1 YEAR) date_in_range_yoy,
	unix_date_in_range,
	unix_date(date_in_range_bom) unix_date_in_range_bom,
	yyyymm,
	first_value(date_in_range) over (partition by yyyymm order by date_in_range desc) monthend_date_in_range
	FROM
	( 
		SELECT 
		date_in_range,
		date_trunc( date_in_range, MONTH) date_in_range_bom,
		unix_date(date_in_range) unix_date_in_range,
		format_date("%Y-%m", date_in_range) AS yyyymm
		FROM UNNEST(
		    GENERATE_DATE_ARRAY(DATE('2016-01-31'), CURRENT_DATE(), INTERVAL 1 DAY)
		) AS date_in_range
	)
)
where date_in_range = monthend_date_in_range
