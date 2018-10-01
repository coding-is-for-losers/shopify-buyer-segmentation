{% set stores = get_column_values(table=ref('stores_proc'), column='store_name', max_records=50, filter_column='platform', filter_value='Shopify') %}

{% if stores != [] %}

with orders as (

	{% for store in stores %}
		SELECT
		'{{store}}' store_name,
		created_at,
		_id order_number,
		code discount_code,
		type discount_type,
		_sdc_sequence,
		first_value(_sdc_sequence) OVER (PARTITION BY order_number, _id ORDER BY _sdc_sequence DESC) lv
		FROM `{{ target.project }}.shopify_{{store}}.orders` 
		cross join unnest(discount_codes)
	
	{% if not loop.last %} UNION ALL {% endif %}
	{% endfor %}

)

SELECT
store_name,
order_number,
discount_code,
discount_type
FROM orders
where lv = _sdc_sequence

{% endif %}	
