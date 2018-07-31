 -- depends_on: {{ ref('stores_proc') }}, {{ ref('shopify_refunds_proc')}}

{% set stores = get_column_values(table=ref('stores_proc'), column='store_name', max_records=50, filter_column='platform', filter_value='Shopify') %}

{% if stores != [] %}

with products as (

	{% for store in stores %}
	SELECT
	'{{store}}' store_name,
	'Shopify' as lookup_platform,
	product_name,
	lower(product_type) product_type,
	product_id,
	sku,
	id variant_id,
	cast(created_at as date) created_at,
	_sdc_sequence,
	first_value(_sdc_sequence) OVER (PARTITION BY product_id ORDER BY _sdc_sequence DESC) lv
	FROM (
		SELECT
		variants,
		product_type,
		title product_name,
		_sdc_sequence
		FROM `{{ target.project }}.shopify_{{store}}.products` 
		)
	cross join unnest(variants)
	{% if not loop.last %} UNION ALL {% endif %}
	{% endfor %}

)

SELECT
b.account,
b.store,
b.platform,
max(product_type) product_type,
product_id,
variant_id,
sku,
created_at,
product_name
FROM products a
LEFT JOIN {{ref('stores_proc')}} b 
ON ( a.store_name = b.store_name
  AND a.lookup_platform = b.platform )
where a.lv = a._sdc_sequence
group by product_id, account, store, platform, sku, variant_id, created_at, product_name

{% endif %}	