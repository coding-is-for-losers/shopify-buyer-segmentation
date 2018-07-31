 -- depends_on: {{ ref('stores_proc') }}

{% set stores = get_column_values(table=ref('stores_proc'), column='store_name', max_records=50, filter_column='platform', filter_value='Shopify') %}

{% if stores != [] %}

with customers as (

	{% for store in stores %}
	SELECT
	'{{store}}' store_name,
	'Shopify' as lookup_platform,
	created_at,
	id,
	first_name,
	last_name,
	email,
	_sdc_sequence,
	first_value(_sdc_sequence) over (partition by id order by _sdc_sequence desc) lv
	FROM `{{ target.project }}.shopify_{{store}}.customers` 
	{% if not loop.last %} UNION ALL {% endif %}
	{% endfor %}

)

SELECT
b.account,
b.store,
b.platform,
created_at,
id,
first_name,
last_name,
email
FROM customers a
LEFT JOIN {{ref('stores_proc')}} b 
ON ( a.store_name = b.store_name
  AND a.lookup_platform = b.platform )
where a.lv = a._sdc_sequence

{% endif %}	 
