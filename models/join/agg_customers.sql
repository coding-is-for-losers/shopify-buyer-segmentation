SELECT 
account,
store,
id,
created_at,
first_name,
last_name,
email,
split(email,'@')[SAFE_ORDINAL(2)] email_domain
FROM
{{ ref('shopify_customers_proc') }}
