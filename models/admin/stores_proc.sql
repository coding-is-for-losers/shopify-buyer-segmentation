select 
store,
store_name,
account,
platform,
max(time_of_entry) time_of_entry

from  ( 

SELECT  
store,
bigquery_name store_name,
account,
platform,
time_of_entry,
first_value(time_of_entry) OVER (PARTITION BY store ORDER BY time_of_entry DESC) lv
FROM `{{ target.project }}.agency_data_pipeline.data_feeds` 
where store_name != ''

) 

WHERE lv = time_of_entry
group by store, store_name, account, platform